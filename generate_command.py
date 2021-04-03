import argparse
from copy import deepcopy
from io import StringIO
from itertools import chain, cycle, groupby, product
from operator import itemgetter
from pathlib import Path
from typing import Any, Dict, List

import yaml

key_separator = "/"


def convert_multi_layer_to_single(multi_dict: Dict[str, Any], root_key: str = ""):
    single_dict = {}
    for key, value in multi_dict.items():
        if isinstance(value, dict) and len(value) > 0:
            d = convert_multi_layer_to_single(value, root_key + key + key_separator)
            single_dict.update(d)
        else:
            single_dict[root_key + key] = value
    return single_dict


def convert_single_layer_to_multi(single_dict: Dict[str, Any]):
    multi_dict = {}
    for key, pair in groupby(
        sorted(single_dict.items()), key=lambda item: item[0].split(key_separator)[0]
    ):
        pair = list(pair)
        if any(key_separator in k for k, v in pair):
            d = {key_separator.join(k.split(key_separator)[1:]): v for k, v in pair}
            multi_dict[key] = convert_single_layer_to_multi(d)
        else:
            multi_dict.update(dict(pair))
    return multi_dict


def convert_all_to_product(all_dict: Dict[str, Any]):
    return {key: [value] for key, value in all_dict.items()}


def convert_product_to_each(product_dict: Dict[str, List[Any]]):
    p = product(*[zip(cycle([key]), value) for key, value in product_dict.items()])
    l = list(map(dict, p))
    return {key: list(map(itemgetter(key), l)) for key in product_dict.keys()}


def merge_each(each_dict1: Dict[str, List[Any]], each_dict2: Dict[str, List[Any]]):
    if len(each_dict1) == 0:
        return each_dict2
    if len(each_dict2) == 0:
        return each_dict1

    l1 = [
        {k: v[i] for k, v in each_dict1.items()}
        for i in range(next(get_lengths(each_dict1)))
    ]
    l2 = [
        {k: v[i] for k, v in each_dict2.items()}
        for i in range(next(get_lengths(each_dict2)))
    ]
    p = product(l1, l2)
    l = [dict(**d1, **d2) for d1, d2 in p]
    return {key: list(map(itemgetter(key), l)) for key in l[0].keys()}


def convert_recipe_to_each(recipe: Dict[str, Dict[str, Any]]):
    d_all = convert_multi_layer_to_single(recipe["all"])
    d_product = convert_multi_layer_to_single(recipe["product"])
    d_each = convert_multi_layer_to_single(recipe["each"])

    d_product = dict(**convert_all_to_product(d_all), **d_product)
    d_each2 = convert_product_to_each(d_product)
    return merge_each(d_each2, d_each)


def is_recipe(d: Dict[str, Any]):
    return "all" in d and "product" in d and "each" in d


def validate(base: Dict[str, Any], each: Dict[str, Any]):
    assert set(each) <= set(base), f"each: {set(each)} / base: {set(base)}"

    for key, value in each.items():
        assert key in base

        if isinstance(value, dict):
            assert isinstance(base[key], dict)
            validate(base[key], value)
        else:
            assert isinstance(value, list)


def get_lengths(each: Dict[str, Any]):
    for key, value in each.items():
        if isinstance(value, dict):
            yield from get_lengths(value)
        else:
            yield len(value)


def replace(base: Dict[str, Any], each: Dict[str, Any], index: int):
    for key, value in each.items():
        if isinstance(value, dict):
            replace(base[key], value, index)
        else:
            base[key] = value[index]


def replace_format(multi_dict: Dict[str, Any], single_dict: Dict[str, Any] = None):
    if single_dict is None:
        single_dict = convert_multi_layer_to_single(multi_dict)

    for key in list(multi_dict.keys()):
        if isinstance(multi_dict[key], str) and "{" in multi_dict[key]:
            multi_dict[key] = multi_dict[key].format(**single_dict)
        if isinstance(multi_dict[key], dict):
            replace_format(multi_dict=multi_dict[key], single_dict=single_dict)


def generate(base: Dict[str, Any], recipe: Dict[str, Any]):
    if is_recipe(recipe):
        each = convert_single_layer_to_multi(convert_recipe_to_each(recipe))
    else:
        each = recipe

    validate(base, each)

    length_list = list(get_lengths(each))
    assert all(l == length_list[0] for l in length_list)

    for i in range(length_list[0]):
        config = deepcopy(base)
        replace(config, each=each, index=i)
        replace_format(config)
        yield config


def generate_command(
    base_config_path: Path,
    recipe_path: Path,
    base_command_path: Path,
    output_path_format: Path,
):
    with base_config_path.open() as f:
        base = yaml.safe_load(f)

    with recipe_path.open() as f:
        recipe_list = list(yaml.safe_load_all(f))

    configs = list(
        chain.from_iterable(generate(base, recipe) for recipe in recipe_list)
    )

    base_command = base_command_path.read_text()

    for i, config in enumerate(configs):
        name = config["project"]["name"]
        with StringIO() as f:
            yaml.safe_dump(config, f)
            config_str = f.getvalue()

        command = base_command.replace(r"{{{name}}}", name).replace(
            r"{{{config}}}", config_str
        )

        output_path = Path(str(output_path_format).format(i=i))
        output_path.write_text(command)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base_config_path", type=Path)
    parser.add_argument("--recipe_path", type=Path)
    parser.add_argument("--base_command_path", type=Path)
    parser.add_argument("--output_path_format", type=Path)
    generate_command(**(vars(parser.parse_args())))
