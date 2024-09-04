import sys
from typing import Dict
import os
import json
import re


def _format_unknown_args(args: list):
    """
    Returns parameters from args

    Returns:
        The arguments variables as dict
    """

    extraarguments = dict(zip(args[::2], args[1::2]))
    return extraarguments


def get_params_from_args() -> Dict:
    """
    Returns argument variables

    Returns:
        The argument variables found, empty if the configuration is empty
    """

    args = sys.argv[1:]
    params = _format_unknown_args(args)
    print(f"Params: {params}")
    PREFIX_REGEX = "^PARAM_"
    arg_params = {}
    for item, value in params.items():
        if re.match(PREFIX_REGEX, item):
            arg_var_original = re.sub(PREFIX_REGEX, "", item)
            arg_params[arg_var_original] = value

    return arg_params


def get_params_from_env() -> Dict:
    """
    Returns argument variables

    Returns:
        The argument variables found, empty if the configuration is empty
    """

    params = dict(os.environ)

    PREFIX_REGEX = "^PARAM_"
    arg_params = {}
    for item, value in params.items():
        if re.match(PREFIX_REGEX, item):
            arg_var_original = re.sub(PREFIX_REGEX, "", item)
            arg_params[arg_var_original] = value

    return arg_params


def get_params_from_hps() -> Dict:
    """
    Returns argument variables from hiperparameters

    Returns:
        The argument variables found, empty if the configuration is empty
    """

    args = dict(os.environ)
    arg_params = {}
    if "SM_HPS" in args.keys():
        arg_params = json.loads(args["SM_HPS"])
        if "PACKAGE_NAME" in arg_params.keys():
            arg_params.pop("PACKAGE_NAME")

    return arg_params
