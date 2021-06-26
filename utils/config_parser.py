"""parse config from config.ini file
"""

import os
from configparser import ConfigParser


def configuration_parser(env: str = "CONFIG") -> ConfigParser:
    """
    Parse the config file
    :param env (str): name of environment variable containing path of config file

    Returns the parsed ConfigParser object
    """
    filename = os.environ.get(env, None)
    config = ConfigParser()
    

    if filename is None:
        raise ValueError(f"Enviroment variable {env} is missing.")

    config.read(filename)
    return config

