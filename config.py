#! /usr/bin/env python3

import yaml


def get_config():
    """Setup configuration and credentials
    """
    path = 'config.yaml'

    with open(path, 'rt') as f:
        config = yaml.safe_load(f.read())

    return config
