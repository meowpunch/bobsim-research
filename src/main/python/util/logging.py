import yaml
import logging
import logging.config

from util.executable import get_destination


def init_logger():
    config_path = 'config/logging.yaml'
    with open(get_destination(config_path)) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    logging.config.dictConfig(config)
    return logging.getLogger('__main__')

