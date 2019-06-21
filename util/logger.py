import logging
from os.path import join, abspath, dirname
from dotenv import dotenv_values

_config = dotenv_values(join(dirname(dirname(abspath(__file__))), '.env'))
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter('%(levelname)s: %(name)s: %(message)s'))

logger = logging.getLogger(_config['LOGGER_NAME'])
logger.setLevel(_config['LOGGING_LEVEL'])
logger.addHandler(_handler)
