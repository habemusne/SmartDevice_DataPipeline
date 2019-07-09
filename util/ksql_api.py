import logging
from os import getenv
from os.path import join, abspath, dirname
from dotenv import load_dotenv

load_dotenv(dotenv_path=join(dirname(dirname(abspath(__file__))), '.env'))
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter('%(levelname)s: %(name)s: %(message)s'))

logger = logging.getLogger(getenv('LOGGER_NAME'))
logger.setLevel(getenv('LOGGING_LEVEL'))
logger.addHandler(_handler)
