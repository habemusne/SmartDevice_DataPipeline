from os.path import join, abspath, dirname
from dotenv import dotenv_values

config = dotenv_values(join(dirname(dirname(abspath(__file__))), '.env'))



