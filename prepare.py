import sys
from os import getenv
from dotenv import load_dotenv
from os.path import join

import util.naming
from util.logger import logger
from util.database import Database
from util.connector import JDBCSource as JDBCSourceConnector, Datagen as DatagenConnector

load_dotenv(dotenv_path='./.env')


if sys.argv[1] in ['db', 'all']:
    anomaly_database = Database(**{
        'data_name': 'anomaly',
        'seed_path': join(getenv('DIR_DATA'), getenv('FILE_DATA_ANOMALY')),
    })
    anomaly_database.delete()
    anomaly_database.create()

    historical_database = Database(**{
        'data_name': 'historical',
        'seed_path': join(getenv('DIR_DATA'), getenv('FILE_DATA_HISTORICAL')),
    })
    historical_database.delete()
    historical_database.create()

if sys.argv[1] in ['hc', 'c', 'all']:
    historical_data_connector = JDBCSourceConnector(**{
        'data_name': 'historical',
        'poll_interval': int(getenv('HISTORICAL_POLL_INTERVAL')),
        'keyfield': getenv('HISTORICAL_KEYFIELD'),
        'query': getenv('HISTORICAL_QUERY'),
        'num_partitions': getenv('NUM_PARTITIONS'),
    })
    historical_data_connector.delete()
    historical_data_connector.create()

if sys.argv[1] in ['rc', 'c', 'all'] and getenv('MODE') == 'dev':
    realtime_data_connector = DatagenConnector(**{
        'data_name': 'realtime',
        'poll_interval': int(getenv('REALTIME_POLL_INTERVAL')),
        'iterations': int(getenv('REALTIME_ITERATIONS')),
        'schema_path': join(getenv('DIR_SCHEMAS'), getenv('FILE_SCHEMA_REALTIME')),
        'schema_keyfield': getenv('REALTIME_KEYFIELD'),
        'num_partitions': getenv('NUM_PARTITIONS'),
    })
    realtime_data_connector.delete()
    realtime_data_connector.create()
