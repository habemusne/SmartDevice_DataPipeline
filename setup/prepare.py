import sys
from os.path import join, abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))

from os import getenv
from dotenv import load_dotenv

import util.naming
from util.logger import logger
from util.database import Database
from util.connector import JDBCSource as JDBCSourceConnector, Datagen as DatagenConnector
from util.ksql_object import Table, Stream

load_dotenv(dotenv_path=join(dirname(dirname(abspath(__file__))), '.env'))


if sys.argv[1] in ['db', 'all']:
    anomaly_database = Database(**{
        'data_name': 'anomaly',
        'seed_path': join(getenv('DATA_DIR'), getenv('ANOMALY_DATA_FILE')),
    })
    anomaly_database.create()

    historical_database = Database(**{
        'data_name': 'historical',
        'seed_path': join(getenv('DATA_DIR'), getenv('HISTORICAL_DATA_FILE')),
    })
    historical_database.create()

if sys.argv[1] in ['hc', 'c', 'all']:
    historical_data_connector = JDBCSourceConnector(**{
        'data_name': 'historical',
        'poll_interval': int(getenv('HISTORICAL_POLL_INTERVAL')),
        'keyfield': getenv('HISTORICAL_TABLE_KEYFIELD'),
        'query': getenv('HISTORICAL_QUERY'),
        'num_partitions': getenv('NUM_PARTITIONS')
    })
    historical_data_connector.delete()
    historical_data_connector.create()

if sys.argv[1] in ['rc', 'c', 'all']:
    realtime_data_connector = DatagenConnector(**{
        'data_name': 'realtime',
        'poll_interval': int(getenv('REALTIME_POLL_INTERVAL')),
        'iterations': int(getenv('REALTIME_ITERATIONS')),
        'schema_path': getenv('REALTIME_SCHEMA_PATH'),
        'schema_keyfield': getenv('REALTIME_SCHEMA_KEYFIELD'),
        'num_partitions': getenv('NUM_PARTITIONS')
    })
    realtime_data_connector.delete()
    realtime_data_connector.create()

if sys.argv[1] in ['ht', 'st', 'all']:
    historical_data_table = Table(**{
        'data_name': 'historical',
        'topic_name': util.naming.topic_name('historical'),
        'field_map': {
            'user_id': 'STRING',
            'latitude': 'DOUBLE',
            'longitude': 'DOUBLE',
        },
        'key_field_name': getenv('HISTORICAL_TABLE_KEYFIELD'),
        'host': getenv('KSQL_HOST'),
        'port': getenv('KSQL_PORT'),
    })
    historical_data_table.create()

if sys.argv[1] in ['rs', 'st', 'all']:
    realtime_data_stream = Stream(**{
        'data_name': 'realtime',
        'field_map': {
            'user_id': 'STRING',
            'zipcode': 'INTEGER',
            'latitude': 'DOUBLE',
            'longitude': 'DOUBLE',
            'city': 'STRING',
            'year': 'INTEGER',
            'month': 'INTEGER',
            'heart_rate': 'INTEGER',
        },
        'key_field_name': getenv('REALTIME_STREAM_KEYFIELD'),
        'host': getenv('KSQL_HOST'),
        'port': getenv('KSQL_PORT'),
        'offset_reset': getenv('REALTIME_STREAM_OFFSET_RESET'),
    })
    realtime_data_stream.create()
