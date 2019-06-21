import sys
from os.path import join, abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))

from dotenv import dotenv_values

import util.naming
from util.logger import logger
from util.database import Database
from util.connector import JDBC as JDBCConnector, Datagen as DatagenConnector
from util.ksql_object import Table, Stream

config = dotenv_values(join(dirname(dirname(abspath(__file__))), '.env'))


if sys.argv[1] in ['db', 'all']:
    historical_database = Database(**{
        'data_name': 'historical',
        'host': config['DB_HOST'],
        'port': config['DB_PORT'],
        'user': config['DB_USER'],
        'password': config['DB_PASS'],
        'db_name': config['DB_NAME'],
        'seed_path': join(config['HISTORICAL_DATA_DIR'], config['HISTORICAL_DATA_FILE']),
    })
    historical_database.create()

if sys.argv[1] in ['hc', 'c', 'all']:
    historical_data_connector = JDBCConnector(**{
        'data_name': 'historical',
        'host': config['CONNECT_HOST'],
        'port': config['CONNECT_PORT'],
        'poll_interval': int(config['HISTORICAL_POLL_INTERVAL']),
        'db_host': config['DB_HOST'],
        'db_port': config['DB_PORT'],
        'db_user': config['DB_USER'],
        'db_password': config['DB_PASS'],
        'db_name': config['DB_NAME'],
        'keyfield': config['HISTORICAL_TABLE_KEYFIELD'],
        'query': config['HISTORICAL_QUERY'],
    })
    historical_data_connector.delete()
    historical_data_connector.create()

if sys.argv[1] in ['rc', 'c', 'all']:
    realtime_data_connector = DatagenConnector(**{
        'data_name': 'realtime',
        'host': config['CONNECT_HOST'],
        'port': config['CONNECT_PORT'],
        'poll_interval': int(config['REALTIME_POLL_INTERVAL']),
        'iterations': int(config['REALTIME_ITERATIONS']),
        'schema_path': config['REALTIME_SCHEMA_PATH'],
        'schema_keyfield': config['REALTIME_SCHEMA_KEYFIELD']
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
        'key_field_name': config['HISTORICAL_TABLE_KEYFIELD'],
        'host': config['KSQL_HOST'],
        'port': config['KSQL_PORT'],
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
        'key_field_name': config['REALTIME_STREAM_KEYFIELD'],
        'host': config['KSQL_HOST'],
        'port': config['KSQL_PORT'],
        'offset_reset': config['REALTIME_STREAM_OFFSET_RESET']
    })
    realtime_data_stream.create()
