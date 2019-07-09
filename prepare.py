import sys
from os import getenv
from dotenv import load_dotenv
from os.path import join

import util.naming
from util.logger import logger
from util.database import Database
from util.topic import Topic
from util.schema import Schema
from util.connector import JDBCSource as JDBCSourceConnector, Datagen as DatagenConnector
from util.ksql_api import Api as KSQLApi

load_dotenv(dotenv_path='./.env')


if sys.argv[1] in ['db', 'all']:
    historical_database = Database(**{
        'data_name': 'historical',
        'seed_path': join(getenv('DIR_DATA'), getenv('FILE_DATA_HISTORICAL')),
    })
    historical_database.delete()
    historical_database.create()

if sys.argv[1] in ['hc', 'c', 'all']:
    historical_data_connector = JDBCSourceConnector(**{
        'host': getenv('CONNECT_HOST'),
        'data_name': 'historical',
        'poll_interval': int(getenv('HISTORICAL_POLL_INTERVAL')),
        'keyfield': getenv('HISTORICAL_KEYFIELD'),
        'query': getenv('HISTORICAL_QUERY'),
        'control_center_host': getenv('CONTROL_CENTER_HOST'),
    })
    historical_data_connector.delete()
    historical_data_connector.create()

if sys.argv[1] in ['rc', 'c', 'all']:
    if getenv('MODE') == 'dev':
        realtime_data_connector = DatagenConnector(**{
            'host': getenv('CONNECT_HOST'),
            'data_name': 'realtime',
            'poll_interval': int(getenv('REALTIME_POLL_INTERVAL')),
            'iterations': int(getenv('REALTIME_ITERATIONS')),
            'schema_path': join(getenv('DIR_SCHEMAS'), getenv('FILE_SCHEMA_REALTIME')),
            'schema_keyfield': getenv('REALTIME_KEYFIELD'),
        })
        realtime_data_connector.delete()
        realtime_data_connector.create()
    else:
        topic_name = util.naming.topic_name('realtime')
        schema_path = join('schemas', getenv('FILE_SCHEMA_REALTIME'))
        topic = Topic(getenv('CONTROL_CENTER_HOST'), topic_name, getenv('NUM_PARTITIONS'))
        schema = Schema(getenv('SCHEMA_REGISTRY_HOST'), schema_path, topic_name)
        topic.create(force_exit=False)
        schema.delete()
        schema.create()

if sys.argv[1] in ['t', 'all']:
    logger.info('creating historical table...')
    api = KSQLApi(host=getenv('KSQL_LEADER'))

    response = api.query({ 'ksql': 'show queries;' })
    for query in response.json()[0]['queries']:
        api.query({ 'ksql': 'terminate {};'.format(query['id']) })

    table_name = util.naming.table_name('historical')
    api.query({ 'ksql': 'drop table {};'.format(table_name) }, force_exit=False, show_output=False)
    api.query({
        'ksql': """
            CREATE TABLE "{name}" (
                user_id STRING,
                min_heart_rate INTEGER,
                max_heart_rate INTEGER
            ) WITH (
                KAFKA_TOPIC = '{topic}',
                VALUE_FORMAT = 'AVRO',
                KEY = '{key}'
            );
        """.format(
            name=table_name,
            topic=util.naming.topic_name('historical'),
            key=getenv('HISTORICAL_KEYFIELD'),
        ),
        'auto.offset.reset': 'earliest'
    }, force_exit=False)
    logger.info('historical table created.')
