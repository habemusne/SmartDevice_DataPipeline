import sys
import json
from os import getenv
from os.path import join, abspath, dirname
from dotenv import load_dotenv
from fire import Fire

import util.naming
from util.logger import logger
from util.ksql_api import Api

load_dotenv(dotenv_path=join(dirname(abspath(__file__)), '.env'))
version_id = getenv('RESOURCE_NAME_VERSION_ID')
interim_1 = 'INTERIM_1_' + version_id
interim_2 = 'INTERIM_2_' + version_id
final = 'FINAL_' + version_id

stmts_setup = [{
    'desc': 'Creating table {} from historical data topic'.format(interim_1),
    'stmt': """
        CREATE TABLE "{name}" (user_id STRING, latitude DOUBLE, longitude DOUBLE)
        WITH (
            KAFKA_TOPIC = '{topic}',
            VALUE_FORMAT = 'AVRO',
            KEY = '{key}'
        );
    """.format(
        name=interim_1,
        topic=util.naming.topic_name('historical'),
        key=getenv('HISTORICAL_KEYFIELD'),
    ),
}, {
    'desc': 'Creating stream {} from real-time data topic'.format(interim_2),
    'stmt': """
        CREATE STREAM "{name}" (
            user_id STRING,
            zipcode INTEGER,
            latitude DOUBLE,
            longitude DOUBLE,
            city STRING,
            year INTEGER,
            month INTEGER,
            heart_rate INTEGER,
            {generated_at} BIGINT,
            {processing_started_at} BIGINT
        ) WITH (
            KAFKA_TOPIC = '{topic}',
            VALUE_FORMAT = 'AVRO',
            KEY = '{key}',
            TIMESTAMP = '{processing_started_at}'
        );
    """.format(
        name=interim_2,
        generated_at=getenv('GENERATED_AT_FIELD'),
        processing_started_at=getenv('PROCESSING_STARTED_AT_FIELD'),
        topic=util.naming.topic_name('realtime'),
        key=getenv('REALTIME_KEYFIELD'),
    ),
}, {
    'desc': 'Creating table {}: user id + num distinct locations'.format(final),
    'stmt': """
        create table {name} with (partitions = {num_partitions}) as
            select
                r.user_id as user_id,
                sum(r.heart_rate)/count(r.heart_rate) as avg_heart_rate
            from "{interim_2}" r
            window tumbling (size {tumbling} seconds)
            group by r.user_id
        ;
    """.format(
        name=final,
        num_partitions=getenv('NUM_PARTITIONS'),
        generated_at=getenv('GENERATED_AT_FIELD'),
        processing_started_at=getenv('PROCESSING_STARTED_AT_FIELD'),
        tumbling=getenv('WINDOW_TUMBLING_SECONDS'),
        interim_1=interim_1,
        interim_2=interim_2,
    ),
}]

stmts_run = [{
    'desc': 'get the users who are still and have heart rate out of their historical range',
    'stmt': """
        select user_id, avg_heart_rate, earliest_generated_at, latest_generated_at, earliest_processing_started_at, latest_processing_started_at
        from "{}"
        window tumbling (size {} seconds)
        ;
    """.format(final, getenv('WINDOW_TUMBLING_SECONDS')),
}]

stmts_teardown = [{
    'stmt': 'drop table "{}";'.format(final),
}, {
    'stmt': 'drop stream "{}";'.format(interim_2),
}, {
    'stmt': 'drop table "{}";'.format(interim_1),
}]

api = Api(getenv('KSQL_LEADER'), getenv('KSQL_PORT'))


def setup():
    for statement in stmts_setup:
        logger.info(statement['desc'] if 'desc' in statement else statement['stmt'])
        api.ksql({
            'ksql': statement['stmt'],
            'auto.offset.reset': 'earliest',
        })


def run():
    logger.info('Health risk detection running. Result will be updated every {} seconds'.format(getenv('WINDOW_TUMBLING_SECONDS')))
    for response in api.stream({
        'ksql': stmts_run[0]['stmt'],
        'auto.offset.reset': 'earliest',
    }):
        print(response.text)
        for line in response.iter_lines():
            if line:
                data = json.loads(line.decode('utf-8'))
                logger.info('Health risk detected: user {}, heart rate {}'.format(data['row']['columns'][0], data['row']['columns'][1]))


def teardown():
    response = api.ksql({ 'ksql': 'show queries;' })
    for query in response.json()[0]['queries']:
        api.ksql({ 'ksql': 'terminate {};'.format(query['id']) })
    for statement in stmts_teardown:
        logger.info(statement['stmt'])
        api.ksql({ 'ksql': statement['stmt'] }, force_exit=False, show_output=False)


if len(sys.argv) > 1:
    Fire()
else:
    try:
        setup()
        run()
    except KeyboardInterrupt:
        pass
    finally:
        teardown()

