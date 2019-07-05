import re
import sys
import json
from os import getenv
from os.path import join
from dotenv import load_dotenv
from fire import Fire
from time import sleep

import util.naming
from util.logger import logger
from util.ksql_api import Api

load_dotenv(dotenv_path='./.env')
version_id = getenv('RESOURCE_NAME_VERSION_ID')
interim_1 = 'INTERIM_1_' + version_id
interim_2 = 'INTERIM_2_' + version_id
final = 'FINAL_' + version_id

stmts_setup = [{
    'desc': 'Creating table {} from historical data topic'.format(interim_1),
    'stmt': """
        CREATE TABLE "{name}" (user_id STRING, min_heart_rate INTEGER, max_heart_rate INTEGER)
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
            heart_rate INTEGER,
            generated_at STRING
        ) WITH (
            KAFKA_TOPIC = '{topic}',
            VALUE_FORMAT = 'AVRO',
            KEY = '{key}'
        );
    """.format(
        name=interim_2,
        generated_at=getenv('GENERATED_AT_FIELD'),
        topic=util.naming.topic_name('realtime'),
        key=getenv('REALTIME_KEYFIELD'),
    ),
}, {
    'desc': 'Creating table {}: user id + num distinct locations'.format(final),
    'stmt': """
        create table {name} with (partitions = {num_partitions}) as
            select
                r.user_id as user_id,
                r.ROWTIME as {processed_at},
                sum(r.heart_rate)/count(r.heart_rate) as avg_heart_rate
            from "{interim_2}" r
            join "{interim_1}" h on h.user_id = r.user_id
            window tumbling (size {tumbling} seconds)
            group by r.user_id, r.ROWTIME
            having count(cast(r.latitude as string) + ' | ' + cast(r.longitude as string)) = 1
            and not(sum(r.heart_rate)/count(r.heart_rate) between sum(h.min_heart_rate) and sum(h.max_heart_rate))
        ;
    """.format(
        name=final,
        num_partitions=getenv('NUM_PARTITIONS'),
        tumbling=getenv('WINDOW_TUMBLING_SECONDS'),
        interim_1=interim_1,
        interim_2=interim_2,
        processed_at=getenv('PROCESSING_AT_FIELD'),
    ),
}]

stmts_run = [{
    'desc': 'get the users who are still and have heart rate out of their historical range',
    'stmt': """
        select user_id, avg_heart_rate, processed_at
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
        api.query({
            'ksql': statement['stmt'],
            'auto.offset.reset': 'earliest',
        })


def stat():
    def parse(pattern, text, default=0):
        result = re.search(pattern, text)
        return default if not resut else result.groups()[0]

    while True:
        try:
            response = api.query({
                'ksql': 'describe extended {};'.format(interim_2),
            })
            message_rate = parse(r'consumer-messages-per-sec:\W+(\d+)', response.text)
            total_bytes = parse(r'consumer-total-bytes:\W+(\d+)', response.text)
            total_messages = parse(r'consumer-total-messages:\W+(\d+)', response.text)
            sleep(5)
        except KeyboardInterrupt:
            break

    # logger.info('Health risk detection running. Result will be updated every {} seconds'.format(getenv('WINDOW_TUMBLING_SECONDS')))
    # for response in api.stream({
    #     'ksql': stmts_run[0]['stmt'],
    #     'auto.offset.reset': 'earliest',
    # }):
    #     print(response.text)
    #     for line in response.iter_lines():
    #         if line:
    #             data = json.loads(line.decode('utf-8'))
    #             logger.info('Health risk detected: user {}, heart rate {}'.format(data['row']['columns'][0], data['row']['columns'][1]))


def teardown():
    response = api.query({ 'ksql': 'show queries;' })
    for query in response.json()[0]['queries']:
        api.query({ 'ksql': 'terminate {};'.format(query['id']) })
    for statement in stmts_teardown:
        logger.info(statement['stmt'])
        api.query({ 'ksql': statement['stmt'] }, force_exit=False, show_output=False)


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
