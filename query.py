import re
import sys
import json
from os import getenv
from os.path import join, abspath, dirname
from dotenv import load_dotenv
from fire import Fire
from time import sleep

import util.naming
from util.logger import logger
from util.ksql_api import Api
from util.topic import Topic
from util.schema import Schema

load_dotenv(dotenv_path='./.env')
interim_1 = util.naming.interim_1_name()
interim_2 = util.naming.interim_2_name()
interim_3 = util.naming.interim_3_name()
final = util.naming.final_table_name()

stmts_setup = [{
    'desc': 'Creating stream {} from real-time data topic'.format(interim_1),
    'stmt': """
        CREATE STREAM "{name}" WITH (
            KAFKA_TOPIC = '{topic}',
            VALUE_FORMAT = '{format}'
        );
    """.format(
        name=interim_1,
        generated_at=getenv('GENERATED_AT_FIELD'),
        topic=util.naming.topic_name('realtime'),
        key=getenv('REALTIME_KEYFIELD'),
        format='AVRO',
    ),
    'auto.offset.reset': 'latest',
}, {
    'desc': 'Creating stream {} from real-time data topic'.format(interim_2),
    'stmt': """
        CREATE STREAM "{name}" WITH (PARTITIONS = {num_partitions}) AS
            SELECT * FROM {interim_1}
            PARTITION BY {key}
        ;
    """.format(
        name=interim_2,
        interim_1=interim_1,
        key=getenv('REALTIME_KEYFIELD'),
        num_partitions=getenv('NUM_PARTITIONS'),
    ),
    'auto.offset.reset': 'latest',
}, {
    'desc': 'Creating table {}: user id'.format(interim_3),
    'stmt': """
        CREATE TABLE {name} WITH (PARTITIONS = {num_partitions}, VALUE_FORMAT = '{format}') AS
            SELECT
                r.user_id AS user_id,
                sum(r.heart_rate)/count(r.heart_rate) AS avg_heart_rate
            FROM "{interim_2}" r
            INNER JOIN "{historical_table}" h ON h.user_id = r.user_id
            WINDOW TUMBLING (SIZE {tumbling} SECONDS)
            GROUP BY r.user_id
            HAVING count(cast(round(r.latitude * 100)/100 AS string) + ' | ' + cast(round(r.longitude * 100)/100 AS string)) = 1
            AND not(sum(r.heart_rate)/count(r.heart_rate) BETWEEN sum(h.min_heart_rate) AND sum(h.max_heart_rate))
        ;
    """.format(
        name=interim_3,
        num_partitions=getenv('NUM_PARTITIONS'),
        tumbling=getenv('WINDOW_TUMBLING_SECONDS'),
        historical_table=util.naming.table_name('historical'),
        interim_2=interim_2,
        processed_at=getenv('PROCESSING_AT_FIELD'),
        format='AVRO',
    ),
    'auto.offset.reset': 'latest',
}]

stmts_teardown = [{
    'stmt': 'drop table "{}";'.format(interim_3),
}, {
    'stmt': 'drop stream "{}";'.format(interim_2),
}, {
    'stmt': 'drop stream "{}";'.format(interim_1),
}]

api = Api(host=getenv('KSQL_LEADER'))


def setup():
    for i, entry in enumerate(stmts_setup):
        logger.info(entry['desc'] if 'desc' in entry else entry['stmt'])
        if i == 3:
            topic_name = util.naming.final_topic_name()
            Topic(getenv('CONTROL_CENTER_HOST'), topic_name, int(getenv('NUM_PARTITIONS'))).create(force_exit=False)
            Schema(getenv('SCHEMA_REGISTRY_HOST'), join('./schemas', getenv('FILE_SCHEMA_FINAL')), topic_name).create()

        api.query({
            'ksql': entry['stmt'],
            'auto.offset.reset': 'latest',
            'ksql.streams.retention.ms': int(getenv('KSQL_STREAMS_RETENTION_MS')),
        })


def stat():
    def parse(pattern, text, default=0):
        result = re.search(pattern, text)
        return default if not resut else result.groups()[0]

    while True:
        try:
            response = api.query({
                'ksql': 'describe extended {};'.format(interim_1),
            })
            message_rate = parse(r'consumer-messages-per-sec:\W+(\d+)', response.text)
            total_bytes = parse(r'consumer-total-bytes:\W+(\d+)', response.text)
            total_messages = parse(r'consumer-total-messages:\W+(\d+)', response.text)
            sleep(5)
        except KeyboardInterrupt:
            break


def teardown():
    response = api.query({ 'ksql': 'show queries;' })
    for query in response.json()[0]['queries']:
        api.query({ 'ksql': 'terminate {};'.format(query['id']) })
    for entry in stmts_teardown:
        logger.info(entry['stmt'])
        api.query({ 'ksql': entry['stmt'] }, force_exit=False, show_output=False)


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
