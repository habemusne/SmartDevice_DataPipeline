import sys
import json
from os import getenv
from os.path import join
from dotenv import load_dotenv
from fire import Fire

import util.naming
from util.logger import logger
from util.ksql_api import Api

load_dotenv(dotenv_path='./.env')
version_id = getenv('RESOURCE_NAME_VERSION_ID')
interim_11 = 'INTERIM_11_' + version_id
interim_12 = 'INTERIM_12_' + version_id
interim_21 = 'INTERIM_21_' + version_id
final = 'FINAL_' + version_id


stmts_setup = [{
    'desc': 'Creating interim table: user id + num distinct locations',
    'stmt': """
        create table {} with (partitions = {}) as
            select user_id, count(cast(latitude as string) + ' | ' + cast(longitude as string)) as num_locations
            from "{}"
            group by user_id
        ;
    """.format(interim_11, getenv('NUM_PARTITIONS'), util.naming.stream_name('realtime')),
}, {
    'desc': 'Creating interim table: user id + avg heart rate',
    'stmt': """
        create table {} with (partitions = {}) as
            select user_id, sum(heart_rate)/count(heart_rate) as avg_heart_rate
            from "{}"
            group by user_id
        ;
    """.format(interim_12, getenv('NUM_PARTITIONS'), util.naming.stream_name('realtime')),
}, {
    'desc': 'Creating interim table: user id + num distinct locations + avg heart rate',
    'stmt': """
        create table {} with (partitions = {}) as
            select t1.user_id as user_id, t1.num_locations as num_locations, t2.avg_heart_rate as avg_heart_rate
            from "{}" t1
            join "{}" t2 on t1.user_id = t2.user_id
        ;
    """.format(interim_21, getenv('NUM_PARTITIONS'), interim_11, interim_12),
}, {
    'desc': 'Final stream: get the users who are still and have heart rate out of their historical range',
    'stmt': """
        create table {} with (partitions = {}) as
            select t1.user_id as user_id, t1.avg_heart_rate as avg_heart_rate, t1.num_locations as num_locations
            from "{}" t1
            join "{}" h on t1.user_id = h.user_id
            window tumbling (size {} seconds)
            where t1.avg_heart_rate < 60 or t1.avg_heart_rate > 100
            and t1.num_locations = 1
        ;
    """.format(final, getenv('NUM_PARTITIONS'), interim_21, util.naming.table_name('historical'), getenv('WINDOW_TUMBLING_SECONDS')),
}]

stmts_run = [{
    'desc': 'get the users who are still and have heart rate out of their historical range',
    'stmt': """
        select user_id, avg_heart_rate, num_locations
        from "{}"
        window tumbling (size {} seconds)
        ;
    """.format(final, getenv('WINDOW_TUMBLING_SECONDS')),
}]

stmts_teardown = [{
    'stmt': 'drop table "{}";'.format(interim_21),
}, {
    'stmt': 'drop table "{}";'.format(interim_12),
}, {
    'stmt': 'drop table "{}";'.format(interim_11),
}, {
    'stmt': 'drop table "{}";'.format(final),
}]

api = Api(getenv('KSQL_HOST'), getenv('KSQL_PORT'))


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
