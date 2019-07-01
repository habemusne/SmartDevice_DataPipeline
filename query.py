import sys
import json
from os.path import join, abspath, dirname
from dotenv import dotenv_values
from fire import Fire

import util.naming
from util.logger import logger
from util.ksql_api import Api

config = dotenv_values(join(dirname(abspath(__file__)), '.env'))


stmts_setup = [{
    'desc': 'Creating interim table: user id + num distinct locations',
    'stmt': """
        create table INTERIM_11 with (partitions = {}) as
            select user_id, count(cast(latitude as string) + ' | ' + cast(longitude as string)) as num_locations
            from "{}"
            group by user_id
        ;
    """.format(config['NUM_PARTITIONS'], util.naming.stream_name('realtime')),
}, {
    'desc': 'Creating interim table: user id + avg heart rate',
    'stmt': """
        create table INTERIM_12 with (partitions = {}) as
            select user_id, sum(heart_rate)/count(heart_rate) as avg_heart_rate
            from "{}"
            group by user_id
        ;
    """.format(config['NUM_PARTITIONS'], util.naming.stream_name('realtime')),
}, {
    'desc': 'Creating interim table: user id + num distinct locations + avg heart rate',
    'stmt': """
        create table INTERIM_21 with (partitions = {}) as
            select t1.user_id as user_id, t1.num_locations as num_locations, t2.avg_heart_rate as avg_heart_rate
            from "INTERIM_11" t1
            join "INTERIM_12" t2 on t1.user_id = t2.user_id
        ;
    """.format(config['NUM_PARTITIONS']),
}]

stmts_run = [{
    'desc': 'get the users who are still and have heart rate out of their historical range',
    'stmt': """
        select t1.user_id, t1.avg_heart_rate, t1.num_locations
        from "INTERIM_21" t1
        join "{}" h on t1.user_id = h.user_id
        window tumbling (size {} seconds)
        where t1.avg_heart_rate < 60 or t1.avg_heart_rate > 100
        and t1.num_locations = 1
        ;
    """.format(util.naming.table_name('historical'), config['WINDOW_TUMBLING_SECONDS']),
}]

stmts_teardown = [{
    'stmt': 'drop table "INTERIM_21";',
}, {
    'stmt': 'drop table "INTERIM_12";',
}, {
    'stmt': 'drop table "INTERIM_11";',
}]

api = Api(config['KSQL_LEADER'], config['KSQL_PORT'])


def setup():
    for statement in stmts_setup:
        logger.info(statement['desc'] if 'desc' in statement else statement['stmt'])
        api.ksql({
            'ksql': statement['stmt'],
            'auto.offset.reset': 'earliest',
        })


def run():
    logger.info('Health risk detection running. Result will be updated every {} seconds'.format(config['WINDOW_TUMBLING_SECONDS']))
    for response in api.stream({
        'ksql': stmts_run[0]['stmt'],
        'auto.offset.reset': 'latest',
    }):
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

