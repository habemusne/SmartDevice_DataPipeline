from os.path import join, abspath, dirname
from dotenv import dotenv_values
from fire import Fire

import util.naming
from util.logger import logger
from util.ksql_api import Api

config = dotenv_values(join(dirname(abspath(__file__)), '.env'))


stmts_setup = [{
    'desc': 'user id + num distinct locations',
    'stmt': """
        create table INTERIM_11 WITH (PARTITIONS = {}) as
            select user_id, count(cast(latitude as string) + ' | ' + cast(longitude as string)) as num_locations
            from "{}"
            group by user_id
            partition by user_id
        ;
    """.format(1, util.naming.stream_name('realtime')),
}, {
    'desc': 'user id + avg heart rate',
    'stmt': """
        create table INTERIM_12 WITH (PARTITIONS = {}) as
            select user_id, sum(heart_rate)/count(heart_rate) as avg_heart_rate
            from "{}"
            group by user_id
            partition by user_id
        ;
    """.format(config['NUM_PARTITIONS'], util.naming.stream_name('realtime')),
}, {
    'desc': 'user id + num distinct locations + avg heart rate',
    'stmt': """
        create table INTERIM_21 WITH (PARTITIONS = {}) as
            select t1.user_id as user_id, t1.num_locations as num_locations, t2.avg_heart_rate as avg_heart_rate
            from "INTERIM_11" t1
            join "INTERIM_12" t2 on t1.user_id = t2.user_id
            partition by user_id
        ;
    """.format(config['NUM_PARTITIONS']),
}, {
    'desc': 'historical data. jdbc connector currently does not support multiple partitions, so we repartition here',
    'stmt': """
        create table INTERIM_22 WITH (PARTITIONS = {}) as
            select user_id, latitude, longitude from "{}"
            partition by user_id
        ;
    """.format(config['NUM_PARTITIONS'], util.naming.table_name('historical')),
}]

stmts_run = [{
    'desc': 'get the users who are still and have heart rate out of their historical range',
    'stmt': """
        select t1.user_id, t1.AVG_HEART_RATE, t1.NUM_LOCATIONS
        from "INTERIM_21" t1
        join "INTERIM_22" h on t1.user_id = h.user_id
        window tumbling (size {} seconds)
        ;
    """.format(config['WINDOW_TUMBLING_SECONDS']),
}]

stmts_teardown = [{
    'stmt': 'drop table "INTERIM_21";',
}, {
    'stmt': 'drop table "INTERIM_22";',
}, {
    'stmt': 'drop table "INTERIM_12";',
}, {
    'stmt': 'drop table "INTERIM_11";',
}]

api = Api(config['KSQL_HOST'], config['KSQL_PORT'])


def setup():
    for statement in stmts_setup:
        import pdb; pdb.set_trace()
        logger.info(statement['desc'] if 'desc' in statement else statement['stmt'])
        api.ksql({
            'ksql': statement['stmt'],
            'auto.offset.reset': 'earliest',
        })


def run():
    response = api.query({
        'ksql': stmts_run[0]['stmt'],
        'auto.offset.reset': 'earliest',
    })
    logger.info(response.text)
    import pdb; pdb.set_trace()


def teardown():
    response = api.ksql({ 'ksql': 'show queries;' })
    for query in response.json()[0]['queries']:
        api.ksql({ 'ksql': 'terminate {};'.format(query['id']) })
    for statement in stmts_teardown:
        logger.info(statement['stmt'])
        api.ksql({ 'ksql': statement['stmt'] }, force_exit=False)


Fire()
