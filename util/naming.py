import logging
from os import getenv
from os.path import join, abspath, dirname
from dotenv import load_dotenv

load_dotenv(dotenv_path=join(dirname(dirname(abspath(__file__))), '.env'))
version_id = getenv('RESOURCE_NAME_VERSION_ID')


def topic_name(data_name, index=''):
    result = '_'.join(['topic', version_id, data_name])
    return result if not index else '{}_{}'.format(result, index)


def jdbc_topic_prefix():
    return '_'.join(['topic', 'jdbc', version_id]) + '_'


def stream_name(data_name):
    return '_'.join(['stream', version_id, data_name])


def table_name(data_name):
    return '_'.join(['table', version_id, data_name])


def topic_name_jdbc(data_name):
    return jdbc_topic_prefix() + data_name


def connector_name(data_name, index=''):
    result = '_'.join(['connect', version_id, data_name])
    return result if not index else '{}_{}'.format(result, index)


def interim_1_name(version_id_override=None):
    return 'INTERIM_1_{}'.format(version_id_override or version_id)


def interim_2_name(version_id_override=None):
    return 'INTERIM_2_{}'.format(version_id_override or version_id)


def interim_3_name(version_id_override=None):
    return 'INTERIM_3_{}'.format(version_id_override or version_id)


def final_table_name(version_id_override=None):
    return 'FINAL_{}'.format(version_id_override or version_id)


def final_topic_name(version_id_override=None):
    return 'topic-final-{}'.format(version_id_override or version_id)
