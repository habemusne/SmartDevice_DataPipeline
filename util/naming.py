import logging
from os.path import join, abspath, dirname
from dotenv import dotenv_values

_config = dotenv_values(join(dirname(dirname(abspath(__file__))), '.env'))
version_id = _config.get('RESOURCE_NAME_VERSION_ID', '1')


def topic_name(data_name):
    return '_'.join(['topic', version_id, data_name])


def jdbc_topic_prefix():
    return '_'.join(['topic', 'jdbc', version_id]) + '_'


def stream_name(data_name):
    return '_'.join(['stream', version_id, data_name])


def table_name(data_name):
    return '_'.join(['table', version_id, data_name])


def topic_name_jdbc(data_name):
    return jdbc_topic_prefix() + data_name


def connector_name(data_name):
    return '_'.join(['connect', version_id, data_name])
