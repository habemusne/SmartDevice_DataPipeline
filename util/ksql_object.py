from os import getenv
from os.path import join

import util.naming
from util.resource import Resource
from util.logger import logger
from util.ksql_api import Api


class KSQLObject(Resource):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api = Api(
            kwargs.get('host', getenv('KSQL_HOST')),
            kwargs.get('port', getenv('KSQL_PORT'))
        )
        self._field_map = kwargs.get('field_map')
        self._key_field_name = kwargs.get('key_field_name')


class Table(KSQLObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._name = util.naming.table_name(self._data_name)
        self._topic_name = kwargs.get('topic_name') if kwargs.get('topic_name') else util.naming.topic_name(self._data_name)

    @Resource.log_notify
    def create(self):
        statement = """
        CREATE TABLE "{name}" ({fields})
        WITH (
            KAFKA_TOPIC = '{topic}',
            VALUE_FORMAT = 'AVRO',
            KEY = '{key}'
        );
        """.format(
            name=self._name,
            fields=','.join([field_name + ' ' + field_type for field_name, field_type in self._field_map.items()]),
            topic=self._topic_name,
            key=self._key_field_name,
        )
        payload = {
            'ksql': statement,
        }
        self.api.ksql(payload)

    @Resource.log_notify
    def delete(self, force_exit=True):
        input('Make sure that streams of table {} are deleted. Press ENTER to continue: '.format(self._name))
        self.api.ksql({
            'ksql': 'DROP TABLE {};'.format(self._name)
        }, force_exit=force_exit)


class Stream(KSQLObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._name = util.naming.stream_name(self._data_name)
        self._offset_reset = kwargs.get('offset_reset', 'earliest')

    @Resource.log_notify
    def create(self):
        statement = """
        CREATE STREAM "{name}" ({fields})
        WITH (
            KAFKA_TOPIC = '{topic}',
            VALUE_FORMAT = 'AVRO',
            Key = '{key}'
        );
        """.format(
            name=self._name,
            fields=','.join([field_name + ' ' + field_type for field_name, field_type in self._field_map.items()]),
            topic=util.naming.topic_name(self._data_name),
            key=self._key_field_name,
        )
        payload = {
            'ksql': statement,
            'streamsProperties': {
                'ksql.streams.auto.offset.reset': self._offset_reset,
            },
        }
        self.api.ksql(payload)

    @Resource.log_notify
    def delete(self, force_exit=True):
        self.api.ksql({
            'ksql': 'DROP STREAM {};'.format(self._name)
        }, force_exit=force_exit)
