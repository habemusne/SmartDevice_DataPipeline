import requests
from os.path import join

import naming
from resource import Resource
from logger import logger


class KSQLObject(Resource):
    def __init__(self, **kwargs):
        super().__init__(kwargs)
        self._field_map = kwargs.get('field_map')
        self._key_field_name = kwargs.get('key_field_name')
        self._api_url = 'http://{}:{}/ksql'.format(kwargs.get('host'), kwargs.get('port'))

    def _run_statement(self, payload, force_exit=True):
        # import pdb; pdb.set_trace()
        response = requests.post(API_URL, json=payload)
        if response.status_code >= 400:
            logger.error('Unable to run statement: {}'.format(payload['ksql']))
            logger.error(response.text)
            exit(1) if force_exit else None


class Table(KSQLObject):
    def __init__(self, **kwargs):
        super().__init__(kwargs)
        self._name = naming.table_name(kwargs.get('data_name'))

    def create(self):
        # user_id STRING, zipcode INTEGER, latitude DOUBLE, longitude DOUBLE, city STRING, state STRING, area STRING
        statement = """
        CREATE TABLE "{}" ({})
        WITH (
            KAFKA_TOPIC = '{}',
            VALUE_FORMAT = 'AVRO',
            KEY = '{}'
        );
        """.format(
            self._name,
            ','.join
            naming.topic_name(),
            self._key_field_name
        )
        payload = {
            'ksql': statement,
        }
        # TODO: delete all runnning queries before deleting the table
        self._run_statement(payload)

    def delete(self, force_exit=True):
        # TODO: remove force_exit=False by checking if table exists
        self._run_statement({
            'ksql': 'DROP TABLE {};'.format(self._name)
        }, force_exit=force_exit)


class Stream(KSQLObject):
    def __init__(self, **kwargs):
        super().__init__(kwargs)
        self._name = naming.stream_name(self._data_name)
        self._tumbling_seconds = kwargs.get('tumbling_seconds', None)
        self._offset_reset = kwargs.get('offset_reset', 'earliest')

    def create(self):
        # user_id STRING, zipcode INTEGER, latitude DOUBLE, longitude DOUBLE, city STRING, year INTEGER, month INTEGER, heart_rate INTEGER
        statement = """
        CREATE STREAM "{}" ({})
        WITH (
            KAFKA_TOPIC = '{}',
            VALUE_FORMAT = 'AVRO',
            Key = '{}'
        );
        """.format(
            self._name,
            ','.join([field_name + ' ' + field_type for field_name, field_type in self._field_map.items()])
            naming.topic_name(self._data_name),
            self._tumbling_seconds,
            self.key_field_name,
        )
        payload = {
            'ksql': statement,
            'streamsProperties': {
                'ksql.streams.auto.offset.reset': self._offset_reset,
            },
        }
        self._run_statement(payload)

    def delete(self, force_exit=True):
        self._run_statement({
            'ksql': 'DROP STREAM {};'.format(self._name)
        }, force_exit=force_exit)
