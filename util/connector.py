# Assumption: use postgres; pg table name consistent with resource name ('historical'/'realtime')...

import requests
from os.path import join

import util.naming
from util.resource import Resource
from util.logger import logger


class Connector(Resource):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._name = util.naming.connector_name(self._data_name)
        self._api_url = 'http://{}:{}/connectors'.format(kwargs.get('host'), kwargs.get('port'))
        self._poll_interval = kwargs.get('poll_interval')

    def _create(self, payload, force_exit=True):
        response = requests.post(self._api_url, json=payload)
        if response.status_code >= 400:
            logger.error('Unable to create new connector {}. Please inspect from the logs'.format(self._name))
            logger.error(response.text)
            exit(1) if force_exit else None

    def _delete(self, force_exit=True):
        if self._get():
            response = requests.delete(join(self._api_url, self._name))
            if response.status_code >= 400:
                logger.error('Unable to delete existing connector {}. Please manually delete it from the web UI.'.format(self._name))
                logger.error(response.text)
                exit(1) if force_exit else None

    def _get(self):
        return requests.get(join(self._api_url, self._name)).status_code == 200


class JDBC(Connector):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._db_host = kwargs.get('db_host')
        self._db_port = kwargs.get('db_port')
        self._db_user = kwargs.get('db_user')
        self._db_password = kwargs.get('db_password')
        self._db_name = kwargs.get('db_name')
        self._keyfield = kwargs.get('keyfield')
        self._query = kwargs.get('query')

    def create(self):
        db_connect_url_jdbc = 'jdbc:postgresql://{}:{}/{}'.format(self._db_host, self._db_port, self._db_name)
        
        payload = {
            'name': self._name,
            'config': {
                'connector.class': 'io.confluent.connect.jdbc.JdbcSourceConnector',
                'connection.url': db_connect_url_jdbc,
                'connection.user': self._db_user,
                'connection.password': self._db_password,
                'poll.interval.ms' : self._poll_interval,
                'numeric.mapping': 'best_fit',
                'mode': 'bulk',
                'transforms': 'createKey,extractInt',
                'transforms.createKey.type': 'org.apache.kafka.connect.transforms.ValueToKey',
                'transforms.createKey.fields': self._keyfield,
                'transforms.extractInt.type': 'org.apache.kafka.connect.transforms.ExtractField$Key',
                'transforms.extractInt.field': self._keyfield,
            }
        }
        if self._query:
            topic_name = util.naming.topic_name(self._data_name)
            payload['config']['query'] = self._query
            payload['config']['topic.prefix'] = topic_name
            input('If topic {} does not exist, please create it first. Hit ENTER when done: '.format(topic_name))
        else:
            payload['config']['table.whitelist'] = self._data_name
            payload['config']['topic.prefix'] = util.naming.jdbc_topic_prefix()
        if self._get():
            self._delete()
        self._create(payload)

    def delete(self):
        self._delete()


class Datagen(Connector):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._iterations = kwargs.get('iterations')
        self._schema_path = kwargs.get('schema_path')
        self._schema_keyfield = kwargs.get('schema_keyfield')

    def create(self):
        topic_name = util.naming.topic_name(self._data_name)
        payload = {
            'name': self._name,
            'config': {
                'connector.class': 'io.confluent.kafka.connect.datagen.DatagenConnector',
                'kafka.topic': topic_name,
                'max.interval': self._poll_interval,
                'iterations': self._iterations,
                'schema.filename': self._schema_path,
                'schema.keyfield': self._schema_keyfield,
            }
        }
        input('If topic {} does not exist, please create it first. Hit ENTER when done: '.format(topic_name))
        self._create(payload)

    def delete(self):
        self._delete()
