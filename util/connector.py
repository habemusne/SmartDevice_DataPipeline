# Assumption: postgres

import requests
from os.path import join

import naming
from resource import Resource
from logger import logger


class Connector(Resource):
    def __init__(self, **kwargs):
        super().__init__(kwargs)
        self._name = naming.connector_name(self._data_name)
        self._api_url = 'http://{}:{}/connectors'.format(kwargs.get('host'), kwargs.get('port'))
        self._poll_interval = kwargs.get('poll_interval')

    def _create(self, payload, force_exit=True):
        response = requests.post(self._api_url, json=payload)
        if response.status_code >= 400:
            logger.error('Unable to create new connector {}. Please inspect from the logs'.format(connector_name))
            logger.error(response.text)
            exit(1) if force_exit else None

    def _delete(self, force_exit=True):
        if self._get(self._name):
            response = requests.delete(join(self._api_url, self._name))
            if response.status_code >= 400:
                logger.error('Unable to delete existing connector {}. Please manually delete it from the web UI.'.format(connector_name))
                logger.error(response.text)
                exit(1) if force_exit else None

    def _get(self):
        return requests.get(join(self._api_url, self._name)).status_code == 200


class JDBC(Connector):
    def __init__(self, **kwargs):
        self._db_host = kwargs.get('db_host')
        self._db_port = kwargs.get('db_port')
        self._db_user = kwargs.get('db_user')
        self._db_password = kwargs.get('db_password')
        self._db_name = kwargs.get('db_name')

    def create(self):
        db_connect_url_jdbc = 'jdbc:postgresql://{}:{}/{}'.format(self._db_host, self._db_port, self._db_name)
        payload = {
            'name': self._name,
            'config': {
                'connector.class': 'io.confluent.connect.jdbc.JdbcSourceConnector',
                'connection.url': db_connect_url_jdbc,
                'connection.user': self._db_user,
                'connection.password': self._db_password,
                'topic.prefix': naming.topic_name_jdbc(),
                'poll.interval.ms' : self._poll_interval,
                'numeric.mapping': 'best_fit',
                'table.whitelist' : 'historical',
                'mode':'bulk',
                'transforms':'createKey,extractInt',
                'transforms.createKey.type':'org.apache.kafka.connect.transforms.ValueToKey',
                'transforms.createKey.fields':'id',
                'transforms.extractInt.type':'org.apache.kafka.connect.transforms.ExtractField$Key',
                'transforms.extractInt.field':'id',
            }
        }
        if self._get():
            self._delete()
        self._create(payload)

    def delete(self), :
        self._delete()


class Datagen(Connector):
    def __init__(self, **kwargs):
        super().__init__(kwargs)
        self._iterations = kwargs.get('iterations')
        self._schema_path = kwargs.get('scheme_path')

    def create(self):
        topic_name = naming.topic_name(self._name)
        payload = {
            'name': self._name,
            'config': {
                'connector.class': 'io.confluent.kafka.connect.datagen.DatagenConnector',
                'kafka.topic': topic_name,
                'max.interval': self._poll_interval,
                'iterations': self._iterations,
                'schema.filename': self._schema_path,
                'schema.keyfield': 'user_id',
            }
        }
        input('If topic {} does not exist, please create it first. Hit ENTER when done: '.format(topic_name))
        self._create(payload)

    def delete(self):
        self._delete()
