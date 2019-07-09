# Assumption: use postgres; pg table name consistent with resource name ('historical'/'realtime')...

import requests
from os import getenv
from os.path import join

import util.naming
from util.resource import Resource
from util.logger import logger
from util.topic import Topic


class Connector(Resource):
    def __init__(self, **kwargs):
        """
        @param host
        @param poll_interval
        """
        self._api_url = 'http://{}:8083/connectors'.format(kwargs.get('host'))
        self._poll_interval = kwargs.get('poll_interval')

    def _create(self, payload, force_exit=True):
        response = requests.post(self._api_url, json=payload)
        if response.status_code >= 400:
            logger.error('Unable to create new connector {}. Please inspect from the logs'.format(self.name))
            logger.error(response.text)
            exit(1) if force_exit else None

    def _delete(self, force_exit=True):
        if self._get():
            response = requests.delete(join(self._api_url, self.name))
            if response.status_code >= 400:
                logger.error('Unable to delete existing connector {}. Please manually delete it from the web UI.'.format(self.name))
                logger.error(response.text)
                exit(1) if force_exit else None

    def _get(self):
        return requests.get(join(self._api_url, self.name)).status_code == 200


class JDBCSource(Connector):
    def __init__(self, **kwargs):
        """
        @param host
        @param poll_interval

        @param data_name
        @param query
        @param keyfield

        @param control_center_host
        """
        super().__init__(**kwargs)
        data_name = kwargs.get('data_name')
        self._table_name = data_name
        self.name = util.naming.connector_name(data_name)
        self._query = kwargs.get('query')
        if self._query:
            self._topic = Topic(kwargs.get('control_center_host'), util.naming.topic_name(data_name), int(getenv('NUM_PARTITIONS')), long_last=True)
        else:
            self._topic_prefix = util.naming.jdbc_topic_prefix()
            self._topic = Topic(kwargs.get('control_center_host'), self._topic_prefix + data_name, int(getenv('NUM_PARTITIONS')), long_last=True)

        self._keyfield = kwargs.get('keyfield')

    @Resource.log_notify
    def create(self):
        db_connect_url_jdbc = 'jdbc:postgresql://{}:5432/postgres'.format(getenv('DB_HOST'))
        
        payload = {
            'name': self.name,
            'config': {
                'connector.class': 'io.confluent.connect.jdbc.JdbcSourceConnector',
                'connection.url': db_connect_url_jdbc,
                'connection.user': 'postgres',
                'connection.password': 'postgres',
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
            payload['config']['query'] = self._query
            payload['config']['topic.prefix'] = self._topic.name
            self._topic.create()
        else:
            logger.warning('"query" parameter is not specified. All messages will be sent to a single partition.')
            payload['config']['table.whitelist'] = self._table_name
            payload['config']['topic.prefix'] = util.naming.jdbc_topic_prefix()
        if self._get():
            self._delete()
        self._create(payload)

    @Resource.log_notify
    def delete(self):
        # self._topic.delete()
        self._delete()


class Datagen(Connector):
    def __init__(self, **kwargs):
        """
        @param host
        @param poll_interval

        @param id
        @param data_name
        @param iterations
        @param schema_path
        @param schema_keyfield
        """
        super().__init__(**kwargs)
        _id = kwargs.get('id', '')
        self.name = util.naming.connector_name(kwargs.get('data_name'), _id)
        topic_name = util.naming.topic_name(kwargs.get('data_name'), _id)
        self._topic = Topic(topic_name, int(getenv('NUM_PARTITIONS')))
        self._iterations = kwargs.get('iterations')
        self._schema_path = kwargs.get('schema_path')
        self._schema_keyfield = kwargs.get('schema_keyfield')

    @Resource.log_notify
    def create(self):
        payload = {
            'name': self.name,
            'config': {
                'connector.class': 'io.confluent.kafka.connect.datagen.DatagenConnector',
                'kafka.topic': self._topic.name,
                'max.interval': self._poll_interval,
                'iterations': self._iterations,
                'schema.filename': self._schema_path,
                'schema.keyfield': self._schema_keyfield,
                'transforms': 'insertGenerationAt',
                'transforms.insertGenerationAt.type': 'org.apache.kafka.connect.transforms.InsertField$Value',
                'transforms.insertGenerationAt.timestamp.field': 'generated_at',
            }
        }
        self._topic.create()
        self._create(payload)

    @Resource.log_notify
    def delete(self):
        # self._topic.delete()
        self._delete()


class S3Sink(Connector):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @Resource.log_notify
    def create(self):
        payload = {
            'name': self.name + '.sink',
            config: {
                'format.class': 'io.confluent.connect.s3.format.avro.AvroFormat',
                'flush.size': 10000,
                # 'rotate.interval.ms': 10000,
                's3.bucket.name': '',
            }
        }
        raise NotImplementedError

    @Resource.log_notify
    def delete(self):
        raise NotImplementedError
