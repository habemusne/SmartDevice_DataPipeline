import requests
import json
from os.path import join
from os import getenv

from util.logger import logger
from util.resource import Resource


class Topic(Resource):
    def __init__(self, topic_name, num_partitions, replication_factor=1):
        self.name = topic_name
        self._num_partitions = num_partitions
        self._replication_factor = replication_factor
        self._base_url = 'http://{}:9021/2.0'.format(getenv('CONTROL_CENTER_HOST'))
        self._cluster_id = json.loads(requests.get(join(self._base_url, 'clusters/kafka')).text)[0]['clusterId']

    @Resource.log_notify
    def create(self, force_exit=True):
        response = requests.put(
            join(self._base_url, 'kafka', self._cluster_id, 'topics?validate=false'),
            json={
                'name': self.name,
                'numPartitions': self._num_partitions,
                'replicationFactor': self._replication_factor,
                'configs': {
                    'cleanup.policy': 'delete',
                    'delete.retention.ms': '300000',  # 5 minutes
                    'max.message.bytes': '1000012',
                    'min.insync.replicas': '1',
                    'retention.bytes': '1073741824',  # 1GB can hold 70-85 streaming files, each 100,000 msgs
                    'retention.ms': 3600000,  # msg lasts at most 1 hour, for demo
                },
            },
        )
        if response.status_code >= 400:
            logger.error('Unable to create topic: {}'.format(self.name))
            logger.error(response.text)
            exit(1) if force_exit else None

    @Resource.log_notify
    def delete(self):
        raise NotImplementedError('Delete\'s REST API was not hacked.')
