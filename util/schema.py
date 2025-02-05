import requests
import json
from os.path import join

from util.logger import logger
from util.resource import Resource

HEADER = { 'Content-Type': 'application/vnd.schemaregistry.v1+json' }


class Schema(Resource):
    def __init__(self, schema_registry_host, schema_filepath, topic_name, schema_type='value'):
        self.name = '-'.join([topic_name, schema_type])
        self._base_url = 'http://{}:8081'.format(schema_registry_host)
        self._topic_name = topic_name
        with open(schema_filepath, 'r') as f:
            self._schema_str = f.read()
        self._schema_filepath = schema_filepath

    @Resource.log_notify
    def create(self):
        url = join(self._base_url, 'subjects', self.name, 'versions')

        # https://gist.github.com/aseigneurin/5730c07b4136a84acb5aeec42310312c
        payload = "{ \"schema\": \"" \
          + self._schema_str.replace("\"", "\\\"").replace("\t", "").replace("\n", "") \
          + "\" }"

        response = requests.post(url, headers=HEADER, data=payload)
        if response.status_code >= 400:
            logger.error('Unable to create topic: {}'.format(self.name))
            logger.error(response.text)
            exit(1)

    @Resource.log_notify
    def delete(self):
        url = join(self._base_url, 'subjects', self.name)
        requests.delete(url, headers=HEADER)
