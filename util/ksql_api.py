import requests

from util.logger import logger


class Api:
    def __init__(self, host, port):
        self._ksql_endpoint = 'http://{}:{}/ksql'.format(host, port)
        self._query_endpoint = 'http://{}:{}/query'.format(host, port)

    def ksql(self, payload, force_exit=True):
        response = requests.post(self._ksql_endpoint, json=payload)
        if response.status_code >= 400:
            logger.error('Unable to run statement: {}'.format(payload['ksql']))
            logger.error(response.text)
            exit(1) if force_exit else None
        return response

    def query(self, payload, force_exit=True):
        response = requests.post(self._query_endpoint, json=payload)
        if response.status_code >= 400:
            logger.error('Unable to run statement: {}'.format(payload['ksql']))
            logger.error(response.text)
            exit(1) if force_exit else None
        return response
