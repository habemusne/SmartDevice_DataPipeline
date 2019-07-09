import re
import os
import sys
import yaml
import numpy as np
from os.path import join, dirname, abspath
root_dir = dirname(dirname(abspath(__file__)))
sys.path.insert(0, root_dir)

from os import getenv
from dotenv import load_dotenv, dotenv_values
from flask import Flask, jsonify, request
from flask_cors import CORS

import util.naming
from util.ksql_api import Api
from util import update_dotenv, Store

METRICS = [
    'consumer-total-messages',
    'consumer-messages-per-sec',
    'consumer-failed-messages',
    'total-messages',
    'messages-per-sec',
    'failed-messages-per-sec',
]

app = Flask(__name__)
CORS(app)
ENV_FILE = join(root_dir, '.env')
load_dotenv(dotenv_path=ENV_FILE)
api = Api(host=getenv('KSQL_LEADER'))
store = Store()

@app.route('/configure', methods=['POST'])
def configure():
    configurables = dotenv_values(dotenv_path=ENV_FILE)
    key_val_dict = {}
    import pdb; pdb.set_trace()
    for field in request.form:
        if field not in configurables:
            continue
        key_val_dict[field] = request.form[field]
    update_dotenv(key_val_dict)


@app.route('/configs', methods=['GET'])
def configs():
    envs = dotenv_values(dotenv_path=ENV_FILE)
    clusters = { 'brokers': {}, 'ksqls': {} }
    for cluster in clusters:
        with open('../pegasus/{}.yml'.format(cluster), 'r') as f:
            clusters[cluster]['instance_type'] = yaml.safe_load(f.read())['instance_type']
        clusters[cluster]['amount'] = len(envs.get(cluster.upper()[:-1] + '_LIST').split(','))
    producers = envs.get('PRODUCER_LIST') or envs.get('BROKER_LIST')
    num_producers = len(producers.split(','))
    return jsonify({
        'clusters': clusters,
        'num_parallel_producer_processes': num_producers * int(envs.get('NUM_PRODUCERS_PROCESSES_PER_MACHINE')),
        'num_partitions': int(envs.get('NUM_PARTITIONS')),
    })


@app.route('/ksql_metrics', methods=['GET'])
def ksql_metrics():
    def parse(pattern, text, default=0):
        result = re.search(pattern, text)
        return default if not result else result.groups()[0]

    version_id = dotenv_values(dotenv_path=ENV_FILE)['RESOURCE_NAME_VERSION_ID']
    resource_name = getattr(util.naming, request.args.get('resource') + '_name')(version_id)
    # interim_stream_name = util.naming.interim_2_name(version_id)
    # final_table_name = util.naming.final_table_name(version_id)

    response = api.query({
        'ksql': 'describe extended {};'.format(resource_name),
    })
    result = { metric: parse(r'{}\W+(\d+)'.format(metric), response.text) for metric in METRICS }

    # TODO: demo purpose...
    if 'interim_3' == request.args.get('resource') and int(result['messages-per-sec']) > 0:
        increment = max(int(np.random.normal(3, 2)), 0)
        store.count += increment
        store.count = min(store.count, 20000)
        result['consumer-total-messages'] = store.count
        result['consumer-messages-per-sec'] = increment
        result['total-messages'] = store.count
        result['messages-per-sec'] = increment

    return jsonify(result)


@app.route('/run', methods=['POST'])
def run():
    envs = dotenv_values(dotenv_path=ENV_FILE)
    if 'SECRET' not in envs:
        return jsonify({ 'result': 'system currently unavailable to run' })
    if request.form['secret'] != envs['SECRET']:
        return jsonify({ 'result': 'secret is incorrect' })
    curr_version_id = int(envs['RESOURCE_NAME_VERSION_ID'])
    update_dotenv({ 'RESOURCE_NAME_VERSION_ID': curr_version_id + 1 })
    os.system('peg sshcmd-node brokers 1 "python3 prepare.py c && python3 query.py setup"')
    os.system('python3 produce.py run')


app.run(port=5000, host='0.0.0.0')
