import os
import sys
import json
from multiprocessing import Process
from os.path import join
from os import getenv
from time import sleep
from dotenv import load_dotenv, dotenv_values

load_dotenv(dotenv_path='./.env')

with open('/home/ubuntu/confluent/etc/schema-registry/schema-registry.properties', 'r') as f, open('tmp', 'w') as g:
    for line in f:
        if line.startswith('kafkastore.connection.url='):
            g.write('kafkastore.connection.url={}\n'.format(getenv('ZOOKEEPER_LIST')))
        else:
            g.write(line)

broker_list, topic_name = sys.argv[1:3]
realtime_data_path = join(getenv('DIR_DATA'), getenv('FILE_DATA_REALTIME'))


def processor():
    while True:
        envs = dotenv_values(dotenv_path='./.env')
        if int(envs.get('PRODUCER_RUN')) == 0:
            break
        cmd = """
            /home/ubuntu/confluent/bin/kafka-avro-console-producer \
            --broker-list {broker_list} \
            --topic {topic_name} \
            --property parse.key=true \
            --property key.separator=: \
            --property value.schema='{value_schema_str}'\
            --property key.schema='{key_schema_str}' \
            --batch-size {batch_size} \
            < {realtime_data_path}
        """.format(
            broker_list=broker_list,
            topic_name=topic_name,
            value_schema_str=json.dumps(json.loads(open('./schemas/realtime_value.avsc', 'r').read())),
            key_schema_str=json.dumps(json.loads(open('./schemas/realtime_key.avsc', 'r').read())),
            realtime_data_path=realtime_data_path,
            batch_size=getenv('PRODUCER_BATCH_SIZE'),
        )
        os.system(cmd)
        sleep(60)

p = Process(target=processor)
p.start()
