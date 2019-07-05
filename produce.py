import os
from os import getenv
from os.path import join
from dotenv import load_dotenv

import util.naming
from util.topic import Topic
from util.schema import Schema
from operations import parallel

load_dotenv(dotenv_path=join(dirname(abspath(__file__)), '.env'))
schema_path = join(getenv('DIR_SCHEMAS'), getenv('FILE_SCHEMA_REALTIME'))
topic_name = util.naming.topic_name('realtime')
topic = Topic(topic_name, getenv('NUM_PARTITIONS'))
schema = Schema(schema_path, topic_name)
topic.create()
schema.create()

cmds = []
for i in range(getenv('NUM_PRODUCERS')):
    cmds.append('peg sshcmd-node noncore 1 "~/kafka/bin/kafka-console-producer.sh --broker-list {broker_list} --topic {topic_name} < {realtime_data_path}"'.format(
        broker_list=getenv('BROKER_LIST'),
        topic_name=topic_name,
        realtime_data_path=join(getenv('DIR_DATA'), getenv('FILE_DATA_REALTIME')),
    ))
