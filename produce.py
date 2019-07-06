import os
from os import getenv
from os.path import join
from dotenv import load_dotenv

import util.naming
from util import parallel

load_dotenv(dotenv_path='./.env')
topic_name = util.naming.topic_name('realtime')


cmds = []
for i in range(len(getenv('BROKER_LIST').split(','))):
# for i in range(0, 1):
    for _ in range(int(getenv('NUM_PRODUCERS_PROCESSES_PER_MACHINE'))):
        cmds.append("""peg sshcmd-node brokers {index} " \
            ~/kafka/bin/kafka-console-producer.sh \
            --broker-list {broker_list} \
            --topic {topic_name} \
            --property parse.key=true \
            --property key.separator=: \
            < {realtime_data_path}"
        """.format(
                index=i + 1,
                broker_list=getenv('BROKER_LIST'),
                topic_name=topic_name,
                realtime_data_path=join(getenv('DIR_DATA'), getenv('FILE_DATA_REALTIME')),
            )
        )
parallel(cmds, prompt=False)

print('~/kafka/bin/kafka-console-consumer.sh --bootstrap-server {broker_leader}:9092 --from-beginning --topic {topic_name}'.format(
    broker_leader=getenv('BROKER_LEADER'),
    topic_name=topic_name,
))
