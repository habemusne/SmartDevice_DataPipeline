import os
import json
from os import getenv
from os.path import join
from dotenv import load_dotenv

import util.naming
from util import parallel, get_cluster_servers

load_dotenv(dotenv_path='./.env')
topic_name = util.naming.topic_name('realtime')

cluster_servers = get_cluster_servers(force=False)
brokers = cluster_servers['brokers']
cmds = []
producer_cluster = 'producers' if getenv('PRODUCER_LIST') else 'brokers'
remote_realtime_data_path = join(getenv('DIR_DATA'), getenv('FILE_DATA_REALTIME'))

for i in range(len(cluster_servers[producer_cluster])):
    for _ in range(int(getenv('NUM_PRODUCERS_PROCESSES_PER_MACHINE'))):
        # cmds.append("""peg sshcmd-node {cluster} {index} " \
        #     ~/kafka/bin/kafka-console-producer.sh \
        #     --broker-list {broker_list} \
        #     --topic {topic_name} \
        #     --property parse.key=true \
        #     --property key.separator=: \
        #     --batch-size {batch_size}
        #     < {remote_realtime_data_path}"
        # """.format(
        #         cluster=producer_cluster,
        #         index=i + 1,
        #         broker_list=','.join(['{}:9092'.format(server) for server in brokers]),
        #         topic_name=topic_name,
        #         batch_size=getenv('PRODUCER_BATCH_SIZE'),
        #         remote_realtime_data_path=remote_realtime_data_path,
        #     )
        # )
        cmds.append("""peg sshcmd-node {cluster} {index} "cd ~/heart_watch && python3 produce_one.py {broker_list} {topic_name}"
        """.format(
            cluster=producer_cluster,
            index=i + 1,
            broker_list=','.join(['{}:9092'.format(server) for server in brokers]),
            topic_name=topic_name,
        ))
parallel(cmds, prompt=False)
