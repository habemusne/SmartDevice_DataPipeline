# Assumption: pegasus
import os
import re
import subprocess
import json
from os import getenv
from os.path import join, relpath, exists, abspath, dirname
from fire import Fire
from time import sleep
from dotenv import load_dotenv

from util import parallel, get_cluster_servers, sync, update_dotenv
from util.logger import logger


LOCAL_ROOT_DIR = dirname(abspath(__file__))
REMOTE_ROOT_DIR = '/home/ubuntu/heart_watch'
CLUSTERS = set(['brokers', 'ksqls'])
load_dotenv(dotenv_path='./.env')


def stage1():
    input('\nOpen pegasus/{}.yml and adjust the settings. Press ENTER when done: '.format(', '.join([key for key in CLUSTERS])))
    parallel(['peg up pegasus/{}.yml'.format(key) for key in CLUSTERS])
    fetch()


def stage2():
    input('\nWARNING: this terminal session will be "destroyed" after running this command. Please open a fresh terminal session for this stage. If you ARE running this in the new session, press ENTER. Otherwise, CTRL + C: ')
    input('\nCopy env.template to .env and adjust .env. Press ENTER to when done: ')
    add_ssh_key()
    configure_remote()
    env_override()


def stage3():
    answer = input('\nAre you running in large scale manner (more than 5 machines in total)? [y/n] ')
    if answer == 'y':
        answer = input('\nHave you followed the "A caveat for running in large scale" section in README? (i.e. you are using my public AMI) [y/n] ')
        if answer != 'y':
            logger.warning('Please follow its instructions before proceeding.')
            exit(0)
        else:
            cmds = [
                'cd ~/heart_watch && pip3 install -r requirements.txt',
                'cd ~/heart_watch && wget http://apache.mirrors.pair.com/kafka/2.2.0/kafka_2.12-2.2.0.tgz',
                'cd ~/heart_watch && tar -xzf kafka_2.12-2.2.0.tgz && mv kafka_2.12-2.2.0 kafka',
            ]
    else:
        cmds = [
            'curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -',
            'sudo add-apt-repository \'deb [arch=amd64] https://download.docker.com/linux/ubuntu xenial stable\'',
            'sudo add-apt-repository --remove -y ppa:andrei-pozolotin/maven3',
            'sudo apt-get -y update',
            'apt-cache policy docker-ce',
            'sudo kill -9 $(ps aux | grep \'dpkg\' | awk \'{print $2}\')',
            'sudo kill -9 $(ps aux | grep \'apt\' | awk \'{print $2}\')',
            'sudo killall -r dpkg',
            'sudo killall -r apt',
            'sudo dpkg --configure -a',
            'sudo apt-get install -y docker-ce python3-pip libpq-dev python-dev maven awscli',
            'sudo usermod -aG docker ubuntu',
            'cd ~/heart_watch && pip3 install -r requirements.txt',
            'cd ~/ && wget http://apache.mirrors.pair.com/kafka/2.2.0/kafka_2.12-2.2.0.tgz',
            'cd ~/ && tar -xzf kafka_2.12-2.2.0.tgz && mv kafka_2.12-2.2.0 kafka',
        ]
    for cmd in cmds:
        parallel(['peg sshcmd-cluster {} "{}"'.format(key, cmd) for key in CLUSTERS])
    logger.info("""
        If you responded that you are NOT running in large scale manner, please manually ssh to each machine and run the following commands

        sudo curl -L https://github.com/docker/compose/releases/download/1.24.1/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose

        Then log out and log back again. Then make sure than you can run docker-compose. If you can't, please manually fix it.
    """)

def start_containers():
    cmds = []
    for i in range(1, int(len(getenv('BROKER_LIST').split(','))) + 1):
        cmds.append('peg sshcmd-node brokers {} "cd ~/heart_watch && docker-compose -f docker-compose/brokers.yml up -d zookeeper"'.format(i))
    parallel(cmds, prompt=False)
    sleep(5)

    cmds = []
    for i in range(1, int(len(getenv('BROKER_LIST').split(','))) + 1):
        cmds.append('peg sshcmd-node brokers {} "cd ~/heart_watch && docker-compose -f docker-compose/brokers.yml up -d broker"'.format(i))
    parallel(cmds, prompt=False)
    sleep(5)

    cmds = []
    cmds.append('peg sshcmd-node brokers 1 "cd ~/heart_watch && docker-compose -f docker-compose/brokers.yml up -d schema-registry connect rest-proxy database"')
    cmds.append('peg sshcmd-cluster ksqls "cd ~/heart_watch && docker-compose -f docker-compose/ksqls.yml up -d"')
    parallel(cmds, prompt=False)

    sleep(20)
    parallel([
        'peg sshcmd-node brokers 1 "cd ~/heart_watch && docker-compose -f docker-compose/brokers.yml up -d control-center"',
    ], prompt=False)


def stop_containers():
    parallel([
        'peg sshcmd-cluster brokers "cd ~/heart_watch && docker-compose -f docker-compose/brokers.yml stop && docker-compose -f docker-compose/brokers.yml rm -f"',
        'peg sshcmd-cluster ksqls "cd ~/heart_watch && docker-compose -f docker-compose/ksqls.yml stop && docker-compose -f docker-compose/ksqls.yml rm -f"',
    ], prompt=False)


def fetch():
    parallel(['peg fetch {}'.format(key) for key in CLUSTERS], prompt=False)


def add_ssh_key():
    cmds = []
    for cluster, servers in get_cluster_servers(force=False).items():
        for i in range(len(servers)):
            cmds.append('peg ssh {} {}'.format(cluster, str(i + 1)))
    processes = []
    for _args in cmds:
        process = subprocess.Popen(_args, shell=True)
        processes.append(process)
    sleep(5)
    for process in processes:
        process.kill()


def force_sync():
    stop_containers()
    parallel(['peg sshcmd-cluster {} "rm -rf ~/heart_watch"'.format(cluster) for cluster in CLUSTERS])
    sync()


def env_override():
    def key_val_str(key, val):
        return '{}={}\n'.format(key, val)

    cluster_servers = get_cluster_servers(force=False)
    ksqls_str = ','.join(['http://{}:8088'.format(s) for s in cluster_servers['ksqls']])
    brokers_str = ','.join(['{}:9092'.format(s) for s in cluster_servers['brokers']])

    # TODO: using multiple zookeepers needs extra distribucted setup
    # zookeepers_str = ','.join(['{}:2181'.format(s) for s in cluster_servers['brokers']])
    zookeepers_str = cluster_servers['brokers'][0] + ':2181'
    schema_registry_kafkastore_bootstrap_servers = ','.join(['PLAINTEXT://{}:9092'.format(s) for s in cluster_servers['brokers']])
    master_dns = cluster_servers['brokers'][0]

    for cluster, servers in cluster_servers.items():
        for i, server in enumerate(servers):
            local_path = '.env.override_{}_{}'.format('brokers', i + 1)
            with open('.env.override/zookeeper', 'w') as f:
                for j in range(len(servers)):
                    f.write(key_val_str(
                        'ZOOKEEPER_SERVER_{}'.format(j + 1),
                        '{}:2888:3888'.format(servers[j]),
                    ))

            with open('.env.override/broker', 'w') as f:
                f.write(key_val_str(
                    'KAFKA_ADVERTISED_LISTENERS',
                    'PLAINTEXT://{}:9092'.format(server)),
                )
                f.write(key_val_str('KAFKA_BROKER_ID', i + 1))
                # f.write(key_val_str('KAFKA_ZOOKEEPER_CONNECT', "{}".format(zookeepers_str)))
                f.write(key_val_str('CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS', brokers_str))
                f.write(key_val_str('CONFLUENT_METRICS_REPORTER_ZOOKEEPER_CONNECT', zookeepers_str))

            with open('.env.override/schema-registry', 'w') as f:
                f.write(key_val_str('SCHEMA_REGISTRY_HOST_NAME', master_dns))
                f.write(key_val_str('SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL', zookeepers_str))

            with open('.env.override/connect', 'w') as f:
                f.write(key_val_str('CONNECT_BOOTSTRAP_SERVERS', '{}:9092'.format(master_dns)))
                f.write(key_val_str('CONNECT_REST_ADVERTISED_HOST_NAME', master_dns))
                f.write(key_val_str('CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL', 'http://{}:8081'.format(master_dns)))

            with open('.env.override/rest-proxy', 'w') as f:
                f.write(key_val_str('KAFKA_REST_BOOTSTRAP_SERVERS', brokers_str))
                f.write(key_val_str('KAFKA_REST_SCHEMA_REGISTRY_URL', 'http://{}:8081'.format(master_dns)))

            with open('.env.override/control-center', 'w') as f:
                f.write(key_val_str('CONTROL_CENTER_BOOTSTRAP_SERVERS', brokers_str))
                f.write(key_val_str('CONTROL_CENTER_ZOOKEEPER_CONNECT', zookeepers_str))
                f.write(key_val_str('CONTROL_CENTER_KSQL_URL', ksqls_str))
                f.write(key_val_str('CONTROL_CENTER_KSQL_ADVERTISED_URL', '{}:8088'.format(master_dns)))
                f.write(key_val_str('CONTROL_CENTER_SCHEMA_REGISTRY_URL', 'http://{}:8081'.format(master_dns)))

            with open('.env.override/ksql-server', 'w') as f:
                f.write(key_val_str('KSQL_BOOTSTRAP_SERVERS', brokers_str))
                f.write(key_val_str('KSQL_HOST', server))
                f.write(key_val_str('KSQL_KSQL_SCHEMA_REGISTRY_URL', 'http://{}:8081'.format(master_dns)))
                f.write(key_val_str('KSQL_HOST_NAME', server))

            services = ['zookeeper', 'broker', 'schema-registry', 'connect', 'rest-proxy', 'control-center', 'ksql-server']
            cmds = []
            for service in services:
                cmds.append('peg scp from-local {cluster} {node} {local_path} {remove_path} && rm {local_path}'.format(
                    cluster=cluster,
                    node=str(i + 1),
                    local_path=join('.env.override', service),
                    remove_path=join(REMOTE_ROOT_DIR, '.env.override', service),
                ))
            parallel(cmds, prompt=False)


def configure_remote():
    cluster_servers = get_cluster_servers(force=False)
    master_dns = cluster_servers['brokers'][0]
    key_val_dict = {}
    for variable in ['SCHEMA_REGISTRY_HOST', 'CONNECT_HOST', 'CONTROL_CENTER_HOST', 'BROKER_LEADER', 'DB_HOST']:
        key_val_dict[variable] = master_dns
    key_val_dict['BROKER_LIST'] = ','.join(['{}:9092'.format(s) for s in cluster_servers['brokers']])
    key_val_dict['ZOOKEEPER_LIST'] = '{}:2181'.format(cluster_servers['brokers'][0])
    key_val_dict['KSQL_LEADER'] = cluster_servers['ksqls'][0]
    key_val_dict['KSQL_LIST'] = ','.join(['http://{}:8088'.format(s) for s in cluster_servers['ksqls']])
    update_dotenv(key_val_dict)


def clean_logs():
    parallel(['peg sshcmd-cluster {} "sudo sh -c \'truncate -s 0 /var/lib/docker/containers/*/*-json.log\'"'.format(key) for key in CLUSTERS], prompt=False)


def generate_data(data_name, schema_path, num_iterations, avro_random_gen_dir, key_field=None):
    # https://github.com/confluentinc/avro-random-generator
    # python3 operations.py generate_data realtime ./schemas/realtime_value_datagen.avsc 100000 /Users/a67/Project/insight/avro-random-generator user_id
    # python3 operations.py generate_data historical ./schemas/historical.avsc 20000 /Users/a67/Project/insight/avro-random-generator
    tmp_path = './{}_{}.data'.format(data_name, num_iterations)
    output_path = 'data/{}_{}.data'.format(data_name, num_iterations)
    os.system('{program} -c -f {schema_path} -i {num_iterations} -o {output_path}'.format(
        program=join(avro_random_gen_dir, 'arg'),
        schema_path=schema_path,
        num_iterations=num_iterations,
        output_path=tmp_path,
    ))
    if key_field:
        with open(tmp_path, 'r') as f, open(output_path, 'w') as g:
            for line in f:
                if not line:
                    continue
                key = json.loads(line.strip())[key_field]
                if key.isdigit():
                    key = '"{}"'.format(key)
                g.write('{}:{}'.format(key, line))
        os.remove(tmp_path)
    else:
        os.system('mv {} {}'.format(tmp_path, output_path))


def configure_producer():
    print("""
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
        sudo add-apt-repository --remove -y ppa:andrei-pozolotin/maven3
        sudo apt-get -y update
        sudo dpkg --configure -a
        sudo apt-get install -y maven unzip python3-pip libpq-dev python-dev
        cd ~/ && wget http://apache.mirrors.pair.com/kafka/2.2.0/kafka_2.12-2.2.0.tgz
        cd ~/ && tar -xzf kafka_2.12-2.2.0.tgz && mv kafka_2.12-2.2.0 kafka
        wget http://packages.confluent.io/archive/3.0/confluent-3.0.0-2.11.zip
        unzip confluent-3.0.0-2.11.zip && mv confluent-3.0.0 confluent
        cd ~/heart_watch && pip3 install -r requirements.txt
    """)


def configure_website():
    print("""
        # Run this on your local
        python3 operations.py sync <host_ip>

        # Run all the following on remote host

        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
        sudo add-apt-repository 'deb [arch=amd64] https://download.docker.com/linux/ubuntu xenial stable'
        sudo add-apt-repository --remove -y ppa:andrei-pozolotin/maven3
        sudo apt-get -y update
        sudo apt-get install -y python3-pip libpq-dev python-dev
        cd ~/heart_watch && pip3 install -r requirements.txt
        sudo apt install -y npm
        cd ~/heart_watch && curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
        . ~/.nvm/nvm.sh
        nvm install 12.4.0
        cd ~/heart_watch/ui && npm i
        sudo npm i react-scripts -g

        mv .env.template .env
        # Then properly edit .env

        # run this if in production
        npm run build
        sudo npm install -g serve
        export SERVER_HOST=<host ip> && sudo serve -s build -l tcp://0.0.0.0:80

        # run this is in development
        npm start

        # Run the flask server in a background process
        cd ~/heart_watch/ui && python3 server.py

        # Run the react server in a background process
        cd ~/heart_watch/ui && npm start
    """)


Fire()
