# Assumption: pegasus
import os
import re
import subprocess
import json
from os import getenv
from os.path import join, relpath
from fire import Fire
from time import sleep
from dotenv import load_dotenv

from util import parallel
from util.logger import logger


LOCAL_ROOT_DIR = '/Users/a67/Project/insight/heart_watch'
REMOTE_ROOT_DIR = '/home/ubuntu/heart_watch'
CLUSTERS = set(['brokers', 'noncore', 'ksqls'])
DNS_LIST = [
    'ec2-3-217-127-70.compute-1.amazonaws.com', # broker 1     
    'ec2-54-173-174-130.compute-1.amazonaws.com', # broker 2       
    'ec2-3-220-6-110.compute-1.amazonaws.com', # broker 2       
    'ec2-3-218-29-232.compute-1.amazonaws.com', # ksql 1       
    'ec2-3-218-217-234.compute-1.amazonaws.com', # ksql 2      
    'ec2-3-218-41-97.compute-1.amazonaws.com', # ksql 2      
    'ec2-174-129-46-139.compute-1.amazonaws.com', # noncore 1        
]
load_dotenv(dotenv_path='./.env')


def stage1():
    input('\nOpen pegasus/{}.yml and adjust the settings. Press ENTER when done: '.format(', '.join([key for key in CLUSTERS])))
    parallel(['peg up pegasus/{}.yml'.format(key) for key in CLUSTERS])
    fetch()


def stage2():
    input('\nWARNING: this terminal session will be "destroyed" after running this command. Please open a fresh terminal session for this stage. If you ARE running this in the new session, press ENTER. Otherwise, CTRL + C: ')
    input('\nCopy env.template to .env and adjust .env. Press ENTER to when done: ')
    add_ssh_key()
    sync()
    env_override()


def stage3():
    answer = input('\nAre you running in large scale manner (more than 5 machines in total)? [y/n]')
    if answer == 'y':
        answer = input('\nHave you followed the "A caveat for running in large scale" section in README? (i.e. you are using my public AMI) [y/n]')
        if answer != 'y':
            logger.warning('Please follow its instructions before proceeding.')
            exit(0)
        else:
            cmds = ['cd ~/heart_watch && pip3 install -r requirements.txt']
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
            'wget http://apache.mirrors.pair.com/kafka/2.2.0/kafka_2.12-2.2.0.tgz',
            'tar -xzf kafka_2.12-2.2.0.tgz && mv kafka_2.12-2.2.0 kafka',
        ]
    for cmd in cmds:
        parallel(['peg sshcmd-cluster {} "{}"'.format(key, cmd) for key in CLUSTERS])
    logger.info("""
        If you responded that you are NOT running in large scale manner, please manually ssh to each machine and run the following commands

        sudo curl -L https://github.com/docker/compose/releases/download/1.24.1/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose

        Then make sure than you can run docker-compose. If you can't please fix it.
    """)


def start_containers():
    parallel([
        'peg sshcmd-cluster brokers "cd ~/heart_watch && docker-compose -f docker-compose/brokers.yml up -d"',
    ])
    logger.info('Waiting for 10 seconds.. ')
    sleep(10)
    parallel([
        'peg sshcmd-node brokers 1 "cd ~/heart_watch && docker-compose -f docker-compose/noncore.yml up -d schema-registry rest-proxy database control-center"',
        'peg sshcmd-cluster noncore "cd ~/heart_watch && docker-compose -f docker-compose/noncore.yml up -d connect"',
        'peg sshcmd-cluster ksqls "cd ~/heart_watch && docker-compose -f docker-compose/ksqls.yml up -d"',
    ], prompt=False)


def stop_containers():
    parallel([
        'peg sshcmd-cluster brokers "cd ~/heart_watch && docker-compose -f docker-compose/brokers.yml stop && docker-compose -f docker-compose/brokers.yml rm -f"',
        'peg sshcmd-node brokers 1 "cd ~/heart_watch && docker-compose -f docker-compose/noncore.yml stop && docker-compose -f docker-compose/noncore.yml rm -f"',
        'peg sshcmd-cluster noncore "cd ~/heart_watch && docker-compose -f docker-compose/noncore.yml stop && docker-compose -f docker-compose/noncore.yml rm -f"',
        'peg sshcmd-cluster ksqls "cd ~/heart_watch && docker-compose -f docker-compose/ksqls.yml stop && docker-compose -f docker-compose/ksqls.yml rm -f"',
    ], prompt=False)


def fetch():
    parallel(['peg fetch {}'.format(key) for key in CLUSTERS], prompt=False)


def add_ssh_key():
    cmds = []
    for key in CLUSTERS:
        server_list = re.findall('Public DNS: (.*)', os.popen('peg fetch {}'.format(key)).read())
        for i in range(len(server_list)):
            cmds.append('peg ssh {} {}'.format(key, str(i + 1)))
    processes = []
    for _args in cmds:
        process = subprocess.Popen(_args, shell=True)
        processes.append(process)
    sleep(5)
    for process in processes:
        process.kill()


def sync():
    command = """rsync -avL --exclude '.env.override/*' --progress -e "ssh -i ~/.ssh/mark-chen-IAM-key-pair.pem" --log-file="/Users/a67/rsync.log" /Users/a67/Project/insight/heart_watch/ ubuntu@{}:~/heart_watch/"""
    parallel([command.format(dns) for dns in DNS_LIST], prompt=False)


def force_sync():
    stop_containers()
    parallel(['peg sshcmd-cluster {} "rm -rf ~/heart_watch"'.format(cluster) for cluster in CLUSTERS])
    sync()


def env_override(cluster='all'):
    def work(cluster, variables):
        remote_path = join(REMOTE_ROOT_DIR, relpath('.env.override/{}'.format(cluster), LOCAL_ROOT_DIR))
        server_list = re.findall('Public DNS: (.*)', os.popen('peg fetch {}'.format(cluster)).read())
        for i in range(len(server_list)):
            with open('.env.override/{}'.format(cluster), 'w') as f:
                if 'KAFKA_ADVERTISED_LISTENERS' in variables:
                    f.write('{}={}\n'.format('KAFKA_ADVERTISED_LISTENERS', 'PLAINTEXT://{}:9092'.format(server_list[i])))
                if 'KAFKA_BROKER_ID' in variables:
                    f.write('{}={}\n'.format('KAFKA_BROKER_ID', i + 1))
                if 'KSQL_HOST' in variables:
                    f.write('{}={}\n'.format('KSQL_HOST', server_list[i]))
            os.system('peg scp from-local {} {} {} {}'.format(cluster, str(i + 1), '.env.override/{}'.format(cluster), remote_path))

    if cluster in ['brokers', 'all']:
        work('brokers', set(['KAFKA_ADVERTISED_LISTENERS', 'KAFKA_BROKER_ID']))

    if cluster in ['ksqls', 'all']:
        work('ksqls', set(['KSQL_HOST']))


def clean_logs():
    parallel(['peg sshcmd-cluster {} "sudo sh -c \'truncate -s 0 /var/lib/docker/containers/*/*-json.log\'"'.format(key) for key in CLUSTERS], prompt=False)


def full_run():
    input('\nProperly change RESOURCE_NAME_VERSION_ID. Press ENTER when done: ')
    sync()
    input('\nRun this on the broker: python3 setup/stage3/prepare.py c. Press ENTER when done: ')
    input('\nRun this on the broker: python3 query.py setup && python3 query.py stat')


def generate_realtime_data(avro_random_gen_dir, num_iterations):
    # https://github.com/confluentinc/avro-random-generator
    # python3 operations.py generate_realtime_data /Users/a67/Project/insight/avro-random-generator 100000
    tmp_path = './realtime_{}.jsonlines'.format(num_iterations)
    output_path = 'data/realtime_{}.data'.format(num_iterations)
    os.system('{program} -c -f {schema_path} -i {num_iterations} -o {output_path}'.format(
        program=join(avro_random_gen_dir, 'arg'),
        schema_path='schemas/{}'.format(getenv('FILE_SCHEMA_REALTIME')),
        num_iterations=num_iterations,
        output_path=tmp_path,
    ))
    with open(tmp_path, 'r') as f, open(output_path, 'w') as g:
        for line in f:
            if not line:
                continue
            key = json.loads(line.strip())['user_id']
            g.write('{}:{}'.format(key, line))
    os.remove(tmp_path)


Fire()
