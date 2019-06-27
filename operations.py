# Assumption: pegasus
import os
import re
import subprocess
from os.path import join, abspath, relpath
from fire import Fire
from time import sleep

from util.logger import logger

LOCAL_ROOT_DIR = '/Users/a67/Project/insight/heart_watch'
REMOTE_ROOT_DIR = '/home/ubuntu/heart_watch'
CLUSTERS = {
    'brokers': {
        'num_nodes': 2,
        'env_overrides': [
            'BROKER_LIST',
            'BROKER_HOST',
        ],
    },
    'extra': {
        'num_nodes': 1,
        'env_overrides': [
            'ZOOKEEPER_LEADER',
            'KSQL_LEADER',
        ],
    },
    # 'ksqls': {
    #     'num_nodes': 2,
    #     'env_overrides': [
    #         '',
    #     ],
    # },
}


def _parallel(cmds, fix_instruction='', prompt=True):
    logger.info('Running commands: \n\n{}'.format('\n'.join(cmds)))
    processes = []
    for _args in cmds:
        process = subprocess.Popen(_args, shell=True)
        processes.append(process)
    for process in processes:
        process.wait()
    if prompt:
        answer = input('\nIs the output signaling success? [y/n]: ')
        while True:
            if answer == 'n':
                logger.info('Here\'s the commands list you are running parallelly: \n\n{}'.format('\n'.join(cmds)))
                logger.info(fix_instruction)
                input('\nAfter you manully fixed them, press ENTER when done: ')
                break
            elif answer != 'y':
                answer = input('\n please enter y or n: ')
            else:
                break


def stage1():
    input('\nOpen setup/stage1/{}.yml and adjust the settings. Press ENTER when done: '.format(', '.join([key for key in CLUSTERS])))
    _parallel(['peg up {}.yml'.format(key) for key in CLUSTERS])
    fetch()


def stage2():
    input('Copy env.template to .env and adjust .env. Press ENTER to when done: ')
    add_ssh_key()
    upload(LOCAL_ROOT_DIR)


def stage3():
    cmds = [
        'curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -',
        'sudo add-apt-repository \'deb [arch=amd64] https://download.docker.com/linux/ubuntu xenial stable\'',
        'sudo add-apt-repository --remove -y ppa:andrei-pozolotin/maven3',
        'sudo apt-get -y update',
        'apt-cache policy docker-ce',
        'sudo kill -9 $(ps aux | grep \'dpkg\' | awk \'{print $2}\')',
        'sudo dpkg --configure -a',
        'sudo apt-get install -y docker-ce python3-pip libpq-dev python-dev maven awscli',
        'sudo usermod -aG docker ubuntu',
        'cd ~/heart_watch && pip3 install -r requirements.txt',
        'sudo curl -L https://github.com/docker/compose/releases/download/1.24.1/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose',
        'sudo chmod +x /usr/local/bin/docker-compose',
    ]
    for cmd in cmds:
        _parallel(['peg sshcmd-cluster {} "{}"'.format(key, cmd) for key in CLUSTERS])


def stage4():
    _parallel(['peg sshcmd-cluster {} "{}"'.format(key, cmd) for key in CLUSTERS])


def fetch():
    _parallel(['peg fetch {}'.format(key) for key in CLUSTERS], prompt=False)


def add_ssh_key():
    cmds = []
    for key, val in CLUSTERS.items():
        for i in range(val['num_nodes']):
            cmds.append('peg ssh {} {}'.format(key, str(i + 1)))
    processes = []
    for _args in cmds:
        process = subprocess.Popen(_args, shell=True)
        processes.append(process)
    sleep(5)
    for process in processes:
        process.kill()


def upload(local_path=LOCAL_ROOT_DIR):
    remote_path = join(REMOTE_ROOT_DIR, relpath(local_path, LOCAL_ROOT_DIR))
    cmds = []
    for key, val in CLUSTERS.items():
        for i in range(val['num_nodes']):
            cmds.append('peg scp from-local {} {} {} {}'.format(key, str(i + 1), local_path, remote_path))
    input('Going to upload local {} to remote {}. Press ENTER to confirm: '.format(abspath(local_path), remote_path))
    _parallel(cmds)


def sync():
    command = """rsync -avL --exclude '.env.override/*' --progress -e "ssh -i ~/.ssh/mark-chen-IAM-key-pair.pem" --log-file="/Users/a67/rsync.log" /Users/a67/Project/insight/heart_watch/ ubuntu@{}:~/heart_watch/"""
    for dns in [
        'ec2-3-214-23-69.compute-1.amazonaws.com',
        'ec2-3-218-144-198.compute-1.amazonaws.com',
        'ec2-3-220-176-43.compute-1.amazonaws.com',
        # 'ec2-3-209-167-77.compute-1.amazonaws.com',
        # 'ec2-3-209-181-191.compute-1.amazonaws.com',
    ]:
        os.system(command.format(dns))


def env_override(cluster='all'):
    if cluster in ['brokers', 'all']:
        remote_path = join(REMOTE_ROOT_DIR, relpath('.env.override/brokers', LOCAL_ROOT_DIR))
        broker_list = re.findall('Public DNS: (.*)', os.popen('peg fetch brokers').read())
        for i in range(CLUSTERS['brokers']['num_nodes']):
            var_to_val = {
                'KAFKA_ADVERTISED_LISTENERS': 'PLAINTEXT://{}:9092'.format(broker_list[i]),
                'KAFKA_BROKER_ID': i + 1,
            }
            with open('.env.override/brokers', 'w') as f:
                [f.write('{}={}\n'.format(key, val)) for key, val in var_to_val.items()]
            os.system('peg scp from-local {} {} {} {}'.format('brokers', str(i + 1), '.env.override/brokers', remote_path))

    if cluster in ['ksqls', 'all']:
        pass
        # ksql_list = re.findall('Public DNS: (.*)', os.popen('peg fetch ksqls').read())

    # var_to_val = {
    #     'ZOOKEEPER_LEADER': broker_list[0],
    #     'KSQL_LEADER': ksql_list[0],
    #     'BROKER_LIST': ','.join([s + ':9092' for s in broker_list]),
    #     'ZOOKEEPER_LIST': ','.join([s + ':2181' for s in broker_list]),
    # }
    # with open('.env.override/extra', 'w') as f:
    #     [f.write('{}={}\n'.format(key, val)) for key, val in var_to_val.items()]


def clean_log():
    _parallel(['peg sshcmd-cluster {} "sudo sh -c \'truncate -s 0 /var/lib/docker/containers/*/*-json.log\'"'.format(key) for key in CLUSTERS], prompt=False)


Fire()
