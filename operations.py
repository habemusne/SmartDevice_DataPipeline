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
CLUSTERS = set(['brokers', 'noncore', 'ksqls'])


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
    _parallel(['peg up setup/stage1/{}.yml'.format(key) for key in CLUSTERS])
    fetch()


def stage2():
    input('WARNING: this terminal session will be "destroyed" after running this command. Please open a fresh terminal session for this stage. If you ARE running this in the new session, press ENTER. Otherwise, CTRL + C: ')
    input('Copy env.template to .env and adjust .env. Press ENTER to when done: ')
    add_ssh_key()
    sync()
    env_override()


def stage3():
    answer = input('Are you running in large scale manner (more than 5 machines in total)? [y/n]')
    if answer == 'y':
        answer = input('Have you followed the "A caveat for running in large scale" section in README? (i.e. you are using my public AMI) [y/n]')
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
        ]
    for cmd in cmds:
        _parallel(['peg sshcmd-cluster {} "{}"'.format(key, cmd) for key in CLUSTERS])
    logger.info("""
        If you responded that you are NOT running in large scale manner, please manually ssh to each machine and run the following commands

        sudo curl -L https://github.com/docker/compose/releases/download/1.24.1/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose

        Then make sure than you can run docker-compose. If you can't please fix it.
    """)


def start_containers():
    _parallel([
        'peg sshcmd-cluster brokers "cd ~/heart_watch && docker-compose -f docker-compose/brokers.yml up -d"',
        'peg sshcmd-node brokers 1 "cd ~/heart_watch && docker-compose -f docker-compose/noncore.yml up -d schema-registry rest-proxy database control-center"',
        'peg sshcmd-cluster noncore "cd ~/heart_watch && docker-compose -f docker-compose/noncore.yml up -d connect"',
        'peg sshcmd-cluster ksqls "cd ~/heart_watch && docker-compose -f docker-compose/ksqls.yml up -d"',
    ])


def stop_containers():
    _parallel([
        'peg sshcmd-cluster brokers "cd ~/heart_watch && docker-compose -f docker-compose/brokers.yml stop && docker-compose -f docker-compose/brokers.yml rm -f"',
        'peg sshcmd-node brokers 1 "cd ~/heart_watch && docker-compose -f docker-compose/noncore.yml stop && docker-compose -f docker-compose/noncore.yml rm -f"',
        'peg sshcmd-cluster noncore "cd ~/heart_watch && docker-compose -f docker-compose/noncore.yml stop && docker-compose -f docker-compose/noncore.yml rm -f"',
        'peg sshcmd-cluster ksqls "cd ~/heart_watch && docker-compose -f docker-compose/ksqls.yml stop && docker-compose -f docker-compose/ksqls.yml rm -f"',
    ])


def fetch():
    _parallel(['peg fetch {}'.format(key) for key in CLUSTERS], prompt=False)


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
    for dns in [
        'ec2-18-213-147-6.compute-1.amazonaws.com', # broker 1
        'ec2-3-209-181-191.compute-1.amazonaws.com', # ksql 1
        'ec2-18-205-10-224.compute-1.amazonaws.com', # ksql 2
        'ec2-3-219-21-38.compute-1.amazonaws.com', # noncore 1
        # 'ec2-3-209-181-191.compute-1.amazonaws.com',
    ]:
        os.system(command.format(dns))


def force_sync():
    stop_containers()
    _parallel(['peg sshcmd-cluster {} "rm -rf ~/heart_watch"'.format(cluster) for cluster in CLUSTERS])
    sync()


def env_override(cluster='all'):
    def work(cluster, vars):
        remote_path = join(REMOTE_ROOT_DIR, relpath('.env.override/{}'.format(cluster), LOCAL_ROOT_DIR))
        server_list = re.findall('Public DNS: (.*)', os.popen('peg fetch {}'.format(cluster)).read())
        for i in range(len(server_list)):
            with open('.env.override/{}'.format(cluster), 'w') as f:
                if 'KAFKA_ADVERTISED_LISTENERS' in vars:
                    f.write('{}={}\n'.format('KAFKA_ADVERTISED_LISTENERS', 'PLAINTEXT://{}:9092'.format(server_list[i])))
                if 'KAFKA_BROKER_ID' in vars:
                    f.write('{}={}\n'.format('KAFKA_BROKER_ID', i + 1))
                if 'KSQL_HOST' in vars:
                    f.write('{}={}\n'.format('KSQL_HOST', server_list[i]))
            os.system('peg scp from-local {} {} {} {}'.format(cluster, str(i + 1), '.env.override/{}'.format(cluster), remote_path))

    if cluster in ['brokers', 'all']:
        work('brokers', set(['KAFKA_ADVERTISED_LISTENERS', 'KAFKA_BROKER_ID']))

    if cluster in ['ksqls', 'all']:
        work('ksqls', set(['KSQL_HOST']))


def clean_logs():
    _parallel(['peg sshcmd-cluster {} "sudo sh -c \'truncate -s 0 /var/lib/docker/containers/*/*-json.log\'"'.format(key) for key in CLUSTERS], prompt=False)


def full_run():
    input('Properly change RESOURCE_NAME_VERSION_ID. Press ENTER when done: ')
    sync()
    input('Run this on the broker: python3 setup/stage3/prepare.py c. Press ENTER when done: ')
    input('Run this on the broker: python3 query.py setup && python3 query.py stat')



Fire()
