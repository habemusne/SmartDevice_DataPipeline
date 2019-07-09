import os
import re
import json
import subprocess
from os import getenv
from os.path import exists, dirname, abspath, join

from util.logger import logger
root_dir = dirname(dirname(abspath(__file__)))
CLUSTERS = getenv('CLUSTERS').split(',')
SERVER_STORE_FILE = join(root_dir, 'cluster_servers.store')
ENV_FILE = join(root_dir, '.env')


def parallel(cmds, fix_instruction='', prompt=True):
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


def get_cluster_servers(force=True):
    if not exists(SERVER_STORE_FILE) or force == True:
        data = {}
        for cluster in CLUSTERS:
            data[cluster] = re.findall('Public DNS: (.*)', os.popen('peg fetch {}'.format(cluster)).read())
        with open(SERVER_STORE_FILE, 'w') as f:
            json.dump(data, f)
    with open(SERVER_STORE_FILE, 'r') as f:
        return json.loads(f.read())


def sync():
    cluster_servers = get_cluster_servers(force=False)
    dns_list = []
    for lis in cluster_servers.values():
        dns_list.extend(lis)
    command = """rsync -avL --exclude '.env.override/*' --exclude 'ui/node_modules/*' --exclude 'ui/.env' --progress -e "ssh -i {pem_path}" --log-file="{rsync_log_path}" {project_dir}/ ubuntu@{host}:~/heart_watch/"""
    parallel([command.format(
        pem_path=getenv('PEM_PATH'),
        rsync_log_path=getenv('RSYNC_LOG_PATH'),
        project_dir=root_dir,
        host=dns,
    ) for dns in dns_list], prompt=False)


def update_dotenv(key_val_dict):
    with open(ENV_FILE, 'r') as f, open('.env.tmp', 'w') as g:
        for line in f:
            if line and line.split('=')[0] in key_val_dict:
                continue
            g.write(line)
        for key, val in key_val_dict.items():
            g.write('{}={}\n'.format(key, val))
    os.system('mv {} {}'.format('.env.tmp', ENV_FILE))
    sync()


class Store:
    count = 0
