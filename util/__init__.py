import subprocess

from util.logger import logger


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
