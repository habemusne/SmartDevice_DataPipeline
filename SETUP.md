# Introduction

This is the setup instruction for this project. This project is complicated, I am not able to guarentee that the procedure works 100% perfect.

### Phase 1: setup the environment

First thing to know is that this project heavily relies on [pegasus](https://github.com/InsightDataScience/pegasus), an aws-cli wrapper. Install it.

Then `pip3 install requirements.txt`. It is recommended that you use [virtualenv](https://virtualenv.pypa.io/en/latest/) rather than using your system's python.

Then modify yaml files under `pegasus/`. If you want to try out everything on a single machine, only modify `all_in_one.yml`, then `peg up pegasus/all_in_one.yml`. Otherwise, modify other yaml files, then modify `CLUSTERS` in `.env` to include those services/clusters you want to deploy, then `python3 operations.py stage1`. One note here is that, if you are running in a large-scale mode (in which you feel too cumbersome ssh-ing into each machine and run bash scripts), please follow the #a_caveat_for_running_in_large_scale instructions beforehand.

In case that your `stage1` or `peg up` fails, it's very likely a Pegasus problem. You probably need to manually spin up the machines by yourself on the AWS console. If this is the case, use `ubuntu`, `x86`, and at least `m4.large`. After you created the EC2 machines, tag each one with 3 key-val pairs: `Role: master`, `Name: brokers` (or `ksqls/producers/website` depending on the role), and `Owner: <your computer user name>`. Then assign Elastic IPs to them. Finally, run `python3 operations.py get_cluster_servers` to generate a local store for the EC2 information. This store file is very important through the project.

Then open another terminal session and run `python3 operations.py stage2`.

Return to your main session, run `python3 operations.py stage3`.

If you want to use S3 Sink Connector to send the final data out to S3, please go to `docker_volumes/connect`, then `cp aws_credentials.template aws_credentials`, then put your AWS credentials in `aws_credentials`.

Up to this step you've already setup the environment! Make sure that you really carefully modified `.env`. It is recommended to start with everything with small numbers and not-so-ambitious goals.


### Phase 2: start services

In this phase, we will start the services.

Firstly log in to each broker machine, run `cd ~/heart_watch && docker-compose up -d brokers`. Use `docker-compose logs -f` to ensure 2 things: 1. zookeeper does not spam messages 2. brokers mostly output `TRACE` logs and do not output things like "not able to connect" "unable to find xxx". This might not be a problem at the beginning, but when your services become many and you frequently start/stop machines, it might occur as a problem.

Open `cluster_servers.store`, find the first broker. It is the "master broker" that hosts other services. On this broker, run `cd ~/heart_watch && docker-compose up -d schema-registry connect database`. Make sure that `docker-compose logs -f schema-registry` says "listening xxx" at the end.

Then ssh into each KSQL machine, run `cd ~/heart_watch && docker-compose up -d ksql-cli`.

Finally, at the master broker run `docker-compose up -d control-center`. Open http://<master broker ip>:9021 and wait until 3 things happen: the main page shows number of brokers, the "connect" tab is working, and the "KSQL" tab is also working. Once all these are done, you are ready to run the project.


### Phase 3: run the project

On the master broker, run `python3 prepare.py db`. This command needs to run only if you destroyed (`docker-compose rm -f`) and restart the database service.

Then run `python3 prepare.py c` to create the historical connector+topic and, the DataGenConnector or the real-time topic+schema. The behavior depends on the `MODE` variable.

Then run `python3 prepare.py t` to create the historical table from the historical topic.

Then run `python3 query.py setup` to create the two real-time streams and also the two real-time streams (introduced in README.md).

If you are running `dev` mode, you should be seeing some data flowing. If not, then I am missing something in this doc...

If you are running `prod` mode, you need to start the producer processes. On your local computer, run `python3 produce_all.py run`. This command will ssh into the producer cluster, and for each machine, run `produce_one.py` which starts a system process that reads the real-time data file forever and send the messages to the broker cluster. As it starts a system process rather than do it itself, it will terminate asynchroneously, so your local computer's session doesn't need to hang there. To stop the run, please continnue reading.

# A non-comprehensive list of operations

### I want to sync the files

`python3 operations.py sync`

### I changed the stream creation code and want to update the streams

`python3 operations.py sync`

Then on the master broker, `python3 query.py teardown && python3 query.py setup`

### I want to recreate the connetors/topics

Increment the `RESOURCE_NAME_VERSION_ID` variable, run `python3 operations.py sync`, then start from Phase 3 (skip `python3 prepare.py dbs`)

### I am running production and want to terminate the producers

Set the `PRODUCER_RUN` to `0`, then run `python3 operations.py sync`


# Other Notes

### A caveat for running in large scale

- When running large scale, it is necessary to install docker on all the machines. If you opt for pegasus for large scale deployment (instead of terraform or cloudfront), you may like to use `peg sshcmd-cluster` for convience. But with this command to run my bash scripts that installs docker-compose, you will see "cannot execute binary file: Exec format error" upon running `docker-compose`. Specifically, `peg sshcmd-cluster` fails to install a valid `docker-compose` with these 2 commands (they are directly copy pasted from docker's official website):

```
sudo curl -L https://github.com/docker/compose/releases/download/1.24.1/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

Unfortunately, I was not able to figure out why. I was guessing that it's because of permission and ACL, but pegasus uses "ubuntu" as the user to login and run `sshcmd-cluster` (see `run_cmd_on_cluster` function in `util.sh`).

A solution is to ssh into every machine, then run the above commands. But this is not scalable at all.

As another solution, I made a public AMI that comes with docker and docker-compose. This AMI is built on top of the AMI used by pegasus (in us-east-1). However, pegasus does not support customized AMI, and I feel like it's too much work to refactor it for that. The solution here is to actually modify pegasus's source code to use my AMI. It is located at `select_ami` method in `util.sh` under your pegasus home directory. Just replace the hard-coded AMI string with `ami-0d9ff4b3c55d0b601`, and you will be good to go.

If you want to know what I did on top of the original pegasus image, here are the commands:

```
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository 'deb [arch=amd64] https://download.docker.com/linux/ubuntu xenial stable'
sudo add-apt-repository --remove -y ppa:andrei-pozolotin/maven3
sudo apt-get -y update
apt-cache policy docker-ce
sudo kill -9 $(ps aux | grep 'dpkg' | awk '{print $2}')
sudo kill -9 $(ps aux | grep 'apt' | awk '{print $2}')
sudo killall -r dpkg
sudo killall -r apt
sudo dpkg --configure -a
sudo apt-get install -y docker-ce python3-pip libpq-dev python-dev maven awscli
sudo usermod -aG docker ubuntu
sudo curl -L https://github.com/docker/compose/releases/download/1.24.1/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose # check if it works
```

### UI Setup

Due to time constraint, setting up the UI is not in the docker deployment pipeline. This means that it needs a few more commands to spin the UI up.

System Requirements

My mac: node v12.4.0, npm 6.9.0

My EC2 ubuntu: node 12.4.0, npm 6.9.0 (installation procedure is provided below)


```
# If operating on EC2, install node and npm
sudo apt install -y npm
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
. ~/.nvm/nvm.sh
nvm install 12.4.0

# install dependencies
cd ~/heart_watch/ui && npm i

# Attention: if you see "found xx vunerabilities", let them be.

# install the start up script to your system. Version on my Mac is react-scripts@3.0.1, EC2 react-scripts@3.0.1
npm i react-scripts -g # on Mac
sudo npm i react-scripts -g # on EC2

# Run the server in a background process
cd ~/heart_watch/ui && python3 server.py

```


### If You Are Insight fellow

If you are an Insight fellow and are not too familiar with DE, I don't recommend you to set up my project. If you do, you might end up hating me and this project, just as how I hated a previous fellow's project --- I wasted 2 weeks on setting it up but failed.
