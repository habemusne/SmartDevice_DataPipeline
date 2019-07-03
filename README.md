# Project description

There are two parts on a thorough workflow demo: the first is setting the project up, and the second is to make a query for the problem that the [previous project](https://github.com/anshu3769/SmartDevice_DataPipeline) addresses.

This query is defined as follows: patients are users whose heart rates are out of their historical range but did not move in the previous X seconds (`WINDOW_TUMBLING_SECONDS` in .env file). Continuously find them out.

## Step 1: setup

Firstly, install [pegasus](https://github.com/InsightDataScience/pegasus). Then

```
pip3 install requirements.txt
python3 operations.py stage1
python3 operations.py stage2
python3 operations.py stage3
```

## Step 2: run it

`python3 operations.py start_containers`

Optional: then ssh into your "controller machine" -- i.e. the EC2 that can access all the other EC2 machines, and preferrably, in the same network (VPC) as they are. This is optional, becauce you can still run the next command on your local computer (as long as it can access all your machines), it's just that inserting seed data to postgres will be very slow.

`python3 operations.py setup/stage3/prepare.py`

## Step 3: start the analytics stream

`python3 query.py setup`

## step 4: see the result

The UI is currently not available. You can go to the ksql server and run these commands to see the analytics result
```
docker-compose -f docker-compose/ksqls.yml exec ksql-cli ksql http://<KSQL_HOST>:8088
set 'auto.offset.reset'='earliest';  # Optional
select user_id, avg_heart_rate, processed_at from "FINAL_<RESOURCE_NAME_VERSION_ID>" window tumbling (size 10 seconds);
```


# Other Notes

## A caveat for running in large scale

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
