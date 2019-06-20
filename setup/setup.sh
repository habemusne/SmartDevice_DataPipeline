# Dependencies: pegasus, virtualenv

peg up setup/broker_master.yml
read -p "Press any key to continue when ready: "
peg up setup/broker_worker.yml
read -p "Press any key to continue when ready: "
peg fetch brokers
peg up setup/consumer.yml

# BROKERS
peg sshcmd-cluster brokers "curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -"
peg sshcmd-cluster brokers 'sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"'
peg sshcmd-cluster brokers "sudo add-apt-repository --remove ppa:andrei-pozolotin/maven3"
peg sshcmd-cluster brokers "sudo apt-get update"
apt-cache policy docker-ce
sudo apt-get install -y docker-ce
sudo systemctl status docker
sudo usermod -aG docker ${USER}
sudo apt install -y docker-compose
sudo apt install -y maven

peg sshcmd-cluster brokers "git clone https://github.com/confluentinc/cp-docker-images"
peg sshcmd-cluster brokers "cd ~/cp-docker-images/examples/cp-all-in-one/ && docker-compose up -d --build"

# CONSUMERS
sudo add-apt-repository --remove ppa:andrei-pozolotin/maven3
sudo apt-get update
sudo apt install -y maven

sudo apt-get install -y libpq-dev python-dev

psycopg2,sqlalchemy,requests,fire,python-dotenv
