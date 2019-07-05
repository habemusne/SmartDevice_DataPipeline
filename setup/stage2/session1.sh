curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo add-apt-repository --remove -y ppa:andrei-pozolotin/maven3
sudo apt-get -y update
apt-cache policy docker-ce
sudo kill -9 $(ps aux | grep 'dpkg' | awk '{print $2}')
sudo dpkg --configure -a
sudo apt-get install -y docker-ce docker-compose python3-pip libpq-dev python-dev maven
sudo usermod -aG docker







 ${USER}
cd ~/ && git clone https://github.com/habemusne/heart_watch
