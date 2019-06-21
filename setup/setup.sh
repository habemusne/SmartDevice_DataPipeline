curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo add-apt-repository --remove ppa:andrei-pozolotin/maven3
sudo apt-get -y update
apt-cache policy docker-ce
sudo kill -9 $(ps aux | grep 'dpkg' | awk '{print $2}')
sudo dpkg --configure -a
sudo apt-get install -y docker-ce docker-compose python3-pip libpq-dev python-dev
sudo usermod -aG docker ${USER}

git clone https://github.com/habemusne/heart_watch
cd heart_watch
cp env.template .env
read -p "Modify .env, press ENTER when ready: "
docker-compose up -d --build
pip3 install -r requirements.txt
read -p "Open <host>:9021 (remember to open SG), wait until \"Kafka Connect\" is loaded, press ENTER when ready: "
python3 setup/prepare.py all
