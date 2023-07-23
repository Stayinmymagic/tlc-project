#!/bin/bash

echo "Downloading anaconda..."
wget "https://repo.anaconda.com/archive/Anaconda3-2021.11-Linux-x86_64.sh"

# -b option means batch mode, -p means to install the anaconda to assigned place
echo "Running anaconda script..."
bash Anaconda3-2021.11-Linux-x86_64.sh -b -p ~/anaconda

echo "Removing anaconda script..."
rm Anaconda3-2021.11-Linux-x86_64.sh 

#activate conda
eval "$($HOME/anaconda/bin/conda shell.bash hook)" 

echo "Running conda init..."
conda init

# using -y flag to auto-approve
echo "Running conda update..."
conda update -y conda

echo "Show installed conda version..."
conda --version

echo "Running sudo apt-get update..."
sudo apt-get update

echo "Installing Docker..."
sudo apt-get -y install docker.io

# Docker in Linux execute docker service by root user in default(need use sudo command)
# if we want to use Docker without sudo
echo "Docker without sudo setup"
sudo groupadd docker 
# gpasswd : 將用戶加入群組或從組中刪除
sudo gpasswd -a $USER docker
sudo service docker restart

echo "Installing docker-compose"
cd 
# mkdir -p 可以創建多層的目錄
mkdir -p bin
cd bin
wget https://github.com/docker/compose/releases/download/v2.3.3/docker-compose-linux-x86_64 -O docker-compose
# 為docker-compose 增加執行權限
sudo chmod +x docker-compose 

echo "Setup .bashrc"
# Use the echo command to append lines to the .bashrc script
echo '' >> ~/.bashrc
echo 'export PATH=${HOME}/bin:${PATH}' >> ~/.bashrc
# tail -n 顯示最後 N 行的訊息 (N 為數字)
# | is called pipe, Ex.It gives the stdout of the first command (cat ~/.bashrc) as the stdin to the second command (tail -n +10).
eval "$(cat ~/.bashrc | tail -n +1)"


mkdir -p ~/.google/credentials