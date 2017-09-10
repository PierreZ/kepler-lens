#!/bin/bash

sudo mv kepler2warp10 /bin
sudo apt update
sudo apt upgrade
sudo apt install vim git python3-pip build-essential python3-tk manpages-dev gcc python3-dev -y
git clone https://github.com/PierreZ/kepler-lens.git kepler
cd kepler
sudo pip3 install -r requirements.txt
