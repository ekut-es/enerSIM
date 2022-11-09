#!/usr/bin/env bash

echo "Prerequisites: Python>=3.8 with pip,  cargo and git installed"
echo "Pip command should be callable from shell."
pip install virtualenv
DIR=$(pwd)
echo $DIR
cd ..
export PATH="/home/`whoami`/.local/bin:$PATH"

virtualenv  venv 
source venv/bin/activate

venv/bin/pip install -r $DIR/requirements.txt
cd $DIR
echo "`pwd`" > ../venv/lib/python3.8/site-packages/household.pth 
echo "`pwd`/batterysim" >> ../venv/lib/python3.8/site-packages/household.pth
echo "`pwd`/householdsim" >> ../venv/lib/python3.8/site-packages/household.pth

echo "`pwd`/householdsim/additionl_datasets" >> ../venv/lib/python3.8/site-packages/household.pth
cd ..
git clone https://gitlab+deploy-token-enerdag:Qhsn9UkL6zZxa4xPkWQf@atreus.informatik.uni-tuebingen.de/SecureEnergyProsumer/abschlussarbeiten/simulation-framework/mosaik-rust-api
