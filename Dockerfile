#
# This Dockerfile provides an environment in which you can run simulations and also serves as documentation on how to setup
# the simulation repo on your own machine.
# This basically is the "Schnellstart"-Part of the README.
# We'll:
#   [ 0. Setup the OS, basic dependencies and Rust toolchain, clone the necessary repos ]
#   1. Set up the virtual environment
#   2. Download the datasets, preprocess them and pre calculate the predictions (SARIMA, Backshift and Perfect)
#   3. Start the Simulation, which includes compiling the mosaik-rust-api


# Base layer stuff, get an OS (Ubuntu 20.04 LTS, update and install requirements)
FROM ubuntu:20.04


RUN apt-get update &&  apt-get upgrade 
RUN apt-get install -y git apt-utils python3-pip psmisc vim curl python3 wget

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN ["/bin/bash", "-c", "source $HOME/.cargo/env"]


# Create the virtual environment
COPY ./requirements.txt requirements.txt
RUN ["/bin/bash", "-c", "pip install virtualenv"]
RUN ["/bin/bash", "-c", "virtualenv venv"]
RUN ["/bin/bash", "-c", "/venv/bin/pip install -r requirements.txt"]

# Add the simulation-Module and submodules to the PYTHONPATH of the venv
RUN ["/bin/bash", "-c", "echo /simulation > venv/lib/python3.8/site-packages/household.pth"]
RUN ["/bin/bash", "-c", "echo /simulation/batterysim >> venv/lib/python3.8/site-packages/household.pth"]
RUN ["/bin/bash", "-c", "echo /simulation/householdsim >> venv/lib/python3.8/site-packages/household.pth"]


# Clone the relevant SEP Repos: enerdag and mosaik-rust-api
RUN ["/bin/bash", "-c", "git clone https://gitlab+deploy-token-enerdag:Qhsn9UkL6zZxa4xPkWQf@atreus.informatik.uni-tuebingen.de/SecureEnergyProsumer/enerdag/"]
RUN ["/bin/bash", "-c", "git clone https://gitlab+deploy-token-enerdag:Qhsn9UkL6zZxa4xPkWQf@atreus.informatik.uni-tuebingen.de/SecureEnergyProsumer/abschlussarbeiten/simulation-framework/mosaik-rust-api"]

# Download the precalculated SARIMA predictions for the HTW-Berlin Dataset.
# Predictions can be calculated with additional_datasets/sarima_sim.py
RUN ["/bin/bash", "-c", "wget https://atreus.informatik.uni-tuebingen.de/seafile/f/048393839aa24d878a19/?dl=1 --output-document=htw_berlin_sarima_predictions.tar.zip --output-file=/dev/null"]
# Download Simulation data
RUN ["/bin/bash", "-c", "wget https://atreus.informatik.uni-tuebingen.de/seafile/f/70414fb8dce64156b10e/?dl=1 --output-document=htw_minimal.tar.zip --output-file=/dev/null"]
RUN ["/bin/bash", "-c", "wget 'https://data.open-power-system-data.org/index.php?package=household_data&version=2020-04-15&action=customDownload&resource=1&filter%5B_contentfilter_utc_timestamp%5D%5Bfrom%5D=2014-12-11&filter%5B_contentfilter_utc_timestamp%5D%5Bto%5D=2019-05-01&filter%5BRegion%5D%5B%5D=DE_KN&filter%5BType%5D%5B%5D=residential_apartment_urban&filter%5BType%5D%5B%5D=residential_building_suburb&filter%5BType%5D%5B%5D=residential_building_urban&filter%5BHousehold%5D%5B%5D=residential1&filter%5BHousehold%5D%5B%5D=residential2&filter%5BHousehold%5D%5B%5D=residential3&filter%5BHousehold%5D%5B%5D=residential4&filter%5BHousehold%5D%5B%5D=residential5&filter%5BHousehold%5D%5B%5D=residential6&filter%5BFeed%5D%5B%5D=grid_export&filter%5BFeed%5D%5B%5D=grid_import&filter%5BFeed%5D%5B%5D=pv&downloadCSV=Download+CSV'  --output-document=household_data_1min_singleindex.csv --output-file=/dev/null"]

# Copy the simulation directory into docker, extract the downloaded data and calculate the referecnce predictions
# TODO: Make this a volume so we can work directly in this
COPY . /simulation 
WORKDIR /simulation
# Extract HTW Berlin Dataset
RUN mv /htw_minimal.tar.zip /simulation/additional_datasets/htw_berlin
WORKDIR /simulation/additional_datasets/htw_berlin
RUN ["/bin/bash", "-c", "tar -xvaf htw_minimal.tar.zip "]
WORKDIR  /simulation

# Extract reference predictions
RUN ["/bin/bash", "-c", "tar -xvaf /htw_berlin_sarima_predictions.tar.zip "]
RUN mkdir /simulation/visualization/data/ && mkdir /simulation/visualization/data/reference_predictions && mkdir /simulation/visualization/data/reference_predictions/htw_berlin
RUN mv htw_berlin_result /simulation/visualization/data/reference_predictions/htw_berlin

# Create SQL Table (Compatible with simulationdatatabseconnection) for htw berlin and then create reference predicitons
RUN ["/bin/bash", "-c", "/venv/bin/python additional_datasets/htw_berlin/create_dataset_csv.py" ]
RUN ["/bin/bash", "-c", "/venv/bin/python additional_datasets/create_reference_predictions.py -s htw_berlin -p B24 -p perfect balance" ]
# Move COSSMIC Dataset, preprocess and create SQL Table,  then create reference predictions
RUN mv /household_data_1min_singleindex.csv /simulation/additional_datasets/preprocessed_householdsim
RUN ["/bin/bash", "-c", "/venv/bin/python additional_datasets/preprocessed_householdsim/ground_truth_creator.py" ]
RUN mkdir /simulation/visualization/data/reference_predictions/preprocessed_householdsim
RUN ["/bin/bash", "-c", "/venv/bin/python additional_datasets/create_reference_predictions.py -s preprocessed_householdsim -p B24 -p perfect balance" ]

# Checkout mosaik-rust-api battery branch and compile the marketplace example
WORKDIR /mosaik-rust-api
RUN ["/bin/bash", "-c", "git config --global  user.email 'you@example.com' && git config --global  user.name 'Docker Image'"]
RUN ["/bin/bash", "-c", "git fetch origin battery"]
RUN ["/bin/bash", "-c", "git checkout battery"]
RUN ["/bin/bash", "-c", "/root/.cargo/bin/cargo build --release --example enerdag_sim"]
RUN mv target/release/examples/enerdag_sim /simulation/enerdag_sim

# Change WORKDIR back to simulation  repo
WORKDIR /simulation
# When starting the container, run simulation
CMD /venv/bin/python cosimulation_city_energy/rust_integrated_battery.py