#!/usr/bin/env bash


dir=$(pwd)
mkdir visualization/data 
mkdir visualization/data/reference_predictions 
mkdir  visualization/data/reference_predictions/htw_berlin
echo "extracting htw berlin sarima predictions"
if [[ ! -e visualization/data/reference_predictions/htw_berlin/htw_berlin_sarima_predictions.tar.zip ]]
then
	wget https://atreus.informatik.uni-tuebingen.de/seafile/f/048393839aa24d878a19/?dl=1 --output-document=visualization/data/reference_predictions/htw_berlin/htw_berlin_sarima_predictions.tar.zip 
fi
cd visualization/data/reference_predictions/htw_berlin 
tar -xvaf htw_berlin_sarima_predictions.tar.zip 
cd $dir 
echo "Downloading minimal HTW Berlin Dataset"
pwd
cd additional_datasets/htw_berlin
if [[ ! -e htw_minimal.tar.zip ]]
then
	wget  https://atreus.informatik.uni-tuebingen.de/seafile/f/70414fb8dce64156b10e/?dl=1 --output-document=htw_minimal.tar.zip
fi
tar -xvaf htw_minimal.tar.zip
cd $dir
cd ../venv
source bin/activate

venvpy=$(pwd)/bin/python
echo $venvpy
cd $dir

echo "Creating Dataset, this may take a while"



$venvpy additional_datasets/htw_berlin/create_dataset_csv.py


echo "Creating reference predicitons dir.."
mkdir visualization
mkdir visualization/data
mkdir visualization/data/reference_predictions
mkdir visualization/data/reference_predictions/htw_berlin


echo "Creating B24 and Backshift predictions"
$venvpy additional_datasets/create_reference_predictions.py -s htw_berlin -p B24 -p perfect balance


cd additional_datasets/preprocessed_householdsim
if [[ ! -e household_data_1min_singleindex.csv ]]
then

	echo "Downloading filtered OpenPowerSystems Dataset"
	wget 'https://data.open-power-system-data.org/index.php?package=household_data&version=2020-04-15&action=customDownload&resource=1&filter%5B_contentfilter_utc_timestamp%5D%5Bfrom%5D=2014-12-11&filter%5B_contentfilter_utc_timestamp%5D%5Bto%5D=2019-05-01&filter%5BRegion%5D%5B%5D=DE_KN&filter%5BType%5D%5B%5D=residential_apartment_urban&filter%5BType%5D%5B%5D=residential_building_suburb&filter%5BType%5D%5B%5D=residential_building_urban&filter%5BHousehold%5D%5B%5D=residential1&filter%5BHousehold%5D%5B%5D=residential2&filter%5BHousehold%5D%5B%5D=residential3&filter%5BHousehold%5D%5B%5D=residential4&filter%5BHousehold%5D%5B%5D=residential5&filter%5BHousehold%5D%5B%5D=residential6&filter%5BFeed%5D%5B%5D=grid_export&filter%5BFeed%5D%5B%5D=grid_import&filter%5BFeed%5D%5B%5D=pv&downloadCSV=Download+CSV'  --output-document=household_data_1min_singleindex.csv 
fi

$venvpy additional_datasets/preprocessed_householdsim/ground_truth_creator.py

$venvpy additional_datasets/create_reference_predictions.py -s preprocessed_householdsim -p B24 -p perfect balance
