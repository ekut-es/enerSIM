from logging import info
import random
import mosaik
from mosaik.scenario import World
from cosimulation_city_energy.battery_simulations import marketplace_battery_sim, get_db_filename, marketplace_smart_battery_sim
import os

sim_config = {
    'DB': {
        'cmd': 'mosaik-hdf5 %(addr)s',
    },
    'HouseholdSim': {
       # 'python': 'householdsim.mosaik:HouseholdSim',
        'cmd': 'python ../householdsim/mosaik.py %(addr)s',
    },
    'Collector': {
        'cmd': 'python ../cosimulation_city_energy/collector.py %(addr)s',
    },
    'Rust_Sim': {
         'connect': '127.0.0.1:3456',
    },
   
    'PowerAggregator': {
        'cmd': 'python ../cosimulation_city_energy/aggregator.py %(addr)s',
    },
    'BatterySim': {
        'cmd': 'python ../batterysim/battery.py %(addr)s',
    },
}

def simulation_days(days) -> int: 
    return 60 * 60 * 24 * days

ALL_PROSUMERS_ACTIVE_START = pd.to_datetime('2016-03-01 00:00:00')
ALL_PROSUMERS_ACTIVE_END = pd.to_datetime('2017-03-01 00:00:00')

step_size = 15 * 60 # 15 Minutes


DATABASE_PATH = r"../cosimulation_city_energy/simulation_data/household_data_prepared_2020.sqlite"



START = '2016-07-01 00:00:00'  #  e.g.str(ALL_PROSUMERS_ACTIVE_START) or '2016-03-01 00:00:00'
END = simulation_days(30) # END = 7 * 24 * 3600 + 2 * 3600  # 7 days and 2 hours

BATTERY_CAPACITY_kWH = 20.
NUM_OF_CONSUMERS = 5
NUM_OF_PROSUMERS = 5
NUM_OF_PV = 0


SPRING = 'SPRING'
SUMMER = 'SUMMER'
AUTUMM = 'AUTUMM'
WINTER = 'WINTER'
SEASONS = {
    SPRING: '2016-05-15 00:00:00',
    SUMMER: '2016-07-15 00:00:00',
    AUTUMM: '2016-10-15 00:00:00',
    WINTER: '2017-01-15 00:00:00', # Not Winter 2016 because there is not enough Prosumer Data
}




