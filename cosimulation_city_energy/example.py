

import random
import mosaik
from pathlib import PurePath
from Panda_Interface import panda_sim, simple_sim
from battery_simulations import simple_battery_sim, battery_sim, marketplace_battery_sim, smart_battery_sim, marketplace_smart_battery_sim
from Rust_Interface import rust_sim
from enum import Enum
import pandas as pd
from pathlib import Path, PurePath
from additional_datasets.htw_berlin.create_dataset_csv import HOUSEHOLD_SIM_KWARGS
from cosimulation_city_energy.aggregator import connect_buildings_to_aggregator, connect_prosumer_to_aggregator, connect_PV_to_aggregator
import additional_datasets
sim_config = {
    'DB': {
        'python': 'mosaik_hdf5:MosaikHdf5'
        # 'cmd': 'mosaik-hdf5 %(addr)s',

    },
    'HouseholdSim': {
        # 'python': 'householdsim.mosaik:HouseholdSim',
        'cmd': 'python householdsim/mosaik.py %(addr)s',
    },
    'PandaPower': {
        'python': 'pandapowermosaik:PandapowerMosaik',
        # 'cmd': 'pandapower_mosaik %(addr)s',
    },
    'WebVis': {
        'cmd': ' mosaik-web -s 0.0.0.0:8000 %(addr)s',
    },
    'Rust_Sim': {
        # 'cmd': '../../mosaik-rust-api/target/debug/examples/marketplace_sim.exe -a %(addr)s',
        # 'cmd': '../../mosaik-rust-api/target/release/examples/marketplace_sim.exe -a %(addr)s',
        'connect': '127.0.0.1:3456',
    },
    'Collector': {
        'cmd': 'python cosimulation_city_energy/collector.py %(addr)s',
    },
    'PowerAggregator': {
        'cmd': 'python cosimulation_city_energy/aggregator.py %(addr)s',
    },

    'SimpleBattery': {
        'cmd': 'python batterysim/simplebattery.py %(addr)s',
    },
    'BatterySim': {
        'cmd': 'python batterysim/battery.py %(addr)s',
    },
}


def simple_household_sim():
    """
    Prerequisite: Having the table cosimulation_city_energy/simulation_data/household_data_prepared_2020.sqlite
    A simple simulation where 5 Prosumers, Consumers and PV-Plants are simulated for 10 minutes.
    The Data gets send from the household sim to an "PowerAggregator" that will simply print the aggregated values at the end.
    """
    # Create Mosaik World based on config from above.
    world = mosaik.World(sim_config)

    # Configuration
    step_size = 60  # 60 Seconds Step size of the simulation

    # Datetime when the simulation Starts. This is the format that should be used. %Y-%m-%d %H:%M:%s
    START = '2016-07-07 12:00:00'

    # Number of Participants
    num_consumers = 5
    num_of_PV = 5
    num_of_prosumers = 5

    hhsim = world.start('HouseholdSim', step_size=step_size)
    agsim = world.start('PowerAggregator', step_size=step_size)

    aggregator = agsim.PowerAggregator()
    sim_data_entities = hhsim.householdsim(num_of_consumer=num_consumers, num_of_PV=num_of_PV,
                                           num_of_prosumer=num_of_prosumers,
                                           start_time=START,
                                           # In the most simple case householdsim only needs the path to the SQL Database
                                           data_base_path=str(PurePath('cosimulation_city_energy/simulation_data/household_data_prepared_2020.sqlite'))).children

    connect_buildings_to_aggregator(world, sim_data_entities, aggregator)
    connect_PV_to_aggregator(world, sim_data_entities, aggregator)
    connect_prosumer_to_aggregator(world, sim_data_entities, aggregator)

    collector = world.start('Collector', step_size=step_size)
    monitor = collector.Monitor(file_output=False,)

    world.connect(aggregator, monitor, 'total_power_consumption_mW',
                  'total_power_generation_mW')

    world.run(step_size * 10)  # Run 10 Simulation steps


def household_sim_htw_berlin_dataset():
    """
    Prerequisite: Having the table additional_datasets/htw_berlin/htw_dataset.sql (Read the additional_datasets/htw_berlin Readme on how to obtain it)
    A simple simulation where 5 Prosumers, Consumers and PV-Plants are simulated for 10 minutes.
    The Data gets send from the household sim to an "PowerAggregator" that will simply print the aggregated values at the end.
    """
    # Create Mosaik World based on config from above.
    world = mosaik.World(sim_config)

    # Configuration
    step_size = 60  # 60 Seconds Step size of the simulation

    # Datetime when the simulation Starts.
    # Berlin Dataset is from 2010-01-01 until 2010-12-31
    START = '2010-07-07 12:00:00'

    # Number of Participants
    # Berlin Dataset only has Prosumers, but consumers work because we just take the consumption
    num_consumers = 5
    num_of_PV = 0   # PV Does not work
    num_of_prosumers = 5

    hhsim = world.start('HouseholdSim', step_size=step_size)
    agsim = world.start('PowerAggregator', step_size=step_size)

    aggregator = agsim.PowerAggregator()
    sim_data_entities = hhsim.householdsim(num_of_consumer=num_consumers, num_of_PV=num_of_PV,
                                           num_of_prosumer=num_of_prosumers,
                                           start_time=START,
                                           # The dataset provides a dictionary to configure the household sim correctly.
                                           # This dict will be passed as keyword arguments (kwargs) to the function via the ** operator
                                           **additional_datasets.htw_berlin.create_dataset_csv.HOUSEHOLD_SIM_KWARGS
                                           ).children

    # Connect the different entities to the aggregator
    connect_buildings_to_aggregator(world, sim_data_entities, aggregator)
    connect_PV_to_aggregator(world, sim_data_entities, aggregator)
    connect_prosumer_to_aggregator(world, sim_data_entities, aggregator)

    collector = world.start('Collector', step_size=step_size)
    monitor = collector.Monitor(file_output=False,)

    world.connect(aggregator, monitor, 'total_power_consumption_mW',
                  'total_power_generation_mW')

    world.run(step_size * 10)  # Run 10 Simulation steps


if __name__ == '__main__':
    simple_household_sim()
    household_sim_htw_berlin_dataset()
