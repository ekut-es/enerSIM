from entity_connection.connect import connect_entire_hhsim_to_entity
from batterysim.battery import SmartBattery
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Iterable, Tuple
from pathlib import Path, PurePath
import mosaik

from mosaik.scenario import World
from cosimulation_city_energy.battery_simulations import marketplace_battery_sim, get_db_filename, marketplace_smart_battery_sim
import subprocess
from time import sleep

from batterysim.power_forecast import SparePowerPredictor, GiveAllPredictor
from cosimulation_city_energy.Connect_Grid import rust_version_connect_prosumer_to_grid
from cosimulation_city_energy.Connect_Grid import connect_many_pv_to_grid
from cosimulation_city_energy.Connect_Grid import connect_many_prosumer_to_grid
from cosimulation_city_energy.Connect_Grid import connect_many_consumer_to_grid

sim_config = {
    'DB': {
        'python': 'mosaik_hdf5:MosaikHdf5',
    },
    'MockHouseholds': {
        # 'python': 'householdsim.mosaik:HouseholdSim',
        # 'python': 'test_market.mock_households:MockHouseholds',
        'cmd': 'python test_market/mock_households.py %(addr)s',
    },
    'Collector': {
        'cmd': 'python cosimulation_city_energy/collector.py %(addr)s',
    },
    'Rust_Sim': {
        # 'cmd': '../../mosaik-rust-api/target/debug/examples/marketplace_sim.exe -a %(addr)s',
        # 'cmd': '../../mosaik-rust-api/target/release/examples/marketplace_sim.exe -a %(addr)s',
        'connect': '127.0.0.1:3456',
    },

    'PowerAggregator': {
        'cmd': 'python cosimulation_city_energy/aggregator.py %(addr)s',
    },
    'BatterySim': {
        'cmd': 'python batterysim/battery.py %(addr)s',
    },
}

STEP_SIZE = 15 * 60

world = mosaik.World(sim_config, debug=True)
hhsim = world.start('MockHouseholds', step_size=STEP_SIZE)
rustAPI = world.start('Rust_Sim', eid_prefix='Model_', step_size=STEP_SIZE)
# Database

# Instantiate models

STEPS = 10
NUM_OF_PROSUMERS = 5
NUM_OF_CONSUMERS = 5

END = STEP_SIZE * STEPS


def create_static_profile(c, steps, num):
    return [
        [c for _ in range(steps)] for _ in range(num)
    ]


prosumer_consumption = create_static_profile(2e-5, STEPS, NUM_OF_PROSUMERS)
prosumer_generation = create_static_profile(1e-5, STEPS, NUM_OF_PROSUMERS)
consumer_profile = create_static_profile(2.e-5, STEPS, NUM_OF_CONSUMERS)

battery_capacity_watt = 10_000

sim_data_entities = hhsim.MockHouseholds(
    prosumer_profiles_consumption=prosumer_consumption, prosumer_profiles_generation=prosumer_generation,
    consumer_profiles=consumer_profile).children
model = rustAPI.Neighborhood(init_balance=2, battery_capacities=[0 for _ in range(NUM_OF_CONSUMERS)] + [battery_capacity_watt for _ in range(NUM_OF_PROSUMERS)],
                             initial_charges=[0 for _ in range(NUM_OF_CONSUMERS)] + [battery_capacity_watt for _ in range(NUM_OF_PROSUMERS)])
connect_many_pv_to_grid(world, sim_data_entities, model)
connect_many_prosumer_to_grid(world, sim_data_entities, model)
connect_many_consumer_to_grid(world, sim_data_entities, model)


collector = None
use_monitor = True
if use_monitor:
    collector = world.start('Collector', step_size=STEP_SIZE)
    monitor = collector.Monitor()
    connect_entire_hhsim_to_entity(world, sim_data_entities, monitor)
    world.connect(model, monitor, 'total')
    world.connect(model, monitor, 'trades')
    world.connect(model, monitor, 'energy_balance')
    world.connect(model, monitor, 'battery_charges')


world.run(until=END)
