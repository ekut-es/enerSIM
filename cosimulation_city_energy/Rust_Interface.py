import mosaik
from mosaik.util import connect_many_to_one
from Connect_Grid import rust_version_connect_prosumer_to_grid
from Connect_Grid import connect_many_pv_to_grid
from Connect_Grid import connect_many_prosumer_to_grid
from Connect_Grid import connect_many_consumer_to_grid
from sys import platform
from batterysim.battery import BatterySim
import batterysim.battery
from datetime import datetime, date
from aggregator import connect_PV_to_aggregator, connect_buildings_to_aggregator, connect_prosumer_to_aggregator
import mosaik
from sys import platform
from entity_connection.connect import *
import typing
from dataclasses import asdict
from entity_connection.connect import connect_entire_hhsim_to_entity


def rust_sim(world, step_size, START, END, DATABASE_PATH,
             num_of_consumers=5, num_of_PV=0, num_of_prosumers=5,
             use_monitor=False,
             results_filepath=None):
    # create_scenario(world)
    hhsim = world.start('HouseholdSim', step_size=step_size)
    rustAPI = world.start('Rust_Sim', eid_prefix='Model_', step_size=step_size)
    # Database
    db = world.start('DB', step_size=step_size, duration=END)

    # Instantiate models
    sim_data_entities = hhsim.householdsim(
        num_of_consumer=num_of_consumers, num_of_PV=num_of_PV,
        num_of_prosumer=num_of_prosumers, data_base_path=DATABASE_PATH,
        start_time=START).children
    model = rustAPI.MarktplatzModel(init_reading=3)
    #models = rustAPI.ExampleModel.create(1, init_reading=3)

    if use_monitor:
        collector = world.start('Collector', step_size=step_size)
        monitor = collector.Monitor()
        world.connect(model, monitor, 'trades', 'total')

    # change filename according to the number of entities in sim_data_entities.

    if not results_filepath:
        results_filepath = "Rust_Simulation_50_entities.hdf5"
    elif results_filepath[-4:] != "hdf5":
        results_filepath = f"{results_filepath}.hdf5"

    hdf5 = db.Database(filename=results_filepath)
    db.set_meta_data({
        'STEP_SIZE_IN_SECONDS': step_size,
        'START': START,
        'NUM_PVS': num_of_PV,
        'NUM_PROSUMERS': num_of_prosumers,
        'NUM_OF_CONSUMERS': num_of_consumers,
        'BATTERY_UNIFORM_CAPACITY': 0.,
        'BATTERY_TYPE': 'NONE', })

    #rust_version_connect_prosumer_to_grid(world, sim_data_entities, models)
    connect_many_pv_to_grid(world, sim_data_entities, model)
    connect_many_prosumer_to_grid(world, sim_data_entities, model)
    connect_many_consumer_to_grid(world, sim_data_entities, model)
    world.connect(model, hdf5, 'total')
    world.connect(model, hdf5, 'trades')
    connect_entire_hhsim_to_entity(world, sim_data_entities, hdf5)

    #connect_many_to_one(world, sim_data_entities, model, ('power_generation_mW', 'p_mw_pv'), ('power_consumption_mW', 'p_mw_load'))
