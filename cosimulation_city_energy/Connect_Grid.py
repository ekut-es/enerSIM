import mosaik
from mosaik.util import connect_many_to_one
def connect_buildings_to_grid(world, sim_data_entities, grid):
    houses = [e for e in sim_data_entities if e.type in (
        'Householdsim_Consumer')]
    consumers = [e for e in grid if e.type in ('Consumer')]

    for i in range(0, len(consumers)):
        world.connect(houses[i], consumers[i],
                      ('power_consumption_mW', 'p_mw'))

def connect_PV_to_grid(world, sim_data_entities, grid):
    data_pvs = [e for e in sim_data_entities if e.type in ('Householdsim_PV')]
    pvs = [e for e in grid if e.type in ('PV')]
    index = 0
    for pv in pvs:
        world.connect(data_pvs[index], pv, ('power_generation_mW_PV', 'p_mw'))
        index += 1


def connect_prosumer_to_grid(world, sim_data_entities, grid):
    data_prosumers = [e for e in sim_data_entities if e.type in (
        'Householdsim_Prosumer')]
    prosumers = [e for e in grid if e.type in ('Prosumer')]
    index = 0
    for prosumer in prosumers:
        world.connect(data_prosumers[index], prosumer, ('power_generation_mW', 'p_mw_pv'),
                      ('power_consumption_mW', 'p_mw_load'))
        index += 1

def rust_version_connect_prosumer_to_grid(world, sim_data_entities, grid):
    data_prosumers = [e for e in sim_data_entities if e.type in (
        'Householdsim_Prosumer')]
    #prosumers = [e for e in grid if e.type in ('Prosumer')]
    index = 0
    for prosumer in grid:
        world.connect(data_prosumers[index], prosumer, ('power_generation_mW', 'p_mw_pv'),
                      ('power_consumption_mW', 'p_mw_load'))
        index += 1

def connect_many_pv_to_grid(world, sim_data_entities, grid):
    data_pvs = [e for e in sim_data_entities if e.type in ('Householdsim_PV')]
    connect_many_to_one(world, data_pvs, grid, ('power_generation_mW_PV', 'p_mw_pv'))

def connect_many_consumer_to_grid(world, sim_data_entities, grid):
    data_consumers = [e for e in sim_data_entities if e.type in ('Householdsim_Consumer')]
    connect_many_to_one(world, data_consumers, grid, ('power_consumption_mW', 'p_mw_load'))


def connect_many_prosumer_to_grid(world, sim_data_entities, grid):
    data_prosumers = [e for e in sim_data_entities if e.type in (
        'Householdsim_Prosumer')]
    connect_many_to_one(world, data_prosumers, grid, ('power_generation_mW', 'p_mw_pv'), ('power_consumption_mW', 'p_mw_load'))

