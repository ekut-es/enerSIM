from mosaik.util import connect_many_to_one
import mosaik
from mosaik.scenario import Entity
from typing import Dict, List
import batterysim.battery as bb
###
# Function for connecting the different household sim entities to other mosaik entities.
# Generic versions of what can be found in Connect_Grid.py
###

KEY_HOUSEHOLD_POWER_GENERATION = 'power_generation_mW'
KEY_HOUSEHOLD_POWER_CONSUMPTION = 'power_consumption_mW'

def connect_entire_hhsim_to_entity(world, sim_data_entities, entity):
    connect_buildings_to_entity(world, sim_data_entities, entity)
    connect_PV_to_entity(world, sim_data_entities, entity)
    connect_prosumer_to_entity(world, sim_data_entities, entity)


def connect_buildings_to_entity(world, sim_data_entities, entity):
    houses = [e for e in sim_data_entities if e.type in (
        'Householdsim_Consumer')]

    connect_many_to_one(world, houses, entity, 'power_consumption_mW')

def connect_PV_to_entity(world, sim_data_entities, entity):
    data_pvs = [e for e in sim_data_entities if e.type in ('Householdsim_PV')]
    connect_many_to_one(world, data_pvs, entity, ('power_generation_mW_PV', 'power_generation_mW' ))

def connect_prosumer_to_entity(world, sim_data_entities, entity):
    data_prosumers = [e for e in sim_data_entities if e.type in (
        'Householdsim_Prosumer')]
    
    for data_prosumer in data_prosumers:
        world.connect(data_prosumer, entity, ('power_generation_mW', 'power_generation_mW'),
                      ('power_consumption_mW', 'power_consumption_mW'))


## Battery Connections
def connect_households_and_batteries(world: mosaik.World, households_with_battery: List[Entity], batteries: List[Entity]):
   for household, battery in zip(households_with_battery, batteries):
       world.connect(household, battery, KEY_HOUSEHOLD_POWER_CONSUMPTION,  KEY_HOUSEHOLD_POWER_GENERATION)