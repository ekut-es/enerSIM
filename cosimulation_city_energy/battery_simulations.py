from mosaik.scenario import World
from batterysim.battery import BatterySim
import batterysim.battery
from datetime import datetime, date
from cosimulation_city_energy.aggregator import connect_PV_to_aggregator, connect_buildings_to_aggregator, connect_prosumer_to_aggregator
import mosaik
from cosimulation_city_energy.Connect_Grid import connect_PV_to_grid
from cosimulation_city_energy.Connect_Grid import connect_buildings_to_grid
from cosimulation_city_energy.Connect_Grid import connect_prosumer_to_grid
from cosimulation_city_energy.network_grid import get_grid
from cosimulation_city_energy.Connect_Grid import connect_many_pv_to_grid
from cosimulation_city_energy.Connect_Grid import connect_many_prosumer_to_grid
from cosimulation_city_energy.Connect_Grid import connect_many_consumer_to_grid
from sys import platform
from mosaik.util import connect_many_to_one
from sys import platform
from entity_connection.connect import *
import typing
from dataclasses import asdict
from typing import Iterable, Optional, Tuple, List

def smart_battery_sim(world, step_size: int, START: str, duration, battery_capacity: float=0, battery_init=0. ,
                 num_consumers=5, num_of_PV=1, num_of_prosumers=5, use_monitor=False, db_path=None, results_filename = None):
    
    hhsim = world.start('HouseholdSim', step_size=step_size)
    sim_data_entities = hhsim.householdsim(num_of_consumer=num_consumers, num_of_PV=num_of_PV, 
                                            num_of_prosumer=num_of_prosumers, data_base_path=db_path,
                                             start_time=START).children
    aggregator = create_aggregator_and_connect_to_households(world, sim_data_entities, step_size)
    battery = create_batteries_and_connect_to_prosumers(world, sim_data_entities, step_size, uniform_capacity=battery_capacity, battery_init=battery_init,
         smart_batteries=True, start=START)
    batteries = battery.children
    
    consumers = [consumer for consumer in sim_data_entities if consumer.type == 'Householdsim_Consumer']
    connect_many_to_one(world, consumers, battery, ('power_consumption_mW'))

    if not results_filename: 
        results_filename = get_sim_db_full_path('smart_battery', START, num_of_prosumers, num_of_PV, num_consumers)

    if not results_filename: 
        results_filename = get_sim_db_full_path('smart_battery', START, num_of_prosumers, num_of_PV, num_consumers)

    
    hdf5 = world.start('DB', step_size=step_size, duration=duration)
    db = hdf5.Database(filename=f"{results_filename}.hdf5")

    if use_monitor:
        collector = world.start('Collector', step_size=step_size)
        monitor = collector.Monitor(file_output=True, 
                name_pattern=results_filename)        
        connect_battery_attrs_to_entity(world, batteries, monitor, model='SmartBattery')
    hdf5.set_meta_data({    
        'STEP_SIZE_IN_SECONDS': step_size, 
        'START': START,
        'NUM_PVS': num_of_PV,
        'NUM_PROSUMERS': num_of_prosumers,
        'NUM_OF_CONSUMERS': num_consumers,
        'BATTERY_UNIFORM_CAPACITY': battery_capacity,
        'BATTERY_TYPE': 'SMART', })
    
    connect_battery_attrs_to_entity(world, batteries, db, model='SmartBattery')
    connect_entire_hhsim_to_entity(world, sim_data_entities, db)
    



def marketplace_smart_battery_sim(world, step_size: int, START: str, duration, battery_capacity: float=0, battery_init=0. ,
                 num_consumers=5, num_of_PV=0, num_of_prosumers=5, use_monitor=False, db_path=None, results_filename = None):
    
    hhsim = world.start('HouseholdSim', step_size=step_size)
    sim_data_entities = hhsim.householdsim(num_of_consumer=num_consumers, num_of_PV=num_of_PV, 
                                            num_of_prosumer=num_of_prosumers, data_base_path=db_path,
                                             start_time=START).children
    battery = create_batteries_and_connect_to_prosumers(world, sim_data_entities, step_size, uniform_capacity=battery_capacity, battery_init=battery_init,
         smart_batteries=True, start=START)
    batteries = battery.children
    
    rustAPI = world.start('Rust_Sim', eid_prefix='Model_', step_size=step_size)
    model = rustAPI.MarktplatzModel(init_reading=3)

    consumers = [consumer for consumer in sim_data_entities if consumer.type == 'Householdsim_Consumer']
    connect_many_to_one(world, consumers, battery, ('power_consumption_mW'))

    if not results_filename: 
        results_filename = get_sim_db_full_path('smart_battery', START, num_of_prosumers, num_of_PV, num_consumers)

    
    hdf5 = world.start('DB', step_size=step_size, duration=duration)
    db = hdf5.Database(filename=f"{results_filename}.hdf5")

    if use_monitor:
        collector = world.start('Collector', step_size=step_size)
        monitor = collector.Monitor(file_output=True, 
                name_pattern=results_filename)        
        connect_battery_attrs_to_entity(world, batteries, monitor, model='SmartBattery')
    hdf5.set_meta_data({    
        'STEP_SIZE_IN_SECONDS': step_size, 
        'START': START,
        'NUM_PVS': num_of_PV,
        'NUM_PROSUMERS': num_of_prosumers,
        'NUM_OF_CONSUMERS': num_consumers,
        'BATTERY_UNIFORM_CAPACITY': battery_capacity,
        'BATTERY_TYPE': 'SMART', })
    
    connect_batteries_and_households_to_marketplace(world, batteries, sim_data_entities, model)
    connect_battery_attrs_to_entity(world, batteries, db, model='SmartBattery')
    connect_entire_hhsim_to_entity(world, sim_data_entities, db)
    
    # Connecting "p_mw_pv", "p_mw_load", "reading" leads to Error TypeError: Object dtype dtype('O') has no native HDF5 equivalent
    world.connect(model, db, "trades")
    world.connect(model, db, "total")


def marketplace_battery_sim(world, step_size: int, START: str, duration, battery_capacity: float=float('infinity'), battery_init=0. ,
                 num_consumers=5, num_of_PV=1, num_of_prosumers=5, use_monitor=False, db_path=None, results_filename = None):
    
    hhsim = world.start('HouseholdSim', step_size=step_size)
    sim_data_entities = hhsim.householdsim(num_of_consumer=num_consumers, num_of_PV=num_of_PV, 
                                            num_of_prosumer=num_of_prosumers, data_base_path=db_path,
                                             start_time=START).children

    rustAPI = world.start('Rust_Sim', eid_prefix='Model_', step_size=step_size)
    model = rustAPI.MarktplatzModel(init_reading=3)

   
    aggregator = create_aggregator_and_connect_to_households(world, sim_data_entities, step_size)

    batteries = create_batteries_and_connect_to_prosumers(world, sim_data_entities, step_size, battery_init=battery_init,
            uniform_capacity=battery_capacity, smart_batteries=False).children
    
    results_filename = get_sim_db_full_path("marketplace_battery", START, num_of_prosumers, num_consumers, num_of_PV) if results_filename is None else results_filename
    hdf5 = world.start('DB', step_size=step_size, duration=duration)
    db = hdf5.Database(filename=f"{results_filename}.hdf5")

    if use_monitor:
        collector = world.start('Collector', step_size=step_size)
        monitor = collector.Monitor(file_output=True, 
                name_pattern=results_filename)
        
        connect_battery_attrs_to_entity(world, batteries, monitor)
        world.connect(model, monitor, "p_mw_pv")
        world.connect(model, monitor, "p_mw_load")
        world.connect(model, monitor, "reading")
        world.connect(model, monitor, "trades")
        world.connect(model, monitor, "total")


    hdf5.set_meta_data({    
        'STEP_SIZE_IN_SECONDS': step_size, 
        'START': START,
        'NUM_PVS': num_of_PV,
        'NUM_PROSUMERS': num_of_prosumers,
        'NUM_OF_CONSUMERS': num_consumers})
    
    connect_batteries_and_households_to_marketplace(world, batteries, sim_data_entities, model)
    connect_battery_attrs_to_entity(world, batteries, db)
    connect_entire_hhsim_to_entity(world, sim_data_entities, db)
    
    # Connecting "p_mw_pv", "p_mw_load", "reading" leads to Error TypeError: Object dtype dtype('O') has no native HDF5 equivalent
    world.connect(model, db, "trades")
    world.connect(model, db, "total")



def battery_sim(world, step_size: int, START: str, duration, battery_capacity: float=10., battery_init=5. ,
                 num_consumers=5, num_of_PV=1, num_of_prosumers=5, use_monitor=False, db_path=None, db_filename: str=None,
                 results_filename=None):
    hhsim = world.start('HouseholdSim', step_size=step_size)
    agsim = world.start('PowerAggregator', step_size=step_size)
    
    sim_data_entities = hhsim.householdsim(num_of_consumer=num_consumers, num_of_PV=num_of_PV, 
                                            num_of_prosumer=num_of_prosumers, data_base_path=db_path,
                                             start_time=START).children
    
    batteries = create_batteries_and_connect_to_prosumers(world, sim_data_entities, step_size, battery_capacity, smart_batteries=False,
    battery_init=battery_init).children
  
    aggregator = agsim.PowerAggregator()
    connect_buildings_to_aggregator(world, sim_data_entities, aggregator)
    connect_PV_to_aggregator(world, sim_data_entities, aggregator)
    
    if not results_filename:
        results_filename = get_sim_db_full_path("household_battery", START, num_of_prosumers, num_of_PV, num_consumers) if db_filename is None else db_filename
    
    hdf5 = world.start('DB', step_size=step_size, duration=duration)
    db = hdf5.Database(filename=f"{results_filename}.hdf5")

    if use_monitor:
        collector = world.start('Collector', step_size=step_size)
        monitor = collector.Monitor(file_output=False, 
                name_pattern=results_filename)
        connect_battery_attrs_to_entity(world, batteries, monitor)
        
    hdf5.set_meta_data({    
        'STEP_SIZE_IN_SECONDS': step_size, 
        'START': START,
        'NUM_PVS': num_of_PV,
        'NUM_PROSUMERS': num_of_prosumers,
        'NUM_OF_CONSUMERS': num_consumers})
    
    connect_battery_attrs_to_entity(world, batteries, db)
    connect_entire_hhsim_to_entity(world, sim_data_entities, db)


def simple_battery_sim(world, step_size: int, START: str, duration, battery_capacity: float=float('infinity'), battery_init=0. ,
                results_filename = None,
                 num_consumers=5, num_of_PV=1, num_of_prosumers=5, use_monitor=False, db_path=None):
    """
    Simulates w
    """
    hhsim = world.start('HouseholdSim', step_size=step_size)
    batsim = world.start('SimpleBattery', step_size=step_size)
    agsim = world.start('PowerAggregator', step_size=step_size)
    
    
    simple_battery = batsim.SimpleBattery(capacity=battery_capacity,
             initial_charge=battery_init)
    sim_data_entities = hhsim.householdsim(num_of_consumer=num_consumers, num_of_PV=num_of_PV, 
                                            num_of_prosumer=num_of_prosumers, data_base_path=db_path,
                                             start_time=START).children
    aggregator = agsim.PowerAggregator()
    
    connect_entire_hhsim_to_entity(world, sim_data_entities, simple_battery)
    
    connect_buildings_to_aggregator(world, sim_data_entities, aggregator)
    connect_PV_to_aggregator(world, sim_data_entities, aggregator)
    connect_prosumer_to_aggregator(world, sim_data_entities, aggregator)
    
    if not results_filename:
        results_filename = get_sim_db_full_path("battery", START, num_of_prosumers, num_of_PV, num_consumers)
    
    hdf5 = world.start('DB', step_size=step_size, duration=duration)
    db = hdf5.Database(filename=f"{results_filename}.hdf5")

    if use_monitor:
        collector = world.start('Collector', step_size=step_size)
        monitor = collector.Monitor(file_output=False, 
                name_pattern=results_filename)
        
        world.connect(simple_battery, monitor, 'current_charge', 'energy_demand_fulfilled')
        world.connect(aggregator, monitor, 'total_power_consumption_mW', 'total_power_generation_mW')

    hdf5.set_meta_data({    
        'STEP_SIZE_IN_SECONDS': step_size, 
        'START': START,
        'NUM_PVS': num_of_PV,
        'NUM_PROSUMERS': num_of_prosumers,
        'NUM_OF_CONSUMERS': num_consumers})
   
    world.connect(simple_battery, db, 'current_charge', 'energy_demand_fulfilled')
    world.connect(aggregator, db, 'total_power_consumption_mW', 'total_power_generation_mW')
    connect_entire_hhsim_to_entity(world, sim_data_entities, db)


def connect_batteries_and_households_to_marketplace(world: World, batteries: Iterable[Entity], sim_data_entities: Iterable[Entity], marketplace: Entity):
    connect_many_pv_to_grid(world, sim_data_entities, marketplace)
    connect_many_consumer_to_grid(world, sim_data_entities, marketplace)
    # Prosumers are connected through battery
    connect_many_to_one(world, batteries, marketplace, ('export_to_public_grid_mW', 'p_mw_pv'),
                      ('import_from_public_grid_mW', 'p_mw_load'))

def get_sim_db_full_path(folder: str, START: str, num_of_prosumers: int,  num_of_PV: int, num_consumers: int):
    prefix = f"visualization/data/{folder}" if folder[0] != '/' else folder
    filename = get_db_filename(START, num_of_prosumers, num_of_PV, num_consumers)
    return f"{prefix}/{filename}"
    
def get_db_filename(START: str, num_of_prosumers: int,  num_of_PV: int, num_consumers: int):
    now = datetime.now()
    day_time = now.strftime("%Y-%m-%d_%H:%M:%S")
    return f"SimDate_{START.split(' ')[0]}_pcp_{num_of_prosumers}_{num_of_PV}_{num_consumers}_{day_time}"

def create_batteries_and_connect_to_prosumers(world: World, sim_data_entities: Iterable[Entity], 
                                                step_size: int, uniform_capacity:float, smart_batteries: bool, start: str=None, battery_init = 0., ) -> Entity:
    batsim = world.start('BatterySim', step_size=step_size)
    battery_configs = list()
    households_with_batteries = list()
    for hh in sim_data_entities:
        if 'prosumer' in hh.eid:
            battery_configs.append(asdict(BatterySim.SingleBatteryConfig(uniform_capacity, battery_init, smart_batteries)))
            households_with_batteries.append(hh)
    
    battery = batsim.BatterySim( battery_configs=battery_configs, start=start)
    batteries = battery.children
    connect_households_and_batteries(world, households_with_batteries, batteries)
    return battery

def connect_battery_attrs_to_entity(world: World, batteries: List[Entity], entity: Entity, model='Battery'):
    for attr in batterysim.battery.META['models'][model]['attrs']:
            connect_many_to_one(world,batteries, entity, attr)

def create_aggregator_and_connect_to_households(world: World, sim_data_entities: List[Entity], step_size) -> Entity:
    agsim = world.start('PowerAggregator', step_size=step_size)
    aggregator = agsim.PowerAggregator()
    connect_buildings_to_aggregator(world, sim_data_entities, aggregator)
    connect_PV_to_aggregator(world, sim_data_entities, aggregator)
    return aggregator