
from batterysim.battery import BatterySim
import batterysim.battery
from datetime import datetime, date
from aggregator import connect_PV_to_aggregator, connect_buildings_to_aggregator, connect_prosumer_to_aggregator
import mosaik
from Connect_Grid import connect_PV_to_grid
from Connect_Grid import connect_buildings_to_grid
from Connect_Grid import connect_prosumer_to_grid
from network_grid import get_grid
from mosaik.util import connect_many_to_one
from sys import platform
from entity_connection.connect import *
import typing
from dataclasses import asdict


def simple_sim(world, step_size, START, num_consumers=5, num_of_PV=1, num_of_prosumers=5,
               hhsim_kwargs={}):
    """
    Simulation which just reads data from the householdsim and accumulates consumed and generated power from
    every Household per time step.
    """
    hhsim = world.start('HouseholdSim', step_size=step_size)
    agsim = world.start('PowerAggregator', step_size=step_size)

    aggregator = agsim.PowerAggregator()
    sim_data_entities = hhsim.householdsim(num_of_consumer=num_consumers, num_of_PV=num_of_PV,
                                           num_of_prosumer=num_of_prosumers,
                                           start_time=START,  **hhsim_kwargs).children

    connect_buildings_to_aggregator(world, sim_data_entities, aggregator)
    connect_PV_to_aggregator(world, sim_data_entities, aggregator)
    connect_prosumer_to_aggregator(world, sim_data_entities, aggregator)

    collector = world.start('Collector', step_size=step_size)
    monitor = collector.Monitor(file_output=True,
                                name_pattern=f"SimTime_{START}_{num_of_prosumers}_prosumer_{num_of_PV}_pvs_{num_consumers}_consumers_{datetime.now()}")

    world.connect(aggregator, monitor, 'total_power_consumption_mW',
                  'total_power_generation_mW')


def panda_sim(world, step_size, START, END, NET, DATABASE_PATH):
    # Start simulators
    pandapower = world.start('PandaPower', step_size=step_size)
    hhsim = world.start('HouseholdSim', step_size=step_size)

    # Instantiate models
    grid = get_grid(NET, pandapower)

    # get entities
    consumers = [e for e in grid if e.type in ('Consumer')]
    buses = [e for e in grid if e.type in ('Bus')]
    transformer = [e for e in grid if e.type in ('Trafo')]
    pvs = [e for e in grid if e.type in ('PV')]
    prosumers = [e for e in grid if e.type in ('Prosumer')]
    lines = [e for e in grid if e.type in ('Line')]
    extgrid = [e for e in grid if e.type in ('ExtGrid')]

    # start and connect householdsim
    sim_data_entities = hhsim.householdsim(num_of_consumer=len(consumers), num_of_PV=len(
        pvs), num_of_prosumer=len(prosumers), data_base_path=DATABASE_PATH, start_time=START).children
    connect_buildings_to_grid(world, sim_data_entities, grid)
    connect_PV_to_grid(world, sim_data_entities, grid)
    connect_prosumer_to_grid(world, sim_data_entities, grid)

    # Database
    db = world.start('DB', step_size=step_size, duration=END)
    # Check which platform is being used
    if platform == "win32" or platform == "win64":
        # File Name for windwows, because windows has a limit
        hdf5 = db.Database(filename=NET + '_Simulation.hdf5')
    else:
        hdf5 = db.Database(filename=('simulation_data_{net}_start_{start}_Duration_{end} ' + 'days.hdf5').format(net=NET,
                                                                                                                 start=START,
                                                                                                                 end=END/(3600*24)))
    connect_many_to_one(world, consumers, hdf5, 'p_mw', 'q_mvar')
    connect_many_to_one(world, pvs, hdf5, 'p_mw', 'q_mvar')
    connect_many_to_one(world, prosumers, hdf5, 'p_mw_total', 'q_mvar_total',
                        'p_mw_load', 'q_mvar_load', 'p_mw_pv', 'q_mvar_pv',)
    connect_many_to_one(world, transformer, hdf5, 'p_hv_mw', 'q_hv_mvar', 'p_lv_mw',
                        'q_lv_mvar', 'pl_mw', 'ql_mvar', 'i_hv_ka', 'i_lv_ka', 'loading_percent')
    connect_many_to_one(world, buses, hdf5, 'vm_pu',
                        'va_degree', 'p_mw', 'q_mvar')
    connect_many_to_one(world, lines, hdf5, 'p_from_mw', 'q_from_mvar', 'p_to_mw', 'q_to_mvar', 'pl_mw', 'ql_mvar',
                        'i_from_ka', 'i_to_ka', 'i_ka', 'loading_percent')
    connect_many_to_one(world, extgrid, hdf5, 'p_mw', 'q_mvar')

    # Web visualization
    webvis = world.start('WebVis', start_date=START, step_size=60)
    webvis.set_config(ignore_types=['Topology', 'ResidentialLoads', 'Grid', 'House', 'VorStadtNetz', 'LandNetz', 'householdsim', 'HouseholdSim', 'Householdsim_Consumer', 'DorfNetz',
                                    'Database', 'Householdsim_PV', 'DemoNetz', 'Householdsim_Prosumer'])

    vis_topo = webvis.Topology()

    connect_many_to_one(world, transformer, vis_topo, 'loading_percent')
    webvis.set_etypes({
        'Trafo': {
            'cls': 'Trafo',
            'attr': 'loading_percent',
            'unit': 'workload',
            'default': 0,
            'min': 0,
            'max': 100,
        },
    })

    connect_many_to_one(world, buses, vis_topo, 'p_mw')
    webvis.set_etypes({
        'Bus': {
            'cls': 'Bus',
            'attr': 'p_mw',
            'unit': 'P [MW]',
            'default': 0,
            'min': 0,
            'max': 0.01,
        },

    })

    connect_many_to_one(world, consumers, vis_topo, 'p_mw')
    webvis.set_etypes({
        'Consumer': {
            'cls': 'Consumer',
            'attr': 'p_mw',
            'unit': 'P [MW]',
            'default': 0,
            'min': 0,
            'max': 0.01,
        },
    })
    connect_many_to_one(world, prosumers, vis_topo, 'p_mw_total')
    webvis.set_etypes({
        'Prosumer': {
            'cls': 'Prosumer',
            'attr': 'p_mw_total',
            'unit': 'P [MW]',
            'default': 0,
            'min': 0,
            'max': 0.01,
        },
    })
    connect_many_to_one(world, pvs, vis_topo, 'p_mw')
    webvis.set_etypes({
        'PV': {
            'cls': 'PV',
            'attr': 'p_mw',
            'unit': 'P [MW]',
            'default': 0,
            'min': -0.01,
            'max': 0,
        },
    })
    connect_many_to_one(world, extgrid, vis_topo, 'p_mw')
    webvis.set_etypes({
        'ExtGrid': {
            'cls': 'ExtGrid',
            'attr': 'p_mw',
            'unit': 'P [MW]',
            'default': 0,
            'min': 0,
            'max': 0.5,
        },
    })


def connect_grid_to_db(world, grid, hdf5):
    consumers = [e for e in grid if e.type in ('Consumer')]
    buses = [e for e in grid if e.type in ('Bus')]
    transformer = [e for e in grid if e.type in ('Trafo')]
    pvs = [e for e in grid if e.type in ('PV')]
    prosumers = [e for e in grid if e.type in ('Prosumer')]
    lines = [e for e in grid if e.type in ('Line')]
    extgrid = [e for e in grid if e.type in ('ExtGrid')]

    connect_many_to_one(world, consumers, hdf5, 'p_mw', 'q_mvar')
    connect_many_to_one(world, pvs, hdf5, 'p_mw', 'q_mvar')
    connect_many_to_one(world, prosumers, hdf5, 'p_mw_total', 'q_mvar_total',
                        'p_mw_load', 'q_mvar_load', 'p_mw_pv', 'q_mvar_pv',)
    connect_many_to_one(world, transformer, hdf5, 'p_hv_mw', 'q_hv_mvar', 'p_lv_mw',
                        'q_lv_mvar', 'pl_mw', 'ql_mvar', 'i_hv_ka', 'i_lv_ka', 'loading_percent')
    connect_many_to_one(world, buses, hdf5, 'vm_pu',
                        'va_degree', 'p_mw', 'q_mvar')
    connect_many_to_one(world, lines, hdf5, 'p_from_mw', 'q_from_mvar', 'p_to_mw', 'q_to_mvar', 'pl_mw', 'ql_mvar',
                        'i_from_ka', 'i_to_ka', 'i_ka', 'loading_percent')
    connect_many_to_one(world, extgrid, hdf5, 'p_mw', 'q_mvar')
