"""
Execute to create scenarios and start the simulations etc.
"""


import random
import mosaik

from Panda_Interface import panda_sim, simple_sim
from battery_simulations import simple_battery_sim, battery_sim, marketplace_battery_sim, smart_battery_sim, marketplace_smart_battery_sim
from Rust_Interface import rust_sim
from enum import Enum
import pandas as pd
from pathlib import Path
from additional_datasets.htw_berlin.create_dataset_csv import HOUSEHOLD_SIM_KWARGS
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


# Seasons 2016
# Dictionary with Dates from each different Season in 2016
SPRING = 'SPRING'
SUMMER = 'SUMMER'
AUTUMM = 'AUTUMM'
WINTER = 'WINTER'
EARLY_SPRING = "EARLY_SPRING"
SEASONS = {
    EARLY_SPRING: '2016-03-15 00:00:00',
    SPRING: '2016-05-15 00:00:00',
    SUMMER: '2016-07-15 00:00:00',
    AUTUMM: '2016-10-15 00:00:00',
    # Not Winter 2016 because there is not enough Prosumer Data
    WINTER: '2017-01-15 00:00:00',
}


def simulation_days(days) -> int:
    return 60 * 60 * 24 * days


# END = 7 * 24 * 3600 + 2 * 3600  # 7 days and 2 hours
ALL_PROSUMERS_ACTIVE_START = pd.to_datetime('2016-03-01 00:00:00')
ALL_PROSUMERS_ACTIVE_END = pd.to_datetime('2017-03-01 00:00:00')


DATABASE_PATH = r"cosimulation_city_energy/simulation_data/household_data_prepared_2020.sqlite"

# The network you want to run the simulation with -> options in readme!
NET = "LandNetzMitPV"
#NET = "DorfNetzMitPVundProsumer"
#NET = "MieterStromNetz"


class CustomSims(Enum):
    RUST_SIM = "Rust_Simulation"
    SIMPLE_SIM = "Simple_Simulation"
    SIMPLE_BATTERY = "Simple_Battery_Simulation"
    INDIVIDUAL_BATTERY = "Battery_Simulation"
    PANDA_SIM = "Panda_Simulation"
    MARKETPLACE_BATTERY = "Marketplace_Battery"
    SMART_BATTERY = "SMART_BATTERY"
    SMART_BATTERY_MARKETPLACE = "SMART_BATT_MARKET"


# The Simulation you want to run.
SIM = CustomSims.SIMPLE_SIM
step_size = 60
NUM_OF_PROSUMERS = 10
NUM_OF_PV = 0
NUM_OF_CONSUMERS = 10
BATTERY_CAPCITY = 10. * NUM_OF_PROSUMERS

SEASON = SUMMER
DAYS = 30
END = 10*60  # simulation_days(DAYS)
# e.g.str(ALL_PROSUMERS_ACTIVE_START) or '2016-03-01 00:00:00'
START = '2016-07-07 12:00:00'


folder = Path("visualization", 'data', 'scratchpad')
RESULTS_FILENAME = str(Path(folder, str(SIM) + "test"))

print(RESULTS_FILENAME)


def run_periodically_battery_sims(f=ALL_PROSUMERS_ACTIVE_START, to=ALL_PROSUMERS_ACTIVE_END - pd.Timedelta(weeks=1),
                                  period=24*3600*7, step_size=step_size):
    starting_dates = pd.date_range(start=f, freq=f"{period}S", end=to)
    for sd in starting_dates:

        start = str(sd)
        random.seed(23)
        print("Starting Simulation at Date: {:s}".format(start))
        world = mosaik.World(sim_config)
        create_scenario(world, scenario=CustomSims.SIMPLE_BATTERY,
                        start=start, until=period)
        world.run(until=period)
        world.shutdown()


def main(run_periodically):
    if run_periodically:
        run_periodically_battery_sims(f='2016-05-31 00:00:00')
    else:
        random.seed(23)
        print("Starting Simulation at Date: {:s}".format(START))
        world = mosaik.World(sim_config)
        create_scenario(world)
        world.run(until=END)


def simulate_seasons():
    for season in [EARLY_SPRING, SPRING, SUMMER, AUTUMM, WINTER]:
        for consumer_per_prosumer in [1, 2, 3]:
            for kWh_cap_per_prosumer in [10., 20.]:
                bat_cap = NUM_OF_PROSUMERS * kWh_cap_per_prosumer
                num_consumers = NUM_OF_PROSUMERS * consumer_per_prosumer
                start = SEASONS[season]
                until = simulation_days(30)
                result_filename = f"{folder}\\{str(SIM)}_kwh_{bat_cap}_{season}_days_{DAYS}_pcp_{NUM_OF_PROSUMERS}_{num_consumers}_{NUM_OF_PV}"
                print(result_filename)
                random.seed(23)
                world = mosaik.World(sim_config)
                simple_battery_sim(world, step_size,
                                   start, until,
                                   num_consumers=int(num_consumers),
                                   num_of_prosumers=NUM_OF_PROSUMERS,
                                   num_of_PV=NUM_OF_PV,
                                   db_path=DATABASE_PATH,
                                   use_monitor=False, battery_capacity=bat_cap,
                                   battery_init=100.,
                                   results_filename=result_filename)
                world.run(until=until)  # As fast as possible


def create_scenario(world, scenario=SIM, start=START, until=END):

    if scenario == CustomSims.RUST_SIM:
        rust_sim(world, step_size, start, until, DATABASE_PATH,
                 results_filepath=RESULTS_FILENAME)
    elif scenario == CustomSims.PANDA_SIM:
        panda_sim(world, step_size, start, until, NET, DATABASE_PATH)
    elif scenario == CustomSims.SIMPLE_SIM:
        simple_sim(world, step_size, start,
                   num_consumers=NUM_OF_CONSUMERS, num_of_PV=NUM_OF_PV,
                   num_of_prosumers=NUM_OF_PROSUMERS,
                   hhsim_kwargs={'data_base_path': DATABASE_PATH}
                   # hhsim_kwargs=HOUSEHOLD_SIM_KWARGS,
                   )
    elif scenario == CustomSims.SIMPLE_BATTERY:
        simple_battery_sim(world, step_size, start, until, db_path=DATABASE_PATH,
                           results_filename=RESULTS_FILENAME)
    elif scenario == CustomSims.INDIVIDUAL_BATTERY:
        battery_sim(world, step_size, start, until, db_path=DATABASE_PATH,
                    battery_capacity=10.,
                    battery_init=5.,
                    num_of_PV=0,
                    results_filename=RESULTS_FILENAME)
    elif scenario == CustomSims.MARKETPLACE_BATTERY:
        marketplace_battery_sim(world, step_size, start, until, db_path=DATABASE_PATH, use_monitor=False, battery_capacity=20.0, battery_init=10.,
                                results_filename=RESULTS_FILENAME)
    elif scenario == CustomSims.SMART_BATTERY:
        smart_battery_sim(world, step_size, start, until,
                          num_consumers=NUM_OF_CONSUMERS,
                          num_of_prosumers=NUM_OF_PROSUMERS,
                          num_of_PV=NUM_OF_PV,
                          db_path=DATABASE_PATH, use_monitor=False, battery_capacity=BATTERY_CAPCITY, battery_init=10.,
                          results_filename=RESULTS_FILENAME),
    elif scenario == CustomSims.SMART_BATTERY_MARKETPLACE:
        marketplace_smart_battery_sim(world, step_size, start, until, db_path=DATABASE_PATH, use_monitor=False,
                                      battery_capacity=20.0, battery_init=10.,
                                      results_filename=RESULTS_FILENAME)
    else:
        raise SyntaxError('No known simulation selected.')


if __name__ == '__main__':
    main(False)
