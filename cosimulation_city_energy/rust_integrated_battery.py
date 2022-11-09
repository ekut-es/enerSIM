"""
This module runs simulations with a enerDAG-Interface.
Batteries and the Market are calculated in Rust.
The `HouseholdDescription` can be serialized to a dict and be used to configure the enerdag simulation.
The connect_{prosumers, pv, consumers} methods are used to connect household sim and rust sim entities.


`run_simulations_with_household` and `rust_sim` are "low level" methods  to configure and run simulations. The other run_*
methods use them.

Configuration is a bit of a mess. at the end of a file are some global variables that need to be tinkered with.
They are explained in comments next to them.

"""

from mosaik.scenario import World, Entity
from cosimulation_city_energy.city_energy_simulation import SIM
from entity_connection.connect import connect_entire_hhsim_to_entity
from dataclasses import asdict, dataclass
from typing import List, Optional
from entity_connection.connect import *
from cosimulation_city_energy.aggregator import connect_PV_to_aggregator, connect_buildings_to_aggregator, connect_prosumer_to_aggregator
from datetime import datetime, date
import batterysim.battery
from batterysim.battery import BatterySim, SmartBattery
from sys import platform
from cosimulation_city_energy.Panda_Interface import connect_grid_to_db
from mosaik.util import connect_many_to_one
import subprocess
import additional_datasets.htw_berlin.create_dataset_csv as htw_berlin
import additional_datasets.preprocessed_householdsim.ground_truth_creator as pp_householdsim
import mosaik
from pathlib import Path, PurePath
import pandas as pd
import os

import getopt
import sys

import time

import logging

from evaluation_utils.pricing import PriceSpec
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)
sim_config = {
    'DB': {
        'python': 'mosaik_hdf5:MosaikHdf5',
    },
    'HouseholdSim': {
        'python': 'householdsim.mosaik:HouseholdSim',
        #   'cmd': 'python ../simulation/householdsim/mosaik.py %(addr)s',

    },
    'Collector': {
        'cmd': 'python ../collector.py %(addr)s',
    },
    'Rust_Sim': {
        'connect': '127.0.0.1:3456',
    },
    'PandaPower': {
        'python': 'cosimulation_city_energy.pandapowermosaik:PandapowerMosaik'
    }


}


def simulation_days(days) -> int:
    return 60 * 60 * 24 * days


def minutes(seconds: int) -> int:
    return seconds * 60


def open_rust_process():

    # os.system("fuser 3456/tcp -k")
    time.sleep(2)
    p = subprocess.Popen(['./enerdag_sim'],
                         )
    time.sleep(2)

    return p


@dataclass
class BatteryConfig:
    disposable_energy_calc: str
    physical_model: str

    def todict(self):
        return {
            "disposable_energy_calc": self.disposable_energy_calc,
            "physical_model": self.physical_model
        }


@dataclass
class HouseholdDescription:

    household_type: str
    battery_capacity: int
    initial_charge: int
    battery_type: str
    battery_config: BatteryConfig = BatteryConfig("UEMA", "IdealBattery")
    initial_energy_balance: float = 0.
    csv_filepath: Optional[Path] = None
    eid_prefix: Optional[str] = None

    def asdict(self):
        return {"household_type": self.household_type,
                "battery_capacity": int(self.battery_capacity),
                "initial_charge": int(self.initial_charge),
                "battery_type": self.battery_type,
                "battery_config": self.battery_config.todict(),
                "initial_energy_balance": float(self.initial_energy_balance),
                "csv_filepath": self.csv_filepath,
                "eid_prefix": self.eid_prefix,
                }


def create_rust_neighborhood(rust_api, specs: List[HouseholdDescription], start_time: str):

    return rust_api.Neighborhood(household_descriptions=[spec.asdict() for spec in specs], start_time=start_time)


def connect_households_to_grid(world, rust_entities, grid_entities):
    filter_and_connect(world, rust_entities, grid_entities, filter_for_prosumer,
                       ('published_p_mW_pv', 'p_mw_pv'),
                       ('published_p_mW_load', 'p_mw_load'))

    filter_and_connect(world, rust_entities, grid_entities, filter_for_consumer,
                       ('published_p_mW_load', 'p_mw'))


def connect_pvs(world, household_sim_entities, rust_entities, db):
    data_pvs = [e for e in household_sim_entities if e.type in (
        'Householdsim_PV')]
    rust_pvs = [e for e in rust_entities if e.type in ('PV', )]
    assert len(data_pvs) == len(rust_pvs)
    for dpv, rpv in zip(data_pvs, rust_pvs):
        world.connect(dpv, rpv, ('power_generation_mW_PV', 'p_mw_pv'))
        world.connect(rpv, db, ('p_mw_pv', 'p_mw_pv'))


def connect_prosumers(world, household_sim_entities, rust_entities, db):
    """
    Connects CSV DAta source to rust entitites based on if the eid of the data prosumer
    is in the eid of the rust entity
    """
    rust_prosumer = filter_for_type(rust_entities, ('Prosumer',))
    household_sim_prosumers = filter_for_type(
        household_sim_entities, ('Householdsim_Prosumer', ))
    connected_rust_prosumers = set()
    checked_pids = []
    assert len(household_sim_prosumers) == len(rust_prosumer)
    for data_prosumers in household_sim_prosumers:
        for rust_pros in rust_prosumer:
            pid_substr = __extract_prosumer_id(data_prosumers.eid)
            pid = "pid%s_" % pid_substr
            checked_pids.append(pid)
            if pid in rust_pros.eid and rust_pros not in connected_rust_prosumers:
                world.connect(data_prosumers, rust_pros, ('power_generation_mW',
                                                          'p_mw_pv'), ('power_consumption_mW', 'p_mw_load'))
                world.connect(rust_pros, db,
                              "p_mw_load",  "energy_balance", "published_energy_balance", "p_mw_pv",
                              "battery_charge", "trades", "disposable_energy", "p2p_traded",
                              "avg_p2p_price"
                              )
                connected_rust_prosumers = connected_rust_prosumers.union([
                    rust_pros])
    assert connected_rust_prosumers == set(rust_prosumer)


def __extract_prosumer_id(eid: str) -> str:
    import re
    m = re.match(r"householdsim_prosumer_(\d+)_prosumer(\d+)", eid)
    m2 = re.match(r"householdsim_prosumer_(\d+)_DE_KN_residential(\d+)", eid)
    if m:
        return m.group(2)
    elif m2:
        return m2.group(2)
    else:

        return str(-1)


def connect_consumers(world, household_sim_entities, rust_entities, db):
    data_consumers = [e for e in household_sim_entities if e.type in (
        'Householdsim_Consumer')]
    rust_consumers = filter_for_type(rust_entities, ('Consumer',))
    logger.debug("Rust Consumers: %d, data consumers: %d" %
                 (len(rust_consumers), len(data_consumers)))
    assert len(data_consumers) == len(rust_consumers)
    for dpv, rpv in zip(data_consumers, rust_consumers):
        world.connect(dpv, rpv,  ('power_consumption_mW', 'p_mw_load'),)
        world.connect(rpv, db, "p_mw_load", "energy_balance", "published_energy_balance", "trades",
                      "battery_charge",  "trades", "p2p_traded",
                      "avg_p2p_price"
                      )


def filter_and_connect(world, ent1, ent2, filter, *connections):
    rust_prosumers = filter(ent1)
    grid_prosumers = filter(ent2)

    for gp, rp in zip(rust_prosumers, grid_prosumers):
        world.connect(gp, rp, *connections)


def filter_for_type(eid_list, type_spec):
    return [e for e in eid_list if e.type in type_spec]


def filter_for_prosumer(eid_list):
    return filter_for_type(eid_list, ('Prosumer', ))


def filter_for_consumer(eid_list):
    return filter_for_type(eid_list, ('Consumer', ))


def filter_for_PV(eid_list):
    return filter_for_type(eid_list, ('PV', ))


step_size = minutes(5)


def run_simulations_no_battery():
    battery_capacity = 0
    households = [HouseholdDescription("Consumer", 0, 0,  "NoBattery", ) for _ in range(NUM_OF_CONSUMERS)] +\
        [HouseholdDescription("PV", 0, 0,  "NoBattery") for _ in range(NUM_OF_PV)] +\
        [HouseholdDescription("Prosumer", battery_capacity, 0,
                              "NoBattery", battery_config=BatteryConfig(
                                  "UEMA", "IdealBattery"),  # Predictor does not do anything here
                              eid_prefix=__eid_prefix_from_index(idx)) for idx in range(NUM_OF_PROSUMERS)]
    run_simulations_with_household(
        households, battery_capacity, battery_type="NoBattery", csv_spec=None, predictor=None)


def run_simulations_simple_battery(battery_capacity):
    households = [HouseholdDescription("Consumer", 0, 0, "NoBattery", ) for _ in range(NUM_OF_CONSUMERS)] +\
        [HouseholdDescription("PV", 0, 0, 0, "NoBattery") for _ in range(NUM_OF_PV)] +\
        [HouseholdDescription("Prosumer", battery_capacity, 0,
                              "SimpleBattery", battery_config=BatteryConfig(
                                  "UEMA", "IdealBattery"),  # Predictor does not do anything here
                              eid_prefix=__eid_prefix_from_index(idx)) for idx in range(NUM_OF_PROSUMERS)]
    run_simulations_with_household(
        households, battery_capacity, battery_type="SimpleBattery", csv_spec=None, predictor=None)


def run_simulations_sarima(battery_capacity,
                           history_sizes=[4, ],
                           ):

    for history in history_sizes:
        for model in SARIMA_MODELS:
            csv_spec = "%s_history_%d" % (model, history)

            households = [HouseholdDescription("Consumer", 0, 0, "NoBattery", ) for _ in range(NUM_OF_CONSUMERS)] +\
                [HouseholdDescription("PV", 0, 0, 0, "NoBattery") for _ in range(NUM_OF_PV)] +\
                [HouseholdDescription("Prosumer", battery_capacity, 0,
                                      "SmartBattery", battery_config=BatteryConfig(
                                          "CSV", "IdealBattery"),
                                      csv_filepath=str(
                                          Path(SARIMA_PREDICTIONS_PATH, f'predictions_{__prosumer_id_from_index(idx)}_balance_{csv_spec}.csv')),
                                      eid_prefix=__eid_prefix_from_index(idx)) for idx in range(NUM_OF_PROSUMERS)]
            run_simulations_with_household(
                households, battery_capacity, battery_type="SmartBattery", predictor="CSV", csv_spec=csv_spec)


def run_simulations_uema(battery_capacity):
    households = [HouseholdDescription("Consumer", 0, 0, "NoBattery", ) for _ in range(NUM_OF_CONSUMERS)] +\
        [HouseholdDescription("PV", 0, 0, 0, "NoBattery") for _ in range(NUM_OF_PV)] +\
        [HouseholdDescription("Prosumer", battery_capacity, 0,
                              "SmartBattery", battery_config=BatteryConfig(
                                  "UEMA", "IdealBattery"),
                              eid_prefix=__eid_prefix_from_index(idx)) for idx in range(NUM_OF_PROSUMERS)]
    run_simulations_with_household(
        households, battery_capacity, battery_type="SmartBattery", predictor="UEMA", csv_spec=None)


def run_simulations_uema_csv(battery_capacity, uema_models=[("1D_1H", "0.81"), ("1W_1H", "0.67")]):

    folder_path = CSV_PREDICTIONS_PATH
    for freq, alpha in uema_models:
        print(freq, alpha)
        uema_fmt_string = f"predictions_%d_balance_uema_{freq}_alpha_{alpha}.csv"
        print(uema_fmt_string % 1)
        run_csv_simulation(battery_capacity, folder_path,
                           uema_fmt_string,
                           csv_spec=f"UEMA_{freq}_{alpha.split('.')[-1]}")


def run_perfect_prediction_sim(battery_capacity, ):
    folder_path = CSV_PREDICTIONS_PATH
    run_csv_simulation(battery_capacity, folder_path,
                       "predictions_%d_balance_perfect.csv",
                       csv_spec="perfect")


def run_backshift_prediction_sim(battery_capacity):
    folder_path = CSV_PREDICTIONS_PATH
    run_csv_simulation(battery_capacity, folder_path,
                       "predictions_%d_balance_backshift_24.csv",
                       csv_spec="backschift_24h")


def run_csv_simulation(battery_capacity, folder_path, prediction_file_name_fmt_str, csv_spec):
    """
    Runs a csv simulation with the given battery capcacity.
    The folder_path  is a path to the directory where the CSV-Files containing the predictions for the energy balance are.
    prediction_file_name_fmt_str should be a formatable string with a single %d-Parameter. When configured with an Prosumer-ID,
        the result should be a filename in folder_path.
    csv_spec is a string that will be part of the result-file of the Simulation and can be chosen at will.
    """
    households = [HouseholdDescription("Consumer", 0, 0, "NoBattery", ) for _ in range(NUM_OF_CONSUMERS)] +\
        [HouseholdDescription("PV", 0, 0, "NoBattery",) for _ in range(NUM_OF_PV)] +\
        [HouseholdDescription("Prosumer", battery_capacity, 0,
                              "SmartBattery", battery_config=BatteryConfig(
                                  "CSV", "IdealBattery"),
                              csv_filepath=str(os.path.abspath(
                                  Path(folder_path, prediction_file_name_fmt_str % (__prosumer_id_from_index(idx), )))),
                              eid_prefix=__eid_prefix_from_index(idx)) for idx in range(NUM_OF_PROSUMERS)]
    logger.debug("Running simulation with: %d Consumers, %d PVs and %d Prosumers: %s",
                 NUM_OF_CONSUMERS, NUM_OF_PV, NUM_OF_PROSUMERS, PROSUMER_IDS)
    run_simulations_with_household(
        households, battery_capacity, battery_type="SmartBattery", predictor="CSV", csv_spec=csv_spec)


def __eid_prefix_from_index(idx):
    return FMT_STRING_PROSUMER_ID % (__prosumer_id_from_index(idx),)


def __prosumer_id_from_index(idx):
    return PROSUMER_IDS[idx % len(PROSUMER_IDS)]


def run_simulations_with_household(households: List[HouseholdDescription], battery_capacity,
                                   battery_type,
                                   csv_spec,
                                   predictor,) -> None:
    rust_process = open_rust_process()

    start_time_real = time.time()

    predictor_csv_string = ""
    if predictor and csv_spec:
        predictor_csv_string = "%s_%s_" % (predictor, csv_spec)
    elif predictor:
        predictor_csv_string = "%s_" % (predictor,)

    world: World = World(sim_config=sim_config,)
    path = Path(RESULTS_DATA_SUBFOLDER,
                f"from_{START.split(' ')[0]}_to_{str(END_DATE).split(' ')[0]}_ConProPV_{NUM_OF_CONSUMERS,NUM_OF_PROSUMERS,NUM_OF_PV}_{predictor_csv_string}{battery_type}_{battery_capacity}Wh")
    logger.debug("Saving Sim file to: %s" % (path, ))
    assert predictor_csv_string in str(path)
    results_db_file = str(path)
    rust_sim(world, step_size, START, END, num_of_consumers=NUM_OF_CONSUMERS,
             num_of_PV=NUM_OF_PV,
             households=households,
             battery_capacity=battery_capacity,
             results_filepath=results_db_file,
             )

    world.run(until=END)

    sim_seconds = time.time() - start_time_real
    logger.info("Simulations took %.2f seconds, ~ %.2f Minutes" %
                (sim_seconds, sim_seconds/60.))

    time.sleep(0.5)
    enerdag_return = rust_process.poll()
    now = time.time()

    if enerdag_return is not None:
        logger.debug("Enerdag has terminated well behaved.")
    while enerdag_return is None:
        logger.info("Waiting for enerdag to terminate")
        time.sleep(0.5)
        enerdag_return = rust_process.poll()

        if enerdag_return is None and time.time() - now > 5:  # Wait 5 Seconds
            logger.warning("Enerdag has not terminated yet, killing..")
            rust_process.kill()
            break


def rust_sim(world, step_size, START, END,
             num_of_consumers=5, num_of_PV=0,  battery_capacity=10_000,
             households=None,
             use_monitor=False,
             results_filepath=None,
             ):
    """
    Responsible for creating the Simulation and connecting all the entities.
    """
    # create_scenario(world)
    hhsim = world.start('HouseholdSim', step_size=step_size)
    rustAPI = world.start('Rust_Sim', step_size=step_size)

    panda = None
    grid = None
    grid_entities = None
    if PANDA_NET:
        panda = world.start('PandaPower', step_size=step_size)
        # Get the Method that creates the given GRID
        grid_creation_method = getattr(panda, PANDA_NET)
        grid = grid_creation_method(num_of_PV=NUM_OF_PV,
                                    num_of_prosumer=NUM_OF_PROSUMERS).children
        grid_entities = get_pandapower_grid_entities(grid)
        num_of_consumers = len(grid_entities['consumers'])
        diff_consumers = num_of_consumers - NUM_OF_CONSUMERS
        if diff_consumers > 0:
            logger.warning("Adding %d consumers to fit Panda Net" %
                           (diff_consumers, ))
            households += [HouseholdDescription("Consumer", 0, 0, "NoBattery", )
                           for _ in range(diff_consumers)]

    # Instantiate models

    if not households:
        raise ValueError("Households need to be specified")

    num_of_prosumers = NUM_OF_PROSUMERS

    sim_data_entities = hhsim.householdsim(
        num_of_consumer=num_of_consumers, num_of_PV=num_of_PV,
        num_of_prosumer=num_of_prosumers,
        start_time=START, **HHSIM_DATASET_KWARGS).children

    model = create_rust_neighborhood(
        rustAPI, households, START)

    if use_monitor:
        collector = world.start('Collector', step_size=step_size)
        monitor = collector.Monitor()
        world.connect(model, monitor, 'trades', 'total')

    # change filename according to the number of entities in sim_data_entities.

    if not results_filepath:

        import datetime
        import random
        results_filepath = f"./Simulation_{START}_{datetime.datetime.now()}_{random.randint(1000,9999)}.hdf5"
        logger.warning(
            "No result filepath specified, storing sim in: %s" % (results_filepath))

    hdf5 = world.start('DB', step_size=step_size, duration=END)
    db = hdf5.Database(filename=f"{results_filepath}.hdf5")
    hdf5.set_meta_data({
        'STEP_SIZE_IN_SECONDS': step_size,
        'START': START,
        'NUM_PVS': num_of_PV,
        'NUM_PROSUMERS': num_of_prosumers,
        'NUM_OF_CONSUMERS': num_of_consumers,
        'BATTERY_UNIFORM_CAPACITY': battery_capacity,
        'BATTERY_TYPE': [h for h in households if h.household_type == "Prosumer"][-1].battery_type})

    connect_pvs(world, sim_data_entities, model.children, db)
    connect_consumers(world, sim_data_entities, model.children, db)
    connect_prosumers(world, sim_data_entities, model.children, db)

    world.connect(model, db, 'total')
    world.connect(model, db, 'trades')
    world.connect(model, db, 'total_disposable_energy')
    world.connect(model, db, 'grid_power_load')

    if panda:
        connect_households_to_grid(world, model.children, grid)
        connect_grid_to_db(world, grid, db)

    connect_entire_hhsim_to_entity(world, sim_data_entities, db)


def get_pandapower_grid_entities(grid) -> Dict[str, List[Entity]]:
    return {
        'consumers': [e for e in grid if e.type in ('Consumer')],
        'buses': [e for e in grid if e.type in ('Bus')],
        'transformer': [e for e in grid if e.type in ('Trafo')],
        'pvs': [e for e in grid if e.type in ('PV')],
        'prosumers': [e for e in grid if e.type in ('Prosumer')],
        'lines': [e for e in grid if e.type in ('Line')],
        'extgrid': [e for e in grid if e.type in ('ExtGrid')]}


##
# Configuration Part
##
step_size = minutes(5)
# Keyword arguments for the household sim. Can be taken from the dataset.
POSSIBLE_DATASETS = {
    'HTW_BERLIN': htw_berlin.HOUSEHOLD_SIM_KWARGS,
    'COSSMIC': pp_householdsim.HOUSEHOLD_SIM_KWARGS,

}
HHSIM_DATASET_KWARGS = POSSIBLE_DATASETS["HTW_BERLIN"]


# Where to store the Results
RESULTS_DATA_SUBFOLDER = Path("/tmp")
#Path(os.getcwd(), 'visualization', 'data', 'enerdag_sim_prosumer', 'htw_berlin')


# Path where the CSV-Files for the predictions are stored.
CSV_PREDICTIONS_PATH = Path(os.curdir,
                            "visualization/data/reference_predictions/htw_berlin")

# The configuration if a sarima Simulation is run.
# Path where the CSV Files for the SARIMA Predictions are stored.

SARIMA_PREDICTIONS_PATH = PurePath(os.path.abspath(CSV_PREDICTIONS_PATH), "htw_berlin_result"
                                   )
#  These are the model parameters. They are needed to select the correct files.
SARIMA_MODELS = [
    "(1, 0, 1)_(1, 0, 1, 24)",
    "(1, 0, 1)_(2, 0, 0, 24)",
]


# This can stay fixed: Prosumer-ID of Rust entities.
FMT_STRING_PROSUMER_ID = "pid%d"

# The simulation runs from START to END.
# e.g.str(ALL_PROSUMERS_ACTIVE_START) or '2016-03-01 00:00:00'
ALL_PROSUMERS_ACTIVE_START = pd.to_datetime('2016-03-01 00:00:00')
ALL_PROSUMERS_ACTIVE_END = pd.to_datetime('2017-03-01 00:00:00')


START = '2010-01-01 00:00:00'
END_DATE = '2010-12-29 00:00:00'


END_DATE = pd.Timestamp(END_DATE)

END = simulation_days((pd.Timestamp(END_DATE) - pd.Timestamp(START)).days)

print(START, END_DATE, pd.Timestamp(START), pd.Timestamp(END_DATE),
      END, pd.Timestamp(END_DATE) - pd.Timestamp(START), sep=" | ")
battery_capacities = [
    5_000,
    10_000,
]

# The ids of the Prosumers you want to simulate.
# HTW Berlin Dataset can be [0,..,74]
# cossmic / householdsim dataset can be [1, 3, 4]
PROSUMER_IDS = range(74)
NUM_OF_CONSUMERS = 74
NUM_OF_PROSUMERS = 74
NUM_OF_PV = 0

# Select the electrical grid  you want to simulate
# None, if you want to omit this
# See cosimulation_city_energy/pandapowermosaik.py  -> META -> models for a list of possible Grids
PANDA_NET = "DorfNetz"
PANDA_NET = None
# All the SIMS that are possible
POSSIBLE_SIMS = (

    "SARIMA",
    "UEMA_FROM_CSV",  # UEMA precalculated in csvs
    "UEMA_CALCULATED_OTF",  # On the fly calculation in enerdag
    "PERFECT",
    "B24",
    "SIMPLE",
    "NO_BATTERY",

)
SIMS_TO_RUN = (
    # POSSIBLE_SIMS[3],
    POSSIBLE_SIMS[0],
    # POSSIBLE_SIMS[4],
    # POSSIBLE_SIMS[5],
    # POSSIBLE_SIMS[6],
)


def log_next(next, capacity):
    time.sleep(2)

    logger.info("Starting to run %s %d kwh" % (next, capacity))


if __name__ == "__main__":
    usage_str = """
    rust_integrated_battery.py [-d DB_FILE]? [-c CSV_PREDICTION_DIR]? [-s SARIMA_PREDICITONS_DIR]? [-o OUTPUT_DIR]
        [DB_FILE]: Path to SQLITE3 Database file
        [SARIMA_PREDICTIONS_DIR]: Directory where precomputed SARIMA predictions are stored
        [CSV_PREDICTION_DIR]: Directory where other precumputed predictions are stored
            -> For the naming conventions, see the additional_datasets/create_reference_predictions.py file
        [OUTPUT_DIR] Where the Output should be stored
        Runs simulations with the mosaik_rust_api. Most of the configuration should be done by editing the source,
        but some can be passed by command line. All command line options have sensible defaults if not specified.

    """
    try:

        logger.warn("Killing dangling processes on port 3456 if there are any")
        subprocess.call(['fuser', "3456/tcp", "-k", ],
                        stdout=subprocess.DEVNULL)
        time.sleep(0.1)
    except Exception as e:
        # If you don't have fuser installed on your system this will fail.
        # You just need to make sure there is nothing running on port 3456/tcp.
        logger.warning("Exception when calling fuser %s" % (str(e), ))

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hd:c:s:o:')
    except getopt.GetoptError as err:
        print(usage_str)
        print(err)
    logger.debug(opts)

    for o, a in opts:
        if o == "-d":
            HHSIM_DATASET_KWARGS['data_base_path'] = a
        elif o == "-c":
            CSV_PREDICTIONS_PATH = a
        elif o == "-s":
            SARIMA_PREDICTIONS_PATH = a
        elif o == "-o":
            RESULTS_DATA_SUBFOLDER = a

    logger.info("Database is read from: %s" %
                (HHSIM_DATASET_KWARGS['data_base_path'] if 'data_base_path' in HHSIM_DATASET_KWARGS else "default loc from hhsim", ))
    logger.info("CSV Predictions will be read from: %s" %
                (CSV_PREDICTIONS_PATH, ))
    logger.info("SARIMA Predictions will be read from: %s" %
                (SARIMA_PREDICTIONS_PATH, ))

    logger.info("Results will be stored in: %s" % (RESULTS_DATA_SUBFOLDER, ))

    logger.info("Running the following predictions: %s" % (SIMS_TO_RUN, ))

    if "NO_BATTERY" in SIMS_TO_RUN:
        log_next("NO_BATTERY", 0)
        run_simulations_no_battery()
    for battery_capacity in battery_capacities:

        for SIM_TO_RUN in SIMS_TO_RUN:
            try:
                if "SARIMA" in SIM_TO_RUN:

                    log_next("SARIMA", battery_capacity)
                    run_simulations_sarima(battery_capacity)

                if "UEMA_CALCULATED_OTF" in SIM_TO_RUN:
                    # The simulations_uema_csv run faster.
                    log_next("UEMA_CALCULATED_OTF", battery_capacity)
                    run_simulations_uema(battery_capacity)
                if "PERFECT" in SIM_TO_RUN:
                    log_next("PERFECT", battery_capacity)

                    run_perfect_prediction_sim(battery_capacity)

                if "B24" in SIM_TO_RUN:
                    log_next("B24", battery_capacity)

                    run_backshift_prediction_sim(battery_capacity)

                if "SIMPLE" in SIM_TO_RUN:
                    log_next("SIMPLE", battery_capacity)

                    run_simulations_simple_battery(
                        battery_capacity=battery_capacity)

                if "UEMA_FROM_CSV" in SIM_TO_RUN:
                    log_next("UEMA_FROM_CSV", battery_capacity)

                    run_simulations_uema_csv(battery_capacity=battery_capacity)
            except Exception as e:
                print("Previous simulation exited with error: %s" % (str(e),))
