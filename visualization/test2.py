import subprocess
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from evaluation_utils import hdf5_to_df

from evaluation_utils.hdf5_to_df import HDF5Keys, convert_hdf5_to_viewdict
from evaluation_utils.evaluation_utils import *
# because i moved to mw_to_kWh method there
import evaluation_utils.pricing as BatterySim
from visualization.vis_utils import *
from evaluation_utils import *
import h5py
import evaluation_utils.hdf5_to_df
import matplotlib.pyplot as plt

# Kleines Testscript zur Überprüfung von Daten aus rust_integrated_battery

all_prosumers = True


NO_BATTERY = "NO Battery"
PERFECT_SMART = "Perfect Smart Battery"
SARIMA_SMART_BATTERY = "SARIMA Smart Battery"
SIMPLE_BATTERY = "Simple Battery"

sim_file = "visualization/from_2010-01-01_to_2010-12-29_ConProPV_(74, 74, 0)_CSV_(1, 0, 1)_(2, 0, 0, 24)_history_4_SmartBattery_5000Wh.hdf5" #"/home/flo/Workspace/Uni/Masterarbeit/simulation/data/second_part/from_2010-01-01_to_2010-12-29_ConProPV_(74, 74, 0)_CSV_(1, 0, 1)_(1, 0, 1, 24)_history_4_SmartBattery_5000Wh.hdf5"
sim_file_view = convert_hdf5_to_viewdict(h5py.File(sim_file))


def select_series(d, scenario, prosumer, series):
    return d[scenario]["Series/Rust_Sim-0.pid%d_prosumer_%d" % (prosumer, prosumer)][series][()]


def select_prosumer0(d, scenario, series):
    return select_series(d, 0, scenario, series)


scenario = SARIMA_SMART_BATTERY
plot_prosumer = 0
series = HDF5Keys.PRODUCTION


def get_total_consumption(d: Dict[str, h5py.Dataset]) -> float:
    return accumulate_value_over_datasets(d, HDF5Keys.CONSUMPTION)


def accumulate_value_over_datasets(d: Dict[str, h5py.Dataset], selector) -> float:
    total = 0
    for eid, val in d.items():
        if not "prosumer" in eid:
            continue

        selected = val[selector][()]
        total += sum(selected)
    return total


def show_file(file: Dict[str, h5py.Dataset], selector):
    for eid in file:
        if 'consumer' in eid:
            continue

        val = file[eid]

        selected1 = val[selector][()]

        length = min(selected1.shape[0], selected1.shape[0])
        selected1 = selected1[:length]

        fig, ax = plt.subplots(1)
        ax.plot(selected1)

        plt.show()
        break


show_file(sim_file_view, HDF5Keys.PRODUCTION)
