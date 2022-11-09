"""
Shared visualization functions for the householdsim and htw berlin comparison notebooks.
"""
from pathlib import Path, PurePath
from matplotlib.axes import Axes
from matplotlib.figure import Figure
import numpy as np
import h5py


from typing import Iterable, List, Tuple, Dict, Optional, AnyStr
from h5py._hl.files import File
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import register_matplotlib_converters
from evaluation_utils.evaluation_utils import combine_10kWH_5kWh_dict
register_matplotlib_converters()
# https://colorbrewer2.org/
diverging_colors = ['#a6611a', '#dfc27d', '#80cdc1', '#018571']
qualitative_colors = ['#a6cee3', '#1f78b4', '#b2df8a', '#33a02c', '#fb9a99']
market_colors = ['#1b9e77', '#d95f02', '#7570b3', '#e7298a']


def plot_energy_from_bat_and_to_p2p(data_energy_source: Dict[str, List[float]], data_sell_to: Dict[str, List[float]], dataset, batterysize, save_to: Optional[PurePath]):

    plt.rcParams.update({'font.size': 22})
    fig, axs = plt.subplots(1, 2, figsize=(18, 8), sharey=True)

    fontsize_x_label = 24
    fontsize_title = fontsize_x_label

    plt.tick_params('both', which='major', labelsize=20)
    ax = axs[0]
    difference = plot_compare_to_baseline(
        data_energy_source, "Perfect", 1, ax)
    ax.set_title("Energy from battery", fontsize=fontsize_title)
    ax.set_xlabel("%", fontsize=fontsize_x_label)
    fig.tight_layout()

    ax = axs[1]
    difference = plot_compare_to_baseline(
        data_sell_to, "Perfect", 0, ax)
    ax.set_title("Energy to P2P", fontsize=fontsize_title)
    ax.set_xlabel("%", fontsize=fontsize_x_label)
    if save_to:
        fig.savefig(save_to)
    plt.rcdefaults()


def plot_combined_energy_source_consumers(consumer_buy_balances, consumer_buy_balances_5kWh, to_save: Optional[PurePath]):

    consumers_buy_balances_combined = combine_10kWH_5kWh_dict(
        consumer_buy_balances, consumer_buy_balances_5kWh)
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_model_horizontal_bar(
        ax, consumers_buy_balances_combined, colors=qualitative_colors[2:])
    fig.legend(["P2P", "P2G"])
    fig.suptitle("Source of energy for consumers")
    fig.tight_layout()
    if to_save:
        fig.savefig(to_save)


def plot_combined_energy_source_sell_to(energy_source_data: Dict[str, List[float]], sell_to_data: Dict[str, List[float]], dataset, save_to: Optional[PurePath] = None):
    source_labels = ["PV", "Battery", "P2P", "P2G"]
    fig, axs = plt.subplots(1, 2, figsize=(13, 4), sharey=True)
    plot_model_horizontal_bar(
        axs[0], energy_source_data, source_labels=source_labels, colors=qualitative_colors)
    fig.legend(source_labels, fontsize=12)
    axs[0].set_title("Source of energy", fontsize=16)
    plot_model_horizontal_bar(axs[1], sell_to_data, source_labels=(
        "P2P", "P2G"), colors=qualitative_colors[2:])
    axs[1].set_title("Purchaser of energy", fontsize=16)
    ax: plt.Axes = axs[0]
    ax.tick_params(axis='y', labelsize=14)
    ax.tick_params(axis='x', labelsize=14)
    axs[1].tick_params(axis='x', labelsize=14)

    fig.tight_layout()
    if save_to:
        fig.savefig(save_to)


def plot_compare_energy_to_p2p_to_perfect(data: Dict[str, List[float]], batterysize, dataset, save_to: Optional[PurePath] = None) -> Dict[str, float]:
    return plot_compare_to_perfect(data, "Energy to P2P", 0, batterysize, dataset, save_to)


def plot_compare_energy_to_p2_grid_to_perfect(data: Dict[str, List[float]], batterysize, dataset, save_to: Optional[PurePath] = None) -> Dict[str, float]:
    return plot_compare_to_perfect(data, "Energy to P2G", 1, batterysize, dataset, save_to)


def plot_compare_energy_from_battery_to_perfect(data: Dict[str, List[float]], batterysize, dataset, save_to: Optional[PurePath] = None) -> Dict[str, float]:
    return plot_compare_to_perfect(data, "Energy from Battery", 1, batterysize, dataset, save_to)


def plot_compare_to_perfect(data: Dict[str, List[float]], title_prefix: str, index: int, batterysize, dataset, save_to: Optional[PurePath] = None) -> Dict[str, float]:
    fig, ax = plt.subplots(figsize=(6, 4))

    difference = plot_compare_to_baseline(
        data, "Perfect", index, ax)
    ax.set_title(create_suptitle(
        title_prefix, batterysize, dataset), fontsize=14)
    ax.set_xlabel("%", fontsize=16)
    fig.tight_layout()
    if save_to:
        fig.savefig(save_to)
    return difference


def plot_compare_to_baseline(data: Dict[str, List[float]], base_key, index: int, ax: plt.Axes, color="darkgray") -> Dict[str, float]:
    """
    Plots a comparison to the given baseline key and index. The key selects the list from the data dict and index the element of the list.
    The same indices are compared to the basesline key. Comparison is in percentage.
    """
    ax.set_xscale("linear")

    base = data[base_key][index]
    difference = [(key, ((values[index] - base) / base) * 100.)
                  for key, values in data.items() if key != base_key]
    difference = dict(difference)

    rects = ax.barh(range(len(difference)), difference.values(), color=color,
                    tick_label=list(difference.keys()))
    return difference


def create_suptitle(prefix: str, batterysize: int, dataset: str) -> str:
    return "%s (%d kWh / %s)" % (prefix, batterysize, dataset)


def plot_energy_source_prosumers(data: Dict[str, Iterable[float]], save_to: Optional[PurePath], battery_size=5, dataset="COSSMIC") -> Tuple[Figure, Axes]:
    return complete_horizontal_bar_plot(create_suptitle("Energy source prosumer", battery_size,
                                                        dataset), data, legend_labels=["PV", "Batterie", "P2P", "P2G"], colors=qualitative_colors, save_to=save_to)


def plot_energy_buyer_prosumer(data: Dict[str, Iterable[float]], save_to: Optional[PurePath], battery_size=5, dataset="COSSMIC") -> Tuple[Figure, Axes]:
    return complete_horizontal_bar_plot(create_suptitle("Trade partners prosumer", battery_size,
                                                        dataset), data, legend_labels=["P2P", "P2G"], colors=qualitative_colors[2:], save_to=save_to)


def plot_energy_source_consumers(data: Dict[str, Iterable[float]], save_to: Optional[PurePath],  battery_size=5, dataset="COSSMIC") -> Tuple[Figure, Axes]:
    return complete_horizontal_bar_plot("Energy Source Consumer (%d kWh / %s)" % (battery_size, dataset), data, [
        "Buy P2P", "Buy Utilities"], colors=qualitative_colors[2:], save_to=save_to,)


def complete_horizontal_bar_plot(suptitle: str, data: Dict[AnyStr, Iterable[float]],
                                 legend_labels: List[str],  colors: List[str], save_to: Optional[PurePath] = None, figsize=(8, 3)) -> Tuple[Figure, Axes]:
    """
    Plots a horizontal bar with the given data (see `plot_model_horizontal_bar`).
    Applies the given suptitle, legend labels, colors and figsize.
    If save_to is not None, the figure will be stored at the given path.
    """
    fig, ax = plt.subplots(figsize=figsize)
    plot_model_horizontal_bar(
        ax, data, colors=colors, source_labels=legend_labels)
    ax.legend()

    fig.suptitle(suptitle)
    fig.tight_layout()
    if save_to:
        fig.savefig(save_to)
    return (fig, ax)


def plot_model_horizontal_bar(ax: plt.Axes,
                              balances: Dict[str, Iterable[float]],
                              source_labels=["P2P", "P2G"],
                              colors=[qualitative_colors[-1], qualitative_colors[0]]):
    """
    Plots a horizontal bar chart where the labels on the y_axis are given by the keys of the dictionary
    and the sections of the bar are specified with the values of the dict (can be any iterable).
    The sections of the bar are labeled with the given source_labels.
    The bars are always normalized to a percentage value.
    """
    i = 0
    ax.set_xscale("linear")

    np_array = np.abs(np.array(list(map(list, balances.values()))))
    data_cumsum = np_array.cumsum(axis=1)
    sum_nbhd = np_array.sum(axis=1).reshape((np_array.shape[0], 1))

    np_array = np_array / sum_nbhd
    data_cumsum = data_cumsum / sum_nbhd

    for i,  (name, color) in enumerate(zip(source_labels, colors)):

        widths = np_array[:, i]
        starts = (data_cumsum[:, i] - widths)
        rects = ax.barh(list(balances.keys()), widths * 100., left=starts * 100., height=0.8,
                        label=name, color=color)
    ax.set_xlabel("%", fontsize=16)


def group_by_week(df: pd.DataFrame, aggregation_functon) -> pd.DataFrame:
    start = df.index[0]
    weeks = df.groupby(lambda index: (index - start) // pd.Timedelta(weeks=1))
    weeks = weeks.aggregate(aggregation_functon)
    weeks.index = pd.Index([start + pd.Timedelta(weeks=i)
                            for i in weeks.index])
    return weeks


def group_by_timespan(df: pd.DataFrame, timedelta: pd.Timedelta, aggregation_functon) -> pd.DataFrame:
    start = df.index[0]
    weeks = df.groupby(lambda index: (index - start) // timedelta)
    weeks = weeks.aggregate(aggregation_functon)
    weeks.index = pd.Index([start + (timedelta * i) for i in weeks.index])
    return weeks


def visualize_energy_balance(path_to_json: str = None) -> Tuple[plt.Figure, plt.Axes, Dict]:
    """
    Visualize the Energy Balance as provided by the aggregator/collector from a household simulation.
    """
    data = None

    if path_to_json is not None:
        data = pd.read_json(path_to_json)

    if data is not None:
        consumption = data['total_power_consumption_mW']
        generation = data['total_power_generation_mW']

        x_grid = data.index

        fig, axs = plt.subplots(2, 1, figsize=(12, 6))
        ax = axs[0]
        ax.plot(x_grid, consumption, label="Consumption")
        ax.plot(x_grid, generation, label="Generation")
        ax.legend()

        ax = axs[1]
        ax.set_title("Cumulative Consumption / Generation")

        ax.plot(x_grid, np.cumsum(consumption), label="Consumption")
        ax.plot(x_grid, np.cumsum(generation), label="Generation")
        ax.legend()
        return fig, ax, data
    return None, None, None


SIMPLE_BATTERY_GROUP_KEY = 'SimpleBattery-0.SimpleBattery'


def create_simple_battery_df_from_hdf5(file: str) -> pd.DataFrame:
    """
    Reads hdf5 file located at the 'file' path and outputs a pd.DataFrame with Columns
    'current_chargs', 'energy_demand_fulfilled' and the correct Datetime Index.
    """
    data = dict()
    start_date = None
    step_size_in_seconds = None
    measurements = ['current_charge', 'energy_demand_fulfilled']
    f = h5py.File(file, 'r+')
    battery_group = f['Series'][SIMPLE_BATTERY_GROUP_KEY]
    for d in measurements:
        data[d] = battery_group[d]
    meta = dict(f.attrs.items())
    start_date = meta['START']
    step_size_in_seconds = meta['STEP_SIZE_IN_SECONDS']
    start_date = pd.to_datetime(start_date)
    pd_idx = pd.date_range(
        start=start_date, freq=f"{step_size_in_seconds}S", periods=len(data[measurements[0]]))
    df = pd.DataFrame(data, index=pd_idx)
    f.close()
    return df


class HouseholdBatterySimulationStats:

    def __init__(self, path: str = None, file: File = None):
        if path is not None:
            self.path = path
            self.file = h5py.File(path, 'r+')
        if file is not None:
            self.file = file
        self.measurements = ['current_charge']

        meta = dict(self.file.attrs.items())
        self.start_date = meta['START']
        self.step_size_in_seconds = meta['STEP_SIZE_IN_SECONDS']
        self.start_date = pd.to_datetime(self.start_date)
        self.pd_index = None
        self.df = None

    def get_prosumers_to_batteries_map(self):
        prosumer_and_battery = dict()
        for eid in self.file['Relations']:
            if not 'prosumer' in eid:
                continue
            for arr in self.file['Relations'][eid][()]:
                # decode from bytes and get eid
                battery = arr[1].decode().split('/')[-1]
                if "Battery" in battery:
                    prosumer_and_battery[eid] = battery

        return prosumer_and_battery

    def get_prosumer_battery_dataframe(self) -> pd.DataFrame:
        data_dict = dict()
        sim_length = None
        for entity in self.file['Series']:
            if 'prosumer' in entity or 'Battery' in entity or "Prosumer" in entity:
                for measurement in self.file['Series'][entity].keys():
                    eid_withoud_sid = entity.split('.')[1]
                    data_view = self.file['Series'][entity][measurement]
                    data_dict[f"{eid_withoud_sid}_{measurement}"] = data_view[()]
                    if sim_length is None:
                        sim_length = data_view.shape[0]
        index = pd.date_range(start=self.start_date,
                              freq=f"{self.step_size_in_seconds}S", periods=sim_length)
        self.df = pd.DataFrame(data_dict, index=index)
        return self.df

    def get_attribute_cols(self, prosumer_eid: str, battery_eid: str, prosumer_attrs: Iterable[str], battery_attrs: Iterable[str]) -> Tuple[List[str], List[str]]:
        """
            For a Dataframe produced by this class, get the column names corresponding to the attributes by a certain Prosumer and Battery
        """
        if self.df is None:
            self.get_prosumer_battery_dataframe()

        # Creates a filter that filters all entries that don't have the eid in them or don't have one of the attr_names in them.
        def custom_filter(eid, attr_names): return lambda x: eid.split('.')[-1] in x and\
            len(list(filter(lambda n: n in x, attr_names))) != 0

        prosumer_filter = custom_filter(prosumer_eid, prosumer_attrs)
        battery_filter = custom_filter(battery_eid, battery_attrs)

        prosumer_cols = list(filter(prosumer_filter, self.df.columns))
        battery_cols = list(filter(battery_filter, self.df.columns))

        return prosumer_cols, battery_cols
