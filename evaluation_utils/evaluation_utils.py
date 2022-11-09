"""
Module for common operations to evaluate simulations created by an enerdag simulation.
The pd.DataFrames are structured as returned by evaluation_utils hdf5_to_df
"""
import numpy as np
import matplotlib.pyplot as plt
from pyrsistent import b
from evaluation_utils.hdf5_to_df import HDF5Keys
import h5py
import pandas as pd
from typing import Callable, Tuple, Dict, Iterable, Any, TypeVar, List
from batterysim.battery import BatterySim
T1 = TypeVar('T1')
T2 = TypeVar('T2')
T3 = TypeVar('T3')


def consumers_buy_from(df: h5py.Dataset) -> Tuple[float, float]:
    """
    Returns a tuple where the first entry is the energy bought from the P2P Market
    and the second entry is the energy bought from the utilities company
    """
    balance = df[HDF5Keys.PUBLISHED_BALANCE][()]
    p2p_traded = df[HDF5Keys.P2P_TRADED][()]
    bought_from_utilities = balance - p2p_traded

    assert (p2p_traded + bought_from_utilities).sum() == balance.sum()
    return (p2p_traded.sum(), bought_from_utilities.sum())


def get_consumers_and_calc_buy_from(sim_dataframes: Dict[str, h5py.Dataset]) -> Tuple[float, float]:
    """
    Returns a tuple where the first entry is the energy bought from the P2P Market  for all the consumers
    and the second entry is the energy bought from the utilities company
    """
    consumers = dict(
        list(filter(lambda x: "consumer" in x[0], list(sim_dataframes.items()))))
    total = list(map(consumers_buy_from, consumers.values()))
    return (sum(map(lambda x: x[0], total)), sum(map(lambda x: x[1], total)))


def map_dict_values(d: Dict[T1, T2], f: Callable[[T2], T3]) -> Dict[T1, T3]:
    """
    Creates a new dictionary by keeping the same keys but applying f to the values.
    """
    new_d = dict()
    for key in d:
        new_d[key] = f(d[key])
    return new_d


def give_percentages_to_horizontal_bar(balances: Dict[str, Iterable[float]]):
    """
    Companion function to `plot_model_horizontal_bar`.
    Prints the percentage values.
    """
    for name, values in balances.items():
        total = sum(values)
        print(name)
        percentage = [x / total for x in values]
        print("\t", percentage)


def prosumers_sell_to(df: h5py.Dataset) -> Tuple[float, float]:
    """
    Returns a tuple where the first entry is the energy sold to  the P2P Market
    and the second entry is the energy sold to the utilities company
    """
    balance_positive = df[HDF5Keys.PUBLISHED_BALANCE][()] > 0
    balance = df[HDF5Keys.PUBLISHED_BALANCE][()][balance_positive]
    p2p_traded = df[HDF5Keys.P2P_TRADED][()][balance_positive]
    sold_to_utilities = balance - p2p_traded

    assert (p2p_traded + sold_to_utilities).sum() == balance.sum()
    return (p2p_traded.sum(), sold_to_utilities.sum())


def nbhd_prosumers_sell_to(sim_dataframes: Dict[str, h5py.Dataset]) -> Tuple[float, float]:
    """
    Returns a tuple where the first entry is the energy bought from the P2P Market  for all the consumers
    and the second entry is the energy bought from the utilities company
    """
    prosumers = dict(
        list(filter(lambda x: "prosumer" in x[0], list(sim_dataframes.items()))))
    total = list(map(consumers_buy_from, prosumers.values()))
    return (sum(map(lambda x: x[0], total)), sum(map(lambda x: x[1], total)))


def prosumers_energy_source(df: h5py.Dataset) -> Tuple[float, float, float, float]:
    """
    Returns a tuple where the entries are
        - amount of energy gotten from pv
        - amount of energy gotten from battery
        - amount of energy gotten from p2p trading
        - amount of energy gotten from utilities company
    """

    HDF5Keys.CONSUMPTION

    def mw_to_kwh_5min(x): return BatterySim.mW_to_kWh(x, 300)

    # Consumption in kWh
    consumption = mw_to_kwh_5min(df[HDF5Keys.CONSUMPTION][()])

    # How much of the demand was statisfied by PV
    pv = mw_to_kwh_5min(df[HDF5Keys.PRODUCTION][()])
    from_pv = pv.copy()
    from_pv[pv < consumption] = pv[pv < consumption]
    from_pv[pv >= consumption] = consumption[pv >= consumption]

    # ... How much was bought
    published_balance_negative = df[HDF5Keys.PUBLISHED_BALANCE][()] < 0
    bought_from_market = df[HDF5Keys.PUBLISHED_BALANCE][()
                                                        ][published_balance_negative]
    bought_from_market = -bought_from_market / 1000.
    p2p_traded = df[HDF5Keys.P2P_TRADED][()][published_balance_negative]
    p2p_traded = -p2p_traded / 1000.
    bought_from_utilities = bought_from_market - p2p_traded

    balance_sum = (p2p_traded + bought_from_utilities).sum()
    assert balance_sum <= bought_from_market.sum(
    ) + 0.1 and balance_sum >= bought_from_market.sum() - 0.1

    # How much was  used from battery
    total_consumption = sum(consumption)
    satisfied_so_far = sum(bought_from_market) + sum(from_pv)
    bat = total_consumption - satisfied_so_far

    sum_bought_from_utils = bought_from_utilities.sum()

    if not (df[HDF5Keys.CHARGE][()] > 0).any():  # Avoid Artifact for no battery
        print("No Battery: Adding %.2f to P2G" % (bat,))
        sum_bought_from_utils += bat
        bat = 0.

    return (from_pv.sum(), bat, p2p_traded.sum(), sum_bought_from_utils, )


def nbhd_prosumers_energy_sources(sim_dataframes: Dict[str, h5py.Dataset]) -> List[float]:
    """
    Returns a list where the entries are (for the whole neighborhood):
        - amount of energy gotten from pv
        - amount of energy gotten from battery
        - amount of energy gotten from p2p trading
        - amount of energy gotten from utilities company
    """
    prosumers = dict(
        list(filter(lambda x: "prosumer" in x[0], list(sim_dataframes.items()))))
    total = list(map(prosumers_energy_source, prosumers.values()))
    return [sum(map(lambda x, i=i: x[i], total)) for i in range(4)]


def filter_eid_and_get_energy_source(simulation_run: Dict[str, h5py.Dataset], eid_filter="prosumer") -> Iterable[Tuple[str, Tuple[float, float, float, float]]]:
    """
    Filters the keys of the dict for the given substring (`eid_filter`, default "prosumer") and uses `prosumer_energy_source` to calculate
    their indididual energy sources
    """
    return [(prosumer, prosumers_energy_source(df))
            for prosumer, df in list(filter(lambda x: eid_filter in x[0], list(simulation_run.items())))]


def combine_10kWH_5kWh_dict(first: Dict[str, Any], second: Dict[str, Any], combine_once_kw=["NoBattery", "No Battery"]) -> Dict[str, Any]:
    """
    Combines two simulation DataFrame dicts that have 10 kWh and 5 kWh hour batteries.
    Appends (10 kWh) to the keys of the first dict and (5 kWh) to the keys of the second.
    Skips the the keys defined in `combine_once_kw` in the second dict if they are present in the first dict.
    """
    new = dict()

    def combine_once(x): return any([x in kw for kw in combine_once_kw])

    for key, value in first.items():
        if combine_once(key):
            continue
        new["%s (10kWh)" % key] = value
    for key, value in second.items():
        if combine_once(key):
            continue
        new["%s (5kWh)" % key] = value
    for key in combine_once_kw:
        if key in first:
            new[key] = first[key]
        elif key in second:
            new[key] = second[key]

    return new
