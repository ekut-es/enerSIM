from __future__ import annotations
import sys

sys.path.append('./evaluation_utils')  # noqa

import h5py
import os
import numpy as np
import pandas as pd
from evaluation_utils.hdf5_to_df import convert_hdf5_to_dfs, convert_hdf5_to_viewdict, get_rust_sim_entities, HDF5Keys, create_time_index
import typing
import dataclasses
from pathlib import PurePath


def mW_to_kWh(power_mW: float, time_in_s: float) -> float:
    kW = power_mW * 1000
    kWh = kW * (time_in_s / 3600.)
    return kWh


def kWh_to_MW(seconds, kWh):
    hours = seconds / (3600.)
    kW = kWh / hours
    W = kW * 1000
    MW = W / (1000 * 1000)
    return MW


class Money:
    """
    Small Class to represent Money. Corresponds to the enerdag-currency
    """
    # From Representation to cents
    FRACTION_MULTIPLIER = 10000
    # FROM Cents to EURO
    CENT_MULTIPLIER = 100

    def __init__(self, enerdag_currency: int) -> None:
        self.enerdag_currency = int(enerdag_currency)

    def __add__(self, money: Money) -> Money:
        if type(money) is not Money:
            raise TypeError("Wrong Type", money)
        return Money(self.enerdag_currency + money.enerdag_currency)

    def __mul__(self, i: int) -> Money:
        return Money(int(self.enerdag_currency * i))

    def __str__(self) -> str:
        return "%dct" % self.to_cents()

    def to_unit(self):
        return self.enerdag_currency

    @staticmethod
    def from_cents(cents: int) -> Money:
        return Money(cents * Money.FRACTION_MULTIPLIER)

    def to_cents(self) -> int:
        return int(self.enerdag_currency / Money.FRACTION_MULTIPLIER)

    def to_euro(self) -> float:
        return float(self.to_cents()) / Money.CENT_MULTIPLIER


@ dataclasses.dataclass
class PriceSpec:
    proceeds_sell_to_grid: Money
    cost_buy_from_grid: Money

    cost_of_generation: Money
    cost_of_storage: Money

    avg_p2p_trade: Money
    enerdag_net_costs: Money


@dataclasses.dataclass(init=True)
class EnergySourceStats:
    """
    total_energy as int in WH (NOT kWH)
    total_money in "Money" Class
    """
    total_energy: int
    total_money: Money

    def update_total_money(self, energy: int, money: Money) -> None:
        self.total_money = self.total_money + money
        self.total_energy = self.total_energy + energy

    def update_avg_money(self, energy: int, money: Money) -> None:
        self.total_money = self.total_money + (money * energy)
        self.total_energy = self.total_energy + energy

    def get_average_money(self) -> float:
        """
        Returns integet that means "€/kWh"
        """
        try:
            return Money(float(self.total_money.enerdag_currency) / (float(self.total_energy) / 1000.)).to_euro()
        except ZeroDivisionError:
            return 0

    @ staticmethod
    def default() -> EnergySourceStats:
        return EnergySourceStats(0, Money(0))

    def to_tuple(self) -> typing.Tuple[int, Money]:
        """
        Returns total energy and total costs
        """
        return (self.total_energy, self.total_money.to_cents())


def calc_costs_pv_wo_enerdag(sim_file: pd.DataFrame, price_spec: PriceSpec):
    """
    Calculates how much a household with PV but without battery would have paid for energy
    """
    df = sim_file
    cost_of_generation = calculate_cost_of_generation(sim_file, price_spec)

    published_balance_positive = df[HDF5Keys.PUBLISHED_BALANCE][()] >= 0
    published_balance_negative = df[HDF5Keys.PUBLISHED_BALANCE][()] < 0

    utilities_balance = df[HDF5Keys.PUBLISHED_BALANCE][()] * -1

    buy_from_utilities = calculate_utilities_price(
        utilities_balance, price_spec.cost_buy_from_grid, published_balance_negative)
    sell_to_utilities = calculate_utilities_price(
        utilities_balance, price_spec.proceeds_sell_to_grid, published_balance_positive)

    return cost_of_generation.total_money + buy_from_utilities.total_money + sell_to_utilities.total_money


def calc_costs_pv_and_battery_wo_enerdag(sim_file: pd.DataFrame, price_spec: PriceSpec):
    """
        Calculates how much a household with PV and battery would have paid for energy
    """
    discharge_cost = calculate_cost_of_battery(sim_file, price_spec)
    other_cost = calc_costs_pv_wo_enerdag(sim_file, price_spec)
    return discharge_cost.total_money + other_cost


@ dataclasses.dataclass(init=True)
class HouseholdEnergyStats:

    total_energy: EnergySourceStats

    buy_from_p2p: EnergySourceStats
    sell_to_p2p: EnergySourceStats

    buy_from_utilities: EnergySourceStats
    sell_to_utilities: EnergySourceStats

    cost_of_generation: EnergySourceStats
    cost_of_discharge: EnergySourceStats

    def calc_total(self, df: pd.DataFrame):
        total_skipped = False
        money = Money(0)
        for key, value in self.to_dict().items():
            if key == "total":
                total_skipped = True

            else:
                money = money + value.total_money

        assert total_skipped
        total_energy_usage_kWh = pd.Series(df[HDF5Keys.CONSUMPTION][()]).apply(
            lambda x: mW_to_kWh(x, 300)).sum()

        total_energy_usage_WH = total_energy_usage_kWh * 1000.
        total_energy_deducted = self.cost_of_generation.total_energy + self.buy_from_p2p.total_energy + \
            self.buy_from_utilities.total_energy + \
            self.sell_to_p2p.total_energy + self.sell_to_utilities.total_energy

        assert total_energy_usage_WH - total_energy_deducted - \
            df[HDF5Keys.CHARGE][()].max() and total_energy_deducted + \
            df[HDF5Keys.CHARGE][()].max()

        self.total_energy = EnergySourceStats(total_energy_usage_WH, money)

    @ staticmethod
    def zero_init() -> HouseholdEnergyStats:
        return HouseholdEnergyStats(EnergySourceStats.default(),
                                    EnergySourceStats.default(),
                                    EnergySourceStats.default(),
                                    EnergySourceStats.default(),
                                    EnergySourceStats.default(),
                                    EnergySourceStats.default(),
                                    EnergySourceStats.default(),
                                    )

    def to_dict(self) -> typing.Dict[str, EnergySourceStats]:
        return {
            'total': self.total_energy,
            'buy_p2p': self.buy_from_p2p,
            'sell_to_p2p': self.sell_to_p2p,
            'buy_from_utilities': self.buy_from_utilities,
            'sell_to_utlitites': self.sell_to_utilities,
            'cost_of_generation': self.cost_of_generation,
            'cost_of_discharge': self.cost_of_discharge,
        }

    def __str__(self) -> str:
        s = ""
        for key, value in self.to_dict().items():
            s += "\t%s: %.2f€ = %.2fkWH * %.2f€ %s" % (
                key, value.total_money.to_euro(), value.total_energy / 1000.,  value.get_average_money(), os.linesep)
        return s.strip(os.linesep)


def household_power_cost(dataset: h5py.Dataset, prices_spec: PriceSpec) -> HouseholdEnergyStats:
    power_stats = HouseholdEnergyStats.zero_init()

    published_balance_negative = dataset[HDF5Keys.PUBLISHED_BALANCE][()] < 0
    power_stats.buy_from_p2p = calculate_p2p_costs(
        dataset, published_balance_negative, price_spec=prices_spec, is_selling=False)

    published_balance_positive = dataset[HDF5Keys.PUBLISHED_BALANCE][()] >= 0
    power_stats.sell_to_p2p = calculate_p2p_costs(
        dataset, published_balance_positive, prices_spec, True)

    utilities_balance = (dataset[HDF5Keys.PUBLISHED_BALANCE][()] -
                         dataset[HDF5Keys.P2P_TRADED][()]) * -1
    # Invert, because costs should be denoted positive

    power_stats.buy_from_utilities = calculate_utilities_price(
        utilities_balance, prices_spec.cost_buy_from_grid, published_balance_negative)
    power_stats.sell_to_utilities = calculate_utilities_price(
        utilities_balance, prices_spec.proceeds_sell_to_grid, published_balance_positive)

    power_stats.cost_of_generation = calculate_cost_of_generation(
        dataset, price_spec=prices_spec)
    power_stats.cost_of_discharge = calculate_cost_of_battery(
        dataset, prices_spec)

    power_stats.calc_total(dataset)

    return power_stats


def calculate_p2p_costs(df: h5py.Dataset, filter: pd.Series[bool], price_spec: PriceSpec, is_selling: bool) -> EnergySourceStats:
    prices = df[HDF5Keys.AVG_P2P_PRICE][()]
    # overide prices from simulation with spec
    new_prices_np = np.ones(prices.shape[0]) * (price_spec.avg_p2p_trade.enerdag_currency +
                                                (0 if is_selling else price_spec.enerdag_net_costs.enerdag_currency))
    new_prices = new_prices_np
    return energy_stats_series(
        df[HDF5Keys.P2P_TRADED][()] * -1, new_prices, filter)[0]


def calculate_utilities_price(utilities_balance: pd.Series, util_price: Money, filter: pd.Series[bool]) -> EnergySourceStats:
    prices_array = np.ones(
        utilities_balance.shape[0]) * util_price.enerdag_currency
    price_series = prices_array
    energy_stats, _ = energy_stats_series(
        utilities_balance, price_series, filter)

    return energy_stats


def energy_stats_series(energy: np.array, money: np.array, filter) -> typing.Tuple[EnergySourceStats, pd.Series]:
    total_energy = energy[filter].sum()
    price = (energy[filter] * money[filter]).sum() / 1000.
    series = np.zeros(energy.shape[0])
    series[filter] = money[filter]
    return EnergySourceStats(int(total_energy), Money(price)), filter


def calculate_cost_of_generation(df: h5py.Dataset, price_spec: PriceSpec) -> EnergySourceStats:
    """
    Calculates EnergySourceStats object for the generated energy

    """
    generated_power = None

    if HDF5Keys.PRODUCTION in df.keys():
        generated_power = pd.Series(df[HDF5Keys.PRODUCTION][()])
    else:
        generated_power = pd.Series([0])
    generated_energy_kwh = generated_power.apply(
        lambda x: mW_to_kWh(x, 300))

    cost_generated = generated_energy_kwh.apply(lambda x: (
        x * price_spec.cost_of_generation.enerdag_currency))

    generated_wh = generated_energy_kwh.sum() * 1000.
    return EnergySourceStats(
        generated_wh, Money(cost_generated.sum()))


def calculate_cost_of_battery(df: h5py.Dataset, price_spec: PriceSpec) -> EnergySourceStats:
    """
    Calculates EnergySourceStats object for the discharged energy
    """
    # Charge in this time frame
    charge = df[HDF5Keys.CHARGE][()]
    charge_diff = np.diff(charge, prepend=0)
    price_discharge = np.zeros(charge.shape[0])
    times_discharge = charge_diff < 0
    price_discharge[times_discharge] = pd.Series(charge_diff[times_discharge]).apply(
        lambda x: (-x * price_spec.cost_of_storage.enerdag_currency / 1000.))

    return EnergySourceStats(-charge_diff[times_discharge].sum(), Money(price_discharge.sum()))


if __name__ == "__main__":

    subfolder = "data/sim_results/htw_berlin/"
    file_name = None

    print(sys.argv)
    dir_content = os.listdir(subfolder)
    # print(dir_content, len(dir_content))
    if len(sys.argv) == 2:
        file_name = dir_content[int(sys.argv[1])]
    else:
        file_name = "from_2016-03-01_to_2017-02-01_ConProPV_(3, 3, 0)_CSV_(1, 0, 1)_(1, 0, 1, 24)_history_4_SmartBattery_10000Wh.hdf5"

    file = h5py.File(PurePath(subfolder,
                              file_name))

    print(file_name)
    dfs = convert_hdf5_to_viewdict(file)
    index = create_time_index(file)
    print(dfs)
    print(file.keys())
    print("Stats from %s to %s" %
          (index[0], index[-1]))

    for name, df in dfs.items():
        if not "prosumer" in name:
            continue
        print(name.split(".")[-1])

        prices = PriceSpec(
            Money.from_cents(7),
            Money.from_cents(31),
            cost_of_generation=Money.from_cents(6),
            cost_of_storage=Money.from_cents(5),
            enerdag_net_costs=Money.from_cents(5),
            avg_p2p_trade=Money.from_cents(19),)
        power_stat = household_power_cost(df, prices)

        consumption_mw = df[HDF5Keys.CONSUMPTION][()].sum()
        print("Total Consumption %.2f KWh" %
              mW_to_kWh(consumption_mw, 300))

        print(power_stat)
