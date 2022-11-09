from pandas import Timestamp, Timedelta
from typing import List
import numpy as np
import pandas as pd

from typing import TypeVar
import abc
from logging import debug
T = TypeVar('T')

def mW_to_kWh(power_mW: float, time_in_s:float) -> float:
        kW = power_mW * 1000
        kWh =  kW * (time_in_s / 3600.) 
        return kWh


def kWh_to_MW(seconds, kWh):
    hours = seconds / (3600.)
    kW = kWh / hours
    W = kW * 1000
    MW = W / (1000 * 1000)
    return MW

def timestamp_to_timeinday(ts: Timestamp) -> Timedelta:
    return Timedelta(hours=ts.hour, minutes=ts.minute, seconds=ts.second)

def timeinday_to_timestamp(timeinday: Timedelta, today: Timestamp) -> Timestamp:
    """
    The Timedelta timeinday represents the time on the clock. A Timestamp with the 
    date of the timestamp and the time represented by the timedelta is returned
    """
    h = timeinday.seconds // 3600
    m = (timeinday.seconds % 3600) // 60
    s = timeinday.seconds % 60
    t = Timestamp(hour=h, minute=m, second=s, year=today.year,
                     month=today.month, day=today.day, tzinfo=today.tzinfo)
    return t

class PowerPredictorInterface(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
       return (hasattr(subclass, 'predict_spare_power_mW') 
                and callable(subclass.predict_spare_power_mW) 
                and hasattr(subclass, 'update_statistics')
                and callable(subclass.update_statistics)
       )

    @abc.abstractmethod
    def predict_spare_power_mW(time: Timestamp):
        raise NotImplementedError
    
    @abc.abstractmethod
    def update_statistics(self, time: Timestamp):
        raise NotImplementedError


@PowerPredictorInterface.register
class GiveAllPredictor():
    """
    This Predictor gives all the Power it can
    """
    def __init__(self, battery):
        self.battery = battery
        debug("Instantiated GiveAllPredictor")
    
    def predict_spare_power_mW(self, time: Timestamp):
        return kWh_to_MW(self.battery.step_size, self.battery.current_charge_kWh/ 4)
    
    def update_statistics(self, time: Timestamp):
        """
        Not Required for this predictor.
        """
        pass

@PowerPredictorInterface.register
class SparePowerPredictor():
    """
    Tries to predict the consumed and produced energy and aims to never let the battery deplete.
    """
    def __init__(self, battery):

        self.battery = battery
        self.energy_consumption_predictor: EnergyPredictor = RandomForecast()
        self.energy_generation_predictor: EnergyPredictor = RandomForecast()

        self.timestamp_last_step = None
        self.last_positive_powerbalance_today: Timedelta = None
        self.time_last_positive_powerbalance: EmpiricalDistribution = EmpiricalDistribution()
        debug("Instantiated SparePowerPredictor")


    def predict_spare_power_mW(self, time: Timestamp) -> float:
        power_balance = self.battery.power_balance_step_mW
        self.update_statistics(time)
        spare_energy = 0
        t: Timestamp = None

        if power_balance <= 0:
            t = self.time_of_next_positive_powerbalance(time)
        else:
            t = self.time_of_last_positive_powerbalance_today(time)

        if t is None:
            return 0.

        g_kWh = self.energy_generation_from_to(time, t)
        c_kWh = self.energy_consumption_from_to(time, t)
        delta = g_kWh - c_kWh

        if power_balance <= 0:
            spare_energy = max(0, self.battery.current_charge_kWh + delta)
        else:
            battery_missing_energy = self.battery.capacity - self.battery.current_charge_kWh
            spare_energy = max(0, delta - battery_missing_energy)
        
        seconds = timestamp_to_timeinday(t).seconds
        spare_power = kWh_to_MW(seconds, spare_energy)
        return spare_power

    def update_statistics(self, time: Timestamp):
        if self.timestamp_last_step == None:
            self.timestamp_last_step = time
            self.last_positive_powerbalance_today = time
        elif self.timestamp_last_step.date() < time.date():
            td = timestamp_to_timeinday(self.last_positive_powerbalance_today)
            self.time_last_positive_powerbalance.add_sample(td)
            self.timestamp_last_step = time

        if self.battery.power_balance_step_mW > 0:
            only_time = timestamp_to_timeinday(time)
            self.last_positive_powerbalance_today = time

        self.__update_energy_predictor(time, self.battery.power_consumption_step_mW,
                                       self.energy_consumption_predictor
                                       )
        self.__update_energy_predictor(time, self.battery.power_generation_step_mW,
                                       self.energy_generation_predictor)

    def __update_energy_predictor(self, time: Timestamp, power: float,
                                  energy_predictor) -> None:
        timespan: Timedelta = energy_predictor.bin_width
        seconds = timespan.seconds
        energy = mW_to_kWh(power, seconds)
        energy_predictor.update(time, energy)
    
    
    def time_of_last_positive_powerbalance_today(self, time: Timestamp) -> Timestamp:
        t: Timedelta = self.time_last_positive_powerbalance.median()
        if t is None:
            return None
        last_pos_pb = timeinday_to_timestamp(t, time)        
        if last_pos_pb < time:
            return time
        return last_pos_pb + Timedelta(seconds=1)
        

    def time_of_next_positive_powerbalance(self, time) -> Timestamp:
        for i in range(24):
            t1: Timestamp = time + Timedelta(hours=i)
            t2: Timestamp = time + Timedelta(hours=(i+1))
            c = self.energy_consumption_from_to(t1, t2)
            g = self.energy_generation_from_to(t1, t2)
            if (g - c) > 0: 
                return t1

    def energy_consumption_from_to(self, frm: Timestamp, to: Timestamp):
        return self.energy_consumption_predictor.predict_energy_from_to(frm, to)

    def energy_generation_from_to(self, frm: Timestamp, to: Timestamp):
        return self.energy_generation_predictor.predict_energy_from_to(frm, to)


class EnergyPredictor():
    def __init__(self):
        """
        This is an interface defintion.
        Instances of this class should not be used.
        """
        pass

    def update(self, time: Timestamp, energy: float) -> None:
        raise EnergyPredictor.InterfaceNotImplementedError()

    def predict_energy_from_to(self, frm: Timestamp, to: Timestamp) -> float:
        raise EnergyPredictor.InterfaceNotImplementedError()

    class InterfaceNotImplementedError(Exception):
        def __init__(self, message="This Method is not implemented in the called Energy Predictor object."):
            super.__init__(self, message)


class RandomForecast(EnergyPredictor):
    """
    This class predicts energy consumption/generation by constructing empirical distributions
    for measurements. Each day of the weeks is divided into `24h/bin_width` time slots. Each
    of the time slots has its own empirical distribution.
    """

    def __init__(self, bin_width: Timedelta = Timedelta(hours=1)):
        self._distributions: List[EmpiricalDistribution] = []
        self.__init_distributions(bin_width)
        self.bin_width = bin_width

    def __init_distributions(self, bin_width: Timedelta):
        weekdays = 7
        timeslots_per_day = int(Timedelta(hours=24) / bin_width)
        self._distributions = []

        for _ in range(weekdays * timeslots_per_day):
            self._distributions.append(EmpiricalDistribution())

    def get_histogramm(self, date: Timestamp):
        day: int = date.dayofweek
        time_in_day = timestamp_to_timeinday(date)
        bin_in_day = int(time_in_day / self.bin_width)
        bins_per_day = Timedelta(days=1) // self.bin_width
        return self._distributions[(day * bins_per_day) + bin_in_day]

    def update(self, date: Timestamp, energy: float) -> None:
        hist = self.get_histogramm(date)
        hist.add_sample(energy)

    def predict_energy_from_to(self, frm: Timestamp, to: Timestamp):
        energy_prediction = 0.
        current_date = frm
        while current_date < to:
            hist = self.get_histogramm(current_date)
            energy_prediction += hist.get_rv()
            current_date += self.bin_width

        return energy_prediction


class EmpiricalDistribution():
    """
    Allows to sample from the empirical distribution.
    """

    def __init__(self):
        self._samples: List[T] = list()

    def add_sample(self, energy: T) -> None:
        self._samples.append(energy)
        self._samples.sort()

    def median(self) -> T:
        num_samples = len(self._samples)
        if num_samples >= 1:
            return self._samples[num_samples // 2]

    def get_rv(self) -> T:
        """
        Sample from the empirical distribution
        """
        n = len(self._samples)
        if n <= 1:
            if n == 0:
                return 0
            return self._samples[0]

        p = np.random.random()
        gamma = self.__interpolation_factor(p)
        index = self.__inverse_quantile_index(p)

        val1 = self._samples[index]
        val2 = self._samples[index + 1]

        return ((1-gamma) * val1) + (gamma * val2)

    def __inverse_quantile_index(self, prob: float) -> int:
        index = int(prob * (len(self._samples) - 1))
        return index

    def __interpolation_factor(self, p):
        n = len(self._samples)
        m = 1-p
        return (n * p + m) - np.floor(n*p + m)

