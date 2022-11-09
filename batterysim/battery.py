import collections
from enum import Enum
import pprint
from sqlite3.dbapi2 import Error
import mosaik_api
import json
import datetime
import mosaik
from mosaik.util import connect_many_to_one
from typing import ClassVar, Dict, Tuple, List
import re
import dataclasses
from pandas import Timestamp, Timedelta
from batterysim.power_forecast import  (PowerPredictorInterface, SparePowerPredictor)


META = {
    'models':{
        'BatterySim': {
            'public': True,
            'any_inputs':True,
            'params':['battery_configs', 'start'],
            'attrs':[],
        },
        'Battery' : {
            'public': False,
            'any_inputs': True,
            'params': ['capacity_kWh', 'initial_charge_kWh'],
            'attrs': ['current_charge_kWh', 'time_step_delta_mW', 'export_to_public_grid_mW', 'import_from_public_grid_mW','feed_in_to_home_mW', 'energy_demand_met']
            },
        'SmartBattery': {
            'public': False,
            'any_inputs': True,
            'params': ['capacity_kWh', 'initial_charge_kWh'],
            'attrs': ['current_charge_kWh', 'time_step_delta_mW', 'export_to_public_grid_mW', 'power_balance_mW',
                        'import_from_public_grid_mW','feed_in_to_home_mW', 'power_to_spare_mW',
                        'total_spare_power_step', 'consumer_demand_step']
        },

    },
}

class BatterySim(mosaik_api.Simulator):


    @dataclasses.dataclass
    class SingleBatteryConfig:
        capacity_kWh: float
        initial_charge_kWh: float
        smart_battery: bool

    
    BATTERY_EID_PREFIX = "BatterySim_Battery"
    EID_REGEX = re.compile(f"({BATTERY_EID_PREFIX})(\\d+)")

    SMART_BATTERY_EID_PREFIX = "BatterySim_SmartBattery"
    HOUSEHOLD_BATTERY_EID_PREFIX = "BatterySim_HouseholdBattery"

    def __init__(self):
        super().__init__(META)
        self.step_size = None
        self.eid = None
        self.household_batteries: List[DumbBattery] = list()
        self.smart_batteries: List[SmartBattery] = list()
        

    def init(self, sid, step_size):
        self.step_size = step_size
        return self.meta

        
    def create(self, num, model, battery_configs=dict(), start:str=None) -> object:

        if num > 1 or self.eid is not None:
            raise RuntimeError('Can only create one instance of BatterySim.')

        if start is not None:
            start = Timestamp(start)
        self.eid = 'BatterySim'
        SmartBattery.BATTERY_EID = self.eid

        index = 0
        children = []
        for  cfg_dict in battery_configs:
            cfg = BatterySim.SingleBatteryConfig(**cfg_dict)
            if cfg.smart_battery:
                continue
            eid = f"{BatterySim.HOUSEHOLD_BATTERY_EID_PREFIX}{index}"
            self.household_batteries.append(DumbBattery(eid=eid, capacity_kWh=cfg.capacity_kWh,
                                             initial_charge_kWh= cfg.initial_charge_kWh,
                                             step_size=self.step_size))
            
            children.append({
                'eid': eid,
                'type': 'Battery',
            })
            index += 1

        smart_battery_index = 0
        for  cfg_dict in battery_configs:
            cfg = BatterySim.SingleBatteryConfig(**cfg_dict)
            if cfg.smart_battery:
                eid = f"{BatterySim.SMART_BATTERY_EID_PREFIX}{smart_battery_index}"
                self.smart_batteries.append(SmartBattery(eid, start=start, step_size=self.step_size, capacity=cfg.capacity_kWh, current_charge=cfg.initial_charge_kWh))
                smart_battery_index += 1
                children.append({
                    'eid': eid,
                    'type': 'SmartBattery'
                })
    
        return [{
                'eid': self.eid,
                 'type': model,
                 'children': children
                 }]

    def step(self, time, inputs):
        self.simple_battery_step(inputs)
        self.smart_battery_step(time, inputs)

        return time + self.step_size
    

    def simple_battery_step(self, inputs):
        for battery in self.household_batteries:
            battery.step(inputs)

    def smart_battery_step(self, time, inputs):
        SmartBattery.calculate_energy_demand_consumers(inputs)
        for smart_battery in self.smart_batteries:
            smart_battery.calculate_energy_needs(time, inputs)
        for smart_battery in self.smart_batteries:
            smart_battery.calculate_power_distribution_step()
        

    @staticmethod
    def mW_to_kWh(power_mW: float, time_in_s:float) -> float:
        kW = power_mW * 1000
        kWh =  kW * (time_in_s / 3600.) 
        return kWh

    @staticmethod
    def kWh_to_MW(seconds, kWh):
        hours = seconds / (3600.)
        kW = kWh / hours
        W = kW * 1000
        MW = W / (1000 * 1000)
        return MW

    def get_data(self, outputs):
        out_values = dict()
        r1 = BatterySim.get_data_from_list(outputs, self.household_batteries)
        r2 = BatterySim.get_data_from_list(outputs, self.smart_batteries)

        out_values.update(r1)
        out_values.update(r2)
        
        return out_values
    
    @staticmethod
    def get_data_from_list(output_spec: Dict,  l: List[object] ) -> Dict:
        output_dict = dict()
        for battery_id, attrs in output_spec.items():
            battery = BatterySim.filter_list_by_eid(battery_id, l)
            if battery is not None:
                output_dict[battery_id] = battery.get_data(attrs)
        return output_dict
    
    @staticmethod
    def filter_list_by_eid(eid: str, entity_list: List[object]) -> object:
        """
            Returns None if object with eid is not in List
        """
        result = list(filter(lambda x, id=eid: x.eid == id, entity_list))
        if len(result) != 1:
            return None
        return result[0]

class DumbBattery:
    def __init__(self, eid, capacity_kWh, initial_charge_kWh, step_size):
        
        self.eid: str = eid
        self.battery_capacity_kWh:  float = capacity_kWh
        self.battery_charge_kWh: float = initial_charge_kWh
        self.time_step_delta_mW: float = 0
        self.export_to_public_grid_mW: float = 0
        self.feed_in_to_home_mW: float = 0
        self.import_from_public_grid_mW: float = 0. 
        self.energy_demand_met: bool =  True
        
        self.step_size = step_size
    
    
    def step(self, inputs): 
        
        data = inputs[self.eid]   
        power_consumption_step_mW = 0.
        power_generation_step_mW = 0.
        for attr, values in data.items():
            if attr == 'power_consumption_mW':
                    power_consumption_step_mW += sum(values.values())
            elif attr == 'power_generation_mW':
                    power_generation_step_mW += sum(values.values())
            elif attr == 'power_generation_mW_PV':
                    power_generation_step_mW += sum(values.values())
            else: 
                    raise KeyError(f"Unknown Attribute: {attr}")
            
        battery_charge_last_step_kWh = self.battery_charge_kWh
        battery_capacity_kWh = self.battery_capacity_kWh
            
        power_balance_mW =  power_generation_step_mW - power_consumption_step_mW
        energy_balance_kWh = BatterySim.mW_to_kWh(power_balance_mW, self.step_size)

        # Calculate new battery charge
        possible_charge_kWh = battery_charge_last_step_kWh + energy_balance_kWh
        possible_charge_kWh_positive = max(0.0, possible_charge_kWh)
        self.battery_charge_kWh = min(possible_charge_kWh_positive, battery_capacity_kWh)
            
        # Excess Energy goes to public grid
        possible_export_to_public_mW = BatterySim.kWh_to_MW(self.step_size, possible_charge_kWh - battery_capacity_kWh)
        self.export_to_public_grid_mW =  max(possible_export_to_public_mW, 0.)
            
        # Difference of stored Energy to last timestep
        time_step_delta_kWh = self.battery_charge_kWh - battery_charge_last_step_kWh
        self.time_step_delta_mW = BatterySim.kWh_to_MW(self.step_size, time_step_delta_kWh)

        self.energy_demand_met = (energy_balance_kWh > 0) or (abs(energy_balance_kWh) <= battery_charge_last_step_kWh)

        # Was it necessary to import Energy from the public grid?
        possible_import_from_public_grid = BatterySim.kWh_to_MW(self.step_size, abs(min(possible_charge_kWh + self.battery_charge_kWh, 0)))
        self.import_from_public_grid_mW = possible_import_from_public_grid

        # How much energy was transferred into the home
        self.feed_in_to_home_mW = abs(
            min(BatterySim.kWh_to_MW(self.step_size, time_step_delta_kWh), 
                0)
                )


    def get_data(self, attrs: List[str]) -> Dict[str, float]:
            battery_data = dict()
            for attr in attrs:
                val = self.get_attribute(attr)
                battery_data[attr] = val
            return battery_data

    def get_attribute(self, attr: str) -> float:
        val = None
        if attr == 'current_charge_kWh':
            val = self.battery_charge_kWh
        elif attr == 'time_step_delta_mW':
            val = self.time_step_delta_mW
        elif attr == 'export_to_public_grid_mW':
            val = self.export_to_public_grid_mW
        elif attr == 'feed_in_to_home_mW':
            val = self.feed_in_to_home_mW
        elif attr == 'energy_demand_met':
            val = self.energy_demand_met
        elif attr == 'import_from_public_grid_mW':
            val = self.import_from_public_grid_mW
        else:
            raise KeyError(f"Unknown Attribute for BatterySim.Battery: {attr}")
        return val

class SmartBattery:
    BATTERY_EID: str
    TOTAL_POWER_DEMAND_STEP_mW: float = 0.
    TOTAL_SPARE_POWER_STEP: float = 0.

    CLASS_POWER_PREDICTOR = SparePowerPredictor

    @staticmethod
    def set_power_predictor(clazz: ClassVar[PowerPredictorInterface]):
        SmartBattery.CLASS_POWER_PREDICTOR = clazz

    def __init__(self, eid, start: Timestamp, step_size, capacity, current_charge):
        self.eid: str = eid
        self.start: Timestamp = start
        self.step_size: int = step_size

        self.capacity: float = capacity
        self.current_charge_kWh = current_charge

        self.power_generation_step_mW = 0.0
        self.power_consumption_step_mW = 0.0
        self.power_balance_step_mW = 0.0

        self.spare_power_predictor: PowerPredictorInterface = SmartBattery.CLASS_POWER_PREDICTOR(self)
        self.spare_power_mW = 0.0
        self.power_to_consumers = 0.0

    @staticmethod 
    def calculate_energy_demand_consumers(inputs):
        SmartBattery.TOTAL_SPARE_POWER_STEP = 0.
        if SmartBattery.BATTERY_EID in inputs:
            data = inputs[SmartBattery.BATTERY_EID]
            for attr, values in data.items():
                if attr == 'power_consumption_mW':
                    power_demand = sum(values.values())
                    SmartBattery.TOTAL_POWER_DEMAND_STEP_mW =  power_demand
                else: 
                    raise KeyError(f"Unknown Attribute: {attr}")

    def calculate_energy_needs(self, time, inputs):

        self.update_power_generation_consumption_step(inputs)
        t = self.start + Timedelta(seconds=time)
        self.spare_power_mW = self.spare_power_predictor.predict_spare_power_mW(t)
        SmartBattery.TOTAL_SPARE_POWER_STEP = SmartBattery.TOTAL_SPARE_POWER_STEP + self.spare_power_mW
    
    def update_power_generation_consumption_step(self, inputs):
        data = inputs[self.eid]
        self.power_consumption_step_mW = 0.
        self.power_generation_step_mW= 0.
        for attr, values in data.items():
                if attr == 'power_consumption_mW':
                    self.power_consumption_step_mW += sum(values.values())
                elif attr == 'power_generation_mW':
                    self.power_generation_step_mW += sum(values.values())
                elif attr == 'power_generation_mW_PV':
                    self.power_generation_step_mW += sum(values.values())
                else: 
                    raise KeyError(f"Unknown Attribute: {attr}")
        self.power_balance_step_mW = self.power_generation_step_mW - self.power_consumption_step_mW

    def calculate_power_distribution_step(self):
        ratio_for_consumers_mW = None

        tsps = SmartBattery.TOTAL_SPARE_POWER_STEP
        if tsps == 0:
            ratio_for_consumers_mW = 0
        else:
            total_power_given = min(tsps, SmartBattery.TOTAL_POWER_DEMAND_STEP_mW)
            ratio_for_consumers_mW = (self.spare_power_mW /tsps) * total_power_given
        

        power_balance = self.power_balance_step_mW

        battery_charge_last_step_kWh = self.current_charge_kWh

        # Calculate new battery charge
        possible_battery_charge_kWh = self.current_charge_kWh + BatterySim.mW_to_kWh(power_balance, self.step_size)
        if possible_battery_charge_kWh < 0:
            self.import_from_public_grid_mW = abs(BatterySim.kWh_to_MW(self.step_size, possible_battery_charge_kWh))
            self.current_charge_kWh =  0
        else:
            self.import_from_public_grid_mW = 0
            self.current_charge_kWh = possible_battery_charge_kWh
        
        energy_for_consumers_kWh = BatterySim.mW_to_kWh(ratio_for_consumers_mW, self.step_size)
        if energy_for_consumers_kWh < self.current_charge_kWh:
            self.current_charge_kWh = self.current_charge_kWh - energy_for_consumers_kWh
            self.export_to_public_grid_mW = ratio_for_consumers_mW 
        else: 
            self.export_to_public_grid_mW = 0
            ratio_for_consumers_mW = 0
            energy_for_consumers_kWh = 0
        
        if self.current_charge_kWh > self.capacity:
            excess_export_kWh = self.current_charge_kWh - self.capacity
            self.current_charge_kWh = self.capacity
            self.export_to_public_grid_mW = self.export_to_public_grid_mW + BatterySim.kWh_to_MW(self.step_size, excess_export_kWh)
                
       
                
        # Difference of stored Energy to last timestep
        time_step_delta_kWh = self.current_charge_kWh - battery_charge_last_step_kWh
        self.time_step_delta_mW = BatterySim.kWh_to_MW(self.step_size, time_step_delta_kWh)

        # How much energy was transferred into the home
        delta_without_consumer_power = self.time_step_delta_mW + ratio_for_consumers_mW
        self.feed_in_to_home_mW = abs(min(delta_without_consumer_power, 0))
         
    def get_data(self, attrs: List[str]) -> dict:
        battery_data = dict()
        for attr in attrs:
                val = None
                if attr == 'current_charge_kWh':
                    val = self.current_charge_kWh
                elif attr == 'time_step_delta_mW':
                    val = self.time_step_delta_mW
                elif attr == 'export_to_public_grid_mW':
                    val = self.export_to_public_grid_mW
                elif attr == 'feed_in_to_home_mW':
                    val = self.feed_in_to_home_mW
                elif attr == 'import_from_public_grid_mW':
                    val = self.import_from_public_grid_mW
                elif attr== 'power_to_spare_mW':
                    val = self.spare_power_mW                    
                elif attr == 'power_balance_mW':
                    val = self.power_balance_step_mW
                elif attr == 'total_spare_power_step':
                    val = SmartBattery.TOTAL_SPARE_POWER_STEP
                elif attr == 'consumer_demand_step':
                    val = SmartBattery.TOTAL_POWER_DEMAND_STEP_mW
                else:
                    raise KeyError(f"Unknown Attribute for BatterySim.SmartBattery: {attr}")
                battery_data[attr] = val
        return battery_data
    
    def is_day(self, time) -> bool:
        current_time = self.start + Timedelta(value=time, unit="S")
        return (current_time.hour < 18) and (current_time.hour > 5)

    def time_until_night(self, time) -> int:
        return self.__time_until_hour( time, 18)

    def time_until_day(self, time):
        return self.__time_until_hour(time, 5)
    
    def __time_until_hour(self, time, hour):
        current_time = self.start + Timedelta(value=time, unit="S")
        target = current_time.replace(hour=hour)
        return (target - current_time).seconds


class UnbiasedExponentialMovingAverage():
    def __init__(self, alpha):
        self.alpha = alpha
        self.weighted_sum = 0    
        self.num_samples = 0
    
    def update_average(self, val: float) -> float:
        self.num_samples += 1
        self.weighted_sum = (self.weighted_sum * self.alpha) + val
        return self.get_current_average()
    
    def get_current_average(self) -> float:
        if self.num_samples == 0:
            return 0.
        return (1-self.alpha)/(1- (self.alpha **self.num_samples)) * self.weighted_sum

import numpy as np


def create_inputs(consumption, generation):
    factor = (10e-3)
    return {"sb": {
        'power_consumption_mW': {
            "a": consumption * factor
        },
        'power_generation_mW': {
           "a": generation * factor
        }
    }
    }
def generation(hour):
    if(hour < 5) or hour > 21:
        return 0.
    else:
        return np.sin((hour - 5)/16 * 3.14)
def consumption(hour):
    total_gen = sum(list(map(generation, range(24))))
  
    
    return (total_gen /2 ) / (4 * 24)
   
if __name__ == "__main__":
    mosaik_api.start_simulation(BatterySim())
