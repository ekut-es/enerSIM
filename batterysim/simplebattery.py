"""
An **oversimpliefied** Battery Simulation. Does not take relevant real world parameters into account.
These parameters would be:
    * maximal charging rate
    * maximal discharging rate

"""

import collections
import pprint
import mosaik_api
import json
import datetime
from mosaik.util import connect_many_to_one
from battery import BatterySim

META = {
    'models':{
        'SimpleBattery': {
            'public': True,
            'any_inputs':True,
            'params':['capacity', 'initial_charge'],
            'attrs':['current_charge', 'energy_demand_fulfilled'],
        },
    },
}

class SimpleBattery(mosaik_api.Simulator):
    def __init__(self):
        super().__init__(META)
        self.step_size = None
        self.eid = None
        self.current_charge_kWh = None
        self.capacity = None
        self.energy_demand_fulfilled = None
        self.data  = collections.defaultdict(list)
        

    def init(self, sid, step_size):
        self.step_size = step_size
        return self.meta

    def create(self, num, model, capacity=float('infinity'), initial_charge = 0.0):
        if num > 1 or self.eid is not None:
            raise RuntimeError('Can only create one instance of SimpleBatterySim.')

        self.eid = 'SimpleBattery'
        self.capacity = capacity
        self.current_charge_kWh = initial_charge
        return [{'eid': self.eid, 'type': model}]

    def step(self, time, inputs):
        data = inputs[self.eid]
        power_consumption_step = 0.
        power_generation_step = 0.
        for attr, values in data.items():
            if attr == 'power_consumption_mW':
                power_consumption_step += sum(values.values())
            elif attr == 'power_generation_mW':
                power_generation_step += sum(values.values())
            elif attr == 'power_generation_mW_PV':
                power_generation_step += sum(values.values())
            else: 
                raise KeyError(f"Unknown Attribute: {attr}")
        energy_balance_mW = power_generation_step - power_consumption_step
        energy_balance_kWh = BatterySim.mW_to_kWh(energy_balance_mW, self.step_size)

        self.energy_demand_fulfilled = energy_balance_mW > 0 or self.current_charge_kWh > abs(energy_balance_kWh)
        self.current_charge_kWh = max(0.0, self.current_charge_kWh + energy_balance_kWh)
        self.current_charge_kWh = min(self.current_charge_kWh, self.capacity)

        self.data['current_charge'].append(self.current_charge_kWh)
        self.data['energy_demand_fulfilled'].append(self.energy_demand_fulfilled)

        return time + self.step_size
        
    
    def get_data(self, outputs):
        # Check if only this single SimpleBattery Instance is requested
        if len(outputs.keys()) != 1 or self.eid not in outputs.keys():
            raise KeyError(f"Either too many or wrong eids in requested outputs: {outputs}")
        attrs = outputs[self.eid]
        out_values = dict()
        for attr in attrs:
            out_values[attr] =  self.data[attr][-1]
        
        return {self.eid: out_values}


if __name__ == '__main__':
    mosaik_api.start_simulation(SimpleBattery())