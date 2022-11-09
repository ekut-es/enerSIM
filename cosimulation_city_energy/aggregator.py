"""
A simple aggregator that sums up power consumption and production of household sim
"""

import collections
import pprint
import mosaik_api
import json
import datetime


import mosaik
from mosaik.util import connect_many_to_one


def connect_buildings_to_aggregator(world, sim_data_entities, aggregator):
    houses = [e for e in sim_data_entities if e.type in (
        'Householdsim_Consumer')]

    connect_many_to_one(world, houses, aggregator, 'power_consumption_mW')


def connect_PV_to_aggregator(world, sim_data_entities, aggregator):
    data_pvs = [e for e in sim_data_entities if e.type in ('Householdsim_PV')]
    connect_many_to_one(world, data_pvs, aggregator, ('power_generation_mW_PV', 'power_generation_mW' ))


def connect_prosumer_to_aggregator(world, sim_data_entities, aggregator):
    data_prosumers = [e for e in sim_data_entities if e.type in (
        'Householdsim_Prosumer')]
    
    for data_prosumer in data_prosumers:
        world.connect(data_prosumer, aggregator, ('power_generation_mW', 'power_generation_mW'),
                      ('power_consumption_mW', 'power_consumption_mW'))
    

META = {
    'models':{
        'PowerAggregator': {
            'public': True,
            'any_inputs':True,
            'params':[ 'start_time'],
            'attrs':['total_power_consumption_mW', 'total_power_generation_mW', 'time'],
        },
    },
}

class PowerAggregator(mosaik_api.Simulator):
    def __init__(self):
        super().__init__(META)
        self.eid = None
        self.data = collections.defaultdict(list)
        self.data
        self.step_size = None
     

    def init(self, sid, step_size):
        self.step_size = step_size
        return self.meta

    def create(self, num, model, start_time=None, file_output=False, output_format='json', name_pattern=None):
        if num > 1 or self.eid is not None:
            raise RuntimeError('Can only create one instance of Aggregator.')

        self.eid = 'Aggregator'

        return [{'eid': self.eid, 'type': model}]

    def step(self, time, inputs):
        data = inputs[self.eid]
        power_consumption_step = 0.
        power_generation_step = 0
        for attr, values in data.items():
            if attr == 'power_consumption_mW':
                power_consumption_step += sum(values.values())
            elif attr == 'power_generation_mW':
                power_generation_step += sum(values.values())
            elif attr == 'power_generation_mW_PV':
                power_generation_step += sum(values.values())
            else: 
                raise KeyError(f"Unknown Attribute: {attr}")
        self.data['total_power_consumption_mW'].append(power_consumption_step)
        self.data['total_power_generation_mW'].append(power_generation_step)
        

        return time + self.step_size


    def get_data(self, outputs):
        # Check if only this single Aggregator Instance is requested
        if len(outputs.keys()) != 1 or self.eid not in outputs.keys():
            raise KeyError(f"Either too many or wrong eids in requested outputs: {outputs}")
        attrs = outputs[self.eid]
        out_values = dict()
        for attr in attrs:
            out_values[attr] =  self.data[attr][-1]
        
        return {self.eid: out_values}



if __name__ == '__main__':
    mosaik_api.start_simulation(PowerAggregator())