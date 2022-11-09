"""
A simple data collector that prints all data when the simulation finishes
"""

import collections
import pprint
import mosaik_api
import json
import datetime

from networkx.readwrite.graph6 import data_to_n

META = {
    'models': {
        'Monitor': {
            'public': True,
            'any_inputs': True,
            'params': ['file_output', 'output_format', 'name_pattern'],
            'attrs': [],
        },
    },
}


class Collector(mosaik_api.Simulator):
    def __init__(self):
        super().__init__(META)
        self.eid = None
        self.data = collections.defaultdict(lambda:
                                            collections.defaultdict(list))
        self.step_size = None
        self.file_output = None
        self.output_format = None
        self.name_pattern = None

    def init(self, sid, step_size):
        self.step_size = step_size
        return self.meta

    def create(self, num, model, file_output=False, output_format='json', name_pattern=None):
        if num > 1 or self.eid is not None:
            raise RuntimeError('Can only create one instance of Monitor.')

        self.eid = 'Monitor'
        self.file_output = file_output
        self.output_format = output_format
        self.name_pattern = name_pattern
        return [{'eid': self.eid, 'type': model}]

    def step(self, time, inputs):
        data = inputs[self.eid]
        for attr, values in data.items():
            for src, value in values.items():
                self.data[src][attr].append(value)

        return time + self.step_size

    def finalize(self):
        print('Collected data:')
        sim = "asf"
        now = datetime.datetime.now()
        print(json.dumps(self.data, indent=4))
        if self.file_output:

            prefix = f"{sim}_{str(now)}" if \
                self.name_pattern is None else self.name_pattern
            filename = f"{prefix}.{self.output_format}"
            with open(filename, "w") as f:
                json.dump(self.data, f, indent=4)


if __name__ == '__main__':
    mosaik_api.start_simulation(Collector())
