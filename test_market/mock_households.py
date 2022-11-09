import logging
from typing import Dict, List

import mosaik_api


MINUTES_PER_HOUR = 60

logger = logging.getLogger('householdsim')
logger.addHandler(logging.StreamHandler())  # Just Print to console

META = {
    'models': {
        'MockHouseholds': {
            'public': True,
            'params': ['prosumer_profiles_consumption', 'prosumer_profiles_generation', 'consumer_profiles', 'pv_profiles'],
            'attrs': [],
        },
        'Householdsim_Consumer': {  # Household or industry without PV
            'public': False,
            'params': [],
            'attrs': ['power_consumption_mW'],  # in mW
        },
        'Householdsim_PV': {  # only PV
            'public': False,
            'params': [],
            'attrs': ['power_generation_mW_PV'],  # in mW #
        },
        'Householdsim_Prosumer': {  # Household or industry with PV
            'public': False,
            'params': [],
            'attrs': ['power_consumption_mW', 'power_generation_mW'],  # in mW
        },
    },
}

DATE_FORMAT = 'YYYY-MM-DDTHH:mm:ssZ'


class MockHouseholds(mosaik_api.Simulator):
    def __init__(self):
        super().__init__(META)
        self.consumer_profiles:  List[List[float]] = []
        self.prosumer_profiles_generation:  List[List[float]] = []
        self.prosumer_profiles_consumption:  List[List[float]] = []
        self.pv_profiles: List[List[float]] = []
        self.current_step = -1
        self.eid = None

    def init(self, sid, step_size):
        self.step_size = step_size
        return self.meta

    def create(self, num, model, prosumer_profiles_consumption=[], prosumer_profiles_generation=[],
               consumer_profiles=[], pv_profiles=[]):
        if num > 1 or self.eid is not None:
            raise RuntimeError(
                'Can only create one instance of SimpleBatterySim.')

        self.eid = 'MockHouseholds'

        self.prosumer_profiles_consumption = prosumer_profiles_consumption
        self.prosumer_profiles_generation = prosumer_profiles_generation
        self.consumer_profiles = consumer_profiles
        self.pv_profiles = pv_profiles

        children = list()
        assert len(prosumer_profiles_consumption) == len(
            prosumer_profiles_generation)
        for idx, _ in enumerate(prosumer_profiles_consumption):
            children.append({'eid': f'Householdsim_Prosumer{idx}',
                            'type': 'Householdsim_Prosumer'})

        for idx, _ in enumerate(pv_profiles):
            children.append({'eid': f'Householdsim_PV{idx}',
                            'type': 'Householdsim_PV'})

        for idx, _ in enumerate(consumer_profiles):
            children.append({'eid': f'Householdsim_Consumer{idx}',
                            'type': 'Householdsim_Consumer'})

        return [{'eid': self.eid, 'type': model, 'children': children}]

    def step(self, time, inputs):
        self.current_step += 1
        return time + self.step_size

    def get_data(self, outputs):
        out = dict()
        for eid, attrs in outputs.items():
            if 'Prosumer' in eid:
                idx = int(eid[-1])
                out[eid] = dict()
                for attr in attrs:
                    if attr == 'power_consumption_mW':
                        out[eid][attr] = self.prosumer_profiles_consumption[idx][self.current_step]
                    elif attr == 'power_generation_mW':
                        out[eid][attr] = self.prosumer_profiles_generation[idx][self.current_step]
                    else:
                        raise KeyError(
                            f"Unknown attribute for prosumer: {attr}")
            elif 'Consumer' in eid:
                idx = int(eid[-1])
                out[eid] = dict()
                for attr in attrs:
                    if attr == 'power_consumption_mW':
                        out[eid][attr] = self.consumer_profiles[idx][self.current_step]
                    else:
                        raise KeyError(
                            f"Unknown attribute for consumer: {attr}")
            elif 'PV' in eid:
                idx = int(eid[-1])
                out[eid] = dict()
                for attr in attrs:
                    if attr == 'power_generation_mW':
                        out[eid][attr] = self.pv_profiles[idx][self.current_step]
                    else:
                        raise KeyError(f"Unknown attribute: {attr}")

        return out


if __name__ == '__main__':
    mosaik_api.start_simulation(MockHouseholds())
