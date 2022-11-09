import csv
import mosaik_api
import pandas as pd
from batterysim.battery import BatterySim

META = {
    'models': {
        'prosumer': {
            'public': True,
            'params': ['csv_path', 'pids', 'start_time'],
            'attrs': ['power_consumption_mW', 'power_generation_mW'],
        },

    },
}


class CSVConnector(mosaik_api.Simulator):
    def __init__(self):
        super().__init__(META)

        self.time = None
        self.step_size = None

        self._entities = {}
        self.csv_readers = dict()
        self.last_lines = dict()

    def init(self, sid, step_size):
        self.step_size = step_size
        self.model = None
        return self.meta

    def create(self, num, model,  csv_path=[], pids=[], start_time=None):

        self.model = model

        self.time = pd.Timestamp(start_time, tz="UTC")
        models = []
        children = []

        assert len(csv_path) == len(pids)
        for pid, path in zip(pids, csv_path):
            models.append({
                'eid': pid,
                'type': 'prosumer',
                'rel': []
            })
            self.csv_readers[pid] = (csv.DictReader(open(path, "r")))

        for eid, reader in self.csv_readers.items():

            line = next(reader)
            assert pd.Timestamp(line['utc_timestamp']) <= self.time
            start_date_file = line['utc_timestamp']
            while pd.Timestamp(line['utc_timestamp']) + pd.Timedelta(5, unit="min") < self.time:
                line = next(reader)
            print("CSV Reader: Skipped entries from %s to %s" %
                  (start_date_file, line['utc_timestamp']))
            self.last_lines[eid] = line
        return models

    def step(self, time, inputs):
        self.time = self.time + pd.Timedelta(self.step_size, unit="S")
        return time + self.step_size

    def get_data(self, outputs):
        data = {}

        for eid, attrs in outputs.items():
            values = self.last_lines[eid]
            for attr in attrs:
                if attr == "power_generation_mW":
                    pv_mw = BatterySim.kWh_to_MW(
                        self.step_size, float(values["pv"]))
                    data.setdefault(eid, {})[attr] = pv_mw
                elif attr == "power_consumption_mW":
                    consumption_mw = BatterySim.kWh_to_MW(self.step_size, float(
                        values["consumption"]))
                    data.setdefault(eid, {})[attr] = consumption_mw
                else:
                    raise KeyError("Unkown Attribute: %s" % (attr, ))

            self.last_lines[eid] = next(self.csv_readers[eid])

        return data


def main():
    return mosaik_api.start_simulation(CSVConnector())


if __name__ == '__main__':
    main()
