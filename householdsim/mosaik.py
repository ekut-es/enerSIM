"""
The interface for the household Simulation
"""

import logging

import mosaik_api
from simulationdatabaseconnection import SimulationDatabaseConnection
from timehandler import TimeHandler


MINUTES_PER_HOUR = 60

logger = logging.getLogger('householdsim')
logger.addHandler(logging.StreamHandler())  # Just Print to console

META = {
    'models': {
        'householdsim': {
            'public': True,
            'params': ['num_of_consumer', 'num_of_PV', 'num_of_prosumer',
                       'data_base_path', 'start_time', 'consumption_profiles', 'household_pv_profiles', 'pv_plant_profiles',
                       'import_data', 'export_data', 'table_name'],
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
CONSUMER_PROFILES = ['DE_KN_residential1',
                     'DE_KN_residential2', 'DE_KN_residential5']
MOD = [{'name': 'MOD_+0min_+0%', 'min_offset': 0, 'scaling': 0},
       {'name': 'MOD_+30min_+10%', 'min_offset': 30, 'scaling': 10},
       {'name': 'MOD_+15min_-5%', 'min_offset': 15, 'scaling': -5},
       {'name': 'MOD_-15min_+5%', 'min_offset': -15, 'scaling': 5},
       {'name': 'MOD_-30min_+2%', 'min_offset': -30, 'scaling': 2}]
PV_PROFILES = ['DE_KN_residential1_pv', 'DE_KN_residential3_pv',
               'DE_KN_residential4_pv', 'DE_KN_industrial2_pv']
PROSUMER_PV_PROFILES = ['DE_KN_residential1_pv',
                        'DE_KN_residential3_pv', 'DE_KN_residential4_pv']


class HouseholdSim(mosaik_api.Simulator):
    def __init__(self):
        super().__init__(META)
        self.net = None
        self.step_size = None
        self.model = None
        self.time = None

        self._entities = {}
        self.simulation_data_base_conn = None
        self._name_dict = {}
        self._mod_dict = {}
        self.consumption_data = {}
        self.production_data = {}

    def init(self, sid, step_size):
        self.step_size = step_size
        logger.debug('Step_size:' + str(step_size))
        return self.meta

    def create(self, num, model, num_of_consumer=0, num_of_PV=0, num_of_prosumer=0,
               data_base_path="",
               consumption_profiles=CONSUMER_PROFILES, household_pv_profiles=PROSUMER_PV_PROFILES, pv_plant_profiles=PV_PROFILES,
               start_time=None, import_data=None, export_data=None, table_name=None):
        """
        TODO: Explain Parameters
        TODO: Every Dataset should have a **kwargs dict that can be used to configure the household sim
        data_base_path: Str | Path: Path to an Sqlite3 Database file.
        consumption_profiles: List[str]: The **prefix** of the column names that contain consumption data. the postfixes of import_data get appended to it.
        household_pv_profiles: List[str]: The **full** column names for the PV-Production columns. 
        pv_plant_profiles: List[str]: Full names of columns containing pv plant generation data.
        import_data: List[str]: The postfixes of column names that contain imported power.
        export_data: List[str]: The postfixes of column names that contain exported power.
        table_name: SQL Table-Name from which values should be read. 
        """
        if num > 1:
            raise self.HouseholdSimException(
                "Can only create one instance of householdsim", None)

        if model != 'householdsim':
            raise self.HouseholdSimException(
                "Model \"%s\" does not exist! Use householdsim Model" % (model, ), None)
        try:
            seconds_per_minute = 60
            self.simulation_data_base_conn = SimulationDatabaseConnection(
                self.step_size // seconds_per_minute, data_base_path, import_data=import_data,
                export_data=export_data, table_name=table_name)
        except Exception as e:
            raise self.HouseholdSimException(
                "Could not establish Database connection", e)

        self.import_data = ['grid_import'] if not import_data else import_data
        self.export_data = ['pv'] if not export_data else export_data

        self.consumption_profiles = consumption_profiles
        self.household_pv_profiles = household_pv_profiles
        self.pv_plant_profiles = pv_plant_profiles

        self.model = model
        self.time = TimeHandler.normal_utc_to_datetime(start_time)
        models = []
        children = []

        consumer_profile_counter = 0
        consumer_mod_counter = 0

        for i in range(num_of_consumer):
            children.append({
                'eid': 'householdsim_consumer_' + str(i),
                'type': 'Householdsim_Consumer',
                'rel': []
            })
            self._name_dict['householdsim_consumer_' +
                            str(i)] = self.consumption_profiles[consumer_profile_counter]
            self._mod_dict['householdsim_consumer_' +
                           str(i)] = MOD[consumer_mod_counter]

            if consumer_profile_counter == (len(self.consumption_profiles) - 1):
                consumer_mod_counter = (consumer_mod_counter + 1) % len(MOD)

            consumer_profile_counter = (
                consumer_profile_counter + 1) % len(self.consumption_profiles)

        pv_profile_counter = 0
        for i in range(num_of_PV):
            children.append({
                'eid': 'householdsim_pv_' + str(i),
                'type': 'Householdsim_PV',
                'rel': []
            })
            self._name_dict['householdsim_pv_' +
                            str(i)] = self.pv_plant_profiles[pv_profile_counter]
            pv_profile_counter = (pv_profile_counter +
                                  1) % len(self.pv_plant_profiles)

        prosumer_profile_load_count = 0
        prosumer_profile_pv_count = 0
        prosumer_mod_counter = 0
        for i in range(num_of_prosumer):

            prosumer_profile = self.consumption_profiles[prosumer_profile_load_count]
            eid = 'householdsim_prosumer_' + str(i) + "_" + prosumer_profile
            children.append({
                'eid': eid,
                'type': 'Householdsim_Prosumer',
                'rel': []
            })
            self._name_dict[eid +
                            "pv"] = self.household_pv_profiles[prosumer_profile_pv_count]
            self._name_dict[eid + "load"] = prosumer_profile
            self._mod_dict[eid + "load"] = MOD[prosumer_mod_counter]

            if prosumer_profile_load_count == len(PROSUMER_PV_PROFILES) - 1:
                prosumer_mod_counter = (prosumer_mod_counter + 1) % len(MOD)
            prosumer_profile_load_count = (
                prosumer_profile_load_count + 1) % len(self.consumption_profiles)
            prosumer_profile_pv_count = (
                prosumer_profile_pv_count + 1) % len(self.household_pv_profiles)

        models.append({
            'eid': self.model,
            'type': model,
            'rel': [],
            'children': children,
        })

        return models

    def step(self, time, inputs):
        self._increase_time()
        return time + self.step_size

    def get_data(self, outputs):
        data = {}
        for eid, attrs in outputs.items():
            for attr in attrs:

                if attr == 'power_consumption_mW' and 'consumer' in eid:
                    key = self.import_data[0]
                    mod_info = self._mod_dict[eid]
                    time_offset = mod_info['min_offset']
                    scaling = mod_info['scaling']
                    new_time = self._mod_time(self.time, time_offset)
                    query = self.simulation_data_base_conn.get_power_consumption(
                        self._name_dict[eid], new_time)
                    consumption_in_kwh = None
                    try:
                        consumption_in_kwh = query[key]
                    except KeyError as e:
                        raise self.HouseholdSimException(
                            """Could not retreive power consumption values.
                             Check if column exists or if simulation time is out of bounds""", e)
                    consumption_in_MW = self.kWh_to_MW(
                        self.step_size, consumption_in_kwh)
                    scaled_consumption_in_MW = consumption_in_MW * \
                        ((100 + scaling) / 100)
                    data.setdefault(eid, {})[attr] = scaled_consumption_in_MW
                if attr == 'power_generation_mW_PV' and 'pv' in eid:
                    generation_in_kwH = self.simulation_data_base_conn.get_pv_power(
                        self._name_dict[eid], self.time)
                    generation_in_MW = self.kWh_to_MW(
                        self.step_size, generation_in_kwH)
                    data.setdefault(eid, {})[attr] = generation_in_MW
                if attr == 'power_generation_mW' and 'prosumer' in eid:
                    generation_in_kwH = self.simulation_data_base_conn.get_pv_power(
                        self._name_dict[eid + "pv"], self.time)
                    generation_in_MW = self.kWh_to_MW(
                        self.step_size, generation_in_kwH)
                    data.setdefault(eid, {})[attr] = generation_in_MW
                if attr == 'power_consumption_mW' and 'prosumer' in eid:
                    key = self.import_data[0]
                    mod_info = self._mod_dict[eid + "load"]
                    time_offset = mod_info['min_offset']
                    scaling = mod_info['scaling']
                    new_time = self._mod_time(self.time, time_offset)
                    consumption_in_kwh = self.simulation_data_base_conn.get_power_consumption(
                        self._name_dict[eid + "load"], new_time)[key]
                    consumption_in_MW = self.kWh_to_MW(
                        self.step_size, consumption_in_kwh)
                    scaled_consumption_in_MW = consumption_in_MW * \
                        ((100 + scaling) / 100)
                    data.setdefault(eid, {})[attr] = scaled_consumption_in_MW
        return data

    def _increase_time(self):
        seconds_per_minute = 60
        step_size_in_minutes = self.step_size // seconds_per_minute
        self.time = TimeHandler.add_minutes(self.time, step_size_in_minutes)

    @staticmethod
    def _mod_time(time, min_offset):
        return TimeHandler.add_minutes(time, min_offset)

    @staticmethod
    def kWh_to_MW(seconds, kWh):
        kW = kWh * MINUTES_PER_HOUR / (seconds / 60)
        MW = kW / 1000
        return MW

    class HouseholdSimException(Exception):
        def __init__(self, message, errors):
            super().__init__(message)
            self.errors = errors
            print("During the Householdsim some errors occured:")
            print(errors)


def main():
    return mosaik_api.start_simulation(HouseholdSim())


if __name__ == '__main__':
    main()
