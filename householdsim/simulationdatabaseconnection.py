"""
Connects to the database for the simulations
"""

from pathlib import PurePath
import sqlite3
from sqlite3 import Error
from sqlite3 import OperationalError
from datetime import datetime, timezone
from timehandler import TimeHandler
from os.path import exists

COSSMIC_ENERGY_IMPORT_DATA = ["grid_import",
                              #  "storage_charge",
                              #  "storage_decharge",
                              #   "area_offices",
                              #  "area_room_chemistry",
                              #  "area_room_clean",
                              #  "area_room_printing1",
                              #  "area_room_printing2",
                              #  "compressor",
                              #  "cooling_aggregate",
                              #  "cooling_pumps",
                              # "dishwasher",
                              #  "ev",
                              #  "facility_cvd_centrotherm",
                              #  "diffusion_centrotherm",
                              #  "diffusion_tecnofirmes",
                              #  "furnace_centrotherm",
                              #  "indus_rena",
                              #  "storage_charge",
                              # "refrigerator",
                              #  "ventilation",
                              # "freezer",
                              # "heat_pump",
                              # "washing_machine",
                              # "circulation_pump",
                              #  "heating",
                              #  "refrigerator"
                              ]
COSSMIC_ENERGY_EXPORT_DATA = ["grid_export",
                              # "pv",
                              #  "pv_1",
                              #  "pv_2",
                              #  "pv_facade",
                              #  "storage_decharge",
                              #  "pv_roof"
                              ]


class SimulationDatabaseConnection:

    def __init__(self, step_size, database_path, import_data=None, export_data=None, table_name=None):
        """
        Initializes the database connection.
        Params
        step_size: int, the step size of the simulation in seconds
        database_path: str/PurePath/Path.. , path to the sqlite3 File containing the data
        import_data: List[str], the postfixes for the columnnames that indicate power consumption. Defaults to COSSMIC_ENERGY_IMPORT_DATA
        export_data: List[str], the postfixes for the columnnames that indicate power generation. Defaults to COSSMIC_ENERGY_EXPORT_DATA
        table_name: str, Name of the table to select data from. If not specified, the programm selects a table name fitting for cossmic data
        """
        self.step_size = step_size
        self.connection = self._create_connection(database_path)
        # https://data.open-power-system-data.org/household_data/
        self.data_import = import_data if import_data else COSSMIC_ENERGY_IMPORT_DATA
        self.data_export = export_data if export_data else COSSMIC_ENERGY_EXPORT_DATA
        self.table_name = table_name

        if not exists(database_path):
            raise self.SimConnectionError(
                "The given path to database '%s' does not exist" % (database_path, ))
        self.error_if_table_not_exists()

    def error_if_table_not_exists(self):
        cursor = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table';")

        tables = cursor.fetchall()
        if not self._select_table() in map(lambda x: x[0], tables):
            raise self.SimConnectionError(
                "The given table name '%s' does not exist in the database. Existing tables are: %s" % (self._select_table(), tables))

    @staticmethod
    def _create_connection(db_file):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None

        conn = sqlite3.connect(db_file)

        return conn

    def get_power_consumption(self, name, end_datetime):
        utc_end_datetime = TimeHandler.datetime_to_utc(end_datetime)
        power_consumption = self._get_power_dictionary(
            name, utc_end_datetime, self.data_import)

        return power_consumption

    def get_power_generation(self, name, end_datetime):
        utc_end_datetime = TimeHandler.datetime_to_utc(end_datetime)
        power_generation = self._get_power_dictionary(
            name, utc_end_datetime, self.data_export)

        return power_generation

    def get_pv_power(self, name, end_datetime):
        utc_end_timestamp = TimeHandler.datetime_to_utc(end_datetime)
        utc_start_timestamp = self._calculate_starttime(utc_end_timestamp)
        table_name = self._select_table()

        generation_rows = self._select_row("", "", table_name, utc_start_timestamp, utc_end_timestamp,
                                           full_col_name=name)

        if len(generation_rows) > 0:
            generation = self._diff_rows(generation_rows)
            return generation
        return 0

    def _get_power_dictionary(self, name, utc_end_timestamp, data):
        table_name = self._select_table()
        # return all cols that correspond with the name and display the power in a specific time span
        # calculate start time
        utc_start_timestamp = self._calculate_starttime(utc_end_timestamp)
        power = self._get_power_values(name,
                                       table_name,
                                       utc_end_timestamp,
                                       utc_start_timestamp,
                                       data)
        return power

    def _get_power_values(self, name, table_name, utc_end_timestamp, utc_start_timestamp, data):
        power_consumption = {}
        # TODO: Perform single query for all data? Or will this not work if not every
        # Household has every column?
        for kind in data:
            consumption_rows = self._select_row(
                name, kind, table_name, utc_start_timestamp, utc_end_timestamp)
            if len(consumption_rows) > 0:
                consumption = self._diff_rows(consumption_rows)
                power_consumption[kind] = consumption
        return power_consumption

    def _select_table(self):
        # choose corresponding table
        # 1-2 minutes and every different not mentions step size -> 1 minute table
        # 3, 6, 9, 12 minutes -> 3 minute table
        # 15, 30, 45 minutes -> 15 minute table
        # 1, ... ,n hours -> 60 minutes table

        if self.table_name:
            return self.table_name

        if self.step_size % 60 == 0:
            table_name = 'household_data_60min_singleindex'
        elif self.step_size % 15 == 0:
            table_name = 'household_data_15min_singleindex'
        elif self.step_size % 3 == 0:
            table_name = 'household_data_3min_singleindex'
        else:
            table_name = 'household_data_1min_singleindex'
        self.table_name = table_name
        return table_name

    def _select_row(self, name, kind, table_name, start_time, end_time, full_col_name=""):
        try:
            if full_col_name == "":
                col_name = name + "_" + kind
            else:
                col_name = full_col_name
            cur = self.connection.cursor()
            cur.execute("SELECT {col_name} "
                        "FROM {tablename} "
                        "WHERE utc_timestamp <= '{endtime}' "
                        "AND utc_timestamp >= '{starttime}'".format(col_name=col_name,
                                                                    tablename=table_name,
                                                                    endtime=end_time,
                                                                    starttime=start_time))

            rows = cur.fetchall()
            if rows is None or len(rows) == 0:

                cur.execute("SELECT min(utc_timestamp), max(utc_timestamp) from {tablename}".format(
                    tablename=table_name))
                result = cur.fetchone()
                min_ts, max_ts = None, None
                if result:
                    min_ts, max_ts = result
                raise self.SimConnectionError(
                    "Queried between timestamps: %s and %s. This is not in utc_timestamps column! Is it between min/max timestamp in series: %s and %s ?" % (start_time, end_time, min_ts, max_ts),)
            if (rows[0])[0] is None or (rows[len(rows) - 1])[0] is None:
                Warning.warn("_select_row got no Values in {colname} "
                             "from {start_time} to {end_time}".format(colname=col_name,
                                                                      start_time=start_time,
                                                                      end_time=end_time))
        except OperationalError as oe:
            raise self.SimConnectionError(
                "Error during operation of the database connection.", str(oe))
        return rows

    @staticmethod
    def _diff_rows(rows):
        if len(rows) == 0:
            return 0

        start_row_val = (rows[0])[0]
        end_row_val = (rows[len(rows)-1])[0]
        if start_row_val is None or end_row_val is None:
            Warning.warn("Null diff!")
            row_diff = 0
        else:
            row_diff = abs(start_row_val - end_row_val)
        return row_diff

    def _calculate_starttime(self, utc_end_timestamp):
        end_timestamp = TimeHandler.utc_to_timestamp(utc_end_timestamp)
        start_timestamp = TimeHandler.add_minutes_to_timestamp(
            end_timestamp, -self.step_size)
        utc_start_timestamp = TimeHandler.timestamp_to_utc(start_timestamp)
        return utc_start_timestamp

    class SimConnectionError(Exception):
        def __init__(self, message, *errors) -> None:
            super().__init__(message, ("The folloing reasons were given:" +
                                       ", ".join(errors)) if errors else " ")
            self.errors = errors


def main():  # test
    database_connection = SimulationDatabaseConnection(
        60*24*30, r"../cosimulation_city_energy/simulation_data/household_data.sqlite")  # step size = 30 days
    end_datetime = datetime(2016, 5, 19, 19, 0, 0, tzinfo=timezone.utc)
    consumption_dictionary = database_connection.get_power_consumption(
        "DE_KN_residential3", end_datetime)  # "2016-05-19T19:00:00Z")
    generation_dictionary = database_connection.get_power_generation(
        "DE_KN_residential3", end_datetime)  # "2016-05-19T19:00:00Z")
    print(consumption_dictionary)
    print("--------------------")
    print(generation_dictionary)

    grid_import = consumption_dictionary["grid_import"]
    devices = consumption_dictionary["dishwasher"] \
        + consumption_dictionary["refrigerator"]  \
        + consumption_dictionary["freezer"] \
        + consumption_dictionary["washing_machine"]  \
        + consumption_dictionary["circulation_pump"]
    print(f"grid_import = {grid_import}")
    print(f"devices = {devices}")
    own_consumption = generation_dictionary["pv"] - \
        generation_dictionary["grid_export"]
    print(f"own_consumption = {own_consumption}")
    return 0


def test_htw_berlin_data():
    database_connection = SimulationDatabaseConnection(60,
                                                       "/home/flo/Workspace/Uni/Masterarbeit/simulation/additional_datasets/htw_berlin/dataset.sql",
                                                       import_data=["consumption"], export_data=["pv"], table_name="htw_berlin_dataset")

    end_datetime = datetime(2010, 5, 19, 19, 0, 0, tzinfo=timezone.utc)

    prosumer_name = "prosumer%d" % (1, )
    consumption_dictionary = database_connection.get_power_consumption(
        prosumer_name, end_datetime)  # "2016-05-19T19:00:00Z")
    generation_dictionary = database_connection.get_power_generation(
        prosumer_name, end_datetime)  # "2016-05-19T19:00:00Z")
    print(consumption_dictionary)
    print("--------------------")
    print(generation_dictionary)


if __name__ == '__main__':
    test_htw_berlin_data()
