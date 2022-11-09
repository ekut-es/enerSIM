"""
File for preparing the database. Execute before the actual Simulation
"""
# To get household sim into path:
from pathlib import PurePath
import logging
import re
import math
import numpy as np
import warnings
from householdsim.timehandler import TimeHandler
from shutil import copyfile
from sqlite3 import Error
import sqlite3
import sys
sys.path.append("../householdsim")

ESTIMATION_THRESHOLD_IN_HOURS = 0.5
MAX_DAYS_PER_GAP = 15

MINUTES_PER_HOUR = 60
HOURS_PER_DAY = 24
SECONDS_PER_MINUTE = 60


POSSIBLE_STEP_SIZE = [1, 3, 15, 60]

# These column names are for an older version of the dataset.
# If you don't want to use them you can autodiscover other colums by setting auto_infer_cols to true
# in the DataPreparation __init__
COLS_TO_FILL = ['DE_KN_industrial1_grid_import', 'DE_KN_industrial2_grid_import', 'DE_KN_industrial3_grid_import',
                'DE_KN_residential1_grid_import', 'DE_KN_residential2_grid_import', 'DE_KN_residential3_grid_import',
                'DE_KN_residential4_grid_import', 'DE_KN_residential5_grid_import', 'DE_KN_residential6_grid_import',
                'DE_KN_industrial2_pv', 'DE_KN_public2_grid_import', 'DE_KN_residential1_pv', 'DE_KN_residential3_grid_export',
                'DE_KN_residential3_pv', 'DE_KN_residential4_grid_export', 'DE_KN_residential4_pv']


class DataPreparation:
    def __init__(self, database_path, new_database_path, step_size, auto_infer_cols=False):

        self.logger = logging.getLogger("DataPreparationLog")
        self.logger.addHandler(logging.StreamHandler())
        self.logger.setLevel(logging.INFO)
        copyfile(database_path, new_database_path)
        self.database_path = new_database_path
        self.step_size = step_size
        assert step_size in POSSIBLE_STEP_SIZE
        self.conn = self._create_connection(self.database_path)
        self.table_name = 'household_data_' + \
            str(self.step_size) + 'min_singleindex'

        self.hours_in_steps = MINUTES_PER_HOUR / self.step_size
        self.days_in_steps = self.hours_in_steps * HOURS_PER_DAY
        self.timeHandler = TimeHandler()
        self.colls_to_fill = COLS_TO_FILL
        if auto_infer_cols:
            self.logger.info(
                "Using Auto Infer Feature to determine the columns prepare.")
            self._infer_cols_to_fill()
        else:
            self.logger.info(
                "Using COLS_TO_FILL constant to determine the columns to prepare.")
            self.logger.info("Auto Inferring is possible")

    def _infer_cols_to_fill(self):
        cursor = self.conn.cursor()
        cursor.execute(
            f"SELECT sql FROM sqlite_master WHERE name = '{self.table_name}';")
        schema = cursor.fetchall()[0][0]
        schema = schema.split('\n')[1:]  # Removes Create Statement
        # Filter all columns depending on if they are "REAL" valued, since you can't interpolate TEXT
        f = filter(lambda x: "REAL," in x, schema)
        col_names = list(map(lambda x: re.findall(r'".+"', x)[0][1:-1], f))
        cursor.close()
        self.logger.debug("Inferred Columns to prepare: ")
        self.logger.debug(col_names)
        self.colls_to_fill = col_names

    def _drop_table(self, table_name):
        cursor = self.conn.cursor()
        try:
            cursor.execute("DROP TABLE " + table_name)
            print("Table dropped " + table_name)
            self.conn.commit()
        except sqlite3.OperationalError:
            print("No such Table, Skipping..")
        finally:
            cursor.close()

    def fill_missing_data(self):

        for col in self.colls_to_fill:
            areas_without_null = self._get_areas_without_null(col)
            for i in range(0, len(areas_without_null) - 1):
                current_area = areas_without_null[i]
                following_area = areas_without_null[i + 1]
                gap_start = current_area['end']
                gap_end = following_area['start']

                gap_duration_in_steps = self.timeHandler.get_time_duration(
                    gap_start, gap_end) / self.step_size
                if gap_duration_in_steps > MAX_DAYS_PER_GAP * self.days_in_steps:
                    self._log_gap_too_long_warning_message(
                        gap_duration_in_steps * self.step_size,
                        col,
                        'is too long to be handled!',
                        gap_start, gap_end
                    )
                elif gap_duration_in_steps > ESTIMATION_THRESHOLD_IN_HOURS * self.hours_in_steps:
                    self._fill_gap_with_copied_data(
                        gap_start, gap_end, col, areas_without_null)
                    print("Filled gap in col {col} from {start} to {end} with copied values.".format(
                        col=col, start=gap_start, end=gap_end))
                elif gap_duration_in_steps <= ESTIMATION_THRESHOLD_IN_HOURS * self.hours_in_steps:
                    self._fill_gap_with_estimation(gap_start, gap_end, col)
                    print("Filled gap in col {col} from {start} to {end} with estimated values.".format(col=col,
                                                                                                        start=gap_start,
                                                                                                        end=gap_end))

    # use linear function for estimation
    def _fill_gap_with_estimation(self, start_time, end_time, col_name):
        # look up value at start and end
        start_val = self._look_up_val_at_utc_time(col_name, start_time)
        end_val = self._look_up_val_at_utc_time(col_name, end_time)

        # calculate estimation function
        x = [self.timeHandler.utc_to_timestamp(
            start_time), self.timeHandler.utc_to_timestamp(end_time)]
        y = [start_val, end_val]
        coefficients = np.polyfit(x, y, 1)

        # fill every gap
        utc_time_rows = self._get_utc_times_between(start_time, end_time)
        for utc_time in utc_time_rows:
            # Linear function: f(x) = mx + b
            val = coefficients[0] * \
                self.timeHandler.utc_to_timestamp(
                    utc_time[0]) + coefficients[1]
            self._update_value(col_name, val, utc_time[0])

    def _get_utc_times_between(self, start_time, end_time):
        cur = self.conn.cursor()
        cur.execute("SELECT utc_timestamp "
                    "FROM {tablename} "
                    "WHERE utc_timestamp < '{endtime}' "
                    "AND utc_timestamp > '{starttime}'".format(tablename=self.table_name,
                                                               endtime=end_time,
                                                               starttime=start_time))
        rows = cur.fetchall()
        return rows

    def _update_value(self, col_name, val, utc_time):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE {table_name} "
                       "SET {col_name} = {val} "
                       "WHERE utc_timestamp = '{utc_time}'".format(table_name=self.table_name,
                                                                   col_name=col_name,
                                                                   val=str(
                                                                       val),
                                                                   utc_time=utc_time))
        self.conn.commit()
        cursor.close()

    def _look_up_val_at_utc_time(self, col_name, time):
        cur = self.conn.cursor()
        cur.execute("SELECT {col_name} "
                    "FROM {table_name} "
                    "WHERE utc_timestamp = '{time}' ".format(col_name=col_name,
                                                             table_name=self.table_name,
                                                             time=time))
        val = cur.fetchall()[0][0]
        cur.close()
        return val

    def _fill_gap_with_copied_data(self, start_time, end_time, col_name, areas_without_null):
        # calculate gap length
        gap_length = self.timeHandler.get_time_duration(start_time, end_time)
        # find nearest value block with at least that length
        possible_areas = self.get_possible_areas(
            areas_without_null, gap_length, start_time)
        if len(possible_areas) == 0:
            self._log_gap_too_long_warning_message(
                gap_length,
                col_name,
                'could not be filled, because there is no timespan that is\
                        long enough to use it to fill! ',
                start_time, end_time
            )
        else:
            # calculate which one is the nearest and take the nearest area
            is_min_area_after_gap, min_time_distance, nearest_area = self.get_nearest_copy_area(end_time,
                                                                                                possible_areas,
                                                                                                start_time)
            shift_limit_in_days = 15
            if min_time_distance / self.step_size > shift_limit_in_days * self.days_in_steps:
                warnings.warn('Gap with length ' + str(
                    gap_length / MINUTES_PER_HOUR / HOURS_PER_DAY) +
                    ' days in col ' + col_name + ' was filled by a timespan that is ' +
                    str(shift_limit_in_days) + ' away.'
                    'The filled data could not fit.'
                    'It starts at ' + start_time + ' and ends at ' + end_time)
            if is_min_area_after_gap:
                copy_area_start = nearest_area['start']
                copy_area_end = self.timeHandler.timestamp_to_utc(self.timeHandler.utc_to_timestamp(nearest_area['start'])
                                                                  + (gap_length * SECONDS_PER_MINUTE))
            else:
                copy_area_start = self.timeHandler.timestamp_to_utc(self.timeHandler.utc_to_timestamp(nearest_area['end'])
                                                                    - (gap_length * SECONDS_PER_MINUTE))
                copy_area_end = nearest_area['end']

            self.fill_values_in_col(
                col_name, copy_area_end, copy_area_start, end_time, start_time)

    def get_nearest_copy_area(self, end_time, possible_areas, start_time):
        is_min_area_after_gap = False
        min_time_distance = math.inf
        nearest_area = None
        for area in possible_areas:
            # area is before gap
            if self.timeHandler.is_later_or_equal(start_time, area['end']):
                area_distance = self.timeHandler.get_time_duration(
                    area['end'], start_time)
                is_current_area_after_gap = False
            # area is after gap
            else:
                area_distance = self.timeHandler.get_time_duration(
                    end_time, area['start'])
                is_current_area_after_gap = True

            if area_distance < min_time_distance:
                min_time_distance = area_distance
                nearest_area = area
                is_min_area_after_gap = is_current_area_after_gap
        return is_min_area_after_gap, min_time_distance, nearest_area

    def get_possible_areas(self, areas_without_null, gap_length, start_time):
        possible_areas = []
        # get all blocks in the same col with at least that length
        for area in areas_without_null:
            area_length = 0
            area_start = self._get_next_day_with_same_start_time(
                area['start'], start_time)
            if self.timeHandler.is_later_or_equal(area['end'], area_start):
                area_length = self.timeHandler.get_time_duration(
                    area_start, area['end'])
            if area_length >= gap_length:
                possible_areas.append(
                    {'start': area_start, 'end': area['end']})
        return possible_areas

    def fill_values_in_col(self, col_name, copy_area_end, copy_area_start, end_time, start_time):
        print("Used fill from {start} to {end}".format(
            start=copy_area_start, end=copy_area_end))
        utc_time_rows_copy_area = self._get_utc_times_between(
            copy_area_start, copy_area_end)
        utc_time_rows_fill_area = self._get_utc_times_between(
            start_time, end_time)
        assert len(utc_time_rows_fill_area) == len(utc_time_rows_copy_area)
        start_val_copy_area = self._look_up_val_at_utc_time(
            col_name, copy_area_start)
        start_val_fill_area = self._look_up_val_at_utc_time(
            col_name, start_time)
        end_val_copy_area = self._look_up_val_at_utc_time(
            col_name, copy_area_end)
        end_val_fill_area = self._look_up_val_at_utc_time(col_name, end_time)
        ratio = (end_val_fill_area - start_val_fill_area) / \
            (end_val_copy_area - start_val_copy_area)
        for i in range(0, len(utc_time_rows_copy_area)):
            copy_val = self._look_up_val_at_utc_time(
                col_name, utc_time_rows_copy_area[i][0])
            adapted_val = start_val_fill_area + \
                (copy_val - start_val_copy_area) * ratio
            self._update_value(col_name, adapted_val,
                               utc_time_rows_fill_area[i][0])

    def _get_next_day_with_same_start_time(self, utc_time, to_utc_time):
        date = utc_time.split("T")[0]
        time = to_utc_time.split("T")[1]
        result_utc = date + "T" + time
        if self.timeHandler.is_later_or_equal(result_utc, utc_time):
            return result_utc
        else:
            result_utc = self.timeHandler.timestamp_to_utc(self.timeHandler.utc_to_timestamp(result_utc)
                                                           + SECONDS_PER_MINUTE * MINUTES_PER_HOUR * HOURS_PER_DAY)
            return result_utc

    def _get_areas_without_null(self, col_name):
        cur = self.conn.cursor()
        cur.execute("SELECT  t.utc_timestamp "
                    "FROM {tablename} t "
                    "WHERE t.{colname} IS NOT NULL".format(tablename=self.table_name, colname=col_name))
        rows = cur.fetchall()
        cur.close()
        first_time = rows[0][0]
        start_time = first_time
        result = []
        for i in range(1, len(rows)):
            current_row = rows[i][0]
            end_time = current_row
            if self._is_time_jump(start_time, end_time):
                result.append({"start": first_time, "end": start_time})
                first_time = end_time
            elif i == len(rows) - 1:
                result.append({"start": first_time, "end": end_time})
            start_time = end_time
        return result

    @staticmethod
    def _create_table(conn, create_table_sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            c = conn.cursor()
            c.execute(create_table_sql)
            c.close()
        except Error as e:
            print(e)

    def _is_time_jump(self, start, end):
        start = self.timeHandler.utc_to_timestamp(start)
        end = self.timeHandler.utc_to_timestamp(end)
        jump = abs(end - start) > SECONDS_PER_MINUTE * self.step_size
        return jump

    @staticmethod
    def _create_connection(db_file):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except Error as e:
            print(e)
        return conn

    def _log_gap_too_long_warning_message(self, gap_length, col_name, details, start_time, end_time):
        self.logger.warn(
            'Gap with length ' + str(gap_length / MINUTES_PER_HOUR / HOURS_PER_DAY) +
            ' days in col ' + col_name + details +
            'It starts at ' + start_time + ' and ends at ' + end_time)


def main():
    database_path = PurePath("cosimulation_city_energy",
                             "simulation_data", "household_data_2020.sqlite")

    new_database_path = PurePath("cosimulation_city_energy",
                                 "simulation_data", "household_data_prepared_2020.sqlite")

    step_size = 60
    # create a database connection
    dataprepare = DataPreparation(
        str(database_path), str(new_database_path), step_size, auto_infer_cols=False)
    dataprepare.fill_missing_data()


if __name__ == '__main__':
    main()
