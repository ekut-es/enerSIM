"""
Running this file as a script creates the htw_berlin_dataset. See the documentation of the main function.
You should run this script from the root of the simulation repo. 
    If you can't/don't want to do this, then you need to change the `base_dir` variable or specify the -w flag. 

Modifications:
- The Name of the Database file can be changed with `DATABASE_FILE_NAME`
- The Name of the SQL Table can be changed with `SQL_TABLE_NAME`

**HOUSEHOLD_SIM_KWARGS**: You can use the `householdsim` to connect this dataset with mosaik.
```
hhsim = world.start('HouseholdSim', step_size=step_size)
sim_data_entities = hhsim.householdsim(num_of_consumer=<num_consumers>, num_of_PV=<num_of_PV>,
                                           num_of_prosumer=<num_of_prosumers>,
                                           start_time=<START>,  **HOUSEHOLD_SIM_KWARGS).children
```

Creates dataset.csv from {PL1, PL2, PL3}.csv and time_datevec_MEZ.csv.
You may need alter the base_dir (or specify -w flag.) variable depending on your setup.

`create_production_and_consumption_df`  creates a pandas DataFrame that contains the energy consumption and production of several households.
The output DataFrame has an UTC Datetime Index. The energy data is in Kilowatthours and cumulative. This means that not the energy used/produced in a
given timestep is shown but the running total.  This ensures compatibility with the Householdsimulation. Attention: The first entry is not necessariliy  0.
PV Production Data is taken from PVGIS. You can query online or use local *.json files.

The Size of the installed wp is estimated from the yearly consumption. It is randomly chose to be between 1.5 and 3.0 times the
amount of the energy usage in kWh. [Source](https://www.photovoltaik.info/wp-content/uploads/2018/12/faustformeln-photovoltaik.pdf)
So if the Energy usage is 3000kWh Per year, at most 9000 Wp (9kWp) of PV are installed. 
"""
from email.mime import base
import getopt
import sys

from numpy.random import rand
import sqlite3
import time
import pandas as pd
from pathlib import PurePath
from os.path import abspath
from additional_datasets.create_pv_profiles.create_pv_profiles import query_pvgis, random_coords_around_center, create_dataframe, interpolate_minutely, koordinaten, to_cumulative_kWh
import json
import logging
from sqlalchemy import create_engine
import os

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


HOUSEHOLD_NAME_FMT = "prosumer%d"
PV_NAME_FMT = HOUSEHOLD_NAME_FMT + "_pv"
CONSUMPTION_NAME_FMT = HOUSEHOLD_NAME_FMT + "_consumption"

SQL_TABLE_NAME = "htw_berlin_dataset"
DATABASE_FILE_NAME = "htw_dataset.sql"

# If you're executing the code from the root of the simulation repo, this will give
# you the correct directory.
base_dir = PurePath(".", "additional_datasets", "htw_berlin")

DATABASE_FILE_PATH = abspath(PurePath(base_dir, DATABASE_FILE_NAME))
# Keyword arguments for the Household Sim to work with a dataset created by this script.
HOUSEHOLD_SIM_KWARGS = {
    'data_base_path': DATABASE_FILE_PATH,
    'consumption_profiles': [HOUSEHOLD_NAME_FMT % (i, ) for i in range(74)],
    'household_pv_profiles': [PV_NAME_FMT % (i, ) for i in range(74)],
    'table_name': SQL_TABLE_NAME,
    'import_data': ['consumption'],
    'export_data': ["pv"],
    'pv_plant_profiles': [],
}


power_phases = [PurePath(base_dir, "PL%d.csv" % i) for i in range(1, 4)]


def create_index(n_rows=None) -> pd.Series:
    df: pd.DataFrame = pd.read_csv(PurePath(base_dir, "time_datevec_MEZ.csv"),
                                   nrows=n_rows, header=None)
    index_size = df.index.size
    first_date = df.iloc[0]
    first_ts = __ts_from_vec_row(first_date)

    return pd.date_range(start=first_ts, periods=index_size, freq=__ts_from_vec_row(df.iloc[1]) - first_ts)


def __ts_from_vec_row(row) -> pd.Timestamp:
    ts = pd.Timestamp(
        year=row[0], month=row[1], day=row[2], hour=row[3], minute=row[4], second=row[5], tz="CET")
    return pd.Timestamp.tz_convert(ts, "UTC")


def read_phases(n_rows=None):
    index = create_index()
    dfs = [pd.read_csv(phase,  header=None, nrows=n_rows, engine="c",)
           for phase in power_phases]
    for df in dfs:
        df.set_index(index.copy(deep=True), inplace=True)

    return dfs


def create_consumption_df(prosumers=None, num_rows=None):
    t = time.time()
    phases = read_phases(n_rows=num_rows)
    logger.debug("Reading Phases took %.2f" % (time.time()-t))
    numpy_phases = [p.to_numpy() for p in phases]
    total_power = sum(numpy_phases)
    df_energy = pd.DataFrame(data=(total_power / 60.) /
                             1000., index=create_index())
    df_energy = df_energy.cumsum()
    df = df_energy.rename(lambda x: CONSUMPTION_NAME_FMT % x, axis=1)
    col_names = df.columns
    to_drop = [] if prosumers is None else [
        CONSUMPTION_NAME_FMT % x for x in range(prosumers, len(col_names))
    ]

    logger.debug("Dropping the following rows: %s" % (to_drop, ))
    df.drop(to_drop, axis=1, inplace=True)
    return df


def create_production_and_consumption_df(con_df: pd.DataFrame, filename=None, json_cache=None):
    """
    Create Production and Consumption datasets.
    if filename is not specified, then pvgis will be queried.
    if filename is a string of path, then the same file will be
    used to populate all of the datasets.
    if filename is a list containing paths, then the list will be iterated and the files used to specify the dataset.
    Files should be the json format as returned by pvgis
    """
    cols = con_df.columns
    num = cols.shape[0]
    logger.debug("Creating Production df with %d PV-Columns" % (num, ))
    lat, lon = koordinaten["Berlin (Nord)"]

    coordinates = random_coords_around_center(lat, lon, 0.05, num)
    total_df = con_df
    min_idx, max_idx = min(total_df.index), max(total_df.index)
    min_idx, max_idx = min_idx.astimezone("UTC"), max_idx.astimezone("UTC")
    logger.debug("(%s, %s), %s", min_idx, max_idx, type(max_idx))

    static_pv_data_df = None
    if type(filename) is str or type(filename) is PurePath:
        static_pv_data_df = _create_static_data_df(filename)

    for i in range(num):
        logger.debug("Houshold Number (loop count): %d" % i)
        start_loop_time = time.time()
        pv_data = None
        if not filename:
            lat, lon = coordinates[i]
            t1 = time.time()
            pv_data = query_pvgis_selection(
                lat, lon, min_idx, max_idx, total_df["prosumer%d_consumption" % (i, )])
            if json_cache is not None:
                if not os.path.exists(json_cache):
                    os.mkdir(json_cache)
                if os.path.isdir(json_cache):
                    f = open(PurePath(json_cache, "prosumer%d.json" % (i, )), "w")
                    logger.info("Writing to: %s" % (f, ))
                    f.write(json.dumps(pv_data))
                else:
                    logger.warning(
                        "Json Cache Points to a file, will not write cache.")

            logger.debug("PVGIS Query took: %.2f" % (time.time() - t1))
        elif type(filename) is list:
            file_path = filename[i % len(filename)]
            t1 = time.time()
            file = open(file_path, "r")
            logger.debug("opening file took %.2f secs" % (time.time() - t1, ))
            pv_data = json.load(file)

        df = None
        if static_pv_data_df is not None:
            df = _rename_static_pv_data_df(static_pv_data_df, i)

        else:
            t1 = time.time()
            df = create_dataframe(pv_data)
            logger.debug("creating df took %.2f secs" % (time.time() - t1, ))
            df = interpolate_minutely(df)
            logger.debug("interpolate df took %.2f secs" %
                         (time.time() - t1, ))

            df = to_cumulative_kWh(df)
            df = df - df["pv"][min_idx]
            df.rename({"pv": PV_NAME_FMT % i}, axis=1, inplace=True)

        t1 = time.time()

        total_df = total_df.join(df, how="left")
        logger.debug("joinings took %.2f secs" % (time.time() - t1))

        now = time.time()
        time_diff = now - start_loop_time
        if time_diff < (1. / 30.) and filename is None:
            sleep_time = (1./30.) - time_diff
            logger.warning(
                "Sleeping %.2f  seconds to avoid pvgis query limit." % (sleep_time,))
            time.sleep(sleep_time)

    total_df.index = total_df.index.strftime(
        "%Y-%m-%dT%H:%M:%SZ")  # Househols Sim Compatible dateformat
    return total_df


def query_pvgis_selection(lat: int, lon: int, begin: pd.Timestamp, end: pd.Timestamp, consumption_data: pd.Series, ):

    kwp_size = chose_kwp_size(lat, lon, consumption_data)
    return query_pvgis(
        lat, lon, kwp_size, startyear=begin.year, endyear=end.year)


def chose_kwp_size(lat: int, lon: int, consumption_data: pd.Series) -> int:
    yearly_usage = consumption_data.iloc[-1]
    base_estimate = yearly_usage // 1000
    multiplier = (rand() * 1.5) + 1.5
    recommened = int(base_estimate * multiplier)
    logger.debug("Base Estimate: %d kwp from %d kwh" %
                 (recommened, yearly_usage))
    return recommened


def _create_static_data_df(filename):
    t1 = time.time()
    file = open(filename, "r")
    logger.debug("opening static pv data took %.2f secs" % (time.time() - t1))
    static_pv_data = json.load(file)
    df = create_dataframe(static_pv_data)
    logger.debug("creating df for static pv data took %.2f secs" %
                 (time.time() - t1))

    static_pv_data_df = interpolate_minutely(df)
    logger.debug("interpolate static pv data took %.2f secs" %
                 (time.time() - t1))
    cumulative_wh = to_cumulative_kWh(static_pv_data_df)
    return cumulative_wh


def _rename_static_pv_data_df(df, i):
    if i == 0:
        df.rename({"pv": "prosumer0_pv"}, axis=1, inplace=True)
    else:
        old_name = PV_NAME_FMT % (i-1)
        logger.debug("Renaming old col to: %s" % (old_name, ))
        df.rename({old_name: PV_NAME_FMT %
                   i}, axis=1, inplace=True)
    return df


def create_dataset(prosumers=None, n_rows=None, filename=None, json_cache=None):
    """
    Creates a dataset from the htw_berlin data and pvgis PV-Data.
    prosumers: How Many Prosumers to create
    n_rows: How many datapoints to create.
    filename: if there is local pv data, use this file. see create_production_and_consumption_df
    json_cache: if we want to cache the queried pvgis results, but them in this folder. if none, nothing happens
    """
    df = create_consumption_df(prosumers=prosumers)

    df = create_production_and_consumption_df(
        df, filename=filename, json_cache=json_cache)

    return df


def main(output=("SQL", "CSV",), NROWS=None, NUM_PROSUMERS=None, output_dir=base_dir, pv_files=None, json_cache=PurePath(base_dir, "json_cache")):
    """
    Creates the htw_berlin dataset based on the specified input.
    arguments are:
    output: ("SQL", "CSV",) -> Remove either one to just create a sqllite or csv file.
    NROWS: int|None  ->  how many rows of data you want to have. None for all of them
    NUM_PROSUMERS: int|None -> how many prosumers should be in the datasett. None for all of them
    pv_files: None if you want to query online for pv production data. see create_production_and_consumption_df documentation.
    json_cache: Stores the PVGIS Queries in the specified directory if not none. 
        In Subsequent calls, you can pass these files to the pv_files parameter.
    """
    logger.info("specified outputs: %s" % (output, ))

    if NROWS:
        logger.warning("NROWS != None, only part of the dataset will be read")
    if NUM_PROSUMERS:
        logger.warning("NUM_PROSUMERS != None, not all prosumers will be read")

    df = create_dataset(prosumers=NUM_PROSUMERS, n_rows=NROWS,
                        filename=pv_files, json_cache=json_cache)
    logger.info(df.head())

    if "SQL" in output:
        print("Saving to sql--")

        logger.info("saving db to: %s" % (output_dir, ))

        sqlite_connection = create_engine(
            'sqlite:///%s' % (PurePath(output_dir, DATABASE_FILE_NAME), ))
        df.to_sql(name=SQL_TABLE_NAME, con=sqlite_connection,
                  index_label="utc_timestamp", method=None, if_exists='replace', chunksize=100)
        sqlite_connection.dispose()

    if "CSV" in output:
        print("Saving to CSV--")

        df.to_csv(PurePath(output_dir, "dataset.csv"))


if __name__ == "__main__":

    usage_str = """
        create_dataset_csv.py [-w WORKDIR]?
        The optional argument -w  specifies where the input should be read from and where 
        the output should be written to.
        See Docstring of module about the needed files in input dir. 
    """

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hw:')
    except getopt.GetoptError as err:
        print(err)
        print(usage_str)
        sys.exit(0)

    predictors = []
    sims = []
    values_to_predict = args

    prosumer_ids = {
        "preprocessed_householdsim": list(range(1, 6)),
        "htw_berlin": list(range(74))
    }

    for o, a in opts:
        if o == "-h":
            print(usage_str)
            sys.exit(1)
        if o == "-w":
            base_dir = a

    power_phases = [PurePath(base_dir, "PL%d.csv" % i) for i in range(1, 4)]
    logger.info("Base dir set to: %s" % (base_dir, ))

    file_paths = [PurePath(base_dir, "json_cache", f)
                  for f in os.listdir(PurePath(base_dir, "json_cache"))]
    main(("SQL", "CSV"), json_cache=None, output_dir=base_dir,

         pv_files=file_paths)
