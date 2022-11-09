"""
This Scripts is the result of Notebook 06_data_prep (from the msc-flo-gehring repo).
The data from the COSSMIC Database is transformed to reflect the true electricity consumption.
Missing data is being interpolated.

"""
import sys
import getopt
import numpy as np
import pandas as pd
from typing import Dict


from pathlib import PurePath


import logging
from sqlalchemy import create_engine

from os.path import abspath


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


household_name_fmt = "prosumer%d"

PV_NAME_FMT = household_name_fmt + "_pv"
CONSUMPTION_NAME_FMT = household_name_fmt + "_consumption"

SQL_TABLE_NAME = "householdsim_preprocessed"
DATABASE_FILE_NAME = "householdsim_preprocessed.sql"

# If you're executing the code from the root of the simulation repo, this will give
# you the correct directory.
base_dir = PurePath(".", "additional_datasets", "preprocessed_householdsim")

DATABASE_FILE_PATH = abspath(PurePath(base_dir, DATABASE_FILE_NAME))


# The indices of the relevant households, 1..5
household_ids = tuple(range(1, 6))
# These households are prosumers
prosumer_ids = (1,  3, 4, )

consumer_ids = (2, 5)


# Keyword arguments for the Household Sim to work with a dataset created by this script.
HOUSEHOLD_SIM_KWARGS = {
    'data_base_path': DATABASE_FILE_PATH,
    'consumption_profiles': [household_name_fmt % (i, ) for i in household_ids],
    'household_pv_profiles': [PV_NAME_FMT % (i, ) for i in prosumer_ids],
    'table_name': SQL_TABLE_NAME,
    'import_data': ['consumption'],
    'export_data': ["pv"],
    'pv_plant_profiles': []
}


"""
Some Configuration here
    calc_prediction_values: Determines the sampling interval, either 5MIN or 1H
    include_pv: If the pv column should be included
    include_energy_balance: if the energy balance collumn should be included
    interpolate_prosumers: ids of the prosumer where data needs to be interpolated.
    path:   path to the CSV File containing the household data.
            The file can be optained from https://data.open-power-system-data.org/household_data/
            -> household_data_1min_singleindex.csv
    output : Specify which output format you want. Either ("CSV", ), ("SQL", ) or ("CSV", "SQL",)

"""
calc_prediction_values = False
# "1H" for prediction values
resampling_duration = '1H' if calc_prediction_values else "5MIN"
include_pv = True
include_energy_balance = True
output = ("CSV", "SQL",)


def col_is_relevant(col_name):
    if col_name == 'utc_timestamp':
        return True

    prefix_check = prefix in col_name
    if not prefix_check:
        return False
    household_id = int(col_name[len(prefix):len(prefix) + 1])
    number_check = household_id in prosumer_ids or household_id in consumer_ids
    postfix_check = False
    for postfix in postfixes:
        postfix_check = postfix_check or postfix in col_name

    return number_check and postfix_check


def create_col_name(prosumer_id, post):
    return "%s%d_%s" % (prefix, prosumer_id, post)


def calc_consumption(data, prosumer) -> pd.DataFrame:
    grid_export_col = create_col_name(prosumer, "grid_export")

    grid_import = data[create_col_name(prosumer, "grid_import")]

    # Don't use pv to calculate consumption, if grid_export is not in the table
    pv = data[create_col_name(
        prosumer, "pv")] if grid_export_col in data.columns else 0
    grid_export = data[grid_export_col] if grid_export_col in data.columns else 0

    consumption_df = (grid_import + pv -
                      grid_export).to_frame(name='consumption')

    return consumption_df


if __name__ == "__main__":
    usage_str = """
        USAGE: ground_truth_creator.py [-i INPUTFILE]? [-o OUTPUT_DIR]?
        All arguments optional, with sensible defaults if not set.
        INPUTFILE: CSV file to read data from
        OUTPUT_DIR: Where to write to
        Further configuration can be done by editing the source file,
        where also more documentation can be found.
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hi:o:')
    except getopt.GetoptError as err:
        print(usage_str)
        print(err)

    # Interpolate Data for the following prosumers.
    interpolate_prosumers = (3, 4,)
    input_file = None
    output_dir = None
    for o, a in opts:
        if o == "-h":
            print(usage_str)
        if o == "-i":
            input_file = a
        elif o == "-o":
            output_dir = a

    datasource_filename = 'household_data_1min_singleindex.csv'
    datasource_filename = PurePath(base_dir,
                                   datasource_filename)
    datasource_filename = datasource_filename if input_file is None else input_file

    print(str(datasource_filename))

    prefix = "DE_KN_residential"
    postfixes = ("pv", "grid_import", "grid_export", )
    data: pd.DataFrame = pd.read_csv(datasource_filename, parse_dates=True, index_col=[
        "utc_timestamp"], usecols=col_is_relevant)

    diff_data = data.diff()
    diff_data.dropna(inplace=True)
    grouped = diff_data.resample(resampling_duration).sum()

    consumption_dfs: Dict[int, pd.DataFrame] = dict([(p, calc_consumption(grouped, p))
                                                    for p in household_ids])

    # interpolate prosumer
    for p in interpolate_prosumers:
        df = consumption_dfs[p]
        selector = df['consumption'] < 0
        df['consumption'].loc[selector] = np.nan
        df.interpolate(inplace=True)

    for p in household_ids:
        df = consumption_dfs[p]
        if include_pv and p in prosumer_ids:
            pv_name = create_col_name(p, "pv")
            df["pv"] = grouped[pv_name]
        if include_energy_balance:
            balance_name = create_col_name(p, "balance")
            pv = grouped[create_col_name(
                p, "pv")] if p in prosumer_ids else 0
            df["balance"] = pv - df['consumption']

    consumption_df_list = list(consumption_dfs.items())
    final_df = None

    for pid, df in consumption_df_list:
        df = df.rename(lambda x, y=pid: "prosumer%d_%s" % (y, x), axis=1)
        final_df = df if final_df is None else final_df.join(df, how='inner', )

    final_df.index = final_df.index.strftime(
        "%Y-%m-%dT%H:%M:%SZ")

    if "SQL" in output:
        print("Saving to sql--")
        save_to = base_dir if output_dir is None else output_dir
        if output_dir is not None:
            logger.info("Output dir specified: %s" % (output_dir, ))
        else:
            logger.info("Output defaults to: %s" % (base_dir, ))

        db_file_name = PurePath(save_to, DATABASE_FILE_NAME)
        logger.info("saving db to: %s" % (db_file_name, ))

        sqlite_connection = create_engine(
            'sqlite:///%s' % (db_file_name, ))
        final_df.to_sql(name=SQL_TABLE_NAME, con=sqlite_connection,
                        index_label="utc_timestamp", method=None, if_exists='replace', chunksize=100)
        sqlite_connection.dispose()

    if "CSV" in output:
        print("Saving to CSV--")

        final_df.to_csv(PurePath(save_to, "dataset.csv"))
