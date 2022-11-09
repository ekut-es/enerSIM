from sqlite3.dbapi2 import connect
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

from pathlib import PurePath
import sqlite3


PROSUMER_PV_PROFILES = ['DE_KN_residential1_pv',
                        'DE_KN_residential3_pv', 'DE_KN_residential4_pv']
CONSUMER_PROFILES = ['DE_KN_residential1', 'DE_KN_residential2', 'DE_KN_residential3', 'DE_KN_residential4',
                     'DE_KN_residential5', 'DE_KN_residential6', 'DE_KN_industrial2']

db_path = str(PurePath('..', 'simulation', 'cosimulation_city_energy',
              'simulation_data', 'household_data_2020.sqlite'))


power_consumption = "grid_import"
power_generation = "grid_export"
PROSUMER_INDEX = 0
prosumer_pv = PROSUMER_PV_PROFILES[PROSUMER_INDEX]

def trim_week(df):
    """
    Trims the weeks at the start and beginning of the df, 
    so weekday/ts combinations have the same data shape
    """
    start_date = None
    for  date in df.index:
        if date.day_of_week == 0 and date.hour ==0:
            start_date = date
            break
    end_date = None
    for  date in df.index[::-1]:
        if date.day_of_week == 0 and date.hour ==0:
            end_date = date
            end_date = end_date.floor('1H')
            break
    
    return df[:][(df.index > start_date) & (df.index < end_date) ]

def rename_func(x):
    """
    Returns more generic names for the column names for PV Production and Grid Import of the table, 
    since every prosumer has its own DataFrame anyway.
    """
    if 'pv' in x:
        return 'pv'
    if 'grid_import' in x:
        return 'grid_import'
    return x

def query_df(i, path=db_path, table='household_data_1min_singleindex'):
    """
    Creates a dataframe of the the prosumer with the given index, where grid_import at
    the given timestamps is calculated and **not** a running total like in the db.
    """
    prosumer_pv = PROSUMER_PV_PROFILES[i]

    prosumer_power_consumption = f'{prosumer_pv[:-2]}{power_consumption}'

    QUERY = f"""SELECT utc_timestamp, {prosumer_power_consumption}, {prosumer_pv} 
            FROM {table} WHERE {prosumer_pv} IS NOT NULL AND {prosumer_power_consumption} IS NOT NULL
            ORDER BY utc_timestamp ASC
    ;"""

    con = sqlite3.connect(path)
    cur = con.cursor()
    df = pd.read_sql_query(QUERY, con)
    df.rename(mapper=rename_func,  axis='columns', inplace=True)
    return df

def offset_difference(df, col):
    diff = df[col][1:].to_numpy() - df[col][:-1].to_numpy()
    return pd.DataFrame(index=pd.to_datetime(df['utc_timestamp'].iloc[:-1], yearfirst=True), data={
                        col: diff})


def create_df_gi(i):
    """
    Creates a dataframe of the the prosumer with the given index, where grid_import at
    the given timestamps is calculated and **not** a running total like in the db.
    If the df is *2*, then we omit the last three entriesm because they are probabl faulty
    """
    
    
    df = query_df(i)
    if i == 1:
        df = df.truncate(after=df.shape[0] - 4)
    
    df_gi = offset_difference(df, 'grid_import')
    return df_gi


def create_df(i, path=db_path, table='household_data_1min_singleindex'):
    """
    Creates a Dataframe for the Prosumer at index i.
    Index is the UTC Datetime, Columns are the total Grid import and power generation since the start and the grid import and power generation in the given time step.
    """
    df = query_df(i, path=path, table=table)
    diff_grid_import = offset_difference(df, 'grid_import')
    diff_power_generation = offset_difference(df, 'pv')
    df = df[:].iloc[:-1]
    df['utc_timestamp'] = pd.to_datetime(df['utc_timestamp'], yearfirst=True)
    df = df.set_index('utc_timestamp')
    return df.join(diff_grid_import, lsuffix="_total").join(diff_power_generation, lsuffix="_total")  


 


    