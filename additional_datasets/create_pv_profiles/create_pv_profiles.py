"""

Use this script to query for pv production data from the PVGIS (https://ec.europa.eu/jrc/en/PVGIS/docs/noninteractive).

The query_pvgis Function returns a JSON-Dict from the PVGIS Server. Alternatively you can load an example with the json.load function.
        f = open("additional_datasets/create_pv_profiles/example_india.json")
        j = json.load(f)
Create a dataframe with create_dataframe(j) or create_dataframe(query_pvgis(lat, lon, peakpower))
Example values for latitude (lan) and longitude (lon) can  be seen in the `koordinaten` dict.
"""

import functools
import json
from datetime import datetime
import logging
from pathlib import PurePath
from typing import Tuple
import pandas as pd
import requests
import numpy as np
from logging import debug, info, Logger
import scipy.stats as stats

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


koordinaten = {"Konstanz": (47.7286, 9.1171),
               "Tübingen (Sand)": (48.536, 9.070),
               "Berlin (Brandenburger Tor)": (52.51319986508516, 13.384881829698736),
               "Berlin (Nord)": (52.63025831235559, 13.39724144931738)
               }


def query_pvgis_kwargs(**kwargs):
    """
        Query the PVGIS for PV-Power at the given coordinated. The Parameters are explained at
        https://ec.europa.eu/jrc/en/PVGIS/docs/noninteractive under Hourly radiation
    TODO: Add all parameters to datatype list in function so they get handled correctly
    """
    float_names = ["loss", "lat", "lon", "peakpower"]
    string_names = ["pvtechchoice", "mountingplace"]
    int_names = ["optimalangles", "trackingtype", "startyear", "endyear"]

    param_strings = []
    for f in float_names:
        if f in kwargs:
            param_strings.append("&%s=%.4f" % (f, kwargs[f]))
    for y in int_names:
        if y in kwargs:
            param_strings.append("&%s=%d" % (y, kwargs[y]))
    for s in string_names:
        if s in kwargs:
            param_strings.append("&%s=%s" % (s, kwargs[s]))
    base_url = "https://re.jrc.ec.europa.eu/api/seriescalc?pvcalculation=1&outputformat=json"

    url_params = functools.reduce(
        lambda x, y: ("%s%s" % (x, y)), param_strings)
    complete_url = "%s%s" % (base_url, url_params)
    logger.debug("PVGIS URL: %s" % (complete_url, ))

    return json.loads(requests.get(complete_url).content)


def query_pvgis(lat, lon, peakpower, optimalangles=True,  startyear=None, endyear=None, trackingtype=0, loss=14, pvtechchoice="crystSi", mountingplace="building", ):
    """
        Convenience Function where the most important parameter names are specified and some options are preselected.
        query_pvgis does the actual querying. 
    """
    kwargs = {
        "lat": lat, "lon": lon, "peakpower": peakpower, "optimalangles": optimalangles, "tracking_type": trackingtype, "loss": loss, "pvtechchoice": pvtechchoice, "mountingplace": mountingplace
    }
    if startyear:
        kwargs["startyear"] = startyear
    if endyear:
        kwargs["endyear"] = endyear
    return query_pvgis_kwargs(**kwargs)


def create_dataframe(json_object):
    """
    Creates a pandas dataframe from a json object as returned from query_pvgis.
    """
    index = []
    pv_generation = []

    data = json_object["outputs"]["hourly"]
    for datapoint in data:
        pv_generation.append(datapoint["P"])
        sample_time = parse_time(datapoint["time"])
        index.append(sample_time)
    return pd.DataFrame({"pv": pv_generation}, index=index)


def create_dataframe_interpolated(json_obj):
    """
    Creates a pv dataframe with minutely resolution 
    with linearly interpolated pv creation values.
    """
    df = create_dataframe(json_obj)
    df = interpolate_minutely(df)
    return df


def interpolate_minutely(df):
    """
    Interpolates the PV Values of the Dataframe minutely.
    Uses linear interpolation
    """

    idx = df.index
    start, end = min(idx), max(idx)
    new_index = pd.date_range(start, end, freq="1MIN")
    new_df = pd.DataFrame({"pv": np.zeros(new_index.shape)}, index=new_index)
    new_df["pv"] = np.nan
    new_df.update(df)

    return new_df.interpolate()


def parse_time(t):
    return pd.Timestamp(ts_input=datetime.strptime(t, "%Y%m%d:%H%M"), tz="UTC")


def to_cumulative_kWh(df: pd.DataFrame):
    duration: pd.Timedelta = df.index[1] - df.index[0]
    duration_in_hours = duration.seconds / (60. * 60.)
    def watt_to_kwh(x): return (x * duration_in_hours) / 1000.
    kwh_df = df.applymap(lambda x: watt_to_kwh(x))
    cumulative = kwh_df.cumsum()
    return cumulative


def random_coords_around_center(lat, lon, width, num):
    """
    Returns a list of num random coordinates in a square around the given coordinates. 
    width could be about 0.05. Wikipedia says 0.1 is city or large district and 0.01 town or village.
    """
    uni = stats.uniform(0, width)
    samples = uni.rvs(num * 2)
    samples = width/2 - samples
    x_offsets, y_offsets = samples[::2], samples[1::2]
    return [(lat + delta_x, lon + delta_y) for delta_x, delta_y in zip(x_offsets, y_offsets)]


def create_dataset(koordinaten: Tuple[float, float], start_year, end_year, num_households: int, output: PurePath):
    lat, lon = koordinaten
    pv_data = query_pvgis(lat, lon, 10, startyear=start_year, endyear=end_year)
    days = 365
    df = create_dataframe_interpolated(pv_data)

    for idx, (lat, lon) in enumerate(random_coords_around_center(lat, lon, 0.01, num_households - 1)):
        logger.info("Starting Household %d of %d", idx + 1, num_households)

        pv_data = query_pvgis(
            lat, lon, 10, startyear=start_year, endyear=end_year)
        df_new = create_dataframe_interpolated(pv_data)
        df = df.join(df_new, rsuffix="_%d" % (idx + 1, ))

    df = df.rename({"pv": "pv_0"})

    logger.info(df.head(10))

    df.to_csv(output)


if __name__ == "__main__":
    create_dataset(koordinaten["Berlin (Brandenburger Tor)"], 2010, 2010,
                   30, PurePath("pvgis_dataset.csv"))
