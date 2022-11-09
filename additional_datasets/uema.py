"""
This module calculates the UEMA predictions

"""
from datetime import date
from pathlib import PurePath
import pandas as pd
import numpy as np
from typing import Union


def trim_week(df):
    """
    Trims the weeks at the start and beginning of the df, 
    so weekday/ts combinations have the same data shape
    """
    start_date = None
    for date in df.index:
        if date.day_of_week == 0 and date.hour == 0:
            start_date = date
            break
    end_date = None
    for date in df.index[::-1]:
        if date.day_of_week == 0 and date.hour == 0:
            end_date = date
            end_date = end_date.floor('1H')
            break

    return df[:][(df.index >= start_date) & (df.index < end_date)]


def create_uema_data(df, field='grid_import',):
    """
    Creates a numpy area with shape ((weekdays * timespans_per_day), x)  contains for each weekday and each
    timeslot per day the measurements that a smart meter would have made.
    """
    df_trim = trim_week(df)

    weekdays = 7
    hours_per_day = 24
    grouped = df_trim.groupby(lambda x: (x.dayofweek * hours_per_day) + x.hour)

    l_uema_series = list(grouped)[0][1][field].shape[0]
    uema_data = np.zeros(
        ((weekdays * hours_per_day), l_uema_series), dtype=np.float64)

    for label, group in grouped:

        d = group[field].to_numpy()

        uema_data[label] = d

    return uema_data


def create_flexible_uema_data(df: pd.DataFrame, first_freq: Union[pd.Timedelta,  str], second_freq: Union[pd.Timedelta,  str], field="grid_import"):
    """
        Creates a numpy area with shape (( (first_freq / second_freq), x / first_freq).
        First Freq (e.g. 1D for 1 Day and Second freq e.g. 1H for 1 Hour) can really be anything that pd.Timedelta accepts (including a pd.Timedelta obj)
        The first freq divides a timeseries into columns and the second freq divides these columns into rows. 
        UEMA will then predict the next value for a timeslot as the uema of the cells to the left.
        The first freq should divide one week without remainder and the second should divide the first without remainder
    """
    df_trim = trim_week(df)

    df_resampled = df_trim.resample(second_freq).sum()
    rows_per_col = int(pd.Timedelta(first_freq) / pd.Timedelta(second_freq))

    np_array = df_resampled[field].to_numpy()
    total_len = np_array.shape[0]

    return np_array.reshape((rows_per_col, int(total_len / rows_per_col)), order="F")


def uema(d, alpha=0.8):
    """
    Calculates n UEMA series along the second axis, so every series has length m. 
    d: 2D NP Array with size(n,m)
    """

    S = np.zeros(shape=d.shape, dtype=np.float64)
    S[:, 0] = d[:, 0].copy()
    for time_step in range(1, d.shape[1]):
        s_ts = (S[:, time_step-1] * alpha) + d[:, time_step]
        S[:, time_step] = s_ts.copy()

    factor = np.ones_like(d)
    factor = factor * alpha
    factor = 1 - (factor ** np.arange(1, d.shape[1]+1))

    return ((1-alpha) / factor) * S


def create_uema_prediction_df(uema_preds: np.ndarray, start_ground_truth: pd.Timestamp, first_freq: Union[str, pd.Timedelta], second_freq: Union[pd.Timedelta, str]) -> pd.DataFrame:
    """
    The index will start at predictions start at `start_ground_truth + first_freq` and have a frequency of `first_freq`. Columns will 
    have a frequency of `second_freq`. 
    The Timestamp of a prediction is index + column of the dataframe.
    The shape of uema_preds.T is the same as the dataframe
    Params: 
        UEMA Preds: like created by create_flexible_uema_data
        start_ground_truth: First pd.Timestamp of the ground truth data. First Prediction then is start_ground_truth + first_freq
        first/second_freq = like in create_flexible_uema_data
    """
    first_freq = pd.Timedelta(first_freq)
    second_freq = pd.Timedelta(second_freq)
    columns = [second_freq * i for i in range(int(first_freq / second_freq))]
    index = pd.date_range(start=start_ground_truth + first_freq, freq=first_freq,
                          periods=uema_preds.shape[1])
    return pd.DataFrame(data=uema_preds.T, columns=columns, index=index)


def create_continuous_prediction_df(uema_preds: np.ndarray, start_ground_truth: pd.Timestamp, first_freq: Union[str, pd.Timedelta], second_freq: Union[pd.Timedelta, str]) -> pd.Series:
    index = pd.date_range(start=start_ground_truth+first_freq,
                          periods=uema_preds.shape[0] * uema_preds.shape[1], freq=second_freq)

    data = uema_preds.reshape((-1,), order="F")
    return pd.Series(index=index, data=data, name="yhat")


def get_matching_uema(gt, pred):
    """
     Parameters
        ----------
            prosumer : int
                the index of the prosumer that you want the ground truth / prediction pairs for
        returns:
            Tuple:( np.ndarray, nd.array) -> (gt shape (168,x), pred shape(168,x))

    """
    return gt[:][1:], pred[:][:-1]


if __name__ == "__main__":
    gt = pd.read_csv(PurePath(
        'EnergyPatternForecast/simulation_data/ground_truth/one_hour/prosumer_1.csv'), index_col=0, parse_dates=True)
    col = "consumption"
    uema_1 = create_uema_data(gt, field=col)

    first_freq = pd.Timedelta("1W")
    second_freq = pd.Timedelta("1H")

    uema_2 = create_flexible_uema_data(gt, first_freq, second_freq, field=col)

    uema_pred = uema(uema_2)

    print(uema_1.shape, uema_2.shape)

    pred_df = create_uema_prediction_df(uema_preds=uema_pred,
                                        start_ground_truth=trim_week(gt).index[0], first_freq=first_freq, second_freq=second_freq,)

    index_to_compare = 4
    continuous_df = create_continuous_prediction_df(uema_preds=uema_pred,
                                                    start_ground_truth=trim_week(gt).index[0], first_freq=first_freq, second_freq=second_freq,)
    from_continuous = continuous_df.iloc[index_to_compare * 7 *
                                         24:(index_to_compare + 1)*7*24]
    compare = pred_df.iloc[index_to_compare].to_numpy()

    print(from_continuous.shape, compare.shape)
    print((from_continuous.to_numpy() ==
           compare).all())
    print((pred_df.iloc[index_to_compare].to_numpy()
          == uema_pred[:, index_to_compare]).all())
