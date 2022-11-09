"""
This Script creates Reference Predictions in CSV Format that can be given to the CSV Predictor in a mosaik simulation.

"""

import sys
import getopt


import uema
import load_data
import pandas as pd
from pathlib import PurePath
from typing import Optional
import numpy as np
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


def create_uema_preds(df: pd.DataFrame, to_predict, alpha: float, first_freq: pd.Timedelta, second_freq: pd.Timedelta, filepath: str):

    uema_data = uema.create_flexible_uema_data(
        df, first_freq, second_freq, field=to_predict)
    preds = uema.uema(uema_data, alpha=alpha)
    pred_series = uema.create_continuous_prediction_df(
        preds, load_data.trim_week(df).index[0], first_freq, second_freq)

    result = pd.DataFrame(data=np.zeros(
        (pred_series.shape[0] - 24, 24)), index=pred_series.index[:-24], columns=list(range(1, 25)))
    for date in pred_series.index[:-24]:
        data = pred_series[(date +
                           pd.Timedelta(hours=1)): (date + pd.Timedelta(hours=24))]
        result.loc[date, :] = data.to_numpy()

    if filepath:
        result.to_csv(filepath)
    else:
        print(result)


def create_backshift_predictions(df: pd.DataFrame, to_predict, backshift: pd.Timedelta, filepath: Optional[str],):

    pred_index_start = df.index[0] + backshift
    pred_index = pd.date_range(
        start=pred_index_start, end=df.index[-1], freq="1H")
    result = pd.DataFrame(data=np.zeros(
        (pred_index.shape[0], 24)), index=pred_index, columns=list(range(1, 25)))

    for date in result.index:
        data = df[to_predict][((date +
                              pd.Timedelta(hours=1)) - backshift): ((date + pd.Timedelta(hours=24)) - backshift)]
        result.loc[date, :] = data.to_numpy()

    if filepath:
        result.to_csv(filepath)
    else:
        print(result)


def create_perfect_predictions(df, to_predict, filepath: Optional[str]):
    pred_index_start = df.index[1]
    pred_index_end = df.index[-25]
    pred_index = pd.date_range(pred_index_start, pred_index_end, freq="1H")
    result = pd.DataFrame(data=np.zeros(
        (pred_index.shape[0], 24)), index=pred_index, columns=list(range(1, 25)))

    for date in result.index:
        data = df[to_predict][date +
                              pd.Timedelta(hours=1): date + pd.Timedelta(hours=24)]
        result.loc[date, :] = data.to_numpy()

    if filepath:
        result.to_csv(filepath)
    else:
        print(result)


if __name__ == "__main__":

    usage_str = """
    Usage:
        python crate_reference_predictions.py [-s SIM]+ [-p PREDICTOR]+  [-o OUTPUT]? [-i INPUT]? [KIND]+
        SIM: htw_berlin | preprocessed_householdsim
        PREDICTOR: B24 | perfect | UEMA_1W_1H | UEMA_1D_1H
        KIND: consumption | balance | pv
        OUTPUT: The output directory. Optional. Should only be used if only one SIM is given, or results are overwritten.
                If not given, the default visualization/data/reference_predictions/<SIM> is used.
        INPUT: The input file. Optional. Should only be used if only one SIM is given.
                If not given, the default additional_datasets/<SIM>/dataset.csv  is used.
        e.g. python crate_reference_predictions.py -s htw_berlin  -s preprocessed_householdsim  -p B24 -p perfect pv balance
        to create perfect and B24 reference predictions for balance and pv dataseries for htw_berlin and householdsim series.
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'ho:i:s:p:')
    except getopt.GetoptError as err:
        print(err)
        print(usage_str)

    predictors = []
    sims = []
    values_to_predict = args

    prosumer_ids = {
        "preprocessed_householdsim": list(range(1, 6)),
        "htw_berlin": list(range(74))
    }
    output_dir = None
    input_file = None
    for o, a in opts:
        if o == "-h":
            print(usage_str)
            sys.exit(1)
        elif o == "-s":
            if a not in prosumer_ids:
                print(a, "not in addidtional datasets.")
                sys.exit(1)
            sims.append(a)
        elif o == "-p":
            predictors.append(a)
        elif o == "-o":
            output_dir = a
        elif o == "-i":
            input_file = a

    if not sims or not predictors:
        print(opts, args, "no predictions will be made")
        print(usage_str)
        sys.exit(-1)
    logger.info("Creating predictions for: %s" % (str(sims,)))
    for additional_dataset in sims:
        logger.info("Starting: %s" % (additional_dataset,))

        destination_dir = ("visualization/data/reference_predictions/%s" %
                           (additional_dataset, )) if output_dir is None else output_dir
        logger.info("Destination dir: %s" % (destination_dir, ))
        destination_dir = PurePath(destination_dir)

        all_prosumers_gt = "additional_datasets/%s/dataset.csv" % (
            additional_dataset, ) if input_file is None else input_file
        logger.info("Prosumer ground truth file: %s" % (all_prosumers_gt,))
        all_prosumers_gt = pd.read_csv(PurePath(all_prosumers_gt),
                                       index_col=0, parse_dates=True)
        logger.debug("Calculating diff")
        all_prosumers_gt = all_prosumers_gt.diff()
        all_prosumers_gt.dropna(inplace=True)
        all_prosumers_gt = all_prosumers_gt.resample("1H").sum()
        logger.debug("Beginning prosumers..")

        sim_prosumer_ids = prosumer_ids[additional_dataset]
        logger.debug("Proumer ids: %s" % (str(prosumer_ids, )))
        for p in sim_prosumer_ids:
            logger.info("Calculating Prosumer %d.." % (p, ))
            pv_str = f"prosumer{p}_pv"
            consumption_str = f"prosumer{p}_consumption"
            balance_str = f"prosumer{p}_balance"

            balance = None
            balance = ((all_prosumers_gt[pv_str] if pv_str in all_prosumers_gt.columns else 0) -
                       all_prosumers_gt[consumption_str])
            all_prosumers_gt[balance_str] = balance

            gt = all_prosumers_gt[[
                f"prosumer{p}_{to_predict}" for to_predict in values_to_predict]]

            for to_predict in values_to_predict:
                to_predict_sel = f"prosumer{p}_{to_predict}"

                if "perfect" in predictors:
                    create_perfect_predictions(gt, to_predict_sel,
                                               PurePath(destination_dir, f"predictions_{p}_{to_predict}_perfect.csv"))

                if "B24" in predictors:
                    backshift_hours = 24

                    create_backshift_predictions(gt, to_predict_sel, pd.Timedelta(hours=backshift_hours),
                                                 PurePath(destination_dir,
                                                          f"predictions_{p}_{to_predict}_backshift_{backshift_hours}.csv"))

                if "UEMA_1W_1H" in predictors:
                    uema_1st_freq = "1W"
                    uema_2nd_freq = "1H"
                    alpha = 0.67
                    create_uema_preds(gt, to_predict_sel, alpha, pd.Timedelta(uema_1st_freq), pd.Timedelta(uema_2nd_freq),
                                      PurePath(destination_dir,  f"predictions_{p}_{to_predict}_uema_{uema_1st_freq}_{uema_2nd_freq}_alpha_{alpha}.csv"))
                if "UEMA_1D_1H" in predictors:
                    uema_1st_freq = "1D"
                    uema_2nd_freq = "1H"
                    alpha = 0.81
                    create_uema_preds(gt, to_predict_sel, alpha, pd.Timedelta(uema_1st_freq), pd.Timedelta(uema_2nd_freq),
                                      PurePath(destination_dir,  f"predictions_{p}_{to_predict}_uema_{uema_1st_freq}_{uema_2nd_freq}_alpha_{alpha}.csv"))
