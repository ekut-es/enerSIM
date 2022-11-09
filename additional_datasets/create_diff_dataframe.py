from pandas.core import series
import uema
import load_data
import pandas as pd
from pathlib import PurePath
from typing import Optional
import numpy as np


if __name__ == "__main__":

    additional_dataset = "htw_berlin"

    prosumer_ids = list(range(74))
    destination_dir = PurePath(
        "visualization/data/reference_predictions/%s" % (additional_dataset, ))
    all_prosumers_gt = pd.read_csv(PurePath(
        "additional_datasets/%s/dataset.csv" % (additional_dataset, )), index_col=0, parse_dates=True)

    to_predict = ["balance"]
    all_prosumers_gt = all_prosumers_gt.diff()
    all_prosumers_gt.dropna(inplace=True)
    all_prosumers_gt = all_prosumers_gt.resample("1H").sum()

    for p in prosumer_ids:
        pv_str = f"prosumer{p}_pv"
        consumption_str = f"prosumer{p}_consumption"
        balance_str = f"prosumer{p}_balance"

        balance = None
        balance = ((all_prosumers_gt[pv_str] if pv_str in all_prosumers_gt.columns else 0) -
                   all_prosumers_gt[consumption_str])
        all_prosumers_gt[balance_str] = balance

    cols_to_select = []
    for col in to_predict:
        cols_to_select += ["prosumer%d_%s" % (i, col) for i in prosumer_ids]
    all_prosumers_gt[cols_to_select].to_csv("diff_%s.csv" % (
        additional_dataset), index_label="utc_timestamp")
