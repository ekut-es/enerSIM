import matplotlib.pyplot as plt
from create_dataset_csv import base_dir
import pandas as pd
from pathlib import PurePath
df = pd.read_csv(PurePath(base_dir, "dataset.csv"),
                 index_col=0, parse_dates=False)
print(df.sum(axis=0) / 1000.)

"""
plt.plot(df['17'][pd.date_range(start=pd.Timestamp(
    "2010-01-17 00:00:00+01:00"), end=pd.Timestamp("2010-01-17 23:59:00+01:00"), freq="1MIN")])
plt.show()
"""
