# -*- coding: utf-8 -*-
"""
Created on Wed Aug 12 17:05:26 2026

@author: agsthk
"""

# %% Package imports
import os
import polars as pl
import polars.selectors as cs
from datetime import datetime, timedelta
import hvplot.polars
import holoviews as hv
import pytz
import yaml
# User defined function
def read_ict(path):
    '''
    Reads ICARTT files

    Parameters
    ----------
    path : str
        Absolute or relative path to ICARTT file.

    Returns
    -------
    df : polars.DataFrame
        DataFrame containing ICARTT data with missing values removed and time 
        converted from seconds since midnight to polars.datatypes.Datetime
        object.

    '''
    with open(path, "r") as f:
        # Number of lines in header
        lines = int(f.readline().split(",")[0])
        # Start and stop date
        for i in range(6):
            date = f.readline()
    y1, m1, d1, y2, m2, d2 = (int(x) for x in date.split(","))
    # Midnight of start date
    start = datetime(year=y1, month=m1, day=d1, hour=0, minute=0, second=0)
    # Reads ICARTT file, skipping header
    df = pl.read_csv(path, skip_lines=lines - 1)
    df = df.with_columns(
        # Converts from seconds since midnight to polars.datatypes.Datetime
        cs.contains("time", "UTC", "FTC").mul(1e6).cast(pl.Duration()).add(start)
        ).with_columns(
            # Removes missing values
            pl.all().replace(-9999, None)
            )
    return df
# %% Path definitions
# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")
# Full path to Vocus ICARTT file
VOCUS_PATH = os.path.join(data_dir, "Instruments_ICARTTData",
                          "Vocus_ICARTTData", "Vocus_ICARTTData_RC",
                          "Keck-VocusCIMS_C200_20260323_RC.ict")
O3_DIR = os.path.join(data_dir, "Instruments_CleanData",
                      "2BTech_205_A_CleanData", "2BTech_205_A_CleanHubData")
# %% Reads ICARTT data
vocus = read_ict(VOCUS_PATH).select(
    cs.contains("UTC").dt.convert_time_zone("America/Denver")
    .dt.offset_by("9m")
    .name.map(
        lambda name: name.replace("_UTC", "")
        ),
    pl.col("C10H16(NH4)+_cps").alias("terpene")
    ).drop_nulls()
o3 = []
for file in os.listdir(O3_DIR):
    path = os.path.join(O3_DIR, file)
    o3.append(
        pl.read_csv(path).with_columns(
            cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        )
o3 = pl.concat(o3).sort(by="FTC_Start").select(
    (~cs.contains("UTC")).name.map(lambda x: x.replace("FTC_", ""))
    ).filter(
        pl.col("SamplingLocation").eq("C200")
        )
# %%
hvplot.show(
    (vocus.hvplot.scatter(
        x="Start",
        y="terpene"
        ) * o3.hvplot.scatter(
            x="Start",
            y="O3_ppb"
            ))#.cols(1)
)