# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 14:43:32 2025

@author: agsthk
"""

import os
import polars as pl
import pytz
from datetime import datetime, timedelta
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import hvplot.polars
import holoviews as hv

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all structured raw data
CAL_DATA_DIR = os.path.join(data_dir, "Instruments_CalibratedData")
# Full path to automated addition times
ADD_TIMES_PATH = os.path.join(data_dir, "Instruments_ManualData", "Instruments_ManualExperiments", "ManualAdditionTimes - Copy.csv")
add_times = pl.read_csv(ADD_TIMES_PATH).with_columns(
    pl.selectors.contains("UTC").str.to_datetime(time_zone="America/Denver").dt.convert_time_zone("UTC")
    )
add_times = {key[0]: df for key, df in 
             add_times.partition_by(
                 "Species", as_dict=True, include_key=False
                 ).items()}
AUTO_ADD_TIMES_PATH = os.path.join(data_dir, "Instruments_DerivedData", "AdditionValves_DerivedData", "AdditionValves_AutomatedAdditionTimes.csv")
auto_add_times = pl.read_csv(AUTO_ADD_TIMES_PATH).with_columns(
    pl.selectors.contains("UTC").str.to_datetime()
    )
auto_add_times = {key[0]: df for key, df in 
             auto_add_times.partition_by(
                 "Species", as_dict=True, include_key=False
                 ).items()}
for key, df in add_times.items():
    add_times[key] = pl.concat([df, auto_add_times[key]])

DOOR_STATUS_PATH = os.path.join(data_dir, "Instruments_DerivedData", "TempRHDoor_DerivedData", "TempRHDoor_DoorStatus.csv")
door_times = pl.read_csv(DOOR_STATUS_PATH).with_columns(
    pl.selectors.contains("UTC").str.to_datetime()
    )
door_times = {key[0]: df for key, df in 
             door_times.partition_by(
                 "DoorStatus", as_dict=True, include_key=False
                 ).items()}

data = []
for root, dirs, files in tqdm(os.walk(CAL_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        if file.find("2BTech_205_A") == -1:
            continue
        path = os.path.join(root, file)
        if path.find("DAQ") == -1:
            continue
        lf = pl.scan_csv(path)
        lf = lf.with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC"),
            pl.selectors.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
      
        df = lf.collect()
        
        if df.is_empty():
            continue
        data.append(df)
        
data = pl.concat(data).sort(by="UTC_Start")
data = {loc[0]: df for loc, df in data.partition_by("SamplingLocation", include_key=False, as_dict=True).items()}
#%%

for loc, df in data.items():
    fig, ax = plt.subplots(figsize=(10, 8))
    for add_start in add_times["O3"]["UTC_Start"]:
        if add_start < datetime(2025, 1, 1, tzinfo=pytz.UTC) or add_start > datetime(2025, 2, 1, tzinfo=pytz.UTC):
            continue
        add_stop = add_start + timedelta(hours=3)
        add_start_2 = add_start - timedelta(minutes=15)
        temp_df = df.filter(
            pl.col("UTC_Start").is_between(add_start_2, add_stop)
            | pl.col("UTC_Stop").is_between(add_start_2, add_stop)
            )
        if temp_df.is_empty():
            continue
        temp_df = temp_df.with_columns(
            pl.col("UTC_Start").sub(add_start).dt.total_seconds()
            )
        
        pre_add = temp_df.filter(
            pl.col("UTC_Start").lt(0)
            )["O3_ppb"].mean()
        post_add = temp_df.filter(
            pl.col("UTC_Start").gt(8000)
            )["O3_ppb"].mean()
        
        temp_df = temp_df.with_columns(
            pl.when(pl.col("UTC_Start").lt(0))
            .then(pre_add)
            .when(pl.col("UTC_Start").gt(8000))
            .then(post_add)
            .alias("BG_O3")
            ).with_columns(
                pl.col("BG_O3").interpolate_by("UTC_Start")
                ).with_columns(
                    pl.col("O3_ppb").sub(pl.col("BG_O3"))
                    )
        max_o3 = temp_df["O3_ppb"].max()
        ax.plot(temp_df["UTC_Start"], temp_df["O3_ppb"],
                label=(add_start.strftime("%Y-%m-%d %H:%M") + " Max O3 = " + f"{max_o3:.2f}")
                )
    ax.set_title(loc)
    ax.set_xlabel("Seconds from addition start")
    ax.set_ylabel("Background-subtracted O3 (ppb, linear interpolation used)")
    ax.legend(bbox_to_anchor=(1.1, 1.05))
