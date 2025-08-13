# -*- coding: utf-8 -*-
"""
Created on Wed Aug 13 15:18:45 2025

@author: agsthk
"""



import os
import polars as pl
import pytz
from datetime import datetime
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all structured raw data
STRUCT_DATA_DIR = os.path.join(data_dir, "Instruments_StructuredData")
# Full path to directory containing all clean raw data
CLEAN_DATA_DIR = os.path.join(data_dir, "Instruments_CleanData")
# Creates Instruments_CleanData/ directory if needed
if not os.path.exists(CLEAN_DATA_DIR):
    os.makedirs(CLEAN_DATA_DIR)
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

insts = ["2BTech_205_A",
         "2BTech_205_B",
         "Aranet4_1F16F",
         "Aranet4_1FB20",
         "LI-COR_LI-840A_A",
         "LI-COR_LI-840A_B"]

data = {inst: {} for inst in insts}

for root, dirs, files in tqdm(os.walk(STRUCT_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        if path.find("DAQ") != -1:
            continue
        for inst in insts:
            if path.find(inst) != -1:
                break
        if path.find(inst) == -1:
            continue
        lf = pl.scan_csv(path)
        lf = lf.with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC"),
            pl.selectors.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
      
        df = lf.collect()
        
        if df.is_empty():
            continue
        data[inst][file.rsplit("_", 1)[-1][:-4]] = df
        # _, source = file[:-17].split("_Structured")
        # f_name = inst + "_Clean" + source + "Data_" + path[-12:-4] + ".csv"
        # f_dir = os.path.join(CLEAN_DATA_DIR,
        #                      inst + "_CleanData",
        #                      inst + "_Clean" + source + "Data")
        # if not os.path.exists(f_dir):
        #     os.makedirs(f_dir)
        # path = os.path.join(f_dir,
        #                     f_name)
        # df.write_csv(path)
        
inst = "2BTech_205_A"

for date, df in data[inst].items():
    # if datetime.strptime(date,"%Y%m%d") > datetime(2024, 8, 15):
    #     continue
    # if date not in ["20240311", "20240416", "20240417", "20240509", "20240514",
    #                 "20240604", "20240605", "20240606", "20240610", "20240612",
    #                 "20240617", "20240620", "20240625", "20240713"]:
    #     continue
    datetime.strptime(date,"%Y%m%d")
    if "UTC_DateTime" in df.columns:
        tcol1 = tcol2 = "UTC_DateTime"
    else:
        tcol1 = "UTC_Start"
        tcol2 = "UTC_Stop"
        
    if "O3_ppb" in df.columns:
        var = "O3_ppb"
    else:
        var = "CO2_ppm"
    tmin = df[tcol1].min()
    tmax = df[tcol2].max()
    date_adds = add_times[var.split("_")[0]].filter(
        pl.col("UTC_Start").is_between(tmin, tmax)
        | pl.col("UTC_Stop").is_between(tmin, tmax))
    date_doors = door_times[1].filter(
        pl.col("UTC_Start").is_between(tmin, tmax)
        | pl.col("UTC_Stop").is_between(tmin, tmax))
    date_bounds = date_adds.with_columns(
        pl.col("UTC_Start").dt.offset_by("-5m").alias("Start_Bound"),
        pl.col("UTC_Stop").dt.offset_by("15m").alias("Stop_Bound")
        )
    for row in date_bounds.iter_rows(named=True):
        temp_df = df.filter(
            pl.col(tcol1).is_between(row["Start_Bound"], row["Stop_Bound"])
            | pl.col(tcol2).is_between(row["Start_Bound"], row["Stop_Bound"])
            )
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(
            df[tcol1],
            df[var],
            color="#D9782D",
            zorder=5
            )
        # ax.set_ylim(0, 5000)
        low = temp_df[var].min() - 5
        up = temp_df[var].max() + 5
        if up > 200:
            up = 200
        if low < -10:
            low = -10
        ax.set_ylim(low, up)
        ax.set_xlim(row["Start_Bound"], row["Stop_Bound"])
        ax.set_ylabel(var)
        ax.vlines(row["UTC_Start"], low, up, color="#1E4D2B", zorder=10)
        ax.vlines(row["UTC_Stop"], low, up, color="#1E4D2B", zorder=10)
        # ax.vlines(date_doors["UTC_Start"], 400, 3000, color="gray", zorder=1)
        # ax.vlines(date_doors["UTC_Stop"], 400, 3000, color="gray", zorder=1)
        ax.xaxis.set_major_locator(
            mdates.AutoDateLocator(tz=pytz.timezone("America/Denver"))
            )
        # ax.xaxis.set_minor_locator(
        #     mdates.HourLocator(interval=1, tz=pytz.timezone("America/Denver"))
        #     )
        ax.xaxis.set_major_formatter(
            mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
            )
        ax.tick_params(axis="x", labelrotation=90)
        ax.tick_params(axis="y", color="#1E4D2B")
        ax.set_title(date)
        ax.grid(zorder=0)
        ax.grid(which="minor", linestyle=":", zorder=0)
