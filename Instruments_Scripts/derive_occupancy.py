# -*- coding: utf-8 -*-
"""
Created on Thu Jan 15 14:34:41 2026

@author: agsthk
"""
# %% Imports and definitions
import os
import polars as pl
import polars.selectors as cs
import hvplot.polars
import holoviews as hv
from datetime import datetime, timedelta
import pytz

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing clean data
CLEAN_DATA_DIR = os.path.join(data_dir,
                              "Instruments_CleanData")
# LI-COR CO2 data directory
LICOR_DATA_DIR = os.path.join(CLEAN_DATA_DIR,
                              "LI-COR_LI-840A_A_CleanData",
                              "LI-COR_LI-840A_A_CleanLoggerData")
os.path.exists(LICOR_DATA_DIR)
# Aranet CO2 data directory
ARANET_DATA_DIR = os.path.join(CLEAN_DATA_DIR,
                               "Aranet4_CleanData",
                               "Aranet4_CleanLoggerData")

# Full path to door status file
door_path = os.path.join(data_dir,
                         "Instruments_DerivedData",
                         "TempRHDoor_DerivedData",
                         "TempRHDoor_DoorStatus.csv")
# Full path to occupancy status file
occ_path = os.path.join(data_dir,
                         "Instruments_ManualData",
                         "Instruments_Occupancy",
                         "OccupancyStatus.txt")
# %% Reads and processes data
# Reads in LI-COR CO2 files
licor = []
for root, dirs, files in os.walk(LICOR_DATA_DIR):
    for file in files:
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        lf = pl.scan_csv(path).with_columns(
            cs.contains("UTC").str.to_datetime(time_zone="UTC"),
            cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        licor.append(lf)
# Concatenates LI-COR CO2 files
licor = pl.concat(licor, how="diagonal_relaxed").collect().sort(
    by=cs.contains("UTC")
    ).with_columns(
        cs.contains("DateTime").dt.replace_time_zone(None)
        )
# Splits LI-COR CO2 files by week
licor = {key[0]: df for key, df in licor.with_columns(
    pl.col("FTC_DateTime").dt.strftime("%Y%W").alias("Week")
    ).partition_by("Week", as_dict=True, include_key=False).items()}
# Reads in Aranet CO2 files
aranet = []
for root, dirs, files in os.walk(ARANET_DATA_DIR):
    for file in files:
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        lf = pl.scan_csv(path).with_columns(
            cs.contains("UTC").str.to_datetime(time_zone="UTC"),
            cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        aranet.append(lf)
# Concatenates Aranet CO2 files
aranet = pl.concat(aranet, how="diagonal_relaxed").collect().sort(
    by=cs.contains("UTC")
    ).with_columns(
        # Casts numeric string type columns to float
        ((~cs.contains("DateTime", "Location")) & cs.by_dtype(pl.String))
        .cast(pl.Float64)
        ).with_columns(
            cs.contains("DateTime").dt.replace_time_zone(None)
            )
# Splits Aranet CO2 files by week
aranet = {key[0]: df for key, df in aranet.with_columns(
    pl.col("FTC_DateTime").dt.strftime("%Y%W").alias("Week")
    ).partition_by("Week", as_dict=True, include_key=False).items()}

# Reads in DoorStatus file
doorstatus = pl.scan_csv(door_path).with_columns(
    cs.contains("UTC").str.to_datetime(time_zone="UTC")
    ).with_columns(
        cs.contains("UTC").dt.convert_time_zone("America/Denver")
        .name.map(lambda x: x.replace("UTC", "FTC"))
        ).with_columns(
            cs.contains("Start")
            .name.map(lambda x: x.replace("Start", "DateTime"))
            ).with_columns(
                cs.contains("DateTime").dt.replace_time_zone(None)
                ).sort(
                    by=cs.contains("UTC")
                    ).collect()
doorstatus = {key[0]: df for key, df in doorstatus.with_columns(
    pl.col("FTC_DateTime").dt.strftime("%Y%W").alias("Week")
    ).partition_by("Week", as_dict=True, include_key=False).items()}
# Reads in OccupancyStatus file
occstatus = pl.scan_csv(occ_path).with_columns(
    cs.contains("UTC").str.to_datetime(time_zone="UTC")
    ).with_columns(
        cs.contains("UTC").dt.convert_time_zone("America/Denver")
        .name.map(lambda x: x.replace("UTC", "FTC"))
        ).sort(
            by=cs.contains("UTC")
            ).collect()
# occstatus = {key[0]: df for key, df in occstatus.with_columns(
#     pl.col("FTC_Start").dt.strftime("%Y%W").alias("Week")
#     ).partition_by("Week", as_dict=True, include_key=False).items()}
# %%
phase1_start = datetime(2024, 3, 1, tzinfo=pytz.timezone("America/Denver"))
phase1_stop = datetime(2024, 8, 16, 23, 59, 59,
                       tzinfo=pytz.timezone("America/Denver"))
phase2_start = datetime(2025, 1, 17, tzinfo=pytz.timezone("America/Denver"))
phase2_stop = datetime(2025, 5, 5, 23, 59, 59,
                       tzinfo=pytz.timezone("America/Denver"))
phase3_start = datetime(2026, 1, 13, 2, tzinfo=pytz.timezone("America/Denver"))

# %%

occstatus = occstatus.with_columns(
    follow_gap=pl.col("UTC_Start").shift(-1).sub(pl.col("UTC_Stop"))
    ).with_columns(
        merge=pl.when(
            # Time gap between stop and next row start <= 10 min
            pl.col("follow_gap").le(timedelta(minutes=10))
            # Time gap between prev row stop and start <= 10 min
            | pl.col("follow_gap").shift(1).le(timedelta(minutes=10))
            )
        .then(True)
        .otherwise(False)
        ).with_columns(
            # Interval number unique to group of rows needing to be merged
            intv=pl.col("merge").rle_id()
            )
# Rows not being merged
unique = occstatus.filter(~pl.col("merge"))
# Merges consecutive rows that are close together
merged = occstatus.filter(
    pl.col("merge")
    ).group_by("intv").agg(
        cs.contains("Start").min(),
        cs.contains("Stop").max()
        )

occstatus_merged = pl.concat(
    [unique, merged],
    how="diagonal"
    ).sort(
        cs.contains("Start")
        ).select(
            cs.contains("Start", "Stop")
            )

occstatus_merged = {key[0]: df for key, df in occstatus_merged.with_columns(
    pl.col("FTC_Start").dt.strftime("%Y%W").alias("Week")
    ).partition_by("Week", as_dict=True, include_key=False).items()}

# %%

for week, df in licor.items():
    if week.find("2025") == -1:
        continue
    
    # co2_cols = [col for col in df.columns if col.find("CO2") != -1]
    
    week_plot = df.hvplot.scatter(
        x="FTC_DateTime",
        y="CO2_ppm"
        )
    if week in occstatus_merged.keys():
        occ_df = occstatus_merged[week].with_columns(
            cs.contains("FTC").dt.replace_time_zone(None)
            )
        week_plot = (
            week_plot
            * hv.VLines(occ_df["FTC_Start"]).opts(color="Red")
            * hv.VLines(occ_df["FTC_Stop"]).opts(color="Green")
            )
    hvplot.show(week_plot)


