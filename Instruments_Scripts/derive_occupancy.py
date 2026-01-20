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
            ).sort(
                by=cs.contains("UTC")
                ).collect()
doorstatus = {key[0]: df for key, df in doorstatus.with_columns(
    pl.col("FTC_DateTime").dt.strftime("%Y%W").alias("Week")
    ).partition_by("Week", as_dict=True, include_key=False).items()}
# %%

for week, df in doorstatus.items():
    if week.find("2023") == -1:
        continue
    if week in licor.keys():
        door_open = df.filter(pl.col("DoorStatus").eq(1))
        door_closed = df.filter(pl.col("DoorStatus").eq(0))
        week_plot = (
            hv.VLines(door_open["FTC_DateTime"]).opts(color="green")
            * hv.VLines(door_closed["FTC_DateTime"]).opts(color="red")
            )
        week_plot = week_plot * licor[week].filter(
            pl.col("CO2_ppm").is_between(0, 5000)
            ).hvplot.line(
                x="FTC_DateTime",
                y="CO2_ppm"
                )
        hvplot.show(week_plot)

