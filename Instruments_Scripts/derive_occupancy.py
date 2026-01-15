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
# %%
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
licor = pl.concat(licor, how="diagonal_relaxed").collect().sort(
    by=cs.contains("UTC")
    )

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
aranet = pl.concat(aranet, how="diagonal_relaxed").collect().sort(
    by=cs.contains("UTC")
    ).with_columns(
        # Casts numeric string type columns to float
        ((~cs.contains("DateTime", "Location")) & cs.by_dtype(pl.String))
        .cast(pl.Float64)
        )

# Reads in DoorStatus file
doorstatus = pl.scan_csv(door_path).with_columns(
    cs.contains("UTC").str.to_datetime(time_zone="UTC")
    ).with_columns(
        cs.contains("UTC").dt.convert_time_zone("America/Denver")
        .name.map(lambda x: x.replace("UTC", "FTC"))
        ).collect().sort(
            by=cs.contains("UTC")
            )
# %%