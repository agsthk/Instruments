# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 10:04:53 2025

@author: agsthk
"""

import os
import polars as pl

# Declares full path to ResearchInstruments_Data/ directory
data_dir = os.getcwd()
# Starts in ResearchInstruments/ directory
if "ResearchInstruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "ResearchInstruments_Data")

# Full path to directory containing structured TempRHDoor raw data
STRUCT_DATA_DIR = os.path.join(data_dir,
                               "ResearchInstruments_StructuredData",
                               "TempRHDoor_StructuredData")
# Full path to directory containing data derived from TempRHDoor
DERIVED_DATA_DIR = os.path.join(data_dir,
                                "ResearchInstruments_DerivedData",
                                "TempRHDoor_DerivedData")
if not os.path.exists(DERIVED_DATA_DIR):
    os.makedirs(DERIVED_DATA_DIR)
# Reads in the UTC_DateTime and DoorStatus columns for all TempRHDoor
# structured data files
door_status = []
for root, dirs, files in os.walk(STRUCT_DATA_DIR):
    for file in files:
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        lf = pl.scan_csv(path).with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC")
            )
        door_status.append(
            lf.select(
                pl.col("UTC_DateTime"),
                pl.col("DoorStatus")
                )
            )
# Concatenates all TempRHDoor DoorStatus LazyFrames
door_status = pl.concat(door_status).sort(
    # Sorts so that rows with rounded DateTimes are appropriately ordered
    by="UTC_DateTime"
    ).collect().with_columns(
        # Assigns a new interval ID at every DoorStatus value change
        pl.col("DoorStatus").rle_id().alias("intv")
        )
# Identifies start of each interval
intv_starts = door_status.unique(
    subset="intv",
    keep="first"
    ).rename(
        {"UTC_DateTime": "UTC_Start"}
        )
# Identifies end of each interval
intv_stops = door_status.unique(
    subset="intv",
    keep="last"
    ).select(
        pl.col("UTC_DateTime").alias("UTC_Stop"),
        pl.col("intv"))
# Joins interval starts and stops
intvs = intv_starts.join(intv_stops,
                         on="intv",
                         coalesce=True,
                         maintain_order="left")

# Filters out intervals where door is closed
intvs = intvs.select(
    pl.selectors.contains("UTC"),
    pl.col("DoorStatus")
    )
# Exports door open intervals as CSVfile
intvs.write_csv(
    os.path.join(
        DERIVED_DATA_DIR,
        "TempRHDoor_DoorStatus.csv"
        )
    )
