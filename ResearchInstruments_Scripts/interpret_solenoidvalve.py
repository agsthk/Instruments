# -*- coding: utf-8 -*-
"""
Created on Mon Jul 28 11:42:06 2025

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

# Full path to directory containing structured Picarro G2307 raw data
STRUCT_DATA_DIR = os.path.join(data_dir,
                               "ResearchInstruments_StructuredData",
                               "Picarro_G2307_StructuredData")
# Full path to directory containing data derived from Picarro G2307
DERIVED_DATA_DIR = os.path.join(data_dir,
                                "ResearchInstruments_StructuredData",
                                "Picarro_G2307_DerivedData")
if not os.path.exists(DERIVED_DATA_DIR):
    os.makedirs(DERIVED_DATA_DIR)
# Reads in the UTC_DateTime and SolenoidValves columns for all Picarro G2307
# structured data files
valve_states = []
for root, dirs, files in os.walk(STRUCT_DATA_DIR):
    for file in files:
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        lf = pl.scan_csv(path).with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC")
            )
        valve_states.append(
            lf.select(
                pl.col("UTC_DateTime"),
                pl.col("SolenoidValves")
                )
            )
# Concatenates all Picarro G2307 valve state LazyFrames
valve_states = pl.concat(valve_states).sort(
    # Sorts so that rows with rounded DateTimes are appropriately ordered
    by=["UTC_DateTime", "SolenoidValves"]
    ).collect().with_columns(
        # Assigns a new sampling interval at every SolenoidValves value change
        pl.col("SolenoidValves").rle_id().alias("intv")
        )
# Identifies start of each sampling interval
intv_starts = valve_states.unique(
    subset="intv",
    keep="first"
    ).rename(
        {"UTC_DateTime": "UTC_Start"}
        )
# Identifies end of each sampling interval
intv_stops = valve_states.unique(
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
# Interprets SolenoidValve values and drops times where valve is switching
# between open and closed
intvs = intvs.select(
    pl.selectors.contains("UTC"),
    pl.when(pl.col("SolenoidValves").eq(0))
    .then(pl.lit("Closed"))
    .when(pl.col("SolenoidValves").eq(1))
    .then(pl.lit("Open"))
    .alias("ValveState")
    ).drop_nulls()
# Exports valve state information as CSV file
intvs.write_csv(
    os.path.join(
        DERIVED_DATA_DIR,
        "Picarro_G2307_SolenoidValveStates.csv"
        )
    )
