# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 11:11:11 2025

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
                               "AdditionValves_StructuredData")
# Full path to directory containing data derived from Picarro G2307
DERIVED_DATA_DIR = os.path.join(data_dir,
                                "ResearchInstruments_DerivedData",
                                "AdditionValves_DerivedData")
if not os.path.exists(DERIVED_DATA_DIR):
    os.makedirs(DERIVED_DATA_DIR)
    
# Reads in all AdditionValves structured data files
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
                pl.exclude("FTC_DateTime")
                )
            )
# Concatenates all AdditionValves LazyFrames
valve_states = pl.concat(valve_states).sort(
    by="UTC_DateTime"
    ).collect()
# Determines the start and end of each automated O3 addition
o3_starts = valve_states.filter(
    pl.col("ReadPosition").eq("O2Open_OzoneOn")
    | pl.col("ReadPosition").eq("O2Flow_OzoneOn")
    ).rename({"UTC_DateTime": "UTC_Start"})
o3_stops = valve_states.filter(
    pl.col("ReadPosition").eq("O2Open_OzoneOff")
    | pl.col("ReadPosition").eq("O2Flow_OzoneOff")
    ).rename({"UTC_DateTime": "UTC_Stop"})
o3_adds = o3_starts.join_asof(
    o3_stops,
    left_on="UTC_Start",
    right_on="UTC_Stop",
    strategy="forward",
    tolerance="10m"
    ).select(
        pl.selectors.contains("UTC"),
        pl.lit("O3").alias("Species")
        ).drop_nulls()
# Determines the start and end of each automated CO2 addition interval
co2_starts = valve_states.filter(
    pl.col("ReadPosition").eq("CO2Open")
    ).rename(
        {"UTC_DateTime": "UTC_Start"}
        ).filter(
            pl.col("UTC_Start").sub(pl.col("UTC_Start").shift(1))
            .dt.total_seconds().fill_null(601).gt(600)
            )
co2_stops = valve_states.filter(
    pl.col("ReadPosition").eq("CO2Close")
    ).rename(
        {"UTC_DateTime": "UTC_Stop"}
        ).filter(
            pl.col("UTC_Stop").shift(-1).sub(pl.col("UTC_Stop"))
            .dt.total_seconds().fill_null(601).gt(600)
            )
co2_adds = co2_starts.join_asof(
    co2_stops,
    left_on="UTC_Start",
    right_on="UTC_Stop",
    strategy="forward"
    ).select(
        pl.selectors.contains("UTC"),
        pl.lit("CO2").alias("Species")
        )
# Combines O3 and CO2 automated addition
auto_adds = pl.concat([o3_adds, co2_adds], how="vertical").sort(by="UTC_Start")
# Exports automated addition intervals to CSV
auto_adds.write_csv(
    os.path.join(
        DERIVED_DATA_DIR,
        "AdditionValves_AutomatedAdditionTimes.csv"
        )
    )