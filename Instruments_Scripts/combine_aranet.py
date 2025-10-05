# -*- coding: utf-8 -*-
"""
Created on Fri Oct  3 16:58:06 2025

@author: agsthk
"""

import os
import polars as pl
import polars.selectors as cs

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")
# Full path to directory containing all clean raw data
CLEAN_DATA_DIR = os.path.join(data_dir, "Instruments_CleanData")

# Reads in all structured Aranet data
data = {}
for root, dirs, files in os.walk(CLEAN_DATA_DIR):
    for file in files:
        if file.startswith("."):
            continue
        if file.find("Aranet") == -1 or file.find("Aranet4_Clean") != -1:
            continue
        inst = file.split("_Clean")[0]
        if inst not in data.keys():
            data[inst] = []
        path = os.path.join(root, file)
        data[inst].append(pl.read_csv(path))
        
# Concatenates Aranet data from individual sensors and converts fahrenheit to 
# celcius
for inst, dfs in data.items():
    data[inst] = pl.concat(dfs).with_columns(
        # Rounds times to standardize
        cs.contains("UTC").str.to_datetime(time_zone="UTC"),
        cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
        ).sort(cs.contains("UTC"),
               "RoomTemp_C").unique(
                   # Drops rows that report fahrenheit temperature if preceding
                   # row reports celcius temperature
                   ~cs.contains("RoomTemp_C"),
                   keep="last"
                   ).with_columns(
                       # Converts faherenheit temperatures to celcius
                       pl.when(
                           pl.col("RoomTemp_C").gt(45)
                           )
                       .then(
                           (pl.col("RoomTemp_C").sub(32)).mul(5/9)
                           .round_sig_figs(3)
                           )
                       .otherwise(
                           pl.col("RoomTemp_C")
                           )
                       .alias("RoomTemp_C")
                       )

# Joins Aranet data from different sensors by start time
for i, (inst, df) in enumerate(data.items()):
    df = df.select(
        cs.contains("TC", "SamplingLocation"),
        (~cs.contains("TC", "SamplingLocation")).name.suffix(inst.split("Aranet4")[-1]),
        )
    if i == 0:
        joined = df
    else:
        joined = joined.join(
            df,
            on="UTC_Start",
            how="full",
            coalesce=True
            )
    joined = joined.with_columns(
        (pl.when(pl.col(col).is_null())
         .then(pl.col(col + "_right"))
         .otherwise(pl.col(col))
         .alias(col)
         for col in joined.columns if col.find("TC") != -1
         and (col + "_right") in joined.columns)
        ).select(
            ~cs.contains("_right")
            ).with_columns(
                # Fixes averaging time issue - not averaged over 1 minute
                cs.contains("Stop").dt.offset_by("-1s")
                .name.map(lambda name: name.replace("Stop", "Start"))
                          ).sort(
                              by="UTC_Start"
                              ).with_columns(
                                  pl.col("UTC_Start").dt.date().alias("Date")
                                  )
                                  
# Splits joined dataset by FTC date
joined = {key[0].strftime("%Y%m%d"): df for key, df in joined.partition_by(
    "Date", as_dict=True, include_key=False
    ).items()}

# Explorts joined dataset by date
export_dir = os.path.join(
    CLEAN_DATA_DIR,
    "Aranet4_CleanData",
    "Aranet4_CleanLoggerData")
if not os.path.exists(export_dir):
    os.makedirs(export_dir)
for date, df in joined.items():
    export_path = os.path.join(
        export_dir,
        "Aranet4_CleanLoggerData_" + date + ".csv"
        )
    df.write_csv(export_path)
    
