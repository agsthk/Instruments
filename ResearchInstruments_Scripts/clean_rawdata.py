# -*- coding: utf-8 -*-
"""
Created on Mon Jul 21 16:52:19 2025

@author: agsthk
"""

import os
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pytz
from datetime import datetime, timedelta

# Declares full path to ResearchInstruments_Data/ directory
data_dir = os.getcwd()
# Starts in ResearchInstruments/ directory
if "ResearchInstruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "ResearchInstruments_Data")

# Full path to directory containing all structured raw data
STRUCT_DATA_DIR = os.path.join(data_dir, "ResearchInstruments_StructuredData")

insts = ["2BTech_202",
         "2BTech_205_A",
         "2BTech_205_B",
         "2BTech_405nm",
         "LI-COR_LI-840A_A",
         "LI-COR_LI-840A_B",
         "Picarro_G2307",
         "TempRHDoor",
         "ThermoScientific_42i-TL"]

sampling_locs = {
    "LI-COR_LI-840A_A": [
        ["2025-01-10T10:10:01-0700", "C200"]
        ],
    "LI-COR_LI-840A_B": [
        ["2025-02-28T17:34:00-0700", "B203"],
        ["2025-02-28T17:37:00-0700", "C200_Vent"]
        ],
    }
sampling_locs = {
    inst: pl.DataFrame(
        locs, schema=["Start", "SamplingLocation"], orient="row"
        ).with_columns(
            pl.col("Start").str.to_datetime()
            ).select(
                pl.col("Start").dt.offset_by("1m"),
                pl.col("Start").shift(-1).dt.offset_by("-1m").alias("Stop"),
                pl.col("SamplingLocation")
                ).fill_null(pl.col("Start").dt.offset_by("1y"))
    for inst, locs in sampling_locs.items()
    }

data = {inst: {} for inst in insts}

for root, dirs, files in os.walk(STRUCT_DATA_DIR):
    for file in files:
        path = os.path.join(root, file)
        for inst in insts:
            if path.find(inst) != -1:
                break
        lf = pl.scan_csv(path).with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC"),
            pl.selectors.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        if inst in sampling_locs.keys():
            lf = lf.with_columns(
                pl.lit(None).alias("SamplingLocation")
                )
            for row in sampling_locs[inst].iter_rows(named=True):
                lf = lf.with_columns(
                    pl.when(
                        pl.col("UTC_DateTime").is_between(
                            row["Start"], row["Stop"]
                            )
                        )
                    .then(
                        pl.lit(row["SamplingLocation"])
                        )
                    .otherwise(
                        pl.col("SamplingLocation")
                        )
                    .alias("SamplingLocation")
                    )
        data[inst][path[-12:-4]] = lf

inst = "LI-COR_LI-840A_A"
for date, lf in data[inst].items():
    if date[:4] != "2025":
        continue
    df = lf.filter(
        pl.col("SamplingLocation").eq("C200")
        ).collect()
    fig, ax = plt.subplots()
    ax.plot(df["UTC_DateTime"], df["CO2_ppm"])
    if date in data[inst[:-1] + "B"].keys():
        df2 = data[inst[:-1] + "B"][date].filter(
            pl.col("SamplingLocation").eq("C200_Vent")
            ).collect()
        ax.plot(df2["UTC_DateTime"], df2["CO2_ppm"])
    ax.xaxis.set_major_formatter(
        mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
        )
    ax.set_title(date)
