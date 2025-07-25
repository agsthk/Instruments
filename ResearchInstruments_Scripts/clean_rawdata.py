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
    "2BTech_205_A": [
        ["2025-01-15T09:03:00-0700", "Calibration Source"],
        ["2025-01-17T10:00:00-0700", "B211"],
        ["2025-01-17T19:08:00-0700", "TG_Line"],
        ],
    "2BTech_205_B": [
        ["2025-01-15T09:18:00-0700", "B211"],
        ["2025-01-15T10:03:00-0700", "Calibration Source"],
        ["2025-01-17T10:00:00-0700", "B211"],
        ["2025-01-20T17:11:00-0700", "C200_Vent"],
        ["2025-02-12T09:21:00-0700", "B203"],
        ["2025-02-12T09:22:00-0700", "C200_Vent"]
        ],
    "LI-COR_LI-840A_A": [
        ["2025-01-10T10:10:01-0700", "C200"]
        ],
    "LI-COR_LI-840A_B": [
        ["2025-02-28T17:34:00-0700", "B203"],
        ["2025-02-28T17:37:00-0700", "C200_Vent"]
        ],
    "Picarro_G2307": [
        ["2025-01-08T17:49:00-0700", "B211"],
        ["2025-01-09T14:55:00-0700", "Calibration Source"],
        ["2025-01-09T18:12:00-0700", "B211"],
        ["2025-01-20T16:29:00-0700", "TG_Line"],
        ["2025-01-27T11:31:00-0700", "B203"],
        ["2025-01-27T11:33:00-0700", "B203/Drierite"],
        ["2025-01-27T16:30:00-0700", "B203"],
        ["2025-01-27T17:17:00-0700", "TG_Line"]
        ],
    "ThermoScientific_42i-TL": [
        ["2025-01-15T09:34:00-0700", "B213"],
        ["2025-01-20T16:29:00-0700", "TG_Line"],
        ["2025-01-20T17:11:00-0700", "B203"],
        ["2025-01-20T17:30:00-0700", "TG_Line"],
        ["2025-01-27T11:25:00-0700", "B203"],
        ["2025-01-27T17:15:00-0700", "TG_Line"],
        ["2025-02-04T14:55:00-0700", "B203"],
        ["2025-02-06T15:49:00-0700", "TG_Line"],
        ["2025-02-06T16:56:00-0700", "B203"],
        ["2025-02-07T08:07:00-0700", "TG_Line"]
        ],
    "TG_Line": [
        ["2025-01-17T19:08:00-0700", "C200/B203/Exhaust"],
        ["2025-01-20T16:20:00-0700", None],
        ["2025-01-20T16:29:00-0700", "C200/B203/Exhaust"],
        ["2025-01-20T17:30:00-0700", "C200/B203"],
        ["2025-01-27T10:37:00-0700", None],
        ["2025-01-27T12:00:00-0700", "C200"],
        ["2025-01-27T17:07:00-0700", None],
        ["2025-01-27T17:45:00-0700", "C200"],
        ["2025-02-03T09:45:00-0700", None],
        ["2025-02-03T12:07:00-0700", "C200"],
        ["2025-02-03T16:22:00-0700", None],
        ["2025-02-03T17:30:00-0700", "C200"],
        ["2025-02-10T10:07:00-0700", None],
        ["2025-02-10T11:37:00-0700", "C200"],
        ["2025-03-17T09:10:00-0600", None],
        ["2025-03-17T10:38:00-0600", "C200"],
        ["2025-03-18T08:55:15-0600", "B203"],
        ["2025-03-18T09:06:05-0600", "C200"],
        ["2025-03-18T09:15:17-0600", "B203"],
        ["2025-03-18T09:24:45-0600", "C200"],
        ["2025-03-18T09:34:30-0600", "B203"],
        ["2025-03-18T09:45:05-0600", "C200"],
        ["2025-03-20T08:32:15-0600", "B203"],
        ["2025-03-20T08:42:55-0600", "C200"],
        ["2025-03-20T08:50:44-0600", "B203"],
        ["2025-03-20T09:03:55-0600", "C200"],
        ["2025-03-20T09:13:15-0600", "B203"],
        ["2025-03-20T09:23:10-0600", "C200"]
        ]
    }
sampling_locs = {
    inst: pl.DataFrame(
        locs, schema=["Start", "SamplingLocation"], orient="row"
        ).with_columns(
            pl.col("Start").str.to_datetime()
            ).select(
                pl.col("Start"),#.dt.offset_by("1m"),
                pl.col("Start").shift(-1).dt.offset_by("-1s").alias("Stop"),
                pl.col("SamplingLocation")
                ).with_columns(
                    pl.col("Stop").fill_null(pl.col("Start").dt.offset_by("1y"))
                    )
    for inst, locs in sampling_locs.items()
    }

for inst, df in sampling_locs.items():
    on_tg = df.filter(
        pl.col("SamplingLocation").eq("TG_Line")
        )
    if on_tg.is_empty():
        continue
    for row in on_tg.iter_rows(named=True):
        tg_locs = sampling_locs["TG_Line"].filter(
            pl.col("Stop").ge(row["Start"])
            & pl.col("Start").le(row["Stop"])
            )
        tg_locs = tg_locs.with_columns(
            pl.when(pl.col("Start").lt(row["Start"]))
            .then(pl.lit(row["Start"]))
            .otherwise(pl.col("Start")).alias("Start"),
            pl.when(pl.col("Stop").gt(row["Stop"]))
            .then(pl.lit(row["Stop"]))
            .otherwise(pl.col("Stop")).alias("Stop")
            )
    df = pl.concat([df, tg_locs]).sort(by="Start").filter(
        pl.col("SamplingLocation").ne("TG_Line")
        )
    sampling_locs[inst] = df
sampling_locs.pop("TG_Line")

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

inst = "2BTech_205_A"
for date, lf in data[inst].items():
    if date[:4] != "2025":
        continue
    df = lf.collect()
    fig, ax = plt.subplots()
    ax.plot(df["UTC_Start"], df["O3_ppb"])
    ax.xaxis.set_major_formatter(
        mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
        )
    ax.set_title(date)

inst = "Picarro_G2307"
for date, lf in data[inst].items():
    if date[:4] != "2025":
        continue
    df = lf.collect()
    fig, ax = plt.subplots()
    ax.plot(df["UTC_DateTime"], df["CH2O_ppm"])
    ax.xaxis.set_major_formatter(
        mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
        )
    ax.set_title(date)

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
