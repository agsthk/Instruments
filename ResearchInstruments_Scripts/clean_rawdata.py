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
from tqdm import tqdm

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
        ["2025-02-12T09:21:00-0700", None], #?
        ["2025-02-12T09:22:00-0700", "C200_Vent"],
        ["2025-02-28T17:35:00-0700", None], #?
        ["2025-02-28T17:37:00-0700", "C200_Vent"], #?
        ["2025-03-03T07:05:00-0700", None], #?
        ["2025-03-03T07:07:00-0700", "C200_Vent"], #?
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
        ["2025-01-15T09:33:00-0700", "B213"],
        ["2025-01-20T16:29:00-0700", "TG_Line"],
        ["2025-01-20T17:11:00-0700", "B203"],
        ["2025-01-20T17:30:00-0700", "TG_Line"],
        ["2025-01-27T11:25:00-0700", "B203"],
        ["2025-01-27T17:15:00-0700", "TG_Line"],
        ["2025-02-04T14:55:00-0700", "B203"],
        ["2025-02-06T15:49:00-0700", "TG_Line"],
        ["2025-02-06T16:54:00-0700", "B203"],
        ["2025-02-07T08:07:00-0700", "TG_Line"]
        ],
    "TG_Line": [
        ["2025-01-17T19:08:00-0700", "C200/B203/Exhaust"],
        ["2025-01-20T16:20:00-0700", None],
        ["2025-01-20T16:29:00-0700", "C200/B203/Exhaust"],
        ["2025-01-20T17:11:00-0700", None],
        ["2025-01-20T17:30:00-0700", "C200/B203"],
        ["2025-01-27T10:37:00-0700", None],
        ["2025-01-27T12:01:00-0700", "C200"],
        ["2025-01-27T17:07:00-0700", None],
        ["2025-01-27T17:45:00-0700", "C200"],
        ["2025-02-03T09:45:00-0700", None],
        ["2025-02-03T12:07:00-0700", "C200"],
        ["2025-02-03T13:55:00-0700", None], #?
        ["2025-02-03T13:56:00-0700", "C200"], #?
        ["2025-02-03T16:22:00-0700", None],
        ["2025-02-03T17:30:00-0700", "C200"],
        ["2025-02-04T09:00:00-0700", None], #?
        ["2025-02-04T09:01:00-0700", "C200"], #?
        ["2025-02-06T14:22:00-0700", None], #?
        ["2025-02-06T14:23:00-0700", "C200"], #?
        ["2025-02-06T15:17:00-0700", None], #?
        ["2025-02-06T16:22:00-0700", "C200"], #?
        ["2025-02-06T16:54:00-0700", None], #?
        ["2025-02-06T16:57:00-0700", "C200"], #?
        ["2025-02-07T08:06:00-0700", None], #?
        ["2025-02-07T08:07:00-0700", "C200"], #?
        ["2025-02-10T10:05:00-0700", None],
        ["2025-02-10T11:37:00-0700", "C200"],
        ["2025-02-12T08:02:00-0700", None], #?
        ["2025-02-12T08:03:00-0700", "C200"], #?
        ["2025-02-12T08:13:00-0700", None], #?
        ["2025-02-12T08:14:00-0700", "C200"], #?
        ["2025-02-12T09:21:00-0700", None], #?
        ["2025-02-12T09:23:00-0700", "C200"], #?
        ["2025-02-12T15:30:00-0700", None], #?
        ["2025-02-13T09:20:00-0700", "C200"], #?
        ["2025-03-06T07:55:00-0700", None], #?
        ["2025-03-06T07:57:00-0700", "C200"], #?
        ["2025-03-06T08:52:00-0700", None], #?
        ["2025-03-06T08:53:00-0700", "C200"], #?
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


data = {inst: {} for inst in insts}

for root, dirs, files in os.walk(STRUCT_DATA_DIR):
    for file in files:
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        for inst in insts:
            if path.find(inst) != -1:
                break
        lf = pl.scan_csv(path).with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC"),
            pl.selectors.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        data[inst][path[-12:-4]] = lf


intvs = []
pic_lf = pl.concat([
     lf.select(
         pl.col("UTC_DateTime"),
         pl.col("SolenoidValves")
         ).with_columns(
             pl.when(pl.col("SolenoidValves").gt(0))
             .then(pl.lit(1))
             .otherwise(pl.col("SolenoidValves")).alias("SolenoidValves")
             )
     for date, lf in data["Picarro_G2307"].items()
     ]).sort(by=["UTC_DateTime", "SolenoidValves"])
pic_df = pic_lf.collect().with_columns(
    pl.col("SolenoidValves").rle_id().alias("intv")
    )
intv_starts = pic_df.unique(
    subset="intv",
    keep="first"
    ).filter(
        pl.col("SolenoidValves").eq(1)
        ).select(
            pl.col("UTC_DateTime").alias("Start"),
            pl.col("intv")
            )
intv_stops = pic_df.unique(
    subset="intv",
    keep="last"
    ).filter(
        pl.col("SolenoidValves").eq(1)
        ).select(
            pl.col("UTC_DateTime").alias("Stop"),
            pl.col("intv")
            )
intvs = intv_starts.join(
    intv_stops, on="intv", how="full"
    ).select(
        pl.col("Start", "Stop")
        )
intvs = intvs.with_columns(
    pl.when(
        pl.col("Start").gt(datetime(2025, 3, 20, 15, 57, tzinfo=pytz.UTC))
        & pl.col("Stop").lt(datetime(2025, 3, 20, 20, 35, tzinfo=pytz.UTC))
        )
    .then(pl.lit("B203"))
    .otherwise(pl.lit("UZA"))
    .alias("SamplingLocation")
    )
gaps = intvs.with_columns(
    pl.col("Stop").shift(1).alias("Start"),
    pl.col("Start").alias("Stop"),
    pl.lit(None).alias("SamplingLocation")
    ).with_columns(
        pl.col("Start").fill_null(pl.col("Stop").dt.offset_by("-1y"))
        )
intvs = pl.concat([intvs, gaps]).sort(by="Start").with_columns(
    pl.col("Start").dt.offset_by("20s"),
    pl.col("Stop").dt.offset_by("-20s")
    )

# Time intervals that Picarro is on TG Line
pic_on_tg = sampling_locs["Picarro_G2307"].filter(
    pl.col("SamplingLocation").eq("TG_Line")
    ).select(
        pl.exclude("SamplingLocation")
        )
# Picarro zeroing/sampling intervals that occur while Picarro on TG Line
intvs_on_tg = intvs.join(
    pic_on_tg, on=None, how="cross").filter(
        (pl.col("Start") < pl.col("Stop_right")) &
        (pl.col("Start_right") < pl.col("Stop"))
        ).select(
            pl.col("Start", "Stop", "SamplingLocation")
            ).unique(maintain_order=True)

# Picarro zeroing/sampling intervals that occur while Picarro on TG Line and
# TG Line sampling from C200 (normal configuration)
intvs_on_tg = intvs_on_tg.join(
    sampling_locs["TG_Line"], on=None, how="cross").filter(
        (pl.col("Start") < pl.col("Stop_right")) &
        (pl.col("Start_right") < pl.col("Stop"))
        ).filter(
            pl.col("SamplingLocation_right").str.contains("C200")
            ).select(
                pl.when(pl.col("Start").lt(pl.col("Start_right")))
                .then(pl.col("Start_right"))
                .otherwise(pl.col("Start"))
                .alias("Start"),
                pl.when(pl.col("Stop").gt(pl.col("Stop_right")))
                .then(pl.col("Stop_right"))
                .otherwise(pl.col("Stop"))
                .alias("Stop"),
                pl.when(pl.col("SamplingLocation").is_null())
                .then(pl.col("SamplingLocation_right"))
                .otherwise(pl.col("SamplingLocation"))
                .alias("SamplingLocation")
                )            
sampling_locs["TG_Line"] = pl.concat(
    [intvs_on_tg,
     sampling_locs["TG_Line"].filter(
         ~pl.col("SamplingLocation").str.contains("C200")
         | pl.col("SamplingLocation").is_null()
         )]
    ).sort(by="Start")

for inst, df in sampling_locs.items():
    sampling_locs[inst] = pl.concat(
        [sampling_locs[inst].filter(
            ~pl.col("SamplingLocation").eq("TG_Line")
            | pl.col("SamplingLocation").is_null()
            ),
        df.join(
            sampling_locs["TG_Line"], on=None, how="cross").filter(
                (pl.col("Start") < pl.col("Stop_right")) &
                (pl.col("Start_right") < pl.col("Stop"))
                ).select(
                    pl.when(pl.col("Start").lt(pl.col("Start_right")))
                    .then(pl.col("Start_right"))
                    .otherwise(pl.col("Start"))
                    .alias("Start"),
                    pl.when(pl.col("Stop").gt(pl.col("Stop_right")))
                    .then(pl.col("Stop_right"))
                    .otherwise(pl.col("Stop"))
                    .alias("Stop"),
                    pl.when(pl.col("SamplingLocation").eq("TG_Line"))
                    .then(pl.col("SamplingLocation_right"))
                    .otherwise(pl.col("SamplingLocation"))
                    .alias("SamplingLocation")
                    )]
        )
                
for inst, lfs in tqdm(data.items()):
    if inst in sampling_locs.keys():
        for date, lf in tqdm(lfs.items()):
            lf = lf.with_columns(
                pl.lit(None).alias("SamplingLocation")
                )
            if "UTC_DateTime" in lf.collect_schema().names():
                rename = "UTC_DateTime"
                compare = rename
            else:
                rename = "UTC_Start"
                compare = "UTC_Stop"
            locs = sampling_locs[inst].rename(
                {"Start": rename, "Stop": "SamplingStop"}
                ).lazy()
            lf = pl.concat(
                [lf, locs], how="diagonal_relaxed"
                ).sort(by=rename).with_columns(
                    pl.col("SamplingLocation").forward_fill(),
                    pl.col("SamplingStop").forward_fill()
                    ).drop_nulls(
                        subset=pl.selectors.float()
                        ).with_columns(
                            pl.when(pl.col(compare).gt(pl.col("SamplingStop")))
                            .then(None)
                            .otherwise(pl.col("SamplingLocation"))
                            .alias("SamplingLocation")
                            )
            data[inst][date] = lf

inst = "2BTech_205_A"
for date, lf in data[inst].items():
    if date[:4] != "2025":
        continue
    fig, ax = plt.subplots()
    df = lf.collect()
    for loc in df["SamplingLocation"].unique():
        if loc is None:
            continue
            temp_df = df.filter(
                pl.col("SamplingLocation").is_null()
                )
            loc = "Invalid"
        else:
            temp_df = df.filter(
                pl.col("SamplingLocation").eq(loc)
                )
        ax.scatter(temp_df["UTC_Start"], temp_df["O3_ppb"], label=loc)
    ax.xaxis.set_major_formatter(
        mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
        )
    ax.set_title(date)
    ax.legend()


inst = "ThermoScientific_42i-TL"
for date, lf in data[inst].items():
    if date[:4] != "2025":
        continue
    fig, ax = plt.subplots()
    df = lf.collect()
    for loc in df["SamplingLocation"].unique():
        if loc is None:
            continue
            temp_df = df.filter(
                pl.col("SamplingLocation").is_null()
                )
            loc = "Invalid"
        else:
            temp_df = df.filter(
                pl.col("SamplingLocation").eq(loc)
                )
        ax.scatter(temp_df["UTC_Start"], temp_df["NO_ppb"], label=loc)
    ax.xaxis.set_major_formatter(
        mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
        )
    ax.set_title(date)
    ax.legend()

inst = "Picarro_G2307"
for date, lf in data[inst].items():
    if date[:4] != "2025":
        continue
    df = lf.filter(
        pl.col("SamplingLocation").eq("UZA")
        ).collect()
    if df.is_empty():
        continue
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
