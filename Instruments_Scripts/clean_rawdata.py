# -*- coding: utf-8 -*-
"""
Created on Mon Jul 21 16:52:19 2025

@author: agsthk
"""

import os
import polars as pl
import pytz
from datetime import datetime
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all structured raw data
STRUCT_DATA_DIR = os.path.join(data_dir, "Instruments_StructuredData")
# Full path to directory containing all clean raw data
CLEAN_DATA_DIR = os.path.join(data_dir, "Instruments_CleanData")
# Creates Instruments_CleanData/ directory if needed
if not os.path.exists(CLEAN_DATA_DIR):
    os.makedirs(CLEAN_DATA_DIR)
# Full path to automated addition times
ADD_TIMES_PATH = os.path.join(data_dir, "Instruments_DerivedData", "AdditionValves_DerivedData", "AdditionValves_AutomatedAdditionTimes.csv")
add_times = pl.read_csv(ADD_TIMES_PATH).with_columns(
    pl.selectors.contains("UTC").str.to_datetime()
    )
add_times = {key[0]: df for key, df in 
             add_times.partition_by(
                 "Species", as_dict=True, include_key=False
                 ).items()}

insts = ["2BTech_202",
         "2BTech_205_A",
         "2BTech_205_B",
         "2BTech_405nm",
         "Aranet4_1F16F",
         "Aranet4_1FB20",
         "LI-COR_LI-840A_A",
         "LI-COR_LI-840A_B",
         "Picarro_G2307",
         "TempRHDoor",
         "ThermoScientific_42i-TL"]

sampling_locs = {
    "2BTech_202" : [
        ["2024-01-18T13:39:00-0700", "Calibration Source"],
        ["2024-01-18T17:14:00-0700", "B213"]
        ],
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
    "2BTech_405nm" : [
        ["2024-12-16T15:59:00-0700", "Calibration Source"]
        ],
    "Aranet4_1F16F": [
        ["2025-03-20T20:13:00-0600", "C200"]
        ],
    "Aranet4_1FB20": [
        ["2025-03-20T20:15:00-0600", "C200"]
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
        ["2025-01-28T01:44:08-0700", "UZA"],
        ["2025-01-28T01:54:05-0700", "C200"],
        ["2025-01-28T05:44:08-0700", "UZA"],
        ["2025-01-28T05:54:05-0700", "C200"],
        ["2025-01-28T09:44:08-0700", "UZA"],
        ["2025-01-28T09:54:05-0700", "C200"],
        ["2025-02-03T09:45:00-0700", None],
        ["2025-02-03T12:07:00-0700", "C200"],
        ["2025-02-03T13:55:00-0700", None], #?
        ["2025-02-03T13:56:00-0700", "C200"], #?
        ["2025-02-03T14:35:00-0700", None], #?
        ["2025-02-03T14:37:00-0700", "C200"], #?
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
        ["2025-02-17T12:33:00-0700", "UZA"],
        ["2025-02-17T12:35:00-0700", "C200"],
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
        ["2025-03-20T09:23:10-0600", "C200"],
        ["2025-05-05T12:47:00-0600", None]
        ]
    }
sampling_locs = {
    inst: pl.DataFrame(
        locs, schema=["UTC_Start", "SamplingLocation"], orient="row"
        ).with_columns(
            pl.col("UTC_Start").str.to_datetime()
            ).select(
                pl.col("UTC_Start"),
                pl.col("UTC_Start").shift(-1).alias("UTC_Stop"),
                pl.col("SamplingLocation")
                ).with_columns(
                    pl.col("UTC_Stop").fill_null(
                        pl.col("UTC_Start").dt.offset_by("1y")
                        )
                    )
    for inst, locs in sampling_locs.items()
    }

valve_states = pl.read_csv(
    os.path.join(
        data_dir,
        "Instruments_DerivedData",
        "Picarro_G2307_DerivedData",
        "Picarro_G2307_SolenoidValveStates.csv"
        )
    ).with_columns(
        pl.selectors.contains("UTC").str.to_datetime()
        )
        
valve_states = valve_states.select(
    pl.selectors.contains("UTC"),
    pl.when(
        pl.col("UTC_Start").gt(datetime(2025, 3, 20, 15, 57, tzinfo=pytz.UTC))
        & pl.col("UTC_Stop").lt(datetime(2025, 3, 20, 20, 35, tzinfo=pytz.UTC))
        & pl.col("SolenoidValves").eq(1))
    .then(pl.lit("B203"))
    .when(pl.col("SolenoidValves").eq(1))
    .then(pl.lit("UZA"))
    .alias("SamplingLocation")
    )
# Intervals that Picarro G2307 is on the Trace Gas Line
pic_on_tg = sampling_locs["Picarro_G2307"].filter(
    pl.col("SamplingLocation").eq("TG_Line")
    ).select(
        pl.exclude("SamplingLocation")
        )
# Intervals that Picarro G2307 is not on the Trace Gas Line
pic_off_tg = pl.concat(
    [pic_on_tg.with_columns(
        pl.col("UTC_Start").alias("UTC_Stop"),
        pl.col("UTC_Stop").shift(1).alias("UTC_Start")
        ),
    pic_on_tg.with_columns(
        pl.col("UTC_Start").shift(-1).alias("UTC_Stop"),
        pl.col("UTC_Stop").alias("UTC_Start")
        )]
    ).unique(maintain_order=True).with_columns(
        pl.col("UTC_Start").fill_null(
            pl.col("UTC_Stop").dt.offset_by("-1y")
            ),
        pl.col("UTC_Stop").fill_null(
            pl.col("UTC_Start").dt.offset_by("1y")
            )
        )

# Valve states while Picarro G2307 is on the Trace Gas Line
valve_on_tg = valve_states.join(pic_on_tg, on=None, how="cross").filter(
    pl.col("UTC_Start").lt(pl.col("UTC_Stop_right"))
    & pl.col("UTC_Start_right").lt(pl.col("UTC_Stop"))
    ).select(
        pl.when(pl.col("UTC_Start").lt(pl.col("UTC_Start_right")))
        .then(pl.col("UTC_Start_right"))
        .otherwise(pl.col("UTC_Start"))
        .alias("UTC_Start"),
        pl.when(pl.col("UTC_Stop").gt(pl.col("UTC_Stop_right")))
        .then(pl.col("UTC_Stop_right"))
        .otherwise(pl.col("UTC_Stop"))
        .alias("UTC_Stop"),
        pl.col("SamplingLocation")
        )
# Assigns sampling locations
valve_on_tg = valve_on_tg.join(
    sampling_locs["TG_Line"], on=None, how="cross"
    ).filter(
        pl.col("UTC_Start").lt(pl.col("UTC_Stop_right"))
        & pl.col("UTC_Start_right").lt(pl.col("UTC_Stop"))
        ).filter(
            pl.col("SamplingLocation_right").str.contains("C200")
            ).select(
                pl.when(pl.col("UTC_Start").lt(pl.col("UTC_Start_right")))
                .then(pl.col("UTC_Start_right"))
                .otherwise(pl.col("UTC_Start"))
                .alias("UTC_Start"),
                pl.when(pl.col("UTC_Stop").gt(pl.col("UTC_Stop_right")))
                .then(pl.col("UTC_Stop_right"))
                .otherwise(pl.col("UTC_Stop"))
                .alias("UTC_Stop"),
                pl.when(pl.col("SamplingLocation").is_null())
                .then(pl.col("SamplingLocation_right"))
                .otherwise(pl.col("SamplingLocation"))
                .alias("SamplingLocation")
                )

valve_off_tg = pic_off_tg.join(
    sampling_locs["TG_Line"], on=None, how="cross"
    ).filter(
        pl.col("UTC_Start").lt(pl.col("UTC_Stop_right"))
        & pl.col("UTC_Start_right").lt(pl.col("UTC_Stop"))
        ).filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).select(
                pl.when(pl.col("UTC_Start").lt(pl.col("UTC_Start_right")))
                .then(pl.col("UTC_Start_right"))
                .otherwise(pl.col("UTC_Start"))
                .alias("UTC_Start"),
                pl.when(pl.col("UTC_Stop").gt(pl.col("UTC_Stop_right")))
                .then(pl.col("UTC_Stop_right"))
                .otherwise(pl.col("UTC_Stop"))
                .alias("UTC_Stop"),
                pl.col("SamplingLocation")
                )

sampling_locs["TG_Line"] = pl.concat([
    valve_on_tg,
    valve_off_tg,
    sampling_locs["TG_Line"].filter(
        ~pl.col("SamplingLocation").str.contains("C200")
        )
    ]).sort(by="UTC_Start")

for inst, df in sampling_locs.items():
    temp_locs = pl.concat(
        [sampling_locs[inst].filter(
            ~pl.col("SamplingLocation").eq("TG_Line")
            | pl.col("SamplingLocation").is_null()
            ),
        df.join(
            sampling_locs["TG_Line"], on=None, how="cross").filter(
                (pl.col("UTC_Start") < pl.col("UTC_Stop_right")) &
                (pl.col("UTC_Start_right") < pl.col("UTC_Stop"))
                ).select(
                    pl.when(pl.col("UTC_Start").lt(pl.col("UTC_Start_right")))
                    .then(pl.col("UTC_Start_right"))
                    .otherwise(pl.col("UTC_Start"))
                    .alias("UTC_Start"),
                    pl.when(pl.col("UTC_Stop").gt(pl.col("UTC_Stop_right")))
                    .then(pl.col("UTC_Stop_right"))
                    .otherwise(pl.col("UTC_Stop"))
                    .alias("UTC_Stop"),
                    pl.when(pl.col("SamplingLocation").eq("TG_Line"))
                    .then(pl.col("SamplingLocation_right"))
                    .otherwise(pl.col("SamplingLocation"))
                    .alias("SamplingLocation")
                    )]
        ).sort(by="UTC_Start")
    sampling_locs[inst] = temp_locs.with_columns(
        pl.col("UTC_Start").dt.offset_by("60s"),
        pl.col("UTC_Stop").dt.offset_by("-60s")
        )

data = {inst: {} for inst in insts}

for root, dirs, files in tqdm(os.walk(STRUCT_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        for inst in insts:
            if path.find(inst) != -1:
                break
        if inst == "ThermoScientific_42i-TL" and path.find(inst) == -1:
            continue
        if inst == "2BTech_405nm":
            lf = pl.scan_csv(path, infer_schema_length=None)
        else:
            lf = pl.scan_csv(path)
        lf = lf.with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC"),
            pl.selectors.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        if inst in sampling_locs.keys():
            if "UTC_DateTime" in lf.collect_schema().names():
                on = "UTC_DateTime"
                compare = on
                locs = sampling_locs[inst].rename(
                    {"UTC_Start": "UTC_DateTime",
                     "UTC_Stop": "Sampling_Stop"}
                    ).lazy()
            else:
                on = "UTC_Start"
                compare = "UTC_Stop"
                locs = sampling_locs[inst].rename(
                    {"UTC_Stop": "Sampling_Stop"}
                    ).lazy()
            lf = lf.join_asof(
                locs, on=on, strategy="backward", coalesce=True
                )
            lf = lf.with_columns(
                pl.when(pl.col(compare).gt(pl.col("Sampling_Stop")))
                .then(pl.lit(None))
                .otherwise(pl.col("SamplingLocation"))
                .alias("SamplingLocation")
                )
        if "WarmUp" in lf.collect_schema().names():
            lf = lf.filter(
                pl.col("WarmUp").eq(0)
                ).select(
                    pl.exclude("WarmUp"))
        if inst == "ThermoScientific_42i-TL":
            lf = lf.filter(
                pl.col("SampleFlow_LPM").gt(0.5)
                )
        if inst == "2BTech_205_A":
            lf_room = lf.filter(
                pl.col("SamplingLocation").str.contains("C200")
                )
            h0 = lf_room.collect().height
            lf_start = lf_room.select("UTC_Start").min().collect().item()
            lf_stop = lf_room.select("UTC_Stop").max().collect().item()
            lf_adds = add_times["O3"].filter(
                pl.col("UTC_Start").is_between(lf_start, lf_stop)
                | pl.col("UTC_Stop").is_between(lf_start, lf_stop)
                )
            lf_adds = lf_adds.with_columns(
                pl.col("UTC_Stop").dt.offset_by("20m")
                ).lazy()
            lf_room = lf_room.join_asof(
                lf_adds,
                on="UTC_Start",
                coalesce=False,
                strategy="backward",
                suffix="_Add"
                ).with_columns(
                    pl.when(pl.col("UTC_Start").le(pl.col("UTC_Stop_Add")))
                    .then(pl.lit(True))
                    .otherwise(pl.lit(False))
                    .alias("KeepDefault")
                    ).select(
                        ~pl.selectors.contains("_Add")
                        )
            lf_room = lf_room.with_columns(
                pl.col("O3_ppb").sub(pl.col("O3_ppb").shift(-1)).abs().alias("f_diff"),
                pl.col("O3_ppb").sub(pl.col("O3_ppb").shift(1)).abs().alias("r_diff"),
                pl.col("UTC_Start").sub(pl.col("UTC_Start").shift(-1)).dt.total_microseconds().truediv(1e6).abs().alias("f_dt"),
                pl.col("UTC_Start").sub(pl.col("UTC_Start").shift(1)).dt.total_microseconds().truediv(1e6).abs().alias("r_dt"),
                pl.col("O3_ppb").rolling_median_by(by="UTC_Start", window_size="10m").alias("median")
                ).with_columns(
                    pl.col("f_diff").truediv(pl.col("f_dt")).alias("f_d/dt"),
                    pl.col("r_diff").truediv(pl.col("r_dt")).alias("r_d/dt"),
                    pl.col("O3_ppb").sub(pl.col("median")).abs().alias("abs_diff")
                    ).with_columns(
                        pl.mean_horizontal("f_d/dt", "r_d/dt").alias("d/dt"),
                        pl.col("abs_diff").rolling_median_by(by="UTC_Start", window_size="10m").mul(1.4826).alias("MAD")
                        )
            lf_room = lf_room.filter(
                (pl.col("O3_ppb").sub(pl.col("median")).abs().le(pl.col("MAD").mul(3))
                | pl.col("d/dt").lt(0.25)
                | pl.col("KeepDefault"))
                & pl.col("O3_ppb").gt(-8)
                )
            hf = lf_room.collect().height
            keep_cols = lf.collect_schema().names()
            lf = pl.concat(
                [lf_room.select(
                    pl.col(keep_cols)
                    ),
                 lf.filter(
                     ~pl.col("SamplingLocation").str.contains("C200")
                     )]
                ).sort(
                    by="UTC_Start"
                    )
        if inst == "2BTech_205_B":
            lf = lf.filter(
                pl.col("O3_ppb").is_between(-5, 200))
        df = lf.collect()
        
        if df.is_empty():
            continue
        data[inst][file.rsplit("_", 1)[-1][:-4]] = df
        # _, source = file[:-17].split("_Structured")
        # f_name = inst + "_Clean" + source + "Data_" + path[-12:-4] + ".csv"
        # f_dir = os.path.join(CLEAN_DATA_DIR,
        #                      inst + "_CleanData",
        #                      inst + "_Clean" + source + "Data")
        # if not os.path.exists(f_dir):
        #     os.makedirs(f_dir)
        # path = os.path.join(f_dir,
        #                     f_name)
        # df.write_csv(path)
