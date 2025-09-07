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
SAMPLING_LOC_DIR = os.path.join(data_dir, "Instruments_ManualData", "Instruments_SamplingLocations")

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

sampling_locs = {}
for sampling_locs_file in os.listdir(SAMPLING_LOC_DIR):
    inst = sampling_locs_file.rsplit("_", 1)[0]
    sampling_locs_path = os.path.join(SAMPLING_LOC_DIR, sampling_locs_file)
    sampling_locs[inst] = pl.read_csv(sampling_locs_path).with_columns(
        UTC_Start=pl.col("FTC_Start").str.to_datetime()
        ).select(
            pl.col("UTC_Start"),
            pl.col("UTC_Start").shift(-1).alias("UTC_Stop"),
            pl.col("SamplingLocation")
            ).with_columns(
                pl.col("UTC_Stop").fill_null(
                    pl.col("UTC_Start").dt.offset_by("1y")
                    )
                )

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
    pl.col("SamplingLocation").eq("TGLine")
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
    sampling_locs["TGLine"], on=None, how="cross"
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
    sampling_locs["TGLine"], on=None, how="cross"
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

sampling_locs["TGLine"] = pl.concat([
    valve_on_tg,
    valve_off_tg,
    sampling_locs["TGLine"].filter(
        ~pl.col("SamplingLocation").str.contains("C200")
        )
    ]).sort(by="UTC_Start")

for inst, df in sampling_locs.items():
    if "TGLine" not in df["SamplingLocation"]:
        continue
    temp_locs = pl.concat(
        [sampling_locs[inst].filter(
            ~pl.col("SamplingLocation").eq("TGLine")
            | pl.col("SamplingLocation").is_null()
            ),
        df.join(
            sampling_locs["TGLine"], on=None, how="cross").filter(
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
                    pl.when(pl.col("SamplingLocation").eq("TGLine"))
                    .then(pl.col("SamplingLocation_right"))
                    .otherwise(pl.col("SamplingLocation"))
                    .alias("SamplingLocation")
                    )]
        ).sort(by="UTC_Start")
    sampling_locs[inst] = temp_locs
    
for inst, df in sampling_locs.items():
    sampling_locs[inst] = df.with_columns(
        pl.col("UTC_Start").dt.offset_by("60s"),
        pl.col("UTC_Stop").dt.offset_by("-60s")
        )

#%%

avg_times = {
    "2BTech_202": {
        "2024-01-18 20:39:16+00:00": "10s",
        "2024-01-19 23:05:11+00:00": "60s"
        },
    "2BTech_205_A": {
        "2022-09-30 18:49:04+00:00": "10s",
        "2024-01-20 02:54:18+00:00": "60s",
        "2024-01-19 17:22:26+00:00": "10s",
        "2024-01-19 23:05:18+00:00": "60s",
        "2024-03-11 19:27:01+00:00": "2s",
        "2024-03-11 23:30:47+00:00": "60s",
        "2024-06-04 16:21:24+00:00": "2s",
        "2024-06-04 19:01:29+00:00": "60s",
        "2024-06-05 21:11:14+00:00": "2s",
        "2024-06-11 14:03:39+00:00": "60s",
        "2025-01-15 16:04:20+00:00": "10s"
        },
    "2BTech_205_B": {
        "2023-05-12 03:50:59+00:00": "10s",
        "2024-06-04 18:52:23+00:00": "2s",
        "2024-06-10 20:39:53+00:00": "60s",
        "2025-01-15 17:17:42+00:00": "10s",
        },
    "2BTech_405nm": {
        "2024-01-17 18:38:28+00:00": "60s",
        "2024-01-19 20:44:52+00:00": "5s",
        "2024-02-01 22:30:59+00:00": "60s",
        "2024-06-05 21:11:35+00:00": "5s",
        "2024-06-11 14:04:55+00:00": "60s",
        "2024-06-25 18:31:08+00:00": "5s",
        "2024-06-25 21:09:43+00:00": "60s",
        "2024-07-02 15:42:36+00:00": "5s",
        "2024-07-02 20:17:40+00:00": "60s",
        },
    "Aranet4_1F16F": {
        "2025-03-21 02:14:00+00:00": "60s"
        },
    "Aranet4_1FB20": {
        "2025-03-21 02:15:00+00:00": "60s"
        },
    "ThermoScientific_42i-TL": {
        "2024-01-19 17:39:00+00:00": "60s"
        }
    }

for inst, times in avg_times.items():
    avg_times[inst] = pl.DataFrame(times).transpose(
        include_header=True,
        header_name="UTC_Start",
        column_names=["AvgT"]
        ).with_columns(
                pl.col("UTC_Start").str.to_datetime()
                ).sort(
                    by="UTC_Start"
                    ).lazy()

data = {inst: {} for inst in insts}

#%%
wrong_dates = {}
for date, df in data["2BTech_202"].items():
    df = df.with_columns(
        pl.col("FTC_Start").dt.strftime("%Y%m%d").alias("Date")
        )
    dfs = {key[0]: d for key, d in df.partition_by(
        "Date", as_dict=True, include_key=False
        ).items()}
    
    wrong_date_keys = [d for d in dfs.keys() if d != date]
    for key in dfs.keys():
        if key == date:
            continue
        if key in wrong_dates.keys():
            wrong_dates[key] = pl.concat(
                [wrong_dates[key],
                 dfs[key]]
                )
        else:
            wrong_dates[key] = dfs[key]
    df = dfs[date]
                     
                     
#%%

for root, dirs, files in tqdm(os.walk(STRUCT_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        for inst in insts:
            if path.find(inst) != -1:
                break
        if path.find(inst) == -1:
            continue
        if inst == "2BTech_405nm":
            lf = pl.scan_csv(path, infer_schema_length=None)
        else:
            lf = pl.scan_csv(path)
        lf = lf.with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC"),
            pl.selectors.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        if inst in avg_times.keys():
            lf = lf.join_asof(
                    avg_times[inst],
                    left_on="UTC_DateTime",
                    right_on="UTC_Start",
                    strategy="backward"
                    )
            lf = lf.select(
                pl.col("UTC_DateTime").dt.offset_by("-" + pl.col("AvgT")).alias("UTC_Start"),
                pl.col("UTC_DateTime").alias("UTC_Stop"),
                pl.col("FTC_DateTime").dt.offset_by("-" + pl.col("AvgT")).alias("FTC_Start"),
                pl.col("FTC_DateTime").alias("FTC_Stop"),
                pl.exclude("UTC_DateTime", "FTC_DateTime", "UTC_Start", "AvgT")
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
                locs.sort(by=on), on=on, strategy="backward", coalesce=True
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
                    pl.exclude("WarmUp")
                    )
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
                pl.col("O3_ppb").is_between(-5, 200)
                )
        lf = lf.select(
            pl.exclude("Sampling_Stop")
            )
        df = lf.collect()
        
        if df.is_empty():
            continue
        #%%
        
        
        data[inst][file.rsplit("_", 1)[-1][:-4]] = df
        _, source = file[:-17].split("_Structured")
        f_name = inst + "_Clean" + source + "Data_" + path[-12:-4] + ".csv"
        f_dir = os.path.join(CLEAN_DATA_DIR,
                             inst + "_CleanData",
                             inst + "_Clean" + source + "Data")
        if not os.path.exists(f_dir):
            os.makedirs(f_dir)
        path = os.path.join(f_dir,
                            f_name)
        df.write_csv(path)
