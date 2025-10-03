# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 14:43:32 2025

@author: agsthk
"""

import os
import polars as pl
import pytz
from datetime import datetime, timedelta
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import hvplot.polars
import holoviews as hv
import polars.selectors as cs

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all structured raw data
CAL_DATA_DIR = os.path.join(data_dir, "Instruments_CalibratedData")
# Full path to automated addition times
ADD_TIMES_PATH = os.path.join(data_dir, "Instruments_ManualData", "Instruments_ManualExperiments", "ManualAdditionTimes - Copy.csv")
add_times = pl.read_csv(ADD_TIMES_PATH).with_columns(
    pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC")
    )
add_times = {key[0]: df for key, df in 
             add_times.partition_by(
                 "Species", as_dict=True, include_key=False
                 ).items()}
AUTO_ADD_TIMES_PATH = os.path.join(data_dir, "Instruments_DerivedData", "AdditionValves_DerivedData", "AdditionValves_AutomatedAdditionTimes.csv")
auto_add_times = pl.read_csv(AUTO_ADD_TIMES_PATH).with_columns(
    pl.selectors.contains("UTC").str.to_datetime()
    )
auto_add_times = {key[0]: df for key, df in 
             auto_add_times.partition_by(
                 "Species", as_dict=True, include_key=False
                 ).items()}
for key, df in add_times.items():
    add_times[key] = pl.concat([df, auto_add_times[key]]).sort(by="UTC_Start")

DOOR_STATUS_PATH = os.path.join(data_dir, "Instruments_DerivedData", "TempRHDoor_DerivedData", "TempRHDoor_DoorStatus.csv")
door_times = pl.read_csv(DOOR_STATUS_PATH).with_columns(
    pl.selectors.contains("UTC").str.to_datetime()
    )
door_times = {key[0]: df for key, df in 
             door_times.partition_by(
                 "DoorStatus", as_dict=True, include_key=False
                 ).items()}
# %% Calibrated 2024 data

cal_dates_2024 = {"2BTech_205_A": "20240604",
                  "2BTech_205_B": "20240604",
                  "Picarro_G2307": "20250625",
                  "ThermoScientific_42i-TL": "20240708"}

cal_dates_2025 = {"2BTech_205_A": "20250115",
                  "2BTech_205_B": "20250115",
                  "Picarro_G2307": "20250625",
                  "ThermoScientific_42i-TL": "20241216"}

data_2024 = {inst: [] for inst in cal_dates_2024.keys()}
data_2025 = {inst: [] for inst in cal_dates_2025.keys()}

for root, dirs, files in tqdm(os.walk(CAL_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        for inst in cal_dates_2024.keys():
            if path.find(inst) != -1:
                break
        if path.find(inst) == -1:
            continue
        
        if path.find("DAQ") != -1:
            if path.find(cal_dates_2025[inst] + "Calibration") != -1:
                data_2025[inst].append(
                    pl.scan_csv(path)
                    )
        else:
            if path.find(cal_dates_2024[inst] + "Calibration") != -1:
                data_2024[inst].append(
                    pl.scan_csv(path)
                    )
            
for inst, lfs in data_2024.items():
    data_2024[inst] = {key[0]: df for key, df in pl.concat(lfs).with_columns(
            cs.contains("UTC").str.to_datetime(time_zone="UTC"),
            cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
            ).sort(
                by=cs.contains("UTC")
                ).with_columns(
                    (cs.contains("FTC") & ~cs.contains("Stop"))
                    .dt.week().alias("Week")
                    ).collect().partition_by(
                        by="Week", include_key=False, as_dict=True
                        ).items()}

                    
for inst, lfs in data_2025.items():
    data_2025[inst] = {key[0]: df for key, df in pl.concat(lfs).with_columns(
            cs.contains("UTC").str.to_datetime(time_zone="UTC"),
            cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
            ).sort(
                by=cs.contains("UTC")
                ).with_columns(
                    (cs.contains("FTC") & ~cs.contains("Stop"))
                    .dt.week().alias("Week")
                    ).collect().partition_by(
                        by="Week", include_key=False, as_dict=True
                        ).items()}
# %%

data_2025["2BTech_205_A_BG"] = {}

for week, df in data_2025["2BTech_205_A"].items():
    week_start = df["UTC_Start"].min()
    week_stop = df["UTC_Stop"].max()
    week_o3_adds = add_times["O3"].filter(
        pl.col("UTC_Stop").ge(week_start)
        & pl.col("UTC_Start").le(week_stop)
        ).with_columns(
            pl.col("UTC_Stop").dt.offset_by("2h")
            )
    if not week_o3_adds.is_empty():
        data_2025["2BTech_205_A_BG"][week] = df.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).join_asof(
                week_o3_adds,
                on="UTC_Start",
                strategy="backward",
                suffix="_Add"
                ).with_columns(
                    pl.when(
                        pl.col("UTC_Start").le(pl.col("UTC_Stop_Add"))
                        )
                    .then(pl.lit(None))
                    .otherwise(pl.col("O3_ppb"))
                    .alias("O3_ppb")
                    ).select(
                        pl.exclude("UTC_Stop_Add")
                        )

data_2025["2BTech_205_A_BG"][15]
# %%

for week, df in data_2025["2BTech_205_A_BG"].items():
    
    bg_o3 = df.filter(
        pl.col("O3_ppb").ge(pl.col("O3_ppb_LOD"))
        ).group_by_dynamic(
            "FTC_Start", every="10m"
            ).agg(
                pl.col("O3_ppb").mean()
                )
    
    plot = df.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).with_columns(
                pl.col("O3_ppb").interpolate_by("FTC_Start")
                ).hvplot.scatter(
                    x="FTC_Start",
                    y="O3_ppb",
                    title=str(week)
                    )
    bg_sub = data_2025["2BTech_205_A"][week].join(
        df.with_columns(
            pl.col("O3_ppb").interpolate_by("FTC_Start")
            ),
        on="FTC_Start",
        suffix="_Background"
        ).with_columns(
            pl.col("O3_ppb").sub(pl.col("O3_ppb_Background")).alias("BG_Sub_O3_ppb")
            )
    plot = plot * bg_sub.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).hvplot.scatter(
                x="FTC_Start",
                y="BG_Sub_O3_ppb"
                )
    if week in data_2025["2BTech_205_B"].keys():
        vent_o3 = data_2025["2BTech_205_B"][week].filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).group_by_dynamic(
                "FTC_Start", every="10m"
                ).agg(
                    pl.col("O3_ppb").mean()
                    )
        io_o3 = bg_o3.join(
            vent_o3,
            on="FTC_Start",
            suffix="_Vent"
            ).with_columns(
                pl.col("O3_ppb").truediv(pl.col("O3_ppb_Vent")).alias("IO")
                )
        # plot = ((plot * data_2025["2BTech_205_B"][week].filter(
        #     pl.col("SamplingLocation").str.contains("C200")
        #     ).hvplot.scatter(
        #         x="FTC_Start",
        #         y="O3_ppb",
        #         title=str(week)
        #         )) + io_o3.hvplot.scatter(
        #             x="FTC_Start",
        #             y="IO"
        #             )).cols(1)
    if week in data_2025["ThermoScientific_42i-TL"].keys():
        plot = (plot + data_2025["ThermoScientific_42i-TL"][week].filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).hvplot.scatter(
                x="FTC_Start",
                y=["NO_ppb", "NO2_ppb"]
                )
                ).cols(1)
        
    hvplot.show(plot)
# %%
data_2025["ThermoScientific_42i-TL"][week]["NO_ppb", 'NO_ppb_Offset']


data_2025["2BTech_205_A"][week]["O3_ppb", 'O3_ppb_Offset']
# %%

for loc, df in data.items():
    fig, ax = plt.subplots(figsize=(10, 8))
    for add_start in add_times["O3"]["UTC_Start"]:
        if add_start < datetime(2025, 1, 24, tzinfo=pytz.UTC) or add_start > datetime(2025, 2, 1, tzinfo=pytz.UTC):
            continue
        add_stop = add_start + timedelta(hours=3)
        add_start_2 = add_start - timedelta(minutes=15)
        temp_df = df.filter(
            pl.col("UTC_Start").is_between(add_start_2, add_stop)
            | pl.col("UTC_Stop").is_between(add_start_2, add_stop)
            )
        if temp_df.is_empty():
            continue
        temp_df = temp_df.with_columns(
            pl.col("UTC_Start").sub(add_start).dt.total_seconds()
            )
        
        pre_add = temp_df.filter(
            pl.col("UTC_Start").lt(0)
            )["O3_ppb"].mean()
        post_add = temp_df.filter(
            pl.col("UTC_Start").gt(8000)
            )["O3_ppb"].mean()
        
        temp_df = temp_df.with_columns(
            pl.when(pl.col("UTC_Start").lt(0))
            .then(pre_add)
            .when(pl.col("UTC_Start").gt(8000))
            .then(post_add)
            .alias("BG_O3")
            ).with_columns(
                pl.col("BG_O3").interpolate_by("UTC_Start")
                ).with_columns(
                    pl.col("O3_ppb").sub(pl.col("BG_O3"))
                    )
        max_o3 = temp_df["O3_ppb"].max()
        ax.plot(temp_df["UTC_Start"], temp_df["O3_ppb"],
                label=(add_start.strftime("%Y-%m-%d %H:%M") + " Max O3 = " + f"{max_o3:.2f}")
                )
    ax.set_title(loc)
    ax.set_xlabel("Seconds from addition start")
    ax.set_ylabel("Background-subtracted O3 (ppb, linear interpolation used)")
    ax.legend(bbox_to_anchor=(1.1, 1.05))

#%%

colors = {"C200": "red",
          "C200/B203": "blue"}
fig, ax = plt.subplots(figsize=(10, 8))
for loc, df in data.items():
    for add_start in add_times["O3"]["UTC_Start"]:
        if add_start < datetime(2025, 1, 25, tzinfo=pytz.UTC) or add_start > datetime(2025, 2, 1, tzinfo=pytz.UTC):
            continue
        add_stop = add_start + timedelta(hours=3)
        add_start_2 = add_start - timedelta(minutes=15)
        temp_df = df.filter(
            pl.col("UTC_Start").is_between(add_start_2, add_stop)
            | pl.col("UTC_Stop").is_between(add_start_2, add_stop)
            )
        if temp_df.is_empty():
            continue
        temp_df = temp_df.with_columns(
            pl.col("UTC_Start").sub(add_start).dt.total_seconds()
            )
        
        pre_add = temp_df.filter(
            pl.col("UTC_Start").lt(0)
            )["O3_ppb"].mean()
        post_add = temp_df.filter(
            pl.col("UTC_Start").gt(8000)
            )["O3_ppb"].mean()

        temp_df = temp_df.with_columns(
            pl.when(pl.col("UTC_Start").lt(0))
            .then(pre_add)
            .when(pl.col("UTC_Start").gt(8000))
            .then(post_add)
            .alias("BG_O3")
            ).with_columns(
                pl.col("BG_O3").interpolate_by("UTC_Start")
                ).with_columns(
                    pl.col("O3_ppb").sub(pl.col("BG_O3"))
                    )
        max_o3 = temp_df["O3_ppb"].max()
        
        temp_df = temp_df.with_columns(
            pl.col("O3_ppb").truediv(max_o3)
            )
                
        max_o3 = temp_df["O3_ppb"].max()
        ax.plot(temp_df["UTC_Start"], temp_df["O3_ppb"],
                color=colors[loc],
                label=(add_start.strftime("%Y-%m-%d %H:%M"))
                )
    ax.set_title("Normalized O3 Decay")
    ax.set_xlabel("Seconds from addition start")
    ax.set_ylabel("Normalized O3 (linear interpolation used)")
    ax.legend(bbox_to_anchor=(1.1, 1.05))
    
    
#%%
fig, ax = plt.subplots(figsize=(10, 8))
for loc, df in data.items():
    for add_start in add_times["O3"]["UTC_Start"]:
        df = df.filter(
            pl.col("UTC_Start"))