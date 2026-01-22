# -*- coding: utf-8 -*-
"""
Created on Thu Jan 15 14:34:41 2026

@author: agsthk
"""
# %% Imports and definitions
import os
import polars as pl
import polars.selectors as cs
import hvplot.polars
import holoviews as hv
from datetime import datetime, timedelta
import pytz

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing clean data
CLEAN_DATA_DIR = os.path.join(data_dir,
                              "Instruments_CleanData")
# LI-COR CO2 data directory
LICOR_DATA_DIR = os.path.join(CLEAN_DATA_DIR,
                              "LI-COR_LI-840A_A_CleanData",
                              "LI-COR_LI-840A_A_CleanLoggerData")
os.path.exists(LICOR_DATA_DIR)
# Aranet CO2 data directory
ARANET_DATA_DIR = os.path.join(CLEAN_DATA_DIR,
                               "Aranet4_CleanData",
                               "Aranet4_CleanLoggerData")

# Full path to door status file
door_path = os.path.join(data_dir,
                         "Instruments_DerivedData",
                         "TempRHDoor_DerivedData",
                         "TempRHDoor_DoorStatus.csv")
# Full path to occupancy status file
occ_path = os.path.join(data_dir,
                         "Instruments_ManualData",
                         "Instruments_Occupancy",
                         "OccupancyStatus.txt")
# Path to occupancy ICARTT data
occ_ict_path = os.path.join(
    data_dir,
    "Instruments_ICARTTData",
    "Occupancy_ICARTTData",
    "Occupancy_ICARTTData_R0"
    )
# %% Reads and processes data
# Reads in LI-COR CO2 files
licor = []
for root, dirs, files in os.walk(LICOR_DATA_DIR):
    for file in files:
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        lf = pl.scan_csv(path).with_columns(
            cs.contains("UTC").str.to_datetime(time_zone="UTC"),
            cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        licor.append(lf)
# Concatenates LI-COR CO2 files
licor = pl.concat(licor, how="diagonal_relaxed").collect().sort(
    by=cs.contains("UTC")
    ).with_columns(
        cs.contains("DateTime").dt.replace_time_zone(None)
        )
# Splits LI-COR CO2 files by week
licor = {key[0]: df for key, df in licor.with_columns(
    pl.col("FTC_DateTime").dt.strftime("%Y%W").alias("Week")
    ).partition_by("Week", as_dict=True, include_key=False).items()}
# Reads in Aranet CO2 files
aranet = []
for root, dirs, files in os.walk(ARANET_DATA_DIR):
    for file in files:
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        lf = pl.scan_csv(path).with_columns(
            cs.contains("UTC").str.to_datetime(time_zone="UTC"),
            cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        aranet.append(lf)
# Concatenates Aranet CO2 files
aranet = pl.concat(aranet, how="diagonal_relaxed").collect().sort(
    by=cs.contains("UTC")
    ).with_columns(
        # Casts numeric string type columns to float
        ((~cs.contains("DateTime", "Location")) & cs.by_dtype(pl.String))
        .cast(pl.Float64)
        ).with_columns(
            cs.contains("DateTime").dt.replace_time_zone(None)
            )
# Splits Aranet CO2 files by week
aranet = {key[0]: df for key, df in aranet.with_columns(
    pl.col("FTC_DateTime").dt.strftime("%Y%W").alias("Week")
    ).partition_by("Week", as_dict=True, include_key=False).items()}

# Reads in DoorStatus file
doorstatus = pl.scan_csv(door_path).with_columns(
    cs.contains("UTC").str.to_datetime(time_zone="UTC")
    ).with_columns(
        cs.contains("UTC").dt.convert_time_zone("America/Denver")
        .name.map(lambda x: x.replace("UTC", "FTC"))
        ).with_columns(
            cs.contains("Start")
            .name.map(lambda x: x.replace("Start", "DateTime"))
            ).with_columns(
                cs.contains("DateTime").dt.replace_time_zone(None)
                ).sort(
                    by=cs.contains("UTC")
                    ).collect()
doorstatus = {key[0]: df for key, df in doorstatus.with_columns(
    pl.col("FTC_DateTime").dt.strftime("%Y%W").alias("Week")
    ).partition_by("Week", as_dict=True, include_key=False).items()}
# Reads in OccupancyStatus file
occstatus = pl.scan_csv(occ_path).with_columns(
    cs.contains("UTC").str.to_datetime(time_zone="UTC")
    ).with_columns(
        cs.contains("UTC").dt.convert_time_zone("America/Denver")
        .name.map(lambda x: x.replace("UTC", "FTC"))
        ).sort(
            by=cs.contains("UTC")
            ).collect()
# occstatus = {key[0]: df for key, df in occstatus.with_columns(
#     pl.col("FTC_Start").dt.strftime("%Y%W").alias("Week")
#     ).partition_by("Week", as_dict=True, include_key=False).items()}
# %%
phase1_start = datetime(2024, 3, 1, tzinfo=pytz.timezone("America/Denver"))
phase1_stop = datetime(2024, 8, 16, 23, 59, 59,
                       tzinfo=pytz.timezone("America/Denver"))
phase2_start = datetime(2025, 1, 17, tzinfo=pytz.timezone("America/Denver"))
phase2_stop = datetime(2025, 5, 5, 23, 59, 59,
                       tzinfo=pytz.timezone("America/Denver"))
phase3_start = datetime(2026, 1, 13, 2, tzinfo=pytz.timezone("America/Denver"))

# %%

occstatus = occstatus.with_columns(
    follow_gap=pl.col("UTC_Start").shift(-1).sub(pl.col("UTC_Stop"))
    ).with_columns(
        merge=pl.when(
            # Time gap between stop and next row start <= 10 min
            pl.col("follow_gap").le(timedelta(minutes=10))
            # Time gap between prev row stop and start <= 10 min
            | pl.col("follow_gap").shift(1).le(timedelta(minutes=10))
            )
        .then(True)
        .otherwise(False)
        ).with_columns(
            # Interval number unique to group of rows needing to be merged
            intv=pl.col("merge").rle_id()
            )
# Rows not being merged
unique = occstatus.filter(~pl.col("merge"))
# Merges consecutive rows that are close together
merged = occstatus.filter(
    pl.col("merge")
    ).group_by("intv").agg(
        cs.contains("Start").min(),
        cs.contains("Stop").max()
        )

occstatus_merged = pl.concat(
    [unique, merged],
    how="diagonal"
    ).sort(
        cs.contains("Start")
        ).select(
            cs.contains("Start", "Stop")
            )

# occstatus_merged = {key[0]: df for key, df in occstatus_merged.with_columns(
#     pl.col("FTC_Start").dt.strftime("%Y%W").alias("Week")
#     ).partition_by("Week", as_dict=True, include_key=False).items()}

# %%

occupied = occstatus_merged.with_columns(OccupancyStatus=1)
# Determines unoccupied times as gaps between occupied times
unoccupied = occstatus_merged.with_columns(
    UTC_Start=pl.col("UTC_Stop").dt.offset_by("1s"),
    UTC_Stop=pl.col("UTC_Start").shift(-1).dt.offset_by("-1s"),
    FTC_Start=pl.col("FTC_Stop").dt.offset_by("1s"),
    FTC_Stop=pl.col("FTC_Start").shift(-1).dt.offset_by("-1s")
    ).drop_nulls().with_columns(OccupancyStatus=0)
# Full occupancy file
occupancy = pl.concat([occupied, unoccupied]).sort(cs.contains("Start"))
# %%
# Beginning and end times for Keck phases
camp_endpoints = {
    1: {"start": datetime(2024, 3, 1,
                          tzinfo=pytz.timezone("America/Denver")),
        "stop": datetime(2024, 8, 16, 23, 59, 59,
                         tzinfo=pytz.timezone("America/Denver"))},
    2: {"start": datetime(2025, 1, 17,
                          tzinfo=pytz.timezone("America/Denver")),
        "stop": datetime(2025, 5, 5, 23, 59, 59,
                               tzinfo=pytz.timezone("America/Denver"))},
    3: {"start": datetime(2026, 1, 13, 2,
                          tzinfo=pytz.timezone("America/Denver")),
        "stop": datetime(2026, 1, 31, 23, 59, 59,
                         tzinfo=pytz.timezone("America/Denver"))},
    }
camp_occ = {}

for camp, endpoints in camp_endpoints.items():
    camp_occ[camp] = occupancy.filter(
        pl.any_horizontal(
            cs.contains("FTC").is_between(
                endpoints["start"], endpoints["stop"]
                )
            )
        ).with_columns(
            FTC_Start=pl.when(
                pl.col("FTC_Start").lt(endpoints["start"])
                )
            .then(pl.lit(endpoints["start"]))
            .otherwise(pl.col("FTC_Start")),
            FTC_Stop=pl.when(
                pl.col("FTC_Stop").gt(endpoints["stop"])
                )
            .then(pl.lit(endpoints["stop"]))
            .otherwise(pl.col("FTC_Stop")),
            ).with_columns(
                cs.contains("FTC").dt.convert_time_zone("UTC")
                .name.map(lambda x: x.replace("FTC", "UTC"))
                )

# %%
# Common ICARTT header for occupancy data
header = "36,1001,V02_2016\n\
Willis, Megan\n\
Colorado State University\n\
CAMPAIGNINST\n\
CAMPAIGNNAME\n\
1,1\n\
BEGINDATE,REVISDATE\n\
0\n\
UTC_Start,seconds,Time_Start,seconds_from_utc_midnight\n\
4\n\
1,1,1,1\n\
-9999,-9999,-9999,-9999\n\
UTC_Stop,seconds,Time_Stop,seconds_from_utc_midnight\n\
FTC_Start,seconds,Time_Start_local,seconds_from_local_midnight\n\
FTC_Stop,seconds,Time_Stop_local,seconds_from_local_midnight\n\
OccupancyStatus,Occupancy_Status\n\
0\n\
18\n\
PI_CONTACT_INFO: Email: Megan.Willis@colostate.edu; Address: Colorado State University, Chemistry Department, Fort Collins, CO\n\
PLATFORM: Chemistry C200\n\
LOCATION: Chemistry C200\n\
ASSOCIATED_DATA: N/A\n\
INSTRUMENT_INFO: CAMPINSTINFO\n\
DATA_INFO: OccupancyStatus 1 for occupied, 0 for unoccupied\n\
UNCERTAINTY: No associated uncertainty\n\
ULOD_FLAG: -7777\n\
ULOD_VALUE: N/A\n\
LLOD_FLAG: -8888\n\
LLOD_VALUE: N/A\n\
DM_CONTACT_INFO: Allison Salamone, Colorado State University, Allison.Salamone@colostate.edu\n\
PROJECT_INFO: PROJINFO\n\
STIPULATIONS_ON_USE: Use of these data requires prior permission from PI\n\
OTHER_COMMENTS: Room was considered occupied when people were present or when the door was left open.\n\
REVISION: R0\n\
R0: Final data."

camp_inst = {1: "SparkFun Magnetic Door Switch Set, Documentation",
             2: "SparkFun Magnetic Door Switch Set, Documentation",
             3: "Documentation"}
camp_inst_info = {1: "Occupany determined using lab notes and door status",
                  2: "Occupany determined using lab notes and door status where available",
                  3: "Occupany determined using lab notes"}
camp_name = {1: "Keck CITRUS",
             2: "Keck O3",
             3: "Keck Phase 3"}
proj_info = {1: "Indoor Biogeochemistry Project - CITRUS",
             2: "Indoor Biogeochemistry Project - O3",
             3: "Indoor Biogeochemistry Project - Phase 3"}
camp_icartt = {}
# %%
# Converts from timestamp to seconds since midnight and fills in variable header information
for camp, df in camp_occ.items():
    camp_header = header.replace("CAMPAIGNINST", 
                                 camp_inst[camp]).replace("CAMPAIGNNAME", 
                                                          camp_name[camp]).replace("CAMPINSTINFO", 
                                                                                   camp_inst_info[camp]).replace("PROJINFO", 
                                                                                                            proj_info[camp])
    # Midnight of the first day of data
    ftc_start = df.select(
        (cs.contains("FTC") & ~cs.contains("Stop")).min()
        .dt.replace(hour=0, minute=0, second=0, microsecond=0)
        )
    # Midnight of the last day of data
    ftc_stop = df.select(
        (cs.contains("FTC") & ~cs.contains("Stop")).max()
        .dt.replace(hour=0, minute=0, second=0, microsecond=0)
        )
    # Converts from local timezone to UTC timezone
    utc_start = ftc_start.select(
        pl.all().dt.replace_time_zone("UTC")
        ).item()
    utc_stop = ftc_stop.select(
        pl.all().dt.replace_time_zone("UTC")
        ).item()
    ftc_start = ftc_start.item()
    ftc_stop = ftc_stop.item()
    
    camp_header = camp_header.replace("BEGINDATE", ftc_start.strftime("%Y,%m,%d")).replace("REVISDATE", ftc_stop.strftime("%Y,%m,%d"))

    # Converts from DateTime format into seconds from midnight of start
    # date
    df = df.with_columns(
        cs.contains("UTC").sub(utc_start).dt.total_microseconds()
        .truediv(1e6).round(1),
        cs.contains("FTC").sub(ftc_start).dt.total_microseconds()
        .truediv(1e6).round(1)
        )
    
    camp_icartt = camp_header + "\n" + df.write_csv()
    
    camp_ict_title = "Keck-Occupancy_C200_" + ftc_start.strftime("%Y%m%d") + "_R0.ict"
    
    ict_path = os.path.join(occ_ict_path, camp_ict_title)
    
    with open(ict_path, "w+") as f:
        f.write(camp_icartt)
    
# %%


# %%
for week, df in licor.items():
    if week.find("2025") == -1:
        continue
    
    # co2_cols = [col for col in df.columns if col.find("CO2") != -1]
    
    week_plot = df.hvplot.scatter(
        x="FTC_DateTime",
        y="CO2_ppm"
        )
    if week in occstatus_merged.keys():
        occ_df = occstatus_merged[week].with_columns(
            cs.contains("FTC").dt.replace_time_zone(None)
            )
        week_plot = (
            week_plot
            * hv.VLines(occ_df["FTC_Start"]).opts(color="Red")
            * hv.VLines(occ_df["FTC_Stop"]).opts(color="Green")
            )
    hvplot.show(week_plot)


