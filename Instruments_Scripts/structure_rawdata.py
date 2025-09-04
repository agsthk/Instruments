# -*- coding: utf-8 -*-
"""
Created on Tue Jul  8 10:14:54 2025

@author: agsthk
"""

import os
import polars as pl
import pandas as pd
from datetime import datetime, timedelta
import yaml

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all raw data
RAW_DATA_DIR = os.path.join(data_dir, "Instruments_RawData")
# Full path to directory containing all structured data
STRUCT_DATA_DIR = RAW_DATA_DIR.replace("Raw", "Structured")
# Creates Instruments_StructuredData/ directory if needed
if not os.path.exists(STRUCT_DATA_DIR):
    os.makedirs(STRUCT_DATA_DIR)

PARAM_DIR = os.path.join(data_dir, "Instruments_ManualData", "Instruments_RawDataParameters")

schemas = {}
date_fmts = {}
time_fmts = {}
datetime_fmts = {}
timezones = {}

for param_file in os.listdir(PARAM_DIR):
    inst = param_file.rsplit("_", 1)[0]
    param_path = os.path.join(PARAM_DIR, param_file)
    with open(param_path, "r") as file:
        params = yaml.load(file, Loader=yaml.Loader)
    schemas[inst] = params["schemas"]
    if "datetime_format" in params.keys():
        datetime_fmts[inst] = params["datetime_format"]
    elif "date_format" in params.keys():
        date_fmts[inst] = params["date_format"]
        time_fmts[inst] = params["time_format"]
    timezones[inst] = params["timezone"]

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

def read_daqdata(path, schema): 
    data = pl.read_csv(path,
                       has_header=False,
                       schema=schema,
                       ignore_errors=True,
                       skip_rows=1,
                       truncate_ragged_lines=True)
    data = data.drop_nulls()
    if data.is_empty():
        col_names = list(schema.keys())
        i = col_names.index("UTC_DateTime")
        col_names.insert(i, "UTC_Date")
        data = pd.read_csv(path,
                           sep=",+\s*|\s+",
                           names=col_names,
                           skiprows=1,
                           engine="python")
        data = data.dropna()
        data["UTC_DateTime"] = pd.to_datetime(
            data["UTC_Date"] + "T" + data["UTC_DateTime"],
            format="ISO8601"
            ) 
        data = data.drop("UTC_Date", axis=1)
        data = pl.from_pandas(data, schema_overrides=schema)
    return data

def read_2bdata(path, schema):
    i = 0
    while i < 2:
        try:
            data = pl.read_csv(path,
                               skip_rows=i,
                               has_header=False,
                               schema=schema,
                               ignore_errors=True)
        except pl.exceptions.ComputeError:
            if i == 1:
                schema = {"index": pl.Int64()} | schema
                i = 0
            else:
                i += 1
        else:
            break
    data = data.drop_nulls()
    return data

def read_temprhdoor(path, schema):
    with open(path, "r", encoding="utf-8") as file:
        first_line = file.readline()
    try:
        int(first_line.split(",")[0])
    except ValueError:
        i = 1
    else:
        i = 0
    data = pl.read_csv(path,
                       skip_rows=i,
                       has_header=False,
                       schema=schema,
                       ignore_errors=True,
                       eol_char="\r")
    data = data.drop_nulls()
    return data

def read_picarro(path, schema):
    rename = {"DATE_TIME": "DateTime",
              "ALARM_STATUS": "AlarmStatus",
              "INST_STATUS": "InstrumentStatus",
              "CavityPressure": "CavityPressure_Torr",
              "CavityTemp": "CavityTemp_C",
              "DasTemp": "DASTemp_C",
              "EtalonTemp": "EtalonTemp_C",
              "WarmBoxTemp": "WarmBoxTemp_C",
              "species": "Species",
              "MPVPosition": "MPVPosition",
              "OutletValve": "OutletValve_DN",
              "solenoid_valves": "SolenoidValves",
              "H2CO": "CH2O_ppb",
              "H2CO_30s": "CH2O_30_ppb",
              "H2CO_2min": "CH2O_2min_ppb",
              "H2CO_5min": "CH2O_5min_ppb",
              "H2O": "H2O_percent",
              "CH4": "CH4_ppm"}
    data = pd.read_hdf(path, "results")
    data = pl.from_pandas(data, schema_overrides=schema)
    data = data.select(
        *rename.keys()
        ).rename(rename)
    return data
    
def read_rawdata(path, inst, source, schema):
    if source == "DAQ":
        data = read_daqdata(path, schema)
    elif inst == "Picarro_G2307":
        data = read_picarro(path, schema)
    elif inst.find("2BTech") != -1:
        data = read_2bdata(path, schema)
    elif inst == "AdditionValves":
        data = pl.read_csv(path,
                           skip_rows=1,
                           has_header=False,
                           schema=schema,
                           ignore_errors=True,
                           eol_char="\r")
    elif inst.find("Aranet4") != -1:
        data = pl.read_csv(path,
                           separator=",",
                           has_header=False,
                           schema=schema,
                           ignore_errors=True,
                           skip_rows=1)
    elif inst == "ThermoScientific_42i-TL":
        data = pd.read_csv(path,
                           sep="\s+",
                           names=schema.keys(),
                           skiprows=6)
        data = pl.from_pandas(data, schema_overrides=schema)
    elif inst.find("LI-COR") != -1:
        data = pl.read_csv(path,
                           separator=" ",
                           has_header=False,
                           schema=schema,
                           ignore_errors=True,
                           skip_rows=2)
    elif inst == "TempRHDoor":
        data = read_temprhdoor(path, schema)
    elif inst == "Teledyne_N300":
        data = pl.read_csv(path,
                           separator=",",
                           has_header=False,
                           schema=schema,
                           ignore_errors=True,
                           skip_rows=1)
    data = data.drop_nulls()
    return data

def define_datetime(df, inst):
    if "DateTime" in df.columns:
        try:
            df = df.with_columns(
                pl.from_epoch(pl.col("DateTime").mul(1e9), time_unit="ns")
                )
        except pl.exceptions.InvalidOperationError:
            df = df.with_columns(
                pl.col("DateTime").str.to_datetime(
                    datetime_fmts[inst],
                    )
                )
    else:
        df = df.select(
            pl.concat_str(
                [pl.col("Date"), pl.col("Time").str.strip_chars()],
                separator=""
                ).str.to_datetime(
                    date_fmts[inst] + time_fmts[inst],
                    strict=False
                    ).alias("DateTime"),
            pl.exclude("Date", "Time")
            )
    try:
        df = df.with_columns(
            pl.col("DateTime").dt.replace_time_zone(
                time_zone=timezones[inst]
                )
            )
    except pl.exceptions.ComputeError:
        df = df.with_row_index()
        dst_i = df.filter(
            pl.col("DateTime").shift(-1).sub(pl.col("DateTime")).lt(0)
            )["index"][0]
        
        df = df.select(
            pl.when(pl.col("index").le(dst_i))
            .then(pl.col("DateTime").dt.replace_time_zone(
                time_zone=timezones[inst],
                ambiguous="earliest"
                ))
            .otherwise(pl.col("DateTime").dt.replace_time_zone(
                time_zone=timezones[inst],
                ambiguous="latest"
                )),
            pl.exclude("index", "DateTime")
            )
    finally:
        df = df.select(
            pl.col("DateTime").dt.convert_time_zone(
                "UTC"
                ).alias("UTC_DateTime"),
            pl.col("DateTime").dt.convert_time_zone(
                "America/Denver"
                ).alias("FTC_DateTime"),
            pl.exclude("DateTime", "FTC_DateTime", "Date", "Time",
                       "UnixTime", "YMD", "HMS", "IgorTime", "index")
            )
    df = df.drop_nulls()
        
    return df

def split_by_date(df):
    if "FTC_DateTime" in df.columns:
        dt_col = "FTC_DateTime"
    else:
        dt_col = "FTC_Start"
    df_with_date = df.with_columns(
        pl.col(dt_col).dt.date().alias("Date")
        )
    df_by_date = df_with_date.partition_by(
        "Date", include_key=False, as_dict=True
        )
    df_by_date = {key[0].strftime("%Y%m%d"): df
                  for key, df in df_by_date.items()}
    return df_by_date

data = {}

for subdir in os.listdir(RAW_DATA_DIR):
    if subdir == "test.yml":
        continue
    path = os.path.join(RAW_DATA_DIR, subdir)
    for subdir2 in os.listdir(path):
        path2 = os.path.join(path, subdir2)
        inst, source = subdir2[:-4].split("_Raw")
        schema = schemas[inst][source]
        if inst not in data.keys():
            data[inst] = {}
        if source not in data[inst].keys():
            data[inst][source] = []
        if inst.find("Teledyne") != -1:
            for folder in os.listdir(path2):
                path3 = os.path.join(path2, folder, "HIRES.txt")
                data[inst][source].append(read_rawdata(path3, inst, source, schema))
            continue
        for file in os.listdir(path2):
            path3 = os.path.join(path2, file)
            data[inst][source].append(read_rawdata(path3, inst, source, schema))

for inst in data.keys():
    for source in data[inst].keys():
        dfs = []
        for df in data[inst][source]:
            if "UTC_DateTime" in df.columns:
                df = df.select(
                    pl.col("UTC_DateTime"),
                    pl.col("UTC_DateTime").dt.convert_time_zone(
                        "America/Denver"
                        ).alias("FTC_DateTime"),
                    pl.exclude("UTC_DateTime", "DateTime", "FTC_DateTime",
                               "Date", "Time", "UnixTime", "YMD", "HMS",
                               "IgorTime", "index")
                    )
                dfs.append(df)
                continue
            dfs.append(define_datetime(df, inst))
        data[inst][source] = dfs

for inst in data.keys():
    for source in data[inst].keys():
        concat_df = pl.concat(data[inst][source]).unique().sort("UTC_DateTime")
        if inst in avg_times.keys():
            for start, avg_time in avg_times[inst].items():
                if "UTC_Start" not in concat_df.columns:
                    concat_df = concat_df.select(
                        pl.when(
                            pl.col("UTC_DateTime").ge(
                                datetime.fromisoformat(start)
                                )
                            )
                        .then(
                            pl.col("UTC_DateTime").dt.offset_by("-" + avg_time)
                            ).alias("UTC_Start"),
                        pl.exclude("UTC_Start")
                        )
                    continue
                concat_df = concat_df.select(
                    pl.when(
                        pl.col("UTC_DateTime").ge(
                            datetime.fromisoformat(start)
                            )
                        )
                    .then(
                        pl.col("UTC_DateTime").dt.offset_by("-" + avg_time)
                        )
                    .otherwise(
                        pl.col("UTC_Start")
                        ).alias("UTC_Start"),
                    pl.exclude("UTC_Start")
                    )
            concat_df = concat_df.select(
                pl.col("UTC_Start"),
                pl.col("UTC_DateTime").alias("UTC_Stop"),
                pl.col("UTC_Start").dt.convert_time_zone("America/Denver").alias("FTC_Start"),
                pl.col("FTC_DateTime").alias("FTC_Stop"),
                pl.exclude("UTC_Start", "UTC_DateTime", "FTC_DateTime")
                )
        if source != "DAQ" and inst in ["2BTech_202", "2BTech_205_A",
                                        "2BTech_205_B", "2BTech_405nm",
                                        "LI-COR_LI-840A", "LI-COR_LI-840B",
                                        "Picarro_G2307",
                                        "ThermoScientific_42i-TL"]:
            if "UTC_Start" not in concat_df.columns:
                concat_df = concat_df.with_columns(
                    pl.col("UTC_DateTime").sub(pl.col("UTC_DateTime").shift(1)).alias("Gap"),
                    pl.lit(timedelta(seconds=60)).alias("AvgT")
                    )
                left_on = "UTC_DateTime"
            else:
                concat_df = concat_df.with_columns(
                    pl.col("UTC_Start").sub(pl.col("UTC_Start").shift(1)).alias("Gap"),
                    pl.col("UTC_Stop").sub(pl.col("UTC_Start")).alias("AvgT")
                    )
                left_on = "UTC_Stop"
            logstart = concat_df.filter(
                (pl.col("Gap").gt(pl.col("AvgT").add(timedelta(seconds=10)))
                | pl.col("Gap").gt((pl.col("AvgT").add(timedelta(seconds=10))).shift(1))
                | pl.col("Gap").is_null())
                ).select(
                    pl.col(left_on).alias("LogStart")
                    )
            concat_df = concat_df.join_asof(
                logstart,
                left_on=left_on,
                right_on="LogStart",
                strategy="backward"
                ).select(
                    pl.exclude("Gap", "AvgT", "LogStart"),
                    pl.col(left_on).sub(pl.col("LogStart"))
                    .dt.total_microseconds().truediv(1e6).mul(-1).add(600)
                    .alias("WarmUp")
                    ).with_columns(
                        pl.when(pl.col("WarmUp").gt(0))
                        .then(pl.col("WarmUp").cast(pl.Int64))
                        .otherwise(pl.lit(0))
                        )
            
            
        if inst == "LI-COR_LI-840A_B":
            concat_df = concat_df.with_columns(
                pl.selectors.contains("UTC", "FTC").dt.offset_by("-12m15s")
                )
        if inst == "ThermoScientific_42i-TL":
            if source == "DAQ":
                concat_df.insert_column(
                    7,
                    pl.col("NO_ppb").add(pl.col("NO2_ppb")).alias("NOx_ppb")
                    )
            else:
                concat_df.insert_column(
                    6,
                    pl.col("NOx_ppb").sub(pl.col("NO_ppb")).alias("NO2_ppb")
                    )
        data[inst][source] = split_by_date(concat_df)
#%%
for inst in data.keys():
    for source in data[inst].keys():
        for date, df in data[inst][source].items():
            f_name = inst + "_Structured" + source + "Data_" + date + ".csv"
            f_dir = os.path.join(STRUCT_DATA_DIR,
                                inst + "_StructuredData",
                                inst + "_Structured" + source + "Data")
            if not os.path.exists(f_dir):
                os.makedirs(f_dir)
            path = os.path.join(f_dir,
                                f_name)
            df.write_csv(path)
