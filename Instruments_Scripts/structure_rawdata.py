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
    data = data.with_row_index("LogNumber")
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
              "H2CO_30s": "CH2O_30s_ppb",
              "H2CO_2min": "CH2O_2min_ppb",
              "H2CO_5min": "CH2O_5min_ppb",
              "H2O": "H2O_percent",
              "CH4": "CH4_ppm"}
    if path.split(".")[-1] == "h5":
        data = pd.read_hdf(path, "results")
        data = pl.from_pandas(data, schema_overrides=schema)
    else:
        data = pl.read_csv(
            path,
            skip_rows=1,
            schema={"ALARM_STATUS": pl.Float64,
                    "datetime": pl.String,
                    "DATE_TIME": pl.Float64,
                    "CH4": pl.Float64,
                    "CavityPressure": pl.Float64,
                    "CavityTemp": pl.Float64,
                    "DasTemp": pl.Float64,
                    "EtalonTemp": pl.Float64,
                    "FracDays": pl.Float64,
                    "FracHours": pl.Float64,
                    "H2CO": pl.Float64,
                    "H2CO_2min": pl.Float64,
                    "H2CO_30s": pl.Float64,
                    "H2CO_5min": pl.Float64,
                    "H2O": pl.Float64,
                    "INST_STATUS": pl.Float64,
                    "JulianDays": pl.Float64,
                    "MPVPosition": pl.Float64,
                    "OutletValve": pl.Float64,
                    "WarmBoxTemp": pl.Float64,
                    "solenoid_valves": pl.Float64,
                    "species": pl.Float64,
                    }
            )
    data = data.select(
        *rename.keys()
        ).rename(rename)
    if not data.filter(pl.col("DateTime").is_duplicated()).is_empty():
        part_data = data.partition_by("DateTime")
        sorted_data = []
        prev_sv = 0
        for df in part_data:
            sv_values = set(df["SolenoidValves"])
            if len(sv_values) == 1:
                descending = False
                if 0 in sv_values:
                    prev_sv = 0
                else:
                    prev_sv = 1
            else:
                if prev_sv == 0:
                    descending = False
                else:
                    descending = True
                prev_sv = 0.5
            sorted_data.append(
                df.sort(by="SolenoidValves", descending=descending)
                )
        data = pl.concat(sorted_data)
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

def define_warmup(df, inst):
    if inst.find("2BTech") != -1:
        log_start = df.filter(
            pl.col("LogNumber").eq(0)
            ).select(
                pl.col("UTC_DateTime").alias("LogStart")
                )
    elif inst == "ThermoScientific_42i-TL":
        df = df.with_columns(
            pl.col("UTC_DateTime").sub(pl.col("UTC_DateTime").shift(1)).alias("dt")
            )
        log_start = df.filter(
            pl.col("dt").ge(pl.duration(seconds=90))
            | pl.col("dt").is_null()
            ).select(
                pl.col("UTC_DateTime").alias("LogStart")
                )
    elif inst.find("LI-COR") != -1:
        df = df.with_columns(
            pl.col("UTC_DateTime").sub(pl.col("UTC_DateTime").shift(1)).alias("dt")
            )
        log_start = df.filter(
            pl.col("dt").ge(pl.duration(seconds=15))
            | pl.col("dt").is_null()
            ).select(
                pl.col("UTC_DateTime").alias("LogStart")
                )
    else:
        df = df.with_columns(
            pl.lit(0).alias("WarmUp")
            )
        return df
    df = df.join_asof(
        log_start,
        left_on="UTC_DateTime",
        right_on="LogStart",
        strategy="backward"
        ).select(
            pl.exclude("dt", "LogStart", "LogNumber"),
            pl.col("UTC_DateTime").sub(pl.col("LogStart")).dt.total_microseconds().truediv(1e6).mul(-1).add(600)
            .alias("WarmUp")
            ).with_columns(
                pl.when(pl.col("WarmUp").gt(0))
                .then(pl.col("WarmUp").cast(pl.Int64))
                .otherwise(pl.lit(0))
                )
    return df

def split_by_date(df):
    df_with_date = df.with_columns(
        pl.col("FTC_DateTime").dt.date().alias("Date")
        )
    df_by_date = df_with_date.partition_by(
        "Date", include_key=False, as_dict=True
        )
    df_by_date = {key[0].strftime("%Y%m%d"): df.unique(maintain_order=True)
                  for key, df in df_by_date.items()}
    return df_by_date

data = {}

for subdir in os.listdir(RAW_DATA_DIR):
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
        concat_df = pl.concat(dfs).unique().sort("UTC_DateTime")
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
        if source != "DAQ":
            concat_df = define_warmup(concat_df, inst)
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

