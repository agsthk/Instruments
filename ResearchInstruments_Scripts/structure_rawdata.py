# -*- coding: utf-8 -*-
"""
Created on Tue Jul  8 10:14:54 2025

@author: agsthk
"""

import os
import polars as pl
import pandas as pd

# Declares full path to ResearchInstruments_Data/ directory
data_dir = os.getcwd()
# Starts in ResearchInstruments/ directory
if "ResearchInstruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "ResearchInstruments_Data")

# Full path to directory containing all raw data
RAW_DATA_DIR = os.path.join(data_dir, "ResearchInstruments_RawData")
# Full path to directory containing all structured data
STRUCT_DATA_DIR = RAW_DATA_DIR.replace("Raw", "Structured")
# Creates ResearchInstruments_StructuredData/ directory if needed
if not os.path.exists(STRUCT_DATA_DIR):
    os.makedirs(STRUCT_DATA_DIR)

schemas = {
    "2BTech_202": {
        "Logger": {
            "O3_ppb": pl.Float64(),
            "CellTemp_C": pl.Float64(),
            "CellPressure_mbar": pl.Float64(),
            "SampleFlow_ccm": pl.Float64(),
            "Date": pl.String(),
            "Time": pl.String()
            }
        },
    "2BTech_405nm": {
        "SD": {
            "NO2_ppb": pl.Float64(),
            "NO_ppb": pl.Float64(),
            "NOx_ppb": pl.Float64(),
            "CellTemp_C": pl.Float64(),
            "CellPressure_mbar": pl.Float64(),
            "SampleFlow_ccm": pl.Float64(),
            "O3Flow_ccm": pl.Float64(),
            "PhotodiodeVoltage_V": pl.Float64(),
            "O3Voltage_V": pl.Float64(),
            "ScrubberTemp_C": pl.Float64(),
            "ErrorByte": pl.String(),
            "Date": pl.String(),
            "Time": pl.String(),
            "InstrumentStatus": pl.Int64()
            }
        },
    "LI-COR_LI-840A_A": {
        "Logger": {
            "Date": pl.String(),
            "Time": pl.String(),
            "CO2_ppm": pl.Float64(),
            "H2O_ppt": pl.Float64(),
            "H2O_C": pl.Float64(),
            "CellTemp_C": pl.Float64(),
            "CellPressure_kPa": pl.Float64(),
            "CO2Absorption": pl.Float64(),
            "H2OAbsorption": pl.Float64()
            }
        },
    "Picarro_G2307": {
        "Logger": {
            "ALARM_STATUS": pl.Float64(),
            "CH4": pl.Float64(),
            "CavityPressure": pl.Float64(),
            "CavityTemp": pl.Float64(),
            "DATE_TIME": pl.Float64(),
            "DasTemp": pl.Float64(),
            "EPOCH_TIME": pl.Float64(),
            "EtalonTemp": pl.Float64(),
            "FRAC_DAYS_SINCE_JAN1": pl.Float64(),
            "FRAC_HRS_SINCE_JAN1": pl.Float64(),
            "H2CO": pl.Float64(),
            "H2CO_2min": pl.Float64(),
            "H2CO_30s": pl.Float64(),
            "H2CO_5min": pl.Float64(),
            "H2O": pl.Float64(),
            "INST_STATUS": pl.Float64(),
            "JULIAN_DAYS": pl.Float64(),
            "MPVPosition": pl.Float64(),
            "OutletValve": pl.Float64(),
            "WarmBoxTemp": pl.Float64(),
            "solenoid_valves": pl.Float64(),
            "species": pl.Float64()
            },
        "DAQ": {
            "UnixTime": pl.Float64(),
            "CavityPressure_Torr": pl.Float64(),
            "CavityTemp_C": pl.Float64(),
            "DASTemp_C": pl.Float64(),
            "EtalonTemp_C": pl.Float64(),
            "WarmBoxTemp_C": pl.Float64(),
            "Species": pl.Float64(),
            "MPVPosition": pl.Float64(),
            "OutletValve_DN": pl.Float64(),
            "SolenoidValves": pl.Float64(),
            "CH2O_ppm": pl.Float64(),
            "CH2O_30s_ppm": pl.Float64(),
            "CH2O_2min_ppm": pl.Float64(),
            "CH2O_5min_ppm": pl.Float64(),
            "H2O_percent": pl.Float64(),
            "CH4_ppm": pl.Float64(),
            "YMD": pl.Float64(),
            "HMS": pl.Float64(),
            "UTC_DateTime": pl.Datetime(time_zone="UTC"),
            "WarmUp": pl.Int64()
            }
        },
    "Teledyne_N300": {
        "Logger": {
            "Local_DateTime": pl.String(),
            "DateTime": pl.String(),
            "AtmosphericPressure_Pa": pl.Float64(),
            "BenchTemp_C": pl.Float64(),
            "BoxTemp_C": pl.Float64(),
            "CO_ppm": pl.Float64(),
            "COStability_ppm": pl.Float64(),
            "DetectorTemp_C": pl.Float64(),
            "PeakIRMeasure_MV": pl.Float64(),
            "PeakIRReference_MV": pl.Float64(),
            "PHTDrive_mV": pl.Float64(),
            "PumpDutyCycle": pl.Float64(),
            "PumpFlow_ccm": pl.Float64(),
            "SamplePressure_inHg": pl.Float64(),
            "SampleTemp_C": pl.Float64(),
            "WheelTemp_C": pl.Float64()
            }
        },
    "TempRHDoor": {
        "Igor": {
            "IgorTime": pl.Int64(),
            "Date": pl.String(),
            "Time": pl.String(),
            "Ch0Voltage_V": pl.Float64(),
            "RoomTemp_C": pl.Float64(),
            "Ch1Voltage_V": pl.Float64(),
            "RoomRH_percent": pl.Float32(),
            "TC3Temp_C": pl.Float64(),
            "TC5Tempe_C": pl.Float64(),
            "TC7Temp_C": pl.Float64(),
            "DoorStatus": pl.Int64()
            }
        },
    "ThermoScientific_42i-TL": {
        "Logger": {
            "Time": pl.String(),
            "Date": pl.String(),
            "Flags": pl.String(),
            "NO_ppb": pl.Float64(),
            "NOx_ppb": pl.Float64(),
            "HighNO": pl.Int64(),
            "HighNOx": pl.Int64(),
            "ChamberPressure_mmHg": pl.Float64(),
            "PMTTemp_C": pl.Float64(),
            "InternalTemp_C": pl.Float64(),
            "ChamberTemp_C": pl.Float64(),
            "NO2ConverterTemp_C": pl.Float64(),
            "SampleFlow_LPM": pl.Float64(),
            "O3Flow_LPM": pl.Float64(),
            "PMTVoltage_V": pl.Float64()
            },
        "DAQ": {
            "Time": pl.String(),
            "Date": pl.String(),
            "Flags": pl.String(),
            "NO_ppb": pl.Float64(),
            "NO2_ppb": pl.Float64(),
            "ChamberTemp_C": pl.Float64(),
            "PMTVoltage_V": pl.Float64(),
            "InternalTemp_C": pl.Float64(),
            "ChamberPressure_mmHg": pl.Float64(),
            "SampleFlow_LPM": pl.Float64(),
            "O3Flow_LPM": pl.Float64(),
            "UTC_DateTime": pl.Datetime(time_zone="UTC"),
            "WarmUp": pl.Int64()
            }
        }
    }

schemas["2BTech_202"]["SD"] = schemas["2BTech_202"]["Logger"]
schemas["2BTech_202"]["DAQ"] = (
    schemas["2BTech_202"]["Logger"]
    | {"UTC_DateTime": pl.Datetime(time_zone="UTC"),
       "WarmUp": pl.Int64()}
    )
schemas["2BTech_205_A"] = schemas["2BTech_205_B"] = schemas["2BTech_202"]
schemas["LI-COR_LI-840A_B"] = schemas["LI-COR_LI-840A_A"]

date_fmts = {"2BTech_202": "%d/%m/%y",
             "LI-COR_LI-840A_A": "%Y-%m-%d",
             "TempRHDoor": "%Y-%m-%d",
             "ThermoScientific_42i-TL": "%m-%d-%y"}

for inst in ["2BTech_205_A", "2BTech_205_B", "2BTech_405nm"]:
    date_fmts[inst] = date_fmts["2BTech_202"]
date_fmts["LI-COR_LI-840A_B"] = date_fmts["LI-COR_LI-840A_A"]

time_fmts = {"2BTech_202": "%H:%M:%S",
             "ThermoScientific_42i-TL": "%H:%M"}

for inst in ["2BTech_205_A", "2BTech_205_B", "2BTech_405nm",
             "LI-COR_LI-840A_A", "LI-COR_LI-840A_B", "TempRHDoor"]:
    time_fmts[inst] = time_fmts["2BTech_202"]
    
datetime_fmts = {"Teledyne_N300": "%m/%d/%Y %H:%M:%S"}

timezones = {"2BTech_202": "MST",
             "2BTech_205_A": "MST",
             "2BTech_205_B": "MST",
             "2BTech_405nm": "MST",
             "LI-COR_LI-840A_A": "America/Denver",
             "LI-COR_LI-840A_B": "America/Denver",
             "Picarro_G2307": "UTC",
             "Teledyne_N300": "UTC", # NOT ALWAYS TRUE
             "TempRHDoor": "America/Denver",
             "ThermoScientific_42i-TL": "MST"}

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
              "H2CO": "CH2O_ppm",
              "H2CO_30s": "CH2O_30_ppm",
              "H2CO_2min": "CH2O_2min_ppm",
              "H2CO_5min": "CH2O_5min_ppm",
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
                ).alias("Local_DateTime"),
            pl.exclude("DateTime", "Local_DateTime", "Date", "Time",
                       "UnixTime", "YMD", "HMS", "IgorTime", "index")
            )
    df = df.drop_nulls()
        
    return df

def split_by_date(df):
    df_with_date = df.with_columns(
        pl.col("UTC_DateTime").dt.date().alias("Date")
        )
    df_by_date = df_with_date.partition_by(
        "Date", include_key=False, as_dict=True
        )
    df_by_date = {key[0].strftime("%Y%m%d"): df
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
                        ).alias("Local_DateTime"),
                    pl.exclude("UTC_DateTime", "DateTime", "Local_DateTime",
                               "Date", "Time", "UnixTime", "YMD", "HMS",
                               "IgorTime", "index")
                    )
                dfs.append(df)
                continue
            dfs.append(define_datetime(df, inst))
        data[inst][source] = dfs

for inst in data.keys():
    for source in data[inst].keys():
        concat_df = pl.concat(data[inst][source]).sort("UTC_DateTime")
        data[inst][source] = split_by_date(concat_df)

for inst in data.keys():
    for source in data[inst].keys():
        for date, df in data[inst][source].items():
            f_name = inst + "_Structured" + source + "Data_" + date + ".csv"
            path = os.path.join(STRUCT_DATA_DIR,
                                inst + "_Structured" + source + "Data",
                                f_name)
            df.write_csv(path)
