# -*- coding: utf-8 -*-
"""
Created on Wed Sep  3 14:34:12 2025

@author: agsthk
"""

import os
import polars as pl
import pandas as pd
from datetime import datetime, timedelta
import hvplot.polars
import holoviews as hv


# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all raw data
RAW_DATA_DIR = os.path.join(data_dir, "Instruments_RawData")

schemas = {
    "2BTech_205_A": {
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
            "CH2O_ppb": pl.Float64(),
            "CH2O_30s_ppb": pl.Float64(),
            "CH2O_2min_ppb": pl.Float64(),
            "CH2O_5min_ppb": pl.Float64(),
            "H2O_percent": pl.Float64(),
            "CH4_ppm": pl.Float64(),
            "YMD": pl.Float64(),
            "HMS": pl.Float64(),
            "UTC_DateTime": pl.Datetime(time_zone="UTC"),
            "WarmUp": pl.Int64()
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

schemas["2BTech_205_A"]["SD"] = schemas["2BTech_205_A"]["Logger"]
schemas["2BTech_205_A"]["DAQ"] = (
    schemas["2BTech_205_A"]["Logger"]
    | {"UTC_DateTime": pl.Datetime(time_zone="UTC"),
       "WarmUp": pl.Int64()}
    )
schemas["2BTech_205_B"] = schemas["2BTech_205_A"]
date_fmts = {"2BTech_205_A": "%d/%m/%y",
             "2BTech_205_B": "%d/%m/%y",
             "ThermoScientific_42i-TL": "%m-%d-%Y"}

time_fmts = {"2BTech_205_A": "%H:%M:%S",
             "2BTech_205_B": "%H:%M:%S",
             "ThermoScientific_42i-TL": "%H:%M:%S"}

timezones = {"2BTech_205_A": "MST",
             "2BTech_205_B": "MST",
             "2BTech_405nm": "MST",
             "Picarro_G2307": "UTC",
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


def define_datetime(df, inst):
    df = df.select(
        pl.concat_str(
            [pl.col("Date"), pl.col("Time").str.strip_chars()],
            separator=" "
            ).str.to_datetime(
                date_fmts[inst] + " " + time_fmts[inst],
                strict=False
                ).alias("Inst_DateTime"),
        pl.exclude("Date", "Time")
            ).with_columns(
                pl.col("Inst_DateTime").dt.replace_time_zone(
                    time_zone=timezones[inst]
                    )
                ).with_columns(
                    pl.col("Inst_DateTime").dt.convert_time_zone(
                        "UTC"
                        ).alias("Inst_UTC_DateTime"),
                    pl.col("Inst_DateTime").dt.convert_time_zone(
                        "America/Denver"
                        ).alias("Inst_FTC_DateTime")
                    )
    return df

data = {}

for subdir in os.listdir(RAW_DATA_DIR):
    if subdir == "test.yml":
        continue
    path = os.path.join(RAW_DATA_DIR, subdir)
    for subdir2 in os.listdir(path):
        if subdir2.find("DAQ") == -1 or subdir2.find("Picarro_G2307") != -1:
            continue
        path2 = os.path.join(path, subdir2)
        inst, source = subdir2[:-4].split("_Raw")
        schema = schemas[inst][source]
        if inst not in data.keys():
            data[inst] = {}
        if source not in data[inst].keys():
            data[inst][source] = []
        for file in os.listdir(path2):
            path3 = os.path.join(path2, file)
            data[inst][source].append(define_datetime(read_daqdata(path3, schema), inst))
   
for inst in data.keys():
    for source, dfs in data[inst].items():
        df = pl.concat(dfs).unique().sort("UTC_DateTime")
        df = df.with_columns(
            pl.col("UTC_DateTime")
            .sub(pl.min("UTC_DateTime"))
            .dt.total_microseconds()
            .truediv(1e6)
            .alias("SecondsPassed")
            ).with_columns(
                pl.col("UTC_DateTime").dt.offset_by(((pl.col("SecondsPassed").mul(5.2e-6)).cast(pl.Int64()).cast(pl.String()) + "s"))
                ).with_columns(
                    pl.col("UTC_DateTime").sub(pl.col("UTC_DateTime").shift(1)).dt.total_microseconds().truediv(1e6).alias("dt"),
                    pl.col("Inst_UTC_DateTime").sub(pl.col("Inst_UTC_DateTime").shift(1)).dt.total_microseconds().truediv(1e6).alias("Inst_dt")
                    ).with_columns(
                        pl.col("Inst_UTC_DateTime")
                        .sub(pl.col("UTC_DateTime"))
                        .dt
                        .total_microseconds()
                        .truediv(1e6)
                        .alias("Offset")
                        ).with_columns(
                            pl.col("Inst_dt")
                            .sub(pl.col("dt"))
                            .alias("dt_Offset")
                            )
        data[inst][source] = df

#%%
for inst in data.keys():
    for source, df in data[inst].items():
        scatter = df.hvplot.scatter(
            x="UTC_DateTime",
            y="Offset",
            title=inst,
            width=1000,
            height=500,
            s=1
            )
        # hvplot.show(scatter)
        dtplot = df.hvplot.scatter(
            x="dt",
            y="Inst_dt",
            title=inst,
            width=1000,
            height=500,
            s=20)
        # hvplot.show(dtplot)
        # hvplot.show(
        #     df.hvplot.scatter(
        #         x="UTC_DateTime",
        #         y="dt_Offset",
        #         title=inst,
        #         width=1000,
        #         height=500,
        #         s=1
        #         )
        #     )
        print(df["dt_Offset"].median())