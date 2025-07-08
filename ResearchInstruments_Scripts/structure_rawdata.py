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

def structure_2btech(raw_dir, struct_dir, inst, source):
    # Path to directory containing raw data from declared instrument and source
    raw_dir = os.path.join(raw_dir,
                           inst + "_RawData",
                           inst + "_Raw" + source + "Data")
    # Path to directory containing corresponding structured data
    struct_dir = os.path.join(struct_dir,
                              inst + "_StructuredData",
                              inst + "_Structured" + source + "Data")
    if not os.path.exists(struct_dir):
        os.makedirs(struct_dir)
    else:
        # PLAN: Create a text file that lists the files that have already been structured and read it in here
        pass
    
    if inst == "2BTech_405nm":
        schemas = [{"no2_ppb": pl.Float64(),
                    "no_ppb": pl.Float64(),
                    "nox_ppb": pl.Float64(),
                    "cell_temp_C": pl.Float64(),
                    "cell_press_mbar": pl.Float64(),
                    "cell_flow_ccm": pl.Float64(),
                    "o3_flow_ccm": pl.Float64(),
                    "photodiode_voltage_V": pl.Float64(),
                    "o3_voltage_V": pl.Float64(),
                    "scrubber_temp_C": pl.Float64(),
                    "error_byte": pl.String(),
                    "date": pl.String(),
                    "time": pl.String(),
                    "status": pl.Int64()}]
    else:
        schemas = [{"o3_ppb": pl.Float64(),
                    "temp_C": pl.Float64(),
                    "press_mbar": pl.Float64(),
                    "flow_ccm": pl.Float64(),
                    "date": pl.String(),
                    "time": pl.String()},
                   {"index": pl.Int64(),
                    "o3_ppb": pl.Float64(),
                    "temp_C": pl.Float64(),
                    "press_mbar": pl.Float64(),
                    "flow_ccm": pl.Float64(),
                    "date": pl.String(),
                    "time": pl.String()}]
    
    for file in os.listdir(raw_dir):
        raw_path = os.path.join(raw_dir, file)
        # Tries all schemas until file read-in is successful
        for schema in schemas:
            data = pl.scan_csv(raw_path,
                               has_header=False,
                               schema=schema,
                               ignore_errors=True)
            try:
                data = data.collect()
            except pl.exceptions.SchemaError:
                data = pl.scan_csv(raw_path,
                                   has_header=False,
                                   schema=schema,
                                   ignore_errors=True,
                                   skip_rows=1)
                try:
                    data = data.collect()
                except:
                    continue
                else:
                    break
            # Skips empty files
            except pl.exceptions.NoDataError:
                data = None
                break
            else:
                break
        if data is None:
            continue
        # Drops rows not matching provided schema
        # data = data.filter(~pl.all_horizontal(pl.all().is_null()))
        data = data.drop_nulls()
        # Stops structuring logs with no real data
        if data.is_empty():
            continue
        # Defines instrument datetime
        data = data.lazy().select(
            pl.concat_str(
                [pl.col("date"), pl.col("time").str.strip_chars()],
                separator=" "
                ).str.to_datetime(
                    "%d/%m/%y %H:%M:%S",
                    strict=False
                    ).dt.replace_time_zone(
                        time_zone="MST"
                        ).alias("mst_datetime"),
            pl.exclude("date", "time", "index")
            )
        # Drops rows where datetime could not be determined
        data = data.drop_nulls()
        # Defines UTC and local datetime
        data = data.select(
            pl.col("mst_datetime").dt.convert_time_zone(
                "UTC"
                ).alias("utc_datetime"),
            pl.col("mst_datetime").dt.convert_time_zone(
                "America/Denver"
                ).alias("local_datetime"),
            pl.exclude("mst_datetime")
            ).collect()
        local_start = data["local_datetime"].min().strftime("%Y%m%d_%H%M%S")
        local_stop = data["local_datetime"].max().strftime("%Y%m%d_%H%M%S")
        
        struct_file_name = (inst
                            + "_Structured"
                            + source
                            + "Data_"
                            + local_start 
                            + "_" 
                            + local_stop 
                            + ".csv")
        struct_path = os.path.join(struct_dir, struct_file_name)
        # Writes structured data to CSV file
        data.write_csv(struct_path)

def structure_thermo(raw_dir, struct_dir):
    
    # Path to directory containing raw ThermoScientific 42i-TL data
    raw_dir = os.path.join(raw_dir,
                           "ThermoScientific_42i-TL_RawData",
                           "ThermoScientific_42i-TL_RawLoggerData")
    # Path to directory containing corresponding structured data
    struct_dir = os.path.join(struct_dir,
                           "ThermoScientific_42i-TL_StructuredData",
                           "ThermoScientific_42i-TL_StructuredLoggerData")
    if not os.path.exists(struct_dir):
        os.makedirs(struct_dir)
    else:
        # PLAN: Create a text file that lists the files that have already been structured and read it in here
        pass
    
    columns = {"Flags": "flags",
               "no": "no_ppb",
               "no2": "no2_ppb",
               "nox": "nox_ppb",
               "pres": "press_mmHg",
               "intt": "internal_temp_C",
               "rctt": "chamber_temp_C",
               "smplf": "sample_flow_LPM",
               "ozonf": "o3_flow_LPM",
               "pmtv": "pmt_voltage_V"}
    
    for file in os.listdir(raw_dir):
        raw_path = os.path.join(raw_dir, file)
        data = pd.read_csv(raw_path, delimiter="\s+", skiprows=5)
        data = pl.from_pandas(data)
        if data.is_empty():
            continue
        # Defines NO2
        data = data.lazy().with_columns(
            no2=pl.col("nox").sub(pl.col("no"))
            )
        # Defines instrument datetime
        data = data.select(
            pl.concat_str(
                [pl.col("Date"), pl.col("Time").str.strip_chars()],
                separator=" "
                ).str.to_datetime(
                    "%m-%d-%y %H:%M",
                    strict=False
                    ).dt.replace_time_zone(
                        time_zone="MST"
                   ).alias("mst_datetime"),
           pl.exclude("Date", "Time", "hino", "hinox", "pmtt", "convt")
           )
        # Defines UTC and local datetime
        data = data.select(
           pl.col("mst_datetime").dt.convert_time_zone(
               "UTC"
               ).alias("utc_datetime"),
           pl.col("mst_datetime").dt.convert_time_zone(
               "America/Denver"
               ).alias("local_datetime"),
           *columns.keys()
           ).rename(columns).collect()
        
        local_start = data["local_datetime"].min().strftime("%Y%m%d_%H%M%S")
        local_stop = data["local_datetime"].max().strftime("%Y%m%d_%H%M%S")
        
        struct_file_name = ("ThermoScientific_42i-TL_StructuredLoggerData_"
                            + local_start 
                            + "_" 
                            + local_stop 
                            + ".csv")
        struct_path = os.path.join(struct_dir, struct_file_name)
        # Writes structured data to CSV file
        data.write_csv(struct_path)

# for inst in ["2BTech_202", "2BTech_205_A","2BTech_205_B", "2BTech_405nm"]:
#     for source in ["Logger", "SD"]:
#         try:
#             structure_2btech(RAW_DATA_DIR, STRUCT_DATA_DIR, inst, source)
#         except FileNotFoundError:
#             continue

structure_thermo(RAW_DATA_DIR, STRUCT_DATA_DIR)
