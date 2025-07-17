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
            "SamplFlow_ccm": pl.Float64(),
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
    try:
        data = pl.read_csv(path,
                           has_header=False,
                           schema=schema,
                           ignore_errors=True,
                           skip_rows=1,
                           truncate_ragged_lines=True)
    except pl.exceptions.SchemaError:
        data = pd.read_csv(path,
                           sep=",\s+|,+|\s+",
                           names=schemas.keys(),
                           skiprows=1,
                           engine="python")
        data = pl.from_pandas(data, schema_overrides=schema)
    finally:
        data = data.drop_nulls()
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
            pl.exclude("DateTime", "Local_DateTime")
            )
        
    return df

data = {}

for subdir in os.listdir(RAW_DATA_DIR):
    path = os.path.join(RAW_DATA_DIR, subdir)
    for subdir2 in os.listdir(path):
        path2 = os.path.join(path, subdir2)
        inst, source = subdir2[:-4].split("_Raw")
        schema = schemas[inst][source]
        if inst not in data.keys():
            data[inst] = []
        if inst.find("Teledyne") != -1:
            for folder in os.listdir(path2):
                path3 = os.path.join(path2, folder, "HIRES.txt")
                data[inst].append(read_rawdata(path3, inst, source, schema))
            continue
        for file in os.listdir(path2):
            path3 = os.path.join(path2, file)
            data[inst].append(read_rawdata(path3, inst, source, schema))
            
for inst in data.keys():
    print(define_datetime(data[inst][-1], inst))
    
df = define_datetime(data["LI-COR_LI-840A_A"][-13], "LI-COR_LI-840A_A")


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

def structure_licor(raw_dir, struct_dir, inst):
    # Path to directory containing raw data from declared instrument and source
    raw_dir = os.path.join(raw_dir,
                           inst + "_RawData",
                           inst + "_RawLoggerData")
    # Path to directory containing corresponding structured data
    struct_dir = os.path.join(struct_dir,
                              inst + "_StructuredData",
                              inst + "_StructuredLoggerData")
    if not os.path.exists(struct_dir):
        os.makedirs(struct_dir)
    else:
        # PLAN: Create a text file that lists the files that have already been structured and read it in here
        pass
    
    columns = {"CO2(ppm)": "co2_ppm",
               "H2O(ppt)": "h2o_ppt",
               "H2O(C)": "h2o_C",
               "Cell_Temperature(C)": "cell_temp_C",
               "Cell_Pressure(kPa)": "cell_press_kPa",
               "CO2_Absorption": "co2_abs",
               "H2O_Absorption": "h2o_abs"}
    
    for file in os.listdir(raw_dir):
        raw_path = os.path.join(raw_dir, file)
        
        data = pd.read_csv(raw_path, delimiter="\s+", skiprows=1)
        data = pl.from_pandas(data).lazy()
        
        # Defines instrument datetime
        data = data.select(
            pl.concat_str(
                [pl.col("Date(Y-M-D)"), pl.col("Time(H:M:S)")],
                separator=" "
                ).str.to_datetime(
                    "%Y-%m-%d %H:%M:%S"
                    ).alias("local_datetime"),
           pl.exclude("Date(Y-M-D)", "Time(H:M:S)")
           )
        try:
            data = data.with_columns(
                pl.col("local_datetime").dt.replace_time_zone(
                    time_zone="America/Denver"
                    )
                ).collect()
        # Handles ambiguous datetimes due to daylight savings
        except pl.exceptions.ComputeError:
            data = data.with_row_index()
            mdt_i = data.filter(
                pl.col(
                    "local_datetime"
                    ).shift(-1).sub(
                        pl.col("local_datetime")
                        ).lt(0)
                        ).collect()["index"][0]
            data = data.select(
                pl.when(pl.col("index").le(mdt_i))
                .then(pl.col("local_datetime").dt.replace_time_zone(
                    time_zone="America/Denver",
                    ambiguous="earliest"
                    ))
                .otherwise(pl.col("local_datetime").dt.replace_time_zone(
                    time_zone="America/Denver",
                    ambiguous="latest"
                    )),
                pl.exclude("index")
                ).collect()
        # Defines UTC datetime
        data = data.select(
           pl.col("local_datetime").dt.convert_time_zone(
               "UTC"
               ).alias("utc_datetime"),
           pl.all()
           ).rename(columns)
        
        local_start = data["local_datetime"].min().strftime("%Y%m%d_%H%M%S")
        local_stop = data["local_datetime"].max().strftime("%Y%m%d_%H%M%S")
        
        struct_file_name = (inst
                            + "_StructuredLoggerData_"
                            + local_start 
                            + "_" 
                            + local_stop 
                            + ".csv")

        struct_path = os.path.join(struct_dir, struct_file_name)
        # Writes structured data to CSV file
        data.write_csv(struct_path)

def structure_picarro(raw_dir, struct_dir, source):
    # Path to directory containing raw data from declared source
    raw_dir = os.path.join(raw_dir,
                           "Picarro_G2307_RawData",
                           "Picarro_G2307_RawLoggerData")
    # Path to directory containing corresponding structured data
    struct_dir = os.path.join(struct_dir,
                              "Picarro_G2307_StructuredData",
                              "Picarro_G2307_StructuredLoggerData")
    if not os.path.exists(struct_dir):
        os.makedirs(struct_dir)
    else:
        # PLAN: Create a text file that lists the files that have already been structured and read it in here
        pass
    
    columns = {"H2CO": "hcho_ppm",
               "H2CO_30s": "hcho_30s_ppm",
               "H2CO_2min": "hcho_2min_ppm",
               "H2CO_5min": "hcho_5min_ppm",
               "CH4": "ch4_ppm",
               "H2O": "h2o_perc",
               "solenoid_valves": "solenoid_valves",
               "MPVPosition": "mpv",
               "ALARM_STATUS": "alarm",
               "INST_STATUS": "status",
               "species": "species",
               "CavityTemp": "cavity_temp_C",
               "CavityPressure": "cavity_press_torr",
               "WarmBoxTemp": "warmbox_temp_C",
               "DasTemp": "das_temp_C",
               "EtalonTemp": "etalon_temp_C",
               "OutletValve": "valve_DN"}
    for file in os.listdir(raw_dir):
        raw_path = os.path.join(raw_dir, file)
        
        data = pd.read_hdf(raw_path, "results")
        data["utc_datetime"] = pd.to_datetime(data["DATE_TIME"],
                                              utc=True,
                                              unit="s")
        
        data = pl.from_pandas(data)
        
        data = data.select(
            pl.col("utc_datetime"),
            pl.col("utc_datetime").dt.convert_time_zone(
                time_zone="America/Denver"
                ).alias("local_datetime"),
            *columns.keys()
            ).rename(columns)
        
        local_start = data["local_datetime"].min().strftime("%Y%m%d_%H%M%S")
        local_stop = data["local_datetime"].max().strftime("%Y%m%d_%H%M%S")
        
        struct_file_name = ("Picarro_G2307_StructuredLoggerData_"
                            + local_start 
                            + "_" 
                            + local_stop 
                            + ".csv")

        struct_path = os.path.join(struct_dir, struct_file_name)
        # Writes structured data to CSV file
        data.write_csv(struct_path)


def structure_temprhdoor(raw_dir, struct_dir):
    raw_dir = os.path.join(raw_dir,
                           "TempRHDoor_RawData",
                           "TempRHDoor_RawIgorData")
    
    struct_dir = os.path.join(struct_dir,
                              "TempRHDoor_StructuredData",
                              "TempRHDoor_StructuredIgorData")

    if not os.path.exists(struct_dir):
        os.makedirs(struct_dir)
    else:
        # PLAN: Create a text file that lists the files that have already been structured and read it in here
        pass
        
    columns = {" Ch0Volts": "ch0_volt_V",
               " Vaisala_Temp": "temp_C",
               " Ch1Volts": "ch1_volt_V",
               " Vaisala_RH": "RH",
               " TC3_Temp": "tc3_temp_C",
               " TC5_Temp": "tc5_temp_C",
               " TC7_Temp": "tc7_temp_C",
               " DoorStatus": "doorstatus"}
        
    for file in os.listdir(raw_dir):
        raw_path = os.path.join(raw_dir, file)
        data = pd.read_csv(raw_path)
        try:
            data = pl.from_pandas(data).lazy()
        except:
            print(raw_path)
            continue
        if " Date" not in data.collect_schema().names():
            print(raw_path)
            continue
        # Defines instrument datetime
        data = data.select(
            pl.concat_str(
                [pl.col(" Date"), pl.col(" Time")],
                separator=" "
                ).str.to_datetime(
                    "%Y-%m-%d %H:%M:%S"
                    ).alias("local_datetime"),
           pl.exclude(" Date", " Time")
           )

        try:
            data = data.with_columns(
                pl.col("local_datetime").dt.replace_time_zone(
                    time_zone="America/Denver"
                    )
                ).collect()
        # Handles ambiguous datetimes due to daylight savings
        except pl.exceptions.ComputeError:
            data = data.with_row_index()
            mdt_i = data.filter(
                pl.col(
                    "local_datetime"
                    ).shift(-1).sub(
                        pl.col("local_datetime")
                        ).lt(0)
                        ).collect()["index"][0]
            data = data.select(
                pl.when(pl.col("index").le(mdt_i))
                .then(pl.col("local_datetime").dt.replace_time_zone(
                    time_zone="America/Denver",
                    ambiguous="earliest"
                    ))
                .otherwise(pl.col("local_datetime").dt.replace_time_zone(
                    time_zone="America/Denver",
                    ambiguous="latest"
                    )),
                pl.exclude("index")
                ).collect()
        # Defines UTC datetime
        data = data.select(
           pl.col("local_datetime").dt.convert_time_zone(
               "UTC"
               ).alias("utc_datetime"),
           pl.exclude("index", "IgorTime")
           ).rename(columns)
            
        local_start = data["local_datetime"].min().strftime("%Y%m%d_%H%M%S")
        local_stop = data["local_datetime"].max().strftime("%Y%m%d_%H%M%S")
        
        struct_file_name = ("TempRHDoor_StructuredIgorData_"
                            + local_start 
                            + "_" 
                            + local_stop 
                            + ".csv")

        struct_path = os.path.join(struct_dir, struct_file_name)
        # Writes structured data to CSV file
    return data
        # data.write_csv(struct_path)
# for inst in ["2BTech_202", "2BTech_205_A","2BTech_205_B", "2BTech_405nm"]:
#     for source in ["Logger", "SD"]:
#         try:
#             structure_2btech(RAW_DATA_DIR, STRUCT_DATA_DIR, inst, source)
#         except FileNotFoundError:
#             continue

# structure_thermo(RAW_DATA_DIR, STRUCT_DATA_DIR)
# structure_licor(RAW_DATA_DIR, STRUCT_DATA_DIR, "LI-COR_LI-840A_B")
# structure_picarro(RAW_DATA_DIR, STRUCT_DATA_DIR, "Logger")

test = structure_temprhdoor(RAW_DATA_DIR, STRUCT_DATA_DIR)

test
