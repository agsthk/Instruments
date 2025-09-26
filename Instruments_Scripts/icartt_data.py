# -*- coding: utf-8 -*-
"""
Created on Thu Sep 25 17:19:15 2025

@author: agsthk
"""

# %% Package imports, dictionary definitions
import os
import polars as pl
import polars.selectors as cs
from tqdm import tqdm
from datetime import datetime
import pytz
import yaml
# from calibrate_instruments import set_ax_ticks
# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all calibrated clean data
CALIBRATED_DATA_DIR = os.path.join(data_dir, "Instruments_CalibratedData")
ICARTT_DATA_DIR = os.path.join(data_dir, "Instruments_ICARTTData")
if not os.path.exists(ICARTT_DATA_DIR):
    os.makedirs(ICARTT_DATA_DIR)
# Full path to directory containing all YAML files with ICARTT headers
ICARTT_HEADER_DIR = os.path.join(data_dir,
                                 "Instruments_ManualData",
                                 "Instruments_ICARTTInputs")

headers = {}
for header_file in os.listdir(ICARTT_HEADER_DIR):
    inst = header_file.split("_ICARTTInputs")[0]
    header_path = os.path.join(ICARTT_HEADER_DIR, header_file)
    with open(header_path, "r") as file:
        header = yaml.load(file, Loader=yaml.Loader)

    inst_cal_data_dir = os.path.join(CALIBRATED_DATA_DIR,
                                     inst + "_CalibratedData",
                                     inst + "_CalibratedDAQData")
    inst_icartt_data_dir = os.path.join(ICARTT_DATA_DIR,
                                        inst + "_ICARTTData",
                                        inst + "_ICARTTData_" + header["REVISION"])
    if not os.path.exists(inst_icartt_data_dir):
        os.makedirs(inst_icartt_data_dir)
        
    cal_date = header["Calibration"]
    correct_cal_files = [file for file in os.listdir(inst_cal_data_dir)
                         if (file.find(cal_date + "Calibration") != -1)]
    camp_start = datetime.strptime(header["Campaign Start"], "%Y%m%d").replace(
        hour=0, minute=0, second=0, tzinfo=pytz.timezone("America/Denver")
        )
    camp_stop = datetime.strptime(header["Campaign Stop"], "%Y%m%d").replace(
        hour=23, minute=59, second=59, tzinfo=pytz.timezone("America/Denver")
        )
    # Gets all data collected between campaign start and stop
    camp_files = []
    for file in correct_cal_files:
        file_date = datetime.strptime(file[-12:-4], "%Y%m%d").replace(
            hour=0, minute=0, second=0, tzinfo=pytz.timezone("America/Denver")
            )
        if file_date >= camp_start and file_date <= camp_stop:
            path = os.path.join(inst_cal_data_dir, file)
            camp_files.append(pl.scan_csv(path).with_columns(
                cs.contains("UTC").str.to_datetime(time_zone="UTC"),
                cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
                ).select(
                    # Keeps only the variables described in the ICARTT header
                    pl.col(
                        [header["ivar"]["shortname"]]
                        + list(header["dvars"].keys())
                        )
                    ))
    camp_data = pl.concat(camp_files, how="diagonal_relaxed")
    camp_data = camp_data.with_columns(
        (cs.contains("FTC") & ~cs.contains("Stop")).dt.week().alias("Week")
        ).collect()
    
    # Shift start times so that they don't overlap with previous intervals
    start_cols = [col for col in camp_data.columns if col.find("Start") != -1]
    for start_col in start_cols:
        stop_col = start_col.replace("Start", "Stop")
        camp_data = camp_data.with_columns(
            pl.when(
                pl.col(start_col).lt(pl.col(stop_col).shift(1))
                )
            .then(pl.col(stop_col).shift(1))
            .otherwise(pl.col(start_col))
            .alias(start_col)
            )
    camp_data = camp_data.partition_by("Week", include_key=False)
    

    
    ivar_line = (
        header["ivar"]["shortname"]
        + "," + header["ivar"]["unit"]
        + "," + header["ivar"]["standardname"]
        )
    if "longname" in header["ivar"].keys():
        ivar_line += ("," + header["ivar"]["longname"])
        
    n_dvars = str(len(header["dvars"].keys()))
    scale_line = ",".join([value["scale"] for value in header["dvars"].values()])
    missing_line = ",".join([value["missingflag"] for value in header["dvars"].values()])
    
    dvar_lines = []
    for dvar, chars in header["dvars"].items():
        dvar_line = ",".join([dvar, chars["unit"], chars["standardname"]])
        if "longname" in chars.keys():
            dvar_line += ("," + chars["longname"])
        dvar_lines.append(dvar_line)
    dvar_lines = "\n".join(dvar_lines)
    
    if "Special Comments" in header.keys():
        spec_coms = "1\n" + header["Special Comments"]
    else:
        spec_coms = "0"
        
    norm_coms = [key + ": " + value for key, value in header.items()
                 if key.isupper() and key != "FFI"]
    n_norm_coms = len(norm_coms)
    norm_coms = "\n".join(norm_coms)
    
    n_lines = 15 + n_norm_coms + int(n_dvars) + int(spec_coms[0])
    n_lines = str(n_lines)
    n_norm_coms = str(n_norm_coms)
    
    fixed_header = (
        n_lines + "," + header["FFI"] + "," + header["Version Number"] + "\n"
        + header["PI Name"] + "\n"
        + header["PI Affiliation"] + "\n"
        + header["Data Source Description"] + "\n"
        + header["Mission Name"] + "\n"
        + header["File Volume Number"] + "," + header["Total Number File Volumes"] + "\n"
        + "COLLECTIONDATE,REVISIONDATE\n"
        + header["Data Interval Code"] + "\n"
        + ivar_line + "\n"
        + n_dvars + "\n"
        + scale_line + "\n"
        + missing_line + "\n"
        + dvar_lines + "\n"
        + spec_coms + "\n"
        + n_norm_coms + "\n"
        + norm_coms
        )
    for df in camp_data:
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
        # Converts from DateTime format into seconds from midnight of start
        # date
        df = df.with_columns(
            cs.contains("UTC").sub(utc_start).dt.total_microseconds()
            .truediv(1e6),
            cs.contains("FTC").sub(ftc_start).dt.total_microseconds()
            .truediv(1e6)
            )
        # Gets collection and revision date from start and stop times
        fixed_header = fixed_header.replace(
            "COLLECTIONDATE", ftc_start.strftime("%Y,%m,%d")
            ).replace(
                "REVISIONDATE", ftc_stop.strftime("%Y,%m,%d")
                )
        header_with_data = fixed_header + "\n" + df.write_csv()
        fname = "_".join([header["dataID"],
                          header["locationID"],
                          ftc_start.strftime("%Y%m%d"),
                          header["REVISION"]]) + ".ict"
        fpath = os.path.join(inst_icartt_data_dir,
                             fname)
        with open(fpath, "w+") as file:
            file.write(header_with_data)

