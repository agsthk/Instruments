# -*- coding: utf-8 -*-
"""
Created on Thu Sep 25 17:19:15 2025

@author: agsthk
"""

# %% Package imports, dictionary definitions
import os
import polars as pl
import polars.selectors as cs
from datetime import datetime, timedelta
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
# Full path to directory containing all cleaned data
CLEAN_DATA_DIR = os.path.join(data_dir, "Instruments_CleanData")
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
    inst_icartt_data_dir = os.path.join(ICARTT_DATA_DIR,
                                        inst + "_ICARTTData",
                                        inst + "_ICARTTData_" + header["REVISION"])
    if not os.path.exists(inst_icartt_data_dir):
        os.makedirs(inst_icartt_data_dir)
    if "Calibration" in header.keys():
        continue
        inst_cal_data_dir = os.path.join(CALIBRATED_DATA_DIR,
                                         inst + "_CalibratedData",
                                         inst + "_CalibratedDAQData")
        cal_date = header["Calibration"]
        correct_cal_files = [file for file in os.listdir(inst_cal_data_dir)
                             if (file.find(cal_date + "Calibration") != -1)]
    else:
        inst_cal_data_dir = os.path.join(CLEAN_DATA_DIR,
                                           inst + "_CleanData")
        inst_cal_data_dir = os.path.join(inst_cal_data_dir,
                                         os.listdir(inst_cal_data_dir)[0])
        correct_cal_files = [file for file in os.listdir(inst_cal_data_dir)]
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
                cs.contains("UTC").str.to_datetime(time_zone="UTC")
                .dt.round(every="100ms"),
                cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
                .dt.round(every="100ms")
                ).select(
                    # Keeps only the variables described in the ICARTT header
                    # and SamplingLocation
                    pl.col(
                        [header["ivar"]["shortname"]]
                        + list(header["dvars"].keys())
                        + ["SamplingLocation"]
                        )
                    ))
    camp_data = pl.concat(camp_files, how="diagonal_relaxed")
    # Removes non-C200 data
    camp_data = camp_data.filter(
        pl.col("SamplingLocation").str.contains("C200")
        ).select(
            pl.exclude("SamplingLocation")
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
    # Adjusts timestamps to be no more than 1 second apart for continuous data
    if len(start_cols) == 0:
        # Rounds timestamps to nearest tenth of a second
        camp_data = camp_data.with_columns(
            cs.contains("DateTime").dt.round(every="1s")
            )
        # Timestamp to compare to
        prev_ts = camp_data["UTC_DateTime"][0]
        # List of adjusted timestamps
        adjusted = [prev_ts]
        for ts in camp_data["UTC_DateTime"][1:]:
            # Replaces timestamp if it isn't exactly 1 second after the
            # previous provided it isn't a true gap
            if (ts < prev_ts + timedelta(seconds=3)):
                ts = prev_ts + timedelta(seconds=1)
            # Adds adjusted timestamp to adjusted and sets it as prev_ts
            adjusted.append(ts)
            prev_ts = ts
        # Replaces UTC timestamps with adjusted ones
        camp_data = camp_data.with_columns(
            pl.Series("UTC_DateTime", adjusted)
            )
        # Identifies times needed to fill measurement gaps
        missing_times = camp_data.select(
            pl.datetime_ranges(
                start=pl.col("UTC_DateTime"), end=pl.col("UTC_DateTime").shift(-1),
                interval="1s", closed="none").list.explode().drop_nulls()
            )
        # Adds missing times to camp_data DataFrame
        camp_data = pl.concat(
            [camp_data, missing_times],
            how="diagonal_relaxed"
            ).sort(
                by="UTC_DateTime"
                ).with_columns(
                    # Gets adjusted FTC_DateTimes
                    pl.col("UTC_DateTime")
                    .dt.convert_time_zone("America/Denver")
                    .alias("FTC_DateTime")
                    )
        # Fills missing values with appropriate flag
        camp_data = camp_data.with_columns(
            [pl.col(dvar).fill_null(float(chars["missingflag"]))
             for dvar, chars in header["dvars"].items()
             if camp_data[dvar].dtype == pl.Float64()
             or camp_data[dvar].dtype == pl.Int64()]
            )
    # Splits campaign data by ISO week
    camp_data = camp_data.with_columns(
        (cs.contains("FTC") & ~cs.contains("Stop")).dt.week().alias("Week")
        ).partition_by(
            "Week",
            include_key=False
            )
    # Creates line containing independent variable information
    ivar_line = (
        header["ivar"]["shortname"]
        + "," + header["ivar"]["unit"]
        + "," + header["ivar"]["standardname"]
        )
    if "longname" in header["ivar"].keys():
        ivar_line += ("," + header["ivar"]["longname"])
        
    n_dvars = str(len(header["dvars"].keys()))
    # Creates scales and missing values lines for dependent variables
    scale_line = ",".join(
        [value["scale"] for value in header["dvars"].values()]
        )
    missing_line = ",".join(
        [value["missingflag"] for value in header["dvars"].values()]
        )
    # Creates descriptor lines for all dependent variables
    dvar_lines = []
    for dvar, chars in header["dvars"].items():
        dvar_line = ",".join([dvar, chars["unit"], chars["standardname"]])
        if "longname" in chars.keys():
            dvar_line += ("," + chars["longname"])
        dvar_lines.append(dvar_line)
    dvar_lines = "\n".join(dvar_lines)
    
    # Creates special comments lines
    if "Special Comments" in header.keys():
        spec_coms = "1\n" + header["Special Comments"]
    else:
        spec_coms = "0"
    # Creates normal comments lines
    norm_coms = [key + ": " + value for key, value in header.items()
                 if key.isupper() and key != "FFI"]
    n_norm_coms = len(norm_coms) + 1
    norm_coms = "\n".join(norm_coms)
    # Calculates number of lines
    n_lines = 14 + n_norm_coms + int(n_dvars) + int(spec_coms[0])
    n_lines = str(n_lines)
    n_norm_coms = str(n_norm_coms)
    # Generates header with all known information common to campaign and
    # placeholders for values that will vary by week
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
        df_header = fixed_header
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
        df_header = df_header.replace(
            "COLLECTIONDATE", ftc_start.strftime("%Y,%m,%d")
            ).replace(
                "REVISIONDATE", ftc_stop.strftime("%Y,%m,%d")
                )
        # Combines header with data
        header_with_data = df_header + "\n" + df.write_csv()
        # Creates ICARTT file name
        fname = "_".join([header["dataID"],
                          header["locationID"],
                          ftc_start.strftime("%Y%m%d"),
                          header["REVISION"]]) + ".ict"
        # Exports ICARTT file
        fpath = os.path.join(inst_icartt_data_dir,
                             fname)
        with open(fpath, "w+") as file:
            file.write(header_with_data)

