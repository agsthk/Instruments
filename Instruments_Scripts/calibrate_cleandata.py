# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 12:28:09 2025

@author: agsthk
"""

import os
import polars as pl
import polars.selectors as cs
from tqdm import tqdm
import matplotlib.pyplot as plt
# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all clean raw data
CLEAN_DATA_DIR = os.path.join(data_dir, "Instruments_CleanData")
# Full path to directory containing calibration results
CAL_RESULTS_DIR = os.path.join(data_dir, "Instruments_ManualData", "Instruments_Calibrations")
# Full path to directory containing all calibrated clean data
CALIBRATED_DATA_DIR = os.path.join(data_dir, "Instruments_CalibratedData")
# Creates Instruments_CleanData/ directory if needed
if not os.path.exists(CALIBRATED_DATA_DIR):
    os.makedirs(CALIBRATED_DATA_DIR)
# Full path to directory containing zero results
ZERO_RESULTS_DIR = os.path.join(data_dir, "Instruments_DerivedData")

# Date to use calibration factor from
cal_dates = {"2BTech_202": "20240118",
             "2BTech_205_A": "20250115",
             "2BTech_205_B": "20250115",
             "2BTech_405nm": "20241216",
             "Picarro_G2307": "20250109",
             "ThermoScientific_42i-TL": "20241216"}

cal_factors = {}
for root, dirs, files in os.walk(CAL_RESULTS_DIR):
    for file in files:
        if file.find("CalibrationResults") == -1:
            continue
        inst = file.rsplit("_", 1)[0]
        path = os.path.join(root, file)
        inst_cal_factors = pl.read_csv(path)
        cal_factors[inst] = inst_cal_factors.filter(
            pl.col("CalDate").cast(pl.String()).eq(cal_dates[inst])
            ).select(
                pl.exclude("CalDate")
                )
        
zeros = {}
for root, dirs, files in os.walk(ZERO_RESULTS_DIR):
    for file in files:
        if file.find("UZAStatistics") == -1:
            continue
        inst = file.rsplit("_", 1)[0]
        path = os.path.join(root, file)
        inst_zeros = pl.read_csv(path)
        zeros[inst] = inst_zeros.with_columns(
            cs.contains("UTC").str.to_datetime()
            )

for inst, factors in cal_factors.items():
    cal_vars = {"_".join(col.split("_", 2)[:2]) for col in factors.columns}

for root, dirs, files in tqdm(os.walk(CLEAN_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        for inst in cal_factors.keys():
            if path.find(inst) != -1:
                break
        if path.find(inst) == -1:
            continue
        # if inst != "2BTech_205_A": continue
        if inst == "2BTech_405nm":
            lf = pl.scan_csv(path, infer_schema_length=None)
        else:
            lf = pl.scan_csv(path)
        lf = lf.with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC")
            )
        inst_cal_factors = cal_factors[inst]
        if inst in zeros.keys():
            inst_uza_stats = zeros[inst]
        cal_vars = {"_".join(col.split("_", 2)[:2])
                    for col in inst_cal_factors.columns}
        for var in cal_vars:
            sens = inst_cal_factors[var + "_Sensitivity"]
            off = inst_cal_factors[var + "_Offset"]
            lf = lf.with_columns(
                (pl.col(var).sub(off)).truediv(sens)
                )
            if inst in zeros.keys():
                # Applies calibration factors to UZA stats
                inst_uza_stats = inst_uza_stats.with_columns(
                    (pl.col(var + "_Mean").sub(off)).truediv(sens),
                    (pl.col(var + "_STD").truediv(sens))
                    )
        if "UTC_DateTime" in lf.collect_schema().names():
            left_on = "UTC_DateTime"
        else:
            left_on = "UTC_Start"
            
        if inst in zeros.keys():
            stats_cols = inst_uza_stats.select(
                cs.contains("Mean", "STD")
                ).columns
            z_starts = inst_uza_stats.select(
                pl.exclude("UTC_Stop")
                ).rename({"UTC_Start": left_on})
            z_stops = inst_uza_stats.select(
                pl.exclude("UTC_Start")
                ).rename({"UTC_Stop": left_on})
            
            lf = pl.concat(
                [lf, z_starts.lazy(), z_stops.lazy()],
                how="diagonal_relaxed"
                ).sort(
                    by=left_on
                    ).with_columns(
                        pl.col(stats_cols)
                        .interpolate_by(left_on)
                        )
            for col in stats_cols:
                lf = lf.with_columns(
                    pl.col(col).fill_null(inst_uza_stats[col].median())
                    )
            lf = lf.drop_nulls(stats_cols[0].rsplit("_", 1)[0])
            for var in cal_vars:
                lf = lf.with_columns(
                    pl.col(var).sub(pl.col(var + "_Mean")),
                    pl.col(var + "_STD").mul(3).alias(var + "_LOD")
                    )
            lf = lf.select(
                ~cs.contains("Mean", "STD")
                )
            lf = lf.filter(
                pl.col("SamplingLocation").ne("UZA"))
        df = lf.collect()
        f_name = file.replace("Clean", "Calibrated").rsplit("_", 1)
        f_name = f_name[0] + "_" + cal_dates[inst] + "Calibration_" + f_name[1]
        f_dir = root.replace("Clean", "Calibrated")
        if not os.path.exists(f_dir):
            os.makedirs(f_dir)
        path = os.path.join(f_dir,
                            f_name)
        df.write_csv(path)
        