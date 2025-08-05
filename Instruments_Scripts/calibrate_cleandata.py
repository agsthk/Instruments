# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 12:28:09 2025

@author: agsthk
"""

import os
import polars as pl
from tqdm import tqdm

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all clean raw data
CLEAN_DATA_DIR = os.path.join(data_dir, "Instruments_CleanData")
# Full path to directory containing all calibrated clean data
CALIBRATED_DATA_DIR = os.path.join(data_dir, "Instruments_CalibratedData")
# Creates Instruments_CleanData/ directory if needed
if not os.path.exists(CALIBRATED_DATA_DIR):
    os.makedirs(CALIBRATED_DATA_DIR)

# Calibration factors to apply
cal_factors = {
    "2BTech_202": {
        "O3_ppb": {"Sensitivity": 0.9773,
                   "Offset": -1.76}
        },
    "2BTech_205_A": {
        "O3_ppb": {"Sensitivity": 0.9795,
                   "Offset":  -1.948}
        },
    "2BTech_205_B": {
        "O3_ppb": {"Sensitivity": 1.0008,
                   "Offset":  0.1224}
        },
    "2BTech_405nm": {
        "NO_ppb": {"Sensitivity": 1.0287,
                   "Offset":  -4.0130},
        "NO2_ppb": {"Sensitivity": 1.0682,
                   "Offset":  -0.2913}
        },
    "Picarro_G2307": {
        "CH2O_ppm": {"Sensitivity": 0.97,
                   "Offset":  -1.36}
        },
    "ThermoScientific_42i-TL": {
        "NO_ppb": {"Sensitivity": 1.0103,
                   "Offset":  4.8689},
        "NO2_ppb": {"Sensitivity": 0.9964,
                   "Offset":  0.3281}
        }
    }
cal_factors = {
    inst: pl.DataFrame(factors)
    for inst, factors in cal_factors.items()
    }


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
        if inst == "2BTech_405nm":
            lf = pl.scan_csv(path, infer_schema_length=None)
        else:
            lf = pl.scan_csv(path)
        lf = lf.with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC"),
            pl.selectors.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        inst_cal_factors = cal_factors[inst]
        for var in inst_cal_factors.columns:
            unnested = inst_cal_factors.unnest(var)
            sens = unnested["Sensitivity"][0]
            off = unnested["Offset"][0]
            lf = lf.with_columns(
                (pl.col(var).sub(off)).truediv(sens)
                )
        df = lf.collect()
        f_name = file.replace("Clean", "Calibrated")
        f_dir = root.replace("Clean", "Calibrated")
        if not os.path.exists(f_dir):
            os.makedirs(f_dir)
        path = os.path.join(f_dir,
                            f_name)
        df.write_csv(path)

