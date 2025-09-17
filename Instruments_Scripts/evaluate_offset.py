# -*- coding: utf-8 -*-
"""
Created on Mon Sep 15 13:50:40 2025

@author: agsthk
"""

import os
import polars as pl
import polars.selectors as cs
from tqdm import tqdm
import matplotlib.pyplot as plt
import hvplot.polars

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
# Full path to directory containing zero results
ZERO_RESULTS_DIR = os.path.join(data_dir, "Instruments_DerivedData")

# Gets results of all performed calibrations
cal_factors = {}
for root, dirs, files in os.walk(CAL_RESULTS_DIR):
    for file in files:
        if file.find("CalibrationResults") == -1:
            continue
        inst = file.rsplit("_", 1)[0]
        path = os.path.join(root, file)
        cal_factors[inst] = pl.read_csv(path,
                                        schema_overrides={"CalDate": pl.String})

# Gets statistics on instrument zeros and temperature-offset correlations
zeros = {}
correlations = {}
for root, dirs, files in os.walk(ZERO_RESULTS_DIR):
    for file in files:
        inst = file.rsplit("_", 1)[0]
        path = os.path.join(root, file)
        if file.find("UZAStatistics") != -1:
            zeros[inst] = pl.read_csv(path).with_columns(
                cs.contains("UTC").str.to_datetime()
                )
        if file.find("UZATemperatureCorrelation") != -1:
            correlations[inst] = pl.read_csv(path)
            
# Reads in all clean data    
data = {}
for root, dirs, files in tqdm(os.walk(CLEAN_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        # Gets the instrument, log type, and date from file name
        inst, source, date = file.rsplit("_", 2)
        source = source.split("Clean")[-1].split("Data")[0]
        date = date[:-4]
        # Skips Phase I data
        if date.find("2025") == -1:
            continue
        # Creates dictionary keys as needed
        if inst not in data.keys():
            data[inst] = {}
        if source not in data[inst].keys():
            data[inst][source] = []
        path = os.path.join(root, file)
        # Reads data
        if inst == "2BTech_405nm":
            lf = pl.scan_csv(path, infer_schema_length=None)
        else:
            lf = pl.scan_csv(path)
        # Converts timestamps to DateTime format
        lf = lf.with_columns(
            cs.contains("UTC").str.to_datetime(time_zone="UTC"),
            cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        # Adds to data dictionary
        data[inst][source].append(lf)
# Concatenates all data from a given instrument source    
for inst, sources in data.items():
    for source, lfs in sources.items():
        data[inst][source] = pl.concat(
            lfs,
            how="vertical_relaxed"
            ).sort(
                by=cs.contains("UTC")
                )

for inst, df in cal_factors.items():
    # If no 2025 data, skip instrument
    if inst not in data.keys():
        continue
    # Transforms calibration offsets into dictionary for easier calling
    cal_offsets = df.select(
        pl.col("CalDate"),
        cs.contains("Offset") & ~cs.contains("NoiseSignal", "Uncertainty")
        ).rows_by_key(
            "CalDate", 
            named=True,
            unique=True
            )
    # Transforms calibration sensitivies into dictionary for easier calling
    cal_sens = df.select(
        pl.col("CalDate"),
        cs.contains("Sensitivity") & ~cs.contains("NoiseSignal", "Uncertainty")
        ).rows_by_key(
            "CalDate", 
            named=True,
            unique=True
            )
    # Adds columns containing fixed offsets determined from calibrations
    for source, lf in data[inst].items():
        for caldate, offsets in cal_offsets.items():
            for var, offset in offsets.items():
                lf = lf.with_columns(
                    pl.lit(offset).alias(var + "_" + caldate + "Calibration")
                    )
        for caldate, sens in cal_sens.items():
            for var, sen in sens.items():
                lf = lf.with_columns(
                    pl.lit(sen).alias(var + "_" + caldate + "Calibration")
                    )
        # Adds columns containing interpolated measured offsets
        if inst in zeros.keys():
            inst_zeros = zeros[inst].select(
                ~cs.contains("STD")
                )
            stats_cols = inst_zeros.select(
                cs.contains("Mean")
                ).columns
            z_starts = inst_zeros.select(
                pl.exclude("UTC_Stop")
                ).rename({"UTC_Start": lf.collect_schema().names()[0]})
            z_stops = inst_zeros.select(
                pl.exclude("UTC_Start")
                ).rename({"UTC_Stop": lf.collect_schema().names()[0]})
            lf = pl.concat(
                [lf, z_starts.lazy(), z_stops.lazy()],
                how="diagonal_relaxed"
                ).sort(
                    by=lf.collect_schema().names()[0]
                    ).with_columns(
                        pl.col(stats_cols)
                        .interpolate_by(lf.collect_schema().names()[0])
                        ).rename(
                            lambda name: name.replace("Mean", "Offset_UZA")
                            )
            lf = lf.drop_nulls(stats_cols[0].rsplit("_", 1)[0])
    
        if inst in correlations.keys():
            # Transforms correlation information into dictionary for easier calling
            inst_corr = correlations[inst].select(
                pl.col("Species", "Slope", "Intercept")
                ).rows_by_key(
                    "Species", 
                    named=True,
                    unique=True
                    )
            if inst.find("2BTech") != -1:
                temp = "CellTemp_C"
            else:
                temp = "InternalTemp_C"
                
            for species, factors in inst_corr.items():
                lf = lf.with_columns(
                    pl.col(temp)
                    .mul(factors["Slope"])
                    .add(factors["Intercept"])
                    .alias(species + "_ppb_Offset_TemperatureCorrelation")
                    )
        # Replaces original LazyFrame with one containing offset columns 
        data[inst][source] = lf

for inst, sources in data.items():
    for source, lf in sources.items():
        # LazyFrame columns
        cols = lf.collect_schema().names()
        # Names of columns with offsets
        offset_cols = [col for col in cols if col.find("Offset") != -1]
        # Names of columns with sensitivities
        sens_cols = [col for col in cols if col.find("Sensitivity") != -1]
        # Names of columns with fixed offsets from calibrations
        fixed_offset_cols = [col for col in offset_cols if col.find("Calibration") != -1]
        # Names of columns with variable offsets
        var_offset_cols = [col for col in offset_cols if col not in fixed_offset_cols]
        # Calibrates data using fixed calibration offsets
        for off_col in fixed_offset_cols:
            species, _, cal = off_col.rsplit("_", 2)
            sens_col = species + "_Sensitivity_" + cal
            lf = lf.with_columns(
                (pl.col(species).sub(pl.col(off_col)))
                .truediv(pl.col(sens_col))
                .alias(species + "_FixedOffset_" + cal)
                )
        # Calibrates data using variable calibration offsets
        for off_col in var_offset_cols:
            species, _, by = off_col.rsplit("_", 2)
            corrected_col = species + "_" + by + "Offset"
            lf = lf.with_columns(
                pl.col(species).sub(pl.col(off_col))
                .alias(corrected_col)
                )
            for sens_col in sens_cols:
                if sens_col.find(species) == -1:
                    continue
                *_, cal = sens_col.rsplit("_")
                lf = lf.with_columns(
                    pl.col(corrected_col)
                    .truediv(pl.col(sens_col))
                    .alias(corrected_col + "_" + cal)
                    )
        data[inst][source] = lf