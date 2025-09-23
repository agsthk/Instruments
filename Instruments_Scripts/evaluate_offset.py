# -*- coding: utf-8 -*-
"""
Created on Mon Sep 15 13:50:40 2025

@author: agsthk
"""
# %% Package imports, directory definitions, declarations of constants
import os
import polars as pl
import polars.selectors as cs
from tqdm import tqdm
# import matplotlib.pyplot as plt
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

# Limits of detection reported by the manufacturer in instrument manual/datasheet
mfr_lods = {
    "2BTech_202": {"O3_ppb": 4.5}, #ppb, 3 sigma, 10s measurement mode
    "2BTech_205_A": {"O3_ppb": 3.0}, #ppb, 3 sigma, 10s averaging
    "2BTech_205_B": {"O3_ppb": 3.0}, #ppb, 3 sigma, 10s averaging
    "2BTech_405nm": {"NO_ppb": 1.5,
                     "NO2_ppb": 1.5}, #ppb, given as <1 ppb at 2 sigma with adaptive filter
    "Picarro_G2307": {"CH2O_ppb": 0.3}, #ppb, 3 sigma, given for 300s, typical performance as 0.18 ppb
    "ThermoScientific_42i-TL": {"NO_ppb": 0.05,
                                "NO2_ppb": 0.05}, # ppb, lower limit at 120 second averaging time
    }
# Constant precision reported by the manufacturer in instrument manual/datasheet
const_mfr_prec = {
    "2BTech_202": {"O3_ppb": 1.5}, #greater of 1.5 ppb or 2% of reading
    "2BTech_205_A": {"O3_ppb": 1}, # greater of 1 ppb or 2% of reading
    "2BTech_205_B": {"O3_ppb": 1}, # greater of 1 ppb or 2% of reading
    "2BTech_405nm": {"NO_ppb": 0.5,
                     "NO2_ppb": 0.5}, #greater of <0.5 ppb or 0.5% of reading
    "ThermoScientific_42i-TL": {"NO_ppb": 0.4,
                                "NO2_ppb": 0.4} # ppb, not sure of source or averaging time for this value
    }
# Precision as percent of measurement if it exceeds constant value
perc_mfr_prec = {
    "2BTech_202": {"O3_ppb": 0.02}, #greater of 1.5 ppb or 2% of reading
    "2BTech_205_A": {"O3_ppb": 0.02}, # greater of 1 ppb or 2% of reading
    "2BTech_205_B": {"O3_ppb": 0.02}, # greater of 1 ppb or 2% of reading
    "2BTech_405nm": {"NO_ppb": 0.005,
                     "NO2_ppb": 0.005}, #greater of <0.5 ppb or 0.5% of reading
    }
# Intercept, Slope of equation used to calculate precision from measurement
eq_mfr_prec = {
    "Picarro_G2307": {"CH2O_ppb": [1.2, 0.001]} #1.2 ppb + 0.1% of reading
    }

# Date to use calibration factors from
inst_cal_dates = {"2BTech_202": "20240118",
                  "2BTech_205_A": "20250115",
                  "2BTech_205_B": "20250115",
                  "2BTech_405nm": "20241216",
                  "Picarro_G2307": "20250625",
                  "ThermoScientific_42i-TL": "20241216"}

# %% Calibration and zero characterization results
# Gets results of all performed calibrations
cal_factors = {}
for root, dirs, files in os.walk(CAL_RESULTS_DIR):
    for file in files:
        if file.find("CalibrationResults") == -1:
            continue
        inst = file.rsplit("_", 1)[0]
        path = os.path.join(root, file)
        cal_factors[inst] = pl.read_csv(
            path,
            schema_overrides={"CalDate": pl.String}
            )
zeros = {}
off_corr = {}
lod_corr = {}
for root, dirs, files in os.walk(ZERO_RESULTS_DIR):
    for file in files:
        if file.find(".png") != -1:
            continue
        if file.find("UZAStatistics") != -1:
            inst = file.rsplit("_", 1)[0]
            path = os.path.join(root, file)
            inst_zeros = pl.read_csv(path)
            zeros[inst] = inst_zeros.with_columns(
                cs.contains("UTC").str.to_datetime()
                )
        if file.find("OffsetTemperatureCorrelation") != -1:
            inst = file.rsplit("_", 1)[0]
            path = os.path.join(root, file)
            off_corr[inst] = pl.read_csv(path)
        if file.find("LODTemperatureCorrelation") != -1:
            inst = file.rsplit("_", 1)[0]
            path = os.path.join(root, file)
            lod_corr[inst] = pl.read_csv(path)
# %% Reads and concatenates all clean data (from 2025)
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
# %% Adds calibration offsets and sensitivities to LazyFrame
for inst, lfs in data.items():
    if inst not in cal_factors.keys():
        continue
    # Sensitivities from calibrations for each variable
    cal_sensitivities = cal_factors[inst].select(
        pl.col("CalDate"),
        cs.contains("Sensitivity") & ~cs.contains("NoiseSignal", "Uncertainty")
        ).rows_by_key(
            "CalDate", 
            named=True,
            unique=True
            )
    # Offsets from calibrations for each variable
    cal_offsets = cal_factors[inst].select(
        pl.col("CalDate"),
        cs.contains("Offset") & ~cs.contains("NoiseSignal", "Uncertainty")
        ).rows_by_key(
            "CalDate", 
            named=True,
            unique=True
            )
    for source, lf in lfs.items():
        # Adds calibration sensitivities to LazyFrame
        for cal_date, sensitivities in cal_sensitivities.items():
            for label, sensitivity in sensitivities.items():
                lf = lf.with_columns(
                    pl.lit(sensitivity).alias(label + "_" + cal_date)
                    )
        # Adds calibration offsets to LazyFrame
        for cal_date, offsets in cal_offsets.items():
            for label, offset in offsets.items():
                lf = lf.with_columns(
                    pl.lit(offset).alias(label + "_" + cal_date)
                    )
        # Replaces original LazyFrame with revised LazyFrame in data dictionary
        data[inst][source] = lf
# %% Interpolated zero measurement offsets and limits of detection
for inst, lfs in data.items():
    if inst not in zeros.keys():
        continue
    for source, lf in lfs.items():
        # Name of first column to sort by DateTime
        sort_name = lf.collect_schema().names()[0]
        # Name of second column to remove zero columns later
        filter_name = lf.collect_schema().names()[1]
        # Uses zero statistics to calculate LOD
        inst_zeros = zeros[inst].with_columns(
                cs.contains("STD").mul(3)
                ).rename(
                    lambda name: name.replace("Mean",
                                              "Offset_UZA").replace("STD",
                                                                    "LOD_UZA")
                    )
        # Times that zeroing interval begins (with mean zero and LOD during
        # interval)
        zero_starts = inst_zeros.select(
            pl.exclude("UTC_Stop")
            ).rename({
                "UTC_Start": sort_name
                }).lazy()
        # Times that zeroing interval ends (with mean zero and LOD during
        # interval)
        zero_stops = inst_zeros.select(
            pl.exclude("UTC_Start")
            ).rename({
                "UTC_Stop": sort_name
                }).lazy()
        # Interpolates between zero measurements to give estimated offset and
        # LOD
        lf = pl.concat(
            [lf, zero_starts, zero_stops],
            how="diagonal_relaxed"
            ).sort(
                by=sort_name
                ).with_columns(
                    cs.contains("Offset", "UZA").interpolate_by(sort_name)
                    ).drop_nulls(filter_name)
        # Replaces original LazyFrame with revised LazyFrame in data dictionary
        data[inst][source] = lf
# %% Temperature correlation-based offsets and limits of detection
for inst, lfs in data.items():
    for source, lf in lfs.items():
        if inst in off_corr.keys():
            for temp_col in ["CellTemp_C", "InternalTemp_C"]:
                if temp_col in lf.collect_schema().names():
                    break
            if temp_col in lf.collect_schema().names():
                # Offset vs. temperature correlation parameters
                inst_off_corr = {key + "_ppb": value for key, value in
                                 off_corr[inst].select(
                                     pl.col("Species", "Slope", "Intercept")
                                     ).rows_by_key(
                                         "Species", 
                                         named=True,
                                         unique=True
                                         ).items()}
                # Estimates zero offset from temperatures
                for species, factors in inst_off_corr.items():
                    lf = lf.with_columns(
                        pl.col(temp_col)
                        .mul(factors["Slope"])
                        .add(factors["Intercept"])
                        .alias(species + "_Offset_TempCorr")
                        )
        if inst in lod_corr.keys():
            for temp_col in ["CellTemp_C", "InternalTemp_C"]:
                if temp_col in lf.collect_schema().names():
                    break
            if temp_col in lf.collect_schema().names():
                # LOD vs. temperature correlation parameters
                inst_lod_corr = {key + "_ppb": value for key, value in
                                 lod_corr[inst].select(
                                     pl.col("Species", "Slope", "Intercept")
                                     ).rows_by_key(
                                         "Species", 
                                         named=True,
                                         unique=True
                                         ).items()}
                # Estimates LOD from temperatures
                for species, factors in inst_off_corr.items():
                    lf = lf.with_columns(
                        pl.col(temp_col)
                        .mul(factors["Slope"])
                        .add(factors["Intercept"])
                        .alias(species + "_LOD_TempCorr")
                        )
        # Replaces original LazyFrame with revised LazyFrame in data dictionary
        data[inst][source] = lf

# %% Calibration LODs and noise-to-signal regression factors
for inst, lfs in data.items():
    # Skips instruments with no calibrations
    if inst not in cal_factors.keys():
        continue
    # Gets information on all noise-to-signal regressions from calibrations
    inst_cal_snrs = cal_factors[inst].select(
        pl.col("AveragingTime"),
        (cs.contains("NoiseSignal")
         & ~cs.contains("Uncertainty")).name.suffix("_Cal")
        )
    # Gets information on all limits of detection from calibrations
    inst_cal_lods = cal_factors[inst].select(
        pl.col("AveragingTime", "CalDate"),
        cs.contains("LOD").name.suffix("_Cal")
        )
    # Identifies variables calibrated for
    cal_vars = {col.rsplit("_", 3)[0] for col in inst_cal_snrs.columns
                if col !="AveragingTime"}
    
    # Identifies averaging times with only one noise-to-signal regression
    unique_cal_snrs = inst_cal_snrs.filter(
        ~pl.col("AveragingTime").is_duplicated()
        )
    # Identifies averaging times with multiple noise-to-signal regressions
    dup_cal_snrs = inst_cal_snrs.filter(
        pl.col("AveragingTime").is_duplicated()
        )
    # Identifies best noise-to-signal regression for each averaging time
    for avg_t in dup_cal_snrs["AveragingTime"].unique():
        # Selects noise-to-signal regressions corresponding to current
        # averaging time
        avgt_snrs = dup_cal_snrs.filter(
            pl.col("AveragingTime").eq(avg_t)
            )
        # Identifies regressions with greatest R2 value for each variable
        for i, var in enumerate(cal_vars):
            var_cal_snrs = avgt_snrs.select(
                pl.col("AveragingTime"),
                cs.contains(var)
                ).filter(
                    pl.col(var + "_NoiseSignal_R2_Cal").eq(
                        pl.max(var + "_NoiseSignal_R2_Cal")
                        )
                    )
            if i == 0:
                best_snr = var_cal_snrs
            # Adds best SNR for subsequent variable to that for first
            # variable
            else:
                best_snr = best_snr.join(var_cal_snrs, on="AveragingTime")
        # Adds greatest SNRs to DataFrame with one selection per averaging time
        unique_cal_snrs = pl.concat(
            [unique_cal_snrs, best_snr],
            how="diagonal_relaxed"
            )
    # Identifies the averaging times that only have one possible LOD
    unique_cal_lods = inst_cal_lods.filter(
        ~pl.col("AveragingTime").is_duplicated()
        )
    # Identifies averaging times with multiple possible LODs
    dup_cal_lods = inst_cal_lods.filter(
        pl.col("AveragingTime").is_duplicated()
        )
    # Identifies unique LOD for each duplicated averaging time
    for avg_t in dup_cal_lods["AveragingTime"].unique():
        # Selects LODs corresponding to current averaging time
        avgt_lods = dup_cal_lods.filter(
            pl.col("AveragingTime").eq(avg_t)
            )
        # Selects LODs from CalDate used to calibrate data when possible
        if inst_cal_dates[inst] in avgt_lods["CalDate"]:
            unique_cal_lods = pl.concat(
                [
                    unique_cal_lods,
                    avgt_lods.filter(
                        pl.col("CalDate").eq(inst_cal_dates[inst])
                        )
                    ]
                )
        # Selects greatest LODs if not LODs available for CalDate used
        else:
            for i, var in enumerate(cal_vars):
                # Identifies greatest LOD for current variable and averaging
                # time
                var_cal_lods = avgt_lods.select(
                    pl.col("AveragingTime"),
                    cs.contains(var)
                    ).filter(
                        pl.col(var + "_LOD_Cal").eq(pl.max(var + "_LOD_Cal"))
                        )
                if i == 0:
                    max_lods = var_cal_lods
                # Adds maximum LOD for subsequent variable to that for first
                # variable
                else:
                    max_lods = max_lods.join(var_cal_lods, on="AveragingTime")
            # Adds greatest LODs to DataFrame with one selection per averaging
            # time
            unique_cal_lods = pl.concat(
                [unique_cal_lods, max_lods],
                how="diagonal_relaxed"
                )
    # Combines unique SNR and LOD DataFrames to make joining easier
    inst_snr_lod = unique_cal_snrs.join(
        unique_cal_lods,
        on="AveragingTime"
        ).select(
            ~cs.contains("CalDate", "R2")
            ).with_columns(
                pl.col("AveragingTime").str.extract(r"(\d+?)s", 1)
                .cast(pl.Int64)
                ).sort(
                    by="AveragingTime"
                    )
    for source, lf in lfs.items():
        # Workaround for time issues in Picarro data
        if inst == "Picarro_G2307":
            for col in inst_snr_lod.columns:
                if col == "AveragingTime":
                    continue
                lf = lf.with_columns(
                    pl.lit(inst_snr_lod[col].item()).alias(col)
                    )
        else:
            lf = lf.with_columns( # Creates "AveragingTime" column
                pl.col("UTC_Stop")
                .sub(pl.col("UTC_Start"))
                .dt.total_seconds()
                .alias("AveragingTime")
                ).sort(
                    by="AveragingTime" # Sorting required for join_asof
                    ).join_asof(
                        inst_snr_lod.lazy(),
                        # Joins on "AveragingTime" column
                        on="AveragingTime",
                        # Shorter calibration AveragingTimes apply to larger
                        # measurement AveragingTimes if exact match not
                        # available
                        strategy="backward",
                        coalesce=True
                        ).sort(
                            by="UTC_Start"
                            ).select(
                                pl.exclude("AveragingTime")
                                )
        # Replaces original LazyFrame with revised LazyFrame in data dictionary
        data[inst][source] = lf

# %% Manufacturer values
for inst, lfs in data.items():
    for source, lf in lfs.items():
        if inst in mfr_lods.keys():
            # Adds constant manufacturer LOD for each variable
            for var, lod in mfr_lods[inst].items():
                lf = lf.with_columns(
                    pl.lit(lod).alias(var + "_LOD_MFR")
                    )
        if inst in eq_mfr_prec.keys():
            # Adds manufacturer slope and intercept to calculate uncertainty
            # for each variable
            for var, factors in eq_mfr_prec[inst].items():
                lf = lf.with_columns(
                    pl.lit(factors[0]).alias(var + "_NoiseSignal_Slope_MFR"),
                    pl.lit(factors[1]).alias(var + "_NoiseSignal_Offset_MFR")
                    )
        if inst in const_mfr_prec.keys():
            # Adds constant manufacturer uncertainty for each variable
            for var, unc in const_mfr_prec[inst].items():
                lf = lf.with_columns(
                    pl.lit(unc).alias(var + "_FixedUnc_MFR")
                    )
        if inst in perc_mfr_prec.keys():
            # Adds percent manufacturer uncertainty for each variable
            for var, unc in perc_mfr_prec[inst].items():
                lf = lf.with_columns(
                    pl.lit(unc).alias(var + "_PercUnc_MFR"))
        # Replaces original LazyFrame with revised LazyFrame in data dictionary
        data[inst][source] = lf
# %% Median offsets and LODs
for inst, lfs in data.items():
    if inst not in zeros.keys():
        continue
    for source, lf in lfs.items():
        # Name of column containing sampling interval starts
        start_name = lf.collect_schema().names()[0]
        # Name of column containing sampling interval stops
        stop_name = start_name.replace("Start", "Stop")
        # Adds index column to help with later join
        lf = lf.with_row_index()
        # Identifies the beginning and end of the windows of time to take
        # zero measurement medians
        windows = lf.select(
            pl.col("index"),
            pl.col(start_name).dt.offset_by("-2w").alias("WindowStart"),
            pl.col(stop_name).dt.offset_by("2w").alias("WindowStop")
            )
        # Calculates the median offset and LOD during each window period
        windows = windows.join(
            zeros[inst].lazy(),
            how="cross"
            ).filter(
                (pl.col("UTC_Start").is_between(pl.col("WindowStart"),
                                                pl.col("WindowStop")))
                | (pl.col("UTC_Stop").is_between(pl.col("WindowStart"),
                                                 pl.col("WindowStop")))
                ).group_by("index").agg(
                    cs.contains("Mean").median()
                    .name.map(lambda c: c.replace("Mean", "Offset_Median")),
                    cs.contains("STD").mul(3).median()
                    .name.map(lambda c: c.replace("STD", "LOD_Median"))
                    )
        # Adds median statistics to original LazyFrame
        lf = lf.join(windows, on="index", how="left").drop("index")
        # Replaces original LazyFrame with revised LazyFrame in data dictionary
        data[inst][source] = lf

# %%
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
        data[inst][source] = lf.collect()
#%%

inst_caldates = {"2BTech_205_A": "20250115",
                 "2BTech_205_B": "20250115",
                 "ThermoScientific_42i-TL": "20241216",
                 "Picarro_G2307": "20250625"}

for inst, sources in data.items():
    if inst not in inst_caldates.keys():
        continue
    # if inst != "2BTech_205_A": continue
    # if inst != "2BTech_205_B": continue
    # if inst != "Picarro_G2307": continue
    # if inst != "ThermoScientific_42i-TL": continue
    for source, df in sources.items():
        if source != "DAQ":
            continue
        tcol = [col for col in df.columns if col.find("FTC") != -1][0]
        fixed = [col for col in df.columns if col.find(inst_caldates[inst]) != -1 and col.find("_Offset") == -1 and col.find("Sensitivity") == -1]
        species = {col.rsplit("_", 2)[0] for col in fixed}
        for spec in species:
            name, unit = spec.split("_")
            spec_cols = [col for col in fixed if col.find(spec) != -1]
            uza_col = [col for col in spec_cols if col.find("UZA") != -1]
            if len(uza_col) != 0:
                uza_col = uza_col[0]
            fixed_col = [col for col in spec_cols if col.find("Fixed") != -1]
            if len(fixed_col) != 0:
                fixed_col = fixed_col[0]
                df = df.with_columns(
                    pl.col(uza_col).sub(pl.col(fixed_col)).alias(spec + "_FixedOffsetDifference")
                    )
            temp_col = [col for col in spec_cols if col.find("Temperature") != -1]
            if len(temp_col) != 0:
                temp_col = temp_col[0]
                df = df.with_columns(
                    pl.col(uza_col).sub(pl.col(temp_col)).alias(spec + "_TemperatureOffsetDifference")
                    )
            spec_cols = [col for col in df.columns if col.find(spec) != -1 and col.find("Diff") != -1]
            
            off_plot = df.hvplot.scatter(
                x=tcol,
                y=spec_cols,
                title=inst,
                width=1200,
                height=400,
                ylabel="Difference in calibrated [" + name + "] Relative to UZA Offset (" + unit + ")",
                )
            uza_plot = df.filter(
                pl.col("SamplingLocation").eq("UZA")
                ).hvplot.scatter(
                    x=tcol,
                    y=spec,
                    ylabel=spec,
                    width=1200,
                    height=400)
            hvplot.show(
                (off_plot + uza_plot).cols(1)
                )
            for col in spec_cols:
                med = df[col].median()
                lq = df[col].quantile(0.25)
                uq = df[col].quantile(0.75)
                low = df[col].min()
                up = df[col].max()
                print(inst + " " + col + f": {med:.4f} ({lq:.4f} - {uq:.4f}), ({low:.4f} - {up:.4f})")
        # dfs = df.filter(
        #     pl.col("SamplingLocation").str.contains("C200")
        #     ).with_columns(
        #         pl.col(tcol).dt.week().alias("Week")
        #         ).partition_by("Week")
        # for df in dfs:
        #     # # Plotting with matplotlib
        #     # for spec in species:
        #     #     fig, ax = plt.subplots()
        #     #     for col in fixed:
        #     #         if col.find(spec) == -1:
        #     #             continue
        #     #         ax.plot(df[tcol], df[col], label=col)
        #     # ax.legend()
        #     # ax.set_title(inst + " " + source + " Week " + str(df["Week"][0]))
        #     # Plotting with hvplot
        #     for spec in species:
        #         spec_cols = [col for col in df.columns if col.find(spec) != -1 and col.find("Diff") != -1]
        #         hvplot.show(
        #             df.hvplot.scatter(
        #                 x=tcol,
        #                 y=spec_cols,
        #                 title=inst + " " + source + " Week " + str(df["Week"][0]),
        #                 width=800,
        #                 height=400
        #                 )
        #             )
