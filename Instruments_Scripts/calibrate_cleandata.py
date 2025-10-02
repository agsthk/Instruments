# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 12:28:09 2025

@author: agsthk
"""
# %% Package imports, dictionary definitions
import os
import polars as pl
import polars.selectors as cs
from tqdm import tqdm
# from calibrate_instruments import set_ax_ticks
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
             "2BTech_205_A": "20240604",
             "2BTech_205_B": "20240604",
             "2BTech_405nm": "20241216",
             "Picarro_G2307": "20250625",
             "ThermoScientific_42i-TL": "20240708"}

# %% Calibration results (calibration factors, LODs, and SNRs)
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
# Limits of detection and noise-to-signal regression results from calibrations
lod_snr_factors = {}
# Gets calibration LODs and noise-to-signal regression factors from cal_factors
for inst, factors in cal_factors.items():
    # Gets information on all noise-to-signal regressions from calibrations
    inst_cal_snrs = factors.select(
        pl.col("AveragingTime"),
        (cs.contains("NoiseSignal")
         & ~cs.contains("Uncertainty")).name.suffix("_Cal")
        )
    # Gets information on all limits of detection from calibrations
    inst_cal_lods = factors.select(
        pl.col("AveragingTime", "CalDate"),
        cs.contains("LOD").name.suffix("_Cal")
        )
    # Identifies variables calibrated for
    cal_vars = {"_".join(col.split("_")[:2]) for col in inst_cal_snrs.columns
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
        # Adds best SNRs to DataFrame with one selection per averaging time
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
        if cal_dates[inst] in avgt_lods["CalDate"]:
            unique_cal_lods = pl.concat(
                [
                    unique_cal_lods,
                    avgt_lods.filter(
                        pl.col("CalDate").eq(cal_dates[inst])
                        )
                    ]
                )
        # Selects greatest LODs if LODs not available for CalDate used
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
    lod_snr_factors[inst] = unique_cal_snrs.join(
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
cal_offsets = {}
cal_sensitivities = {}
for inst, factors in cal_factors.items():
    cal_offsets[inst] = factors.filter(
        pl.col("CalDate").eq(cal_dates[inst])
        ).select(
            cs.contains("Offset")
            & ~cs.contains("NoiseSignal", "Uncertainty")
            )
    cal_sensitivities[inst] = factors.filter(
        pl.col("CalDate").eq(cal_dates[inst])
        ).select(
            cs.contains("Sensitivity")
            & ~cs.contains("NoiseSignal", "Uncertainty")
            )

# %% Zero characteristics (including temperature correlations)
# Gets characterized zeros
zeros = {}
off_corr = {}
lod_corr = {}
year = 2024
for root, dirs, files in os.walk(ZERO_RESULTS_DIR):
    for file in files:
        if file.find(".png") != -1:
            continue
        if file.find("UZAStatistics_" + str(year)) != -1:
            inst = file.rsplit("_", 1)[0]
            path = os.path.join(root, file)
            inst_zeros = pl.read_csv(path)
            zeros[inst] = inst_zeros.with_columns(
                cs.contains("UTC").str.to_datetime(),
                # Calculates LOD from STD
                cs.contains("STD").mul(3)
                ).rename(lambda name: name.replace("Mean",
                                                   "Offset").replace("STD",
                                                                     "LOD"))
        if file.find("OffsetTemperatureCorrelation_" + str(year)) != -1:
            inst = file.rsplit("_", 1)[0]
            path = os.path.join(root, file)
            inst_off_corr = pl.read_csv(path)
            # Converts DataFrame into dictionary that is easier to access
            inst_off_corr = {
                key + "_ppb": value for key, value in inst_off_corr.select(
                    pl.col("Species", "Slope", "Intercept")
                    ).rows_by_key(
                        "Species",
                        named=True,
                        unique=True
                        ).items()
                        }
            off_corr[inst] = inst_off_corr
        if file.find("LODTemperatureCorrelation_" + str(year)) != -1:
            inst = file.rsplit("_", 1)[0]
            path = os.path.join(root, file)
            inst_lod_corr = pl.read_csv(path)
            # Converts DataFrame into dictionary that is easier to access
            inst_lod_corr = {
                key + "_ppb": value for key, value in inst_lod_corr.select(
                    pl.col("Species", "Slope", "Intercept")
                    ).rows_by_key(
                        "Species",
                        named=True,
                        unique=True
                        ).items()
                        }
            lod_corr[inst] = inst_lod_corr

zero_active = {}
rolling_zeros = {}
for inst, zero_df in zeros.items():
    # Identifies starts and stops of periods with no gaps between zeros greater
    # than 6 hours
    z_active_starts = zero_df.filter(
        (pl.col("UTC_Start")
        .sub(pl.col("UTC_Stop").shift(1))
        .gt(pl.duration(hours=6)))
        | (pl.col("UTC_Start")
           .sub(pl.col("UTC_Stop").shift(1))
           .is_null())
        ).select(
            pl.col("UTC_Start").alias("ZActive_Start")
            )
    z_active_stops = zero_df.filter(
        (pl.col("UTC_Start").shift(-1)
         .sub(pl.col("UTC_Stop"))
         .gt(pl.duration(hours=6)))
        | (pl.col("UTC_Start").shift(-1)
           .sub(pl.col("UTC_Stop"))
           .is_null())
        ).select(
            pl.col("UTC_Stop").alias("ZActive_Stop")
            )
    zero_active[inst] = pl.concat(
        [z_active_starts, z_active_stops],
        how="horizontal"
        )
    # Calculates rolling median offsets and LODs
    with_mid = zero_df.select(
        # Gets mid point of zero interval
        pl.col("UTC_Start").add(
            (pl.col("UTC_Stop").sub(pl.col("UTC_Start"))).truediv(2)
            ).alias("UTC_Mid"),
        cs.contains("Offset", "LOD")
        )
    # Extends last UZA measurement to 1 week out to prevent gap after shifting
    # midpoints back by 1 week
    with_mid = pl.concat(
        [with_mid,
         with_mid.tail(1).with_columns(
             pl.col("UTC_Mid").dt.offset_by("2w"))]
        )
    # Calculates 2 week rolling median of offsets and LODs
    rolling_zeros[inst] = with_mid.select(
        # Shifts midpoint to be in the center of the 2 week interval
        pl.col("UTC_Mid").dt.offset_by("-1w"),
        cs.contains("Offset", "LOD")
        .rolling_median_by("UTC_Mid", window_size="2w")
        )

    
# %% Calibrating data
all_cols = {}
start_cols = {}
stop_cols = {}
species = {}
for root, dirs, files in tqdm(os.walk(CLEAN_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        if file.find(str(year)) == -1:
            continue
        path = os.path.join(root, file)
        # Identifies instrument that produced clean data
        inst = file.split("_Clean")[0]
        # Doesn't waste time reading files with no calibrations to apply
        if inst not in cal_dates.keys():
            continue
        # Prevents error from reading ErrorByte as an integer
        if inst == "2BTech_405nm":
            lf = pl.scan_csv(path, schema_overrides={"ErrorByte": pl.String})
        else:
            lf = pl.scan_csv(path)
        # Converts UTC timestamps to DateTime format
        lf = lf.with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC")
            )
        # Gets relevant column names while avoiding collecting schemas multiple
        # times for one instrument
        if inst not in all_cols.keys():
            # All LazyFrame columns
            lf_cols = lf.collect_schema().names()
            # Start and stop time columns (may be identical)
            start_col = [col for col in lf_cols if col.find("UTC") != -1][0]
            stop_col = start_col.replace("Start", "Stop")
            # Variables to calibrate for
            specs = [col for col in lf_cols if (
                (col.find("pp") != -1 or col.find("perc") != -1)
                and col.find("NOx") == -1 and col.find("30s") == -1 
                and col.find("2min") == -1 and col.find("5min") == -1
                )]
            all_cols[inst] = lf_cols
            start_cols[inst] = start_col
            stop_cols[inst] = stop_col
            species[inst] = specs
        else:
            lf_cols = all_cols[inst]
            start_col = start_cols[inst]
            stop_col = stop_cols[inst]
            specs = species[inst]

        # Uses periodic UZA measurements to determine offsets and LODs
        if inst in zeros.keys():
            # Identifies data collected during active zeroing periods
            lf = lf.join_asof(
                zero_active[inst].lazy(),
                left_on=start_col,
                right_on="ZActive_Start",
                strategy="backward",
                coalesce=False
                ).with_columns(
                    pl.when(
                        pl.col(start_col).is_between(pl.col("ZActive_Start"),
                                                     pl.col("ZActive_Stop"))
                        | pl.col(stop_col).is_between(pl.col("ZActive_Start"),
                                                       pl.col("ZActive_Stop"))
                        )
                    .then(pl.lit(True))
                    .otherwise(pl.lit(False))
                    .alias("ZeroingActive")
                    ).select(
                        ~cs.contains("ZActive")
                        )
            # Times that zeroing intervals begin (with mean zero and LOD during
            # interval)
            zero_starts = zeros[inst].select(
                pl.exclude("UTC_Stop")
                ).rename({
                    "UTC_Start": start_col
                    }).lazy()
            # Times that zeroing intervals end (with mean zero and LOD during
            # interval)
            zero_stops = zeros[inst].select(
                pl.exclude("UTC_Start")
                ).rename({
                    "UTC_Stop": start_col
                    }).lazy()
            # Interpolates between zero measurements for offset and LOD
            lf = pl.concat(
                [lf, zero_starts, zero_stops],
                how="diagonal_relaxed"
                ).sort(
                    by=start_col
                    ).with_columns(
                        cs.contains("Offset", "LOD").interpolate_by(start_col)
                        ).drop_nulls(specs[0])
            # Replaces interpolated offset with temperature correlation-based
            # offset when zeroing not active if available
            if inst in off_corr.keys():
                for temp_col in ["CellTemp_C", "InternalTemp_C"]:
                    if temp_col in lf_cols:
                        break
                # Estimates zero offset from temperatures where zeroing
                # inactive
                for var, factors in off_corr[inst].items():
                    lf = lf.with_columns(
                        pl.when(~pl.col("ZeroingActive"))
                        .then(
                            pl.col(temp_col)
                            .mul(factors["Slope"])
                            .add(factors["Intercept"])
                            )
                        .otherwise(pl.col(var + "_Offset"))
                        .alias(var + "_Offset")
                        )
            # Replaces interpolated offset with median value when zeroing not
            # active if temperature correlation not available
            else:
                rolling_offsets = rolling_zeros[inst].select(
                    cs.contains("UTC_Mid", "Offset")
                    ).rename(lambda name: name.replace("Offset", "Offset_Med"))
                lf = lf.join_asof(
                    rolling_offsets.lazy(),
                    left_on=start_col,
                    right_on="UTC_Mid",
                    strategy="nearest",
                    coalesce=True
                    )
                for var in specs:
                    lf = lf.with_columns(
                        pl.when(~pl.col("ZeroingActive"))
                        .then(pl.col(var + "_Offset_Med"))
                        .otherwise(pl.col(var + "_Offset"))
                        .alias(var + "_Offset")
                        ).select(
                            pl.exclude(var + "_Offset_Med")
                            )
            # Replaces interpolated offset with temperature correlation-based
            # offset when zeroing not active if available
            if inst in lod_corr.keys():
                for temp_col in ["CellTemp_C", "InternalTemp_C"]:
                    if temp_col in lf_cols:
                        break
                # Estimates LOD from temperatures where zeroing inactive
                for var, factors in lod_corr[inst].items():
                    lf = lf.with_columns(
                        pl.when(~pl.col("ZeroingActive"))
                        .then(
                            pl.col(temp_col)
                            .mul(factors["Slope"])
                            .add(factors["Intercept"])
                            )
                        .otherwise(pl.col(var + "_LOD"))
                        .alias(var + "_LOD")
                        )
            # Replaces interpolated LOD with median value when zeroing not
            # active if temperature correlation not available
            else:
                rolling_lods = rolling_zeros[inst].select(
                    cs.contains("UTC_Mid", "LOD")
                    ).rename(lambda name: name.replace("LOD", "LOD_Med"))
                lf = lf.join_asof(
                    rolling_lods.lazy(),
                    left_on=start_col,
                    right_on="UTC_Mid",
                    strategy="nearest",
                    coalesce=True
                    )
                for var in specs:
                    lf = lf.with_columns(
                        pl.when(~pl.col("ZeroingActive"))
                        .then(pl.col(var + "_LOD_Med"))
                        .otherwise(pl.col(var + "_LOD"))
                        .alias(var + "_LOD")
                        ).select(
                            pl.exclude(var + "_LOD_Med")
                            )
        # Applies offset from calibration if instrument never zeroed
        else:
            if inst not in cal_offsets.keys():
                continue
            for off_col in cal_offsets[inst].columns:
                lf = lf.with_columns(
                    pl.lit(cal_offsets[inst][off_col].item()).alias(off_col)
                    )
        # Adds calibration LOD and SNR factors (no averaging time dependence)
        if start_col == stop_col:
            for fact_col in lod_snr_factors[inst].columns:
                lf = lf.with_columns(
                    pl.lit(lod_snr_factors[inst][fact_col].item())
                    .alias(fact_col)
                    )
        # Adds appropriate calibration LOD and SNR factors based on averaging time
        else:
            lf = lf.with_columns(
                pl.col(stop_col)
                .sub(pl.col(start_col))
                .dt.total_seconds()
                .alias("AveragingTime")
                ).sort(
                    by="AveragingTime" # Sorting required for join_asof
                    ).join_asof(
                        lod_snr_factors[inst].lazy(),
                        # Joins on "AveragingTime" column
                        on="AveragingTime",
                        # Shorter calibration AveragingTimes apply to larger
                        # measurement AveragingTimes if exact match not
                        # available
                        strategy="backward",
                        coalesce=True
                        ).sort(
                            # Fixes sorting
                            by=start_col
                            ).select(
                                pl.exclude("AveragingTime")
                                )
        # Removes calibration LOD column if better method exists
        if inst in zeros.keys():
            lf = lf.select(
                ~cs.contains("Cal")
                | cs.contains("NoiseSignal")
                )
        # Removes "Cal" qualifier from calibration SNR and LODs
        lf = lf.rename(lambda name: name.replace("_Cal", ""))
        # Applies calibration sensitivity factors
        inst_sens = {key.rsplit("_", 1)[0]: value[0] for key, value in
                     cal_sensitivities[inst].to_dict(as_series=False).items()}
        for spec, sens in inst_sens.items():
            lf = lf.with_columns(
                pl.col(spec).truediv(sens)
                )
        # Calculates uncertainties from noise to signal regressions
        snr_specs = {"_".join(col.split("_")[:2])
                     for col in lod_snr_factors[inst].columns
                     if col.find("NoiseSignal") != -1}
        for spec in snr_specs:
            lf = lf.with_columns(
                pl.col(spec)
                .mul(pl.col(spec + "_NoiseSignal_Slope"))
                .add(pl.col(spec + "_NoiseSignal_Offset"))
                .alias(spec + "_Uncertainty")
                )
        lf = lf.select(
            ~cs.contains("NoiseSignal")
            )
        # Collects LazyFrame for exporting and exports calibrated data
        df = lf.collect()
        f_name = file.replace("Clean", "Calibrated").rsplit("_", 1)
        f_name = f_name[0] + "_" + cal_dates[inst] + "Calibration_" + f_name[1]
        f_dir = root.replace("Clean", "Calibrated")
        if not os.path.exists(f_dir):
            os.makedirs(f_dir)
        path = os.path.join(f_dir,
                            f_name)
        df.write_csv(path)
        