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
import hvplot.polars
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
             "2BTech_205_A": "20250115",
             "2BTech_205_B": "20250115",
             "2BTech_405nm": "20241216",
             "Picarro_G2307": "20250625",
             "ThermoScientific_42i-TL": "20241216"}

mfr_lod = {"2BTech_202": 3,
           "2BTech_205_B": 3}

cal_factors = {}
sn_factors = {}
cal_lods = {}
for root, dirs, files in os.walk(CAL_RESULTS_DIR):
    for file in files:
        if file.find("CalibrationResults") == -1:
            continue
        inst = file.rsplit("_", 1)[0]
        path = os.path.join(root, file)
        inst_cal_factors = pl.read_csv(path)
        sn_factors[inst] = inst_cal_factors.select(
            pl.col("CalDate"),
            pl.selectors.contains("AveragingTime"),
            pl.selectors.contains("NoiseSignal")
            )
        cal_factors[inst] = inst_cal_factors.filter(
            pl.col("CalDate").cast(pl.String()).eq(cal_dates[inst])
            ).select(
                pl.exclude("CalDate")
                )
        cal_lods[inst] = inst_cal_factors.select(
            pl.col("CalDate"),
            pl.selectors.contains("AveragingTime"),
            pl.selectors.contains("LOD"))

sn_selected = {inst: {} for inst in sn_factors.keys()}
for inst, factors in sn_factors.items():
    species = {col.split("_NoiseSignal")[0] for col in factors.select(
        pl.exclude("CalDate", "AveragingTime")
        ).columns}
    if inst == "Picarro_G2307":
        by_avt = {"1s": factors.select(pl.exclude("AveragingTime"))}
    else:
        by_avt = {key[0]: value for key, value in 
                  factors.partition_by(
                      "AveragingTime", as_dict=True, include_key=False
                      ).items()
                  }
    for avt, facts in by_avt.items():
        if avt not in sn_selected[inst].keys():
            sn_selected[inst][avt] = {}
        for spec in species:
            spec_avt_factors = facts.select(
                cs.contains(spec)
                & ~cs.contains("Uncertainty")
                ).filter(
                    pl.col(spec + "_NoiseSignal_R2")
                    .eq(pl.max(spec + "_NoiseSignal_R2"))
                    ).with_columns(
                        pl.when(pl.col(spec + "_NoiseSignal_Slope").lt(0))
                        .then(0)
                        .otherwise(pl.col(spec + "_NoiseSignal_Slope"))
                        .alias(spec + "_NoiseSignal_Slope")
                        ).select(
                            ~cs.contains("R2")
                            ).to_dict(as_series=False)
            
            sn_selected[inst][avt] = (
                sn_selected[inst][avt] |
                {key.replace("NoiseSignal_", ""): val[0]
                 for key, val in spec_avt_factors.items()}
                )
                #%%
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
        # if inst != "ThermoScientific_42i-TL": continue
        # if inst != "2BTech_205_A": continue
        if inst != "2BTech_205_B": continue
        # if inst != "Picarro_G2307": continue
        if inst == "2BTech_405nm":
            continue
            lf = pl.scan_csv(path, infer_schema_length=None)
        else:
            lf = pl.scan_csv(path)
        lf = lf.with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC")
            )
        inst_cal_factors = cal_factors[inst]

        cal_vars = {"_".join(col.split("_", 2)[:2])
                    for col in inst_cal_factors.columns
                    if col != "CalDate" and col != "AveragingTime" and col.find("Temp") == -1}
        if "UTC_DateTime" in lf.collect_schema().names():
            left_on = "UTC_DateTime"
        else:
            left_on = "UTC_Start"
        if inst in zeros.keys():
            inst_uza_stats = zeros[inst]
            # Identifies gaps in zeroing greater than 6 hours
            z_active_starts = inst_uza_stats.filter(
                pl.col("UTC_Start").sub(pl.col("UTC_Stop").shift(1)).gt(pl.duration(hours=6))
                | pl.col("UTC_Start").sub(pl.col("UTC_Stop").shift(1)).is_null()
                ).select(
                    pl.col("UTC_Start")
                    )
            z_active_stops = inst_uza_stats.filter(
                pl.col("UTC_Start").shift(-1).sub(pl.col("UTC_Stop")).gt(pl.duration(hours=6))
                | pl.col("UTC_Start").shift(-1).sub(pl.col("UTC_Stop")).is_null()
                ).select(
                    pl.col("UTC_Stop")
                    )
            z_active = pl.concat(
                [z_active_starts, z_active_stops],
                how="horizontal"
                ).rename(
                    {"UTC_Start": "Active_Start",
                     "UTC_Stop": "Active_Stop"}
                    )
            # Labels data as "ZeroingActive" when collected during active
            # zeroing periods
            if "UTC_DateTime" in lf.collect_schema().names():
                t_start = t_stop = "UTC_DateTime"
            else:
                t_start = "UTC_Start"
                t_stop = "UTC_Stop"
            lf = lf.join_asof(
                z_active.lazy(),
                left_on=t_start,
                right_on="Active_Start",
                strategy="backward",
                coalesce=False
                ).with_columns(
                    pl.when(
                        pl.col(t_start).is_between(pl.col("Active_Start"),
                                                   pl.col("Active_Stop"))
                        | pl.col(t_stop).is_between(pl.col("Active_Start"),
                                                   pl.col("Active_Stop"))
                        )
                    .then(pl.lit(True))
                    .otherwise(pl.lit(False))
                    .alias("ZeroingActive")
                    ).select(
                        ~cs.contains("Active_")
                        )
            # Uses interpolation between zero periods to determine zero offset
            # during zeroing active periods
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
                        pl.when(pl.col("ZeroingActive"))
                        .then(
                            pl.col(stats_cols)
                            .interpolate_by(left_on)
                            )
                        ).rename(
                            lambda name: name.replace("Mean", "Offset")
                            ).with_columns(
                                cs.contains("STD").mul(3)
                                ).rename(
                                    lambda name: name.replace("STD", "LOD")
                                    )
            lf = lf.drop_nulls(stats_cols[0].rsplit("_", 1)[0])
            # Applies temperature correlation where available to estimate zero
            # offset outside of zeroing active periods
            if inst in off_corr.keys():
                inst_corr = off_corr[inst].select(
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

                for name, factors in inst_corr.items():
                    lf = lf.with_columns(
                        pl.when(pl.col("ZeroingActive"))
                        .then(pl.col(name + "_ppb_Offset"))
                        .otherwise(
                            pl.col(temp)
                            .mul(factors["Slope"])
                            .add(factors["Intercept"])
                            )
                        .alias(name + "_ppb_Offset")
                            )
            # Calculates median offset and applies as zero offset outside of
            # zeroing active periods if no temperature correlation available
            else:
                for col in stats_cols:
                    if col.find("STD") != -1:
                        continue
                    spec = col.replace("Mean", "Offset")
                    year_stats = inst_uza_stats.filter(
                        pl.col("UTC_Start").dt.year().eq(2025)
                        )
                    med_val = year_stats[col].median()
                    
                    lf = lf.with_columns(
                        pl.when(pl.col("ZeroingActive"))
                        .then(pl.col(spec))
                        .otherwise(med_val)
                        .alias(spec))
            # Repeats correlation for LOD determination
            if inst in lod_corr.keys():
                inst_corr = lod_corr[inst].select(
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

                for name, factors in inst_corr.items():
                    lf = lf.with_columns(
                        pl.when(pl.col("ZeroingActive"))
                        .then(pl.col(name + "_ppb_LOD"))
                        .otherwise(
                            pl.col(temp)
                            .mul(factors["Slope"])
                            .add(factors["Intercept"])
                            )
                        .alias(name + "_ppb_LOD")
                            )
            else:
                for col in stats_cols:
                    if col.find("STD") == -1:
                        continue
                    spec = col.replace("STD", "LOD")
                    year_stats = inst_uza_stats.filter(
                        pl.col("UTC_Start").dt.year().eq(2025)
                        )
                    med_val = year_stats[col].median() * 3
                    
                    lf = lf.with_columns(
                        pl.when(pl.col("ZeroingActive"))
                        .then(pl.col(spec))
                        .otherwise(med_val)
                        .alias(spec))
        # Applies offset from calibration as constant zero offset for
        # instruments never zeroed
        else:
            inst_cal_lods = cal_lods[inst].sort(by="AveragingTime")
            unique = inst_cal_lods.filter(
                ~pl.col("AveragingTime").is_duplicated()
                )
            duplicated = inst_cal_lods.filter(
                pl.col("AveragingTime").is_duplicated()
                )
            selected_list = []
            for avg_t in duplicated["AveragingTime"].unique():
                avg_t_lods = duplicated.filter(
                    pl.col("AveragingTime").eq(avg_t)
                    )
                for var in cal_vars:
                    var_lod = avg_t_lods.filter(
                        pl.col("CalDate").cast(pl.String()).eq(cal_dates[inst])
                        ).select(pl.col("AveragingTime", var + "_LOD"))
                    
                    if var_lod.height == 1:
                        selected_list.append(var_lod)
                        continue
                    var_lod = avg_t_lods.filter(
                        pl.col(var + "_LOD").eq(pl.max(var + "_LOD"))
                        ).select(
                            pl.col("AveragingTime", var + "_LOD")
                            )
                    selected_list.append(var_lod)
            for i, lod_df in enumerate(selected_list):
                if i == 0:
                    selected = lod_df
                else:
                    selected = selected.join(lod_df, on="AveragingTime")
                
            unique = pl.concat(
                [unique, selected],
                how="diagonal_relaxed"
                ).select(
                    pl.exclude("CalDate")
                    ).with_columns(
                        pl.col("AveragingTime").str.extract(r"(\d+?)s", 1)
                        .cast(pl.Int64)
                        ).lazy()

            lf = lf.with_columns(
                pl.col("UTC_Stop").sub(pl.col("UTC_Start")).dt.total_seconds().alias("AveragingTime")
                ).sort(by="AveragingTime").join_asof(
                    unique.sort(by="AveragingTime"),
                    on="AveragingTime",
                    strategy="backward",
                    coalesce=True
                    ).sort(by="UTC_Start")
            for var in cal_vars:
                lf = lf.with_columns(
                    pl.lit(inst_cal_factors[var + "_Offset"].item())
                    .alias(var + "_Offset")
                    ).select(
                        ~cs.contains("AveragingTime")
                        )

                
        for var in cal_vars:
            sens = inst_cal_factors[var + "_Sensitivity"].item()
            lf = lf.with_columns(
                pl.col(var).sub(pl.col(var + "_Offset")).truediv(sens)
                .alias(var + "_Calibrated")
                )
            
        if inst in sn_selected.keys():
            if len(sn_selected[inst]) == 1:
                inst_sn_facts = list(sn_selected[inst].values())[0]
                for var in cal_vars:
                    lf = lf.with_columns(
                        (pl.col(var)
                         .mul(inst_sn_facts[var + "_Slope"]))
                        .add(inst_sn_facts[var + "_Offset"])
                        .alias(var + "_Uncertainty")
                        )
            else:
                inst_sn_facts = pl.DataFrame(
                    sn_selected[inst]
                    ).transpose(
                        include_header=True, header_name="AvgT"
                        ).unnest("column_0").with_columns(
                            pl.col("AvgT").str.extract(r"(\d+?)s", 1).cast(pl.Int64)
                            ).lazy()
                lf = lf.with_columns(
                    pl.col("UTC_Stop").sub(pl.col("UTC_Start")).dt.total_seconds().alias("AvgT")
                    ).sort(by="AvgT").join_asof(
                        inst_sn_facts.sort(by="AvgT"),
                        on="AvgT",
                        strategy="backward",
                        coalesce=True
                        ).sort(by="UTC_Start")
                for var in cal_vars:
                    lf = lf.with_columns(
                        (pl.col(var)
                         .mul(pl.col(var + "_Slope")))
                        .add(pl.col(var + "_Offset_right"))
                        .alias(var + "_Uncertainty")
                        )
                lf = lf.select(
                    ~cs.contains("right", "Slope", "AvgT")
                    )
        df = lf.collect()
        if file.find("2025020") == -1:
            continue
        for var in cal_vars:
            hvplot.show(
                df.hvplot.scatter(
                    x=df.columns[0],
                    y=var + "_Calibrated",
                    by="SamplingLocation",
                    title=inst) * (
                        df.hvplot.errorbars(
                            x=df.columns[0],
                            y=var + "_Calibrated",
                            yerr1=var + "_Uncertainty")
                        ) * (
                            df.hvplot.line(
                                x=df.columns[0],
                                y=var + "_LOD"
                                )
                            )
                )

#%%
        # f_name = file.replace("Clean", "Calibrated").rsplit("_", 1)
        # f_name = f_name[0] + "_" + cal_dates[inst] + "Calibration_" + f_name[1]
        # f_dir = root.replace("Clean", "Calibrated")
        # if not os.path.exists(f_dir):
        #     os.makedirs(f_dir)
        # path = os.path.join(f_dir,
        #                     f_name)
        # df.write_csv(path)
        