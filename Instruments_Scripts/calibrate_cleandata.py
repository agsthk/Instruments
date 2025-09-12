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
sn_factors = {}
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
        
zeros = {}
correlations = {}
for root, dirs, files in os.walk(ZERO_RESULTS_DIR):
    for file in files:
        if file.find("UZAStatistics") != -1:
            inst = file.rsplit("_", 1)[0]
            path = os.path.join(root, file)
            inst_zeros = pl.read_csv(path)
            zeros[inst] = inst_zeros.with_columns(
                cs.contains("UTC").str.to_datetime()
                )
        if file.find("UZATemperatureCorrelation") != -1:
            inst = file.rsplit("_", 1)[0]
            path = os.path.join(root, file)
            correlations[inst] = pl.read_csv(path)
        

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
        if inst != "2BTech_205_B": continue
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
                    for col in inst_cal_factors.columns
                    if col != "CalDate"}
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
            lf = lf.drop_nulls(stats_cols[0].rsplit("_", 1)[0])        
        for var in cal_vars:
            sens = inst_cal_factors[var + "_Sensitivity"].item()
            off = inst_cal_factors[var + "_Offset"].item()
            # lf = lf.with_columns(
            #     pl.lit(off).alias(var + "_Fixed"))
            # lf = lf.with_columns(
            #     pl.col(var).sub(pl.col(var + "_Mean")).truediv(sens).alias(var + "_TrueOffset"),
            #     pl.col(var).sub(off).truediv(sens).alias(var +"_FixedOffset")
            #     )
            # if (var + "_Mean") not in lf.collect_schema().names():
            #     lf = lf.with_columns(
            #         pl.lit(off).alias(var + "_Mean"),
            #         pl.lit(0).alias(var + "_STD")
            #         )
            # lf = lf.with_columns(
            #     pl.col(var + "_Mean").fill_null(off),
            #     pl.col(var + "_STD").fill_null(0)
            #     )
            # lf = lf.with_columns(
            #     pl.col(var).sub(pl.col(var + "_Mean")),
            #     pl.col(var + "_STD").mul(3).alias(var + "_LOD")
            #     )
            if inst in correlations.keys():
                if var.split("_")[0] in correlations[inst]["Species"]:
                    if inst == "2BTech_205_A":
                        temp = "CellTemp_C"
                    else:
                        temp = "InternalTemp_C"
                    corr = correlations[inst].filter(
                        pl.col("Species").eq(var.split("_")[0])
                        )
                    m = corr["Slope"].item()
                    b = corr["Intercept"].item()
                    lf = lf.with_columns(
                        pl.col(temp).mul(m).add(b).alias(var + "_Predicted")
                        )
                    lf = lf.with_columns(
                        pl.col(var).sub(pl.col(var + "_Predicted")).truediv(sens).alias(var + "_TempOffset"))
            
            # lf = lf.select(
            #     ~cs.contains("Mean", "STD")
            #     )
            # lf = lf.with_columns(
            #     pl.col(var).truediv(sens)
            #     )

            # if file[-12:-6] == "202501":
            #     lf = lf.with_columns(
            #         (pl.col(var + "_TempOffset").sub(pl.col(var + "_TrueOffset"))).alias(var + "_AbsDiff"),
            #         (pl.col(var + "_TempOffset").add(pl.col(var + "_TrueOffset"))).truediv(2).alias(var + "_Avg")
            #         ).with_columns(
            #             pl.col(var + "_AbsDiff").truediv(pl.col(var + "_Avg")).alias(var + "_RelDiff")
            #             ).filter(
            #                 pl.col("SamplingLocation").str.contains("C200")
            #                 )
            #     df = lf.collect()
            #     cols = [var + "_TrueOffset", var + "_TempOffset"]#, var + "_FixedOffset"]
            #     cols = [col for col in cols if col in df.columns]
            #     hvplot.show(
            #         (df.hvplot.scatter(
            #             x=left_on,
            #             y=cols,
            #             title=inst + " Offsets Comparison: " + file[-12:-4]
            #             )
            #         + (df.hvplot.scatter(
            #             x=left_on,
            #             y=var + "_AbsDiff"
            #             ) * (
            #                 df.hvplot.scatter(
            #                     x=left_on,
            #                     y=var + "_RelDiff"
            #                     )
            #                 )
            #                 ).opts(multi_y=True)
            #         ).cols(1)
                    
            #         )
            if file[-12:-8] != "2025": continue
        
            df = lf.collect()
            hvplot.show(
                df.hvplot.scatter(
                    x=left_on,
                    y="CellTemp_C",
                    by="SamplingLocation",
                    title=inst + " Instrument Temperature: " + file[-12:-4])
                )
            
        
#%%
        f_name = file.replace("Clean", "Calibrated").rsplit("_", 1)
        f_name = f_name[0] + "_" + cal_dates[inst] + "Calibration_" + f_name[1]
        f_dir = root.replace("Clean", "Calibrated")
        if not os.path.exists(f_dir):
            os.makedirs(f_dir)
        path = os.path.join(f_dir,
                            f_name)
        df.write_csv(path)
        