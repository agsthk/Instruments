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
        if inst != "2BTech_205_A": continue
        if inst == "2BTech_405nm":
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
                            )
            lf = lf.drop_nulls(stats_cols[0].rsplit("_", 1)[0])
            # Applies temperature correlation where available to estimate zero
            # offset outside of zeroing active periods
            if inst in correlations.keys():
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
                    spec_off = col.replace("Mean", "Offset")
                    spec_med = lf.select(spec_off).median().collect().item()
                    lf = lf.with_columns(
                        pl.when(pl.col("ZeroingActive"))
                        .then(pl.col(spec_off))
                        .otherwise(spec_med)
                        .alias(spec_off)
                        )
        # Applies offset from calibration as constant zero offset for
        # instruments never zeroed
        else:
            for var in cal_vars:
                lf = lf.with_columns(
                    pl.lit(inst_cal_factors[var + "_Offset"].item())
                    .alias(var + "_Offset")
                    )
                    
        # mintemp = inst_cal_factors.select(
        #     cs.contains("C_Min")
        #     ).item()
        # maxtemp = inst_cal_factors.select(
        #     cs.contains("C_Max")
        #     ).item()
        # meddt = inst_cal_factors.select(
        #     cs.contains("Derivative")
        #     ).item()
        for var in cal_vars:
            sens = inst_cal_factors[var + "_Sensitivity"].item()
            lf = lf.with_columns(
                pl.col(var).sub(pl.col(var + "_Offset")).truediv(sens)
                .alias(var + "_Calibrated")
                )
        df = lf.collect()
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


            # if file[-12:-8] != "2025": continue
        
            # df = lf.collect()
            
            # # Identifies instrument temperature data
            # temp_data = df.select(
            #     cs.contains("UTC", "CellTemp", "InternalTemp", "CavityTemp")
            #     )
            # # Identifies the column name for temperature data
            # temp_col = temp_data.columns[-1]
            # # Assigns unique ID every time instrument temperature changes
            # temp_data = temp_data.with_columns(
            #     pl.col(temp_col).rle_id().alias("ID")
            #     )
            # # Calculates d(Temp)/dt
            # ddt_data = temp_data.filter(
            #         pl.col("ID").is_first_distinct()
            #         ).with_columns(
            #             pl.col(temp_col).shift(-1).sub(pl.col(temp_col)).alias("dTemp"),
            #             pl.col(left_on).shift(-1).sub(pl.col(left_on)).dt.total_microseconds().truediv(1e6).alias("dt")
            #             ).select(
            #                 pl.col("ID"),
            #                 pl.col("dTemp").truediv(pl.col("dt")).alias("d/dt")
            #                 )
            # temp_data = temp_data.join(ddt_data, on="ID", how="left")

            # # Plots temperature data over the calibration
            # temp_fig, temp_ax = plt.subplots(figsize=(8, 6))
            # change_ax = temp_ax.twinx()
            # change_ax.scatter(
            #     temp_data[left_on],
            #     temp_data["d/dt"],
            #     s=25,
            #     color="#D9782D")
            # temp_ax.scatter(
            #     temp_data[left_on],
            #     temp_data[temp_col],
            #     s=25,
            #     color="#1E4D2B"
            #     )
            # temp_ax.set_xlim(temp_data[left_on].min(), temp_data[left_on].max())
            # temp_ax.axhline(mintemp, color="#1E4D2B", lw=3, ls="-.")
            # temp_ax.axhline(maxtemp, color="#1E4D2B", lw=3, ls="-.")
            # temp_ax.text(temp_data[left_on].min(), mintemp, "Min. Cal. Temp.", va="center", ha="right", color="#1E4D2B")
            # temp_ax.text(temp_data[left_on].min(), maxtemp, "Max. Cal. Temp.", va="center", ha="right", color="#1E4D2B")
            
            # change_ax.axhline(meddt, color="#D9782D", lw=3, ls="--")
            # change_ax.text(temp_data[left_on].max(), meddt, "Med. Cal. d/dt", va="center", ha="left", color="#D9782D")
            # temp_ax.set_zorder(change_ax.get_zorder() + 1)
            # temp_ax.patch.set_visible(False)
            # set_ax_ticks(temp_ax, ts=True)
            # temp_ax.set_title(file[-12:-4])
            # temp_ax.set_ylabel(temp_col.replace("_", " (") + ")", color="#1E4D2B")
            # change_ax.set_ylabel("d(" + temp_col.split("_")[0] + ")/dt (C/s)", color="#D9782D")
            # temp_fig.suptitle(inst.replace("_", " ") + " Instrument Temperature Time Series",
            #                   size=15)
            # temp_fig.savefig(inst + "_TemperatureTimeSeries_" + cal_dates[inst] + "Calibration_" + file[-12:-4] + ".png")
            # plt.close()
            # hvplot.show(
            #     df.hvplot.scatter(
            #         x=left_on,
            #         y="CellTemp_C",
            #         by="SamplingLocation",
            #         title=inst + " Instrument Temperature: " + file[-12:-4])
            #     )
            
        
#%%
        # f_name = file.replace("Clean", "Calibrated").rsplit("_", 1)
        # f_name = f_name[0] + "_" + cal_dates[inst] + "Calibration_" + f_name[1]
        # f_dir = root.replace("Clean", "Calibrated")
        # if not os.path.exists(f_dir):
        #     os.makedirs(f_dir)
        # path = os.path.join(f_dir,
        #                     f_name)
        # df.write_csv(path)
        