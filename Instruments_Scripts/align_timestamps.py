# -*- coding: utf-8 -*-
"""
Created on Fri Sep 26 16:09:14 2025

@author: agsthk
"""
# %% Package imports and dictionary definitions
import os
import polars as pl
import polars.selectors as cs
import pytz
from datetime import datetime, timedelta
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import hvplot.polars
import holoviews as hv

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all structured raw data
STRUCT_DATA_DIR = os.path.join(data_dir, "Instruments_StructuredData")
AVG_TIME_DIR = os.path.join(data_dir, "Instruments_ManualData", "Instruments_AveragingTimes")

insts = ["2BTech_205_A",
         #"2BTech_205_B",
         # "2BTech_405nm",
         "Picarro_G2307",
         "ThermoScientific_42i-TL"]
avg_times = {}
for avg_times_file in os.listdir(AVG_TIME_DIR):
    inst = avg_times_file.rsplit("_", 1)[0]
    avg_times_path = os.path.join(AVG_TIME_DIR,
                                  avg_times_file)
    avg_times[inst] = pl.read_csv(avg_times_path).with_columns(
        pl.col("UTC_Start").str.to_datetime()
        ).sort(
            by="UTC_Start"
            ).lazy()

# %% Data read-in
data = {inst: {} for inst in insts}
for root, dirs, files in tqdm(os.walk(STRUCT_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        for inst in insts:
            if path.find(inst) != -1:
                break
        if path.find(inst) == -1:
            continue
        _, source = file[:-17].split("_Structured")
        date = file.rsplit("_", 1)[-1][:-4]
        if date.find("2024") == -1:
            continue
        if inst == "2BTech_405nm":
            lf = pl.scan_csv(path, schema_overrides={"ErrorByte": pl.String()})
        else:
            lf = pl.scan_csv(path)
        lf = lf.with_columns(
            pl.selectors.contains("UTC")
            .str.to_datetime(time_zone="UTC"),
            pl.selectors.contains("FTC")
            .str.to_datetime(time_zone="America/Denver")
            )
        if inst in avg_times.keys():
            lf = lf.join_asof(
                    avg_times[inst],
                    left_on="UTC_DateTime",
                    right_on="UTC_Start",
                    strategy="backward"
                    )
            lf = lf.select(
                pl.col("UTC_DateTime")
                .dt.offset_by("-" + pl.col("AveragingTime"))
                .alias("UTC_Start"),
                pl.col("UTC_DateTime").alias("UTC_Stop"),
                pl.col("FTC_DateTime")
                .dt.offset_by("-" + pl.col("AveragingTime"))
                .alias("FTC_Start"),
                pl.col("FTC_DateTime").alias("FTC_Stop"),
                pl.exclude(
                    "UTC_DateTime",
                    "FTC_DateTime",
                    "UTC_Start",
                    "AveragingTime"
                    )
                )
        if source not in data[inst].keys():
            data[inst][source] = [lf]
        else:
            data[inst][source].append(lf)

#%% Data concatenation and timestamp correction
for inst, sources in data.items():
    for source, lfs in sources.items():
        lf = pl.concat(lfs)
        if inst == "2BTech_205_A":
            # Declares Timestamps known to be synched with real time
            real_ts = [
                datetime(2024, 5, 23, 15, 57, 5, tzinfo=pytz.UTC),
                datetime(2024, 6, 25, 19, 35, 18, tzinfo=pytz.UTC)
                ]
            # Given instrument 15 seconds ahead before second sync, determines
            # extra seconds added by instrument per real second
            diff = 15 / (real_ts[1] - real_ts[0]).total_seconds()
            # Approximates an initial synchronized timestamp given instrument
            # ~1 minute ahead before first sync
            real_ts = [real_ts[0] - timedelta(seconds=(60 / diff))] + real_ts
            real_ts = pl.DataFrame(real_ts, schema=["RealTimestamp"])
            # Calculates time since last known synced timestamp for each
            # measurement as determined by instrument and converts to real time
            # passed
            lf = lf.join_asof(
                real_ts.lazy(),
                left_on="UTC_Stop",
                right_on="RealTimestamp",
                strategy="backward"
                ).with_columns(
                    cs.contains("UTC").sub(pl.col("RealTimestamp"))
                    .dt.total_microseconds().truediv((1 + diff))
                    .cast(pl.Int64).cast(pl.String).add("us")
                    .name.suffix("_Passed")
                    )
            # Determines corrected time from time passed since last synced
            # timestamp
            lf = lf.with_columns((
                pl.col("RealTimestamp").dt.offset_by(pl.col(col))
                .alias(col.replace("_Passed", ""))
                for col in cs.expand_selector(lf, cs.contains("_Passed"))
                )).select(
                    ~cs.contains("_Passed", "RealTimestamp")
                    ).with_columns(
                        # Converts corrected UTC timestamp to local timestamp
                        cs.contains("UTC")
                        .dt.convert_time_zone("America/Denver")
                        .name.map(lambda name: name.replace("UTC", "FTC"))
                        )
        if inst == "Picarro_G2307":
            # Corrects drift (estimated)
            diff = (9.476
                    / ((datetime(2024, 6, 30, 23, 44, 1)
                        - datetime(2024, 6, 25, 13, 44, 17)).total_seconds()))
            lf = lf.with_columns(
                pl.col("UTC_DateTime").sub(pl.min("UTC_DateTime"))
                .dt.total_microseconds().truediv(1 + diff)
                .cast(pl.Int64).cast(pl.String).add("us")
                .alias("Passed")
                ).with_columns(
                    pl.min("UTC_DateTime").dt.offset_by(pl.col("Passed"))
                    .alias("UTC_DateTime")
                    ).with_columns(
                        # Converts corrected UTC timestamp to local timestamp
                        cs.contains("UTC")
                        .dt.convert_time_zone("America/Denver")
                        .name.map(lambda name: name.replace("UTC", "FTC"))
                        )
        lf = lf.with_columns(
            # Adds dt column based on gap between consecutive measurements
            (cs.contains("FTC") & ~cs.contains("Stop"))
            .sub((cs.contains("FTC") & ~cs.contains("Stop")).shift(1))
            .dt.total_microseconds().truediv(1e6)
            .alias("dt"),
            # Adds week of year column
            (cs.contains("FTC") & ~cs.contains("Stop")).dt.week()
            .alias("Week")
            )
        # Calculates derivative of variable with respect to time
        lf = lf.with_columns(
            cs.contains("O3_ppb", "NO2_ppb", "CH2O_ppb")
            .sub(cs.contains("O3_ppb", "NO2_ppb", "CH2O_ppb").shift(1))
            .truediv(pl.col("dt"))
            .alias("ddt")
            )
        data[inst][source] = lf.collect()
# %% Identifies adjusted valve state times from corrected Picarro timestamps

valve_open = data["Picarro_G2307"]["Logger"].filter(
    pl.col("SolenoidValves").ne(0) & pl.col("SolenoidValves").shift(1).eq(0)
    ).select(
        pl.col("UTC_DateTime").unique().alias("ValveOpen")
        )
valve_closed = data["Picarro_G2307"]["Logger"].filter(
    pl.col("SolenoidValves").ne(1) & pl.col("SolenoidValves").shift(1).eq(1)
    ).select(
        pl.col("UTC_DateTime").unique().alias("ValveClosed")
        )
# %% Identifies starts and stops of UZA measurements
uza_starts = {inst: {} for inst in data.keys()}
uza_stops = {inst: {} for inst in data.keys()}
for inst, sources in data.items():
    for source, df in sources.items():
        # First time column
        time_col = df.columns[0]
        # Identifies points where d/dt is more than 1.5 IQRs below/above the
        # lower/upper quantile
        outliers = df.drop_nulls().with_columns(
            pl.col("ddt").rolling_quantile_by(by=time_col,
                                              window_size="360m",
                                              quantile=0.25).alias("lq"),
            pl.col("ddt").rolling_quantile_by(by=time_col,
                                              window_size="360m",
                                              quantile=0.75).alias("uq"),
            ).with_columns(
                pl.col("lq").sub((pl.col("uq").sub(pl.col("lq"))).mul(3))
                .alias("llim"),
                pl.col("lq").add((pl.col("uq").sub(pl.col("lq"))).mul(3))
                .alias("ulim")
                ).with_columns(
                    pl.when(
                        pl.col("ddt").is_between(pl.col("llim"),
                                                 pl.col("ulim"))
                        )
                    .then(False)
                    .otherwise(True)
                    .alias("Outlier")
                    ).with_columns(
                        # Keeps only the first of consecutive outliers
                        pl.when(
                            pl.col("Outlier") & pl.col("Outlier").shift(1)
                            )
                        .then(False)
                        .otherwise(pl.col("Outlier"))
                        .alias("Outlier")
                        ).filter(
                           pl.col("Outlier")
                           )
        var_col = [col for col in df.columns if col in ["O3_ppb", "NO2_ppb", "CH2O_ppb"]][0]

                    
        if inst == "Picarro_G2307":
            strat = "forward"
        else:
            strat = "forward"
        # Identifies which outliers correspond to the first/last UZA
        # measurement for each valve opening/closing
        uza_starts[inst][source] = valve_open.join_asof(
            outliers,
            left_on="ValveOpen",
            right_on=time_col,
            coalesce=False,
            strategy=strat,
            tolerance="10m"
            ).drop_nulls(time_col).select(
                pl.col("ValveOpen"),
                # Adds instrument to time columns for later joining
                cs.contains("TC").name.map(
                        lambda name: name + "_" + inst
                        )
                )
        uza_stops[inst][source] = valve_closed.join_asof(
            outliers,
            left_on="ValveClosed",
            right_on=time_col,
            coalesce=False,
            strategy=strat,
            tolerance="10m"
            ).drop_nulls(time_col).select(
                pl.col("ValveClosed"), 
                # Adds instrument to time columns for later joining
                cs.contains("TC").name.map(
                        lambda name: name + "_" + inst
                        )
                )
                
# Joins the UZA measurement starts/stops for all instruments
uza_starts_joined = uza_starts["Picarro_G2307"]["Logger"].join(
    uza_starts["2BTech_205_A"]["SD"],
    on="ValveOpen",
    how="full",
    coalesce=True
    ).join(
        uza_starts["ThermoScientific_42i-TL"]["Logger"],
        on="ValveOpen",
        how="full",
        coalesce=True
        )
uza_stops_joined = uza_stops["Picarro_G2307"]["Logger"].join(
     uza_stops["2BTech_205_A"]["SD"],
     on="ValveClosed",
     how="full",
     coalesce=True
     ).join(
         uza_stops["ThermoScientific_42i-TL"]["Logger"],
         on="ValveClosed",
         how="full",
         coalesce=True
         )                   

# %% Partitions data by week
all_weeks = []
for inst, sources in data.items():
    for source, df in sources.items():
        # Partitions data by week
        by_week = {
            key[0]: df for key, df in df.partition_by(
                    "Week", as_dict=True, include_key=False
                    ).items()
                    }
        all_weeks += list(by_week.keys())
        data[inst][source] = by_week
        
all_weeks = list(set(all_weeks))
all_weeks.sort()

uza_starts_joined = {
    key[0]: df for key, df in uza_starts_joined.with_columns(
        pl.col("ValveOpen")
        .dt.convert_time_zone("America/Denver")
        .dt.week()
        .alias("Week")
        ).partition_by(
            "Week", as_dict=True, include_key=False
            ).items()
            }
uza_stops_joined = {
    key[0]: df for key, df in uza_stops_joined.with_columns(
        pl.col("ValveClosed")
        .dt.convert_time_zone("America/Denver")
        .dt.week()
        .alias("Week")
        ).partition_by(
            "Week", as_dict=True, include_key=False
            ).items()
            }
# %% Plotting

for week in all_weeks:
    week_plots = []
    if week < 23 or week > 33:
        continue
    if week in data["2BTech_205_A"]["SD"].keys():
        week_plots.append(
            data["2BTech_205_A"]["SD"][week].hvplot.scatter(
                x="FTC_Start",
                y="O3_ppb",
                shared_axes=True
                )
            )
    if week in data["Picarro_G2307"]["Logger"].keys():
        week_plots.append(
            data["Picarro_G2307"]["Logger"][week].hvplot.scatter(
                x="FTC_DateTime",
                y="CH2O_ppb",
                shared_axes=True
                )
            )
    for i, plot in enumerate(week_plots):
        if i == 0:
            week_plot = plot
        else:
            week_plot = week_plot * plot
    week_uza_starts = uza_starts_joined[week].with_columns(
        pl.col("ValveOpen").dt.convert_time_zone("America/Denver")
        .alias("FTC_Start")
        )
    week_uza_stops = uza_stops_joined[week].with_columns(
        pl.col("ValveClosed").dt.convert_time_zone("America/Denver")
        .alias("FTC_Start")
        )
    
    week_uza_start_diffs = week_uza_starts.select(
        pl.col("FTC_Start"),
        (cs.contains("2BTech") & cs.contains("FTC"))
        .sub(pl.col("FTC_DateTime_Picarro_G2307"))
        .dt.total_microseconds().truediv(1e6).name.map(lambda name: name.replace("2BTech_205_A", "Diff"))
        ).drop_nulls(cs.contains("_Diff"))
    week_uza_stop_diffs = week_uza_stops.select(
        pl.col("FTC_Start"),
        (cs.contains("2BTech") & cs.contains("FTC"))
        .sub(pl.col("FTC_DateTime_Picarro_G2307"))
        .dt.total_microseconds().truediv(1e6).name.map(lambda name: name.replace("2BTech_205_A", "Diff"))
        ).drop_nulls(cs.contains("_Diff"))
    
    
    diff_plot = week_uza_start_diffs.hvplot.scatter(
                x="FTC_Start",
                y=["FTC_Start_Diff", "FTC_Stop_Diff"],
                shared_axes=True
                ) * week_uza_stop_diffs.hvplot.scatter(
                            x="FTC_Start",
                            y=["FTC_Start_Diff", "FTC_Stop_Diff"],
                            shared_axes=True)
                
    hvplot.show(
        (week_plot + diff_plot).cols(1)
        )