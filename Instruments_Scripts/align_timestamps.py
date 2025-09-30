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
         "2BTech_405nm",
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
            
# %% Reads solenoid valve states
valve_states = pl.read_csv(
    os.path.join(
        data_dir,
        "Instruments_DerivedData",
        "Picarro_G2307_DerivedData",
        "Picarro_G2307_SolenoidValveStates.csv"
        )
    ).with_columns(
        pl.selectors.contains("UTC").str.to_datetime()
        )
valve_open = valve_states.filter(
    pl.col("SolenoidValves").eq(1)
    ).select("UTC_Start")
valve_closed = valve_states.filter(
    pl.col("SolenoidValves").eq(1)
    ).select("UTC_Stop")

# %% Data read-in and concatenation
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

#%%
all_weeks = []
uza_starts = {inst: {} for inst in data.keys()}
uza_stops = {inst: {} for inst in data.keys()}
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
            # measurement as determined by instrument and converts to real time passed
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
            
            diff = 9.476 / (datetime(2024, 6, 30, 23, 44, 1) - datetime(2024, 6, 25, 13, 44, 17)).total_seconds()
            lf = lf.with_columns(
                pl.lit(lf.select(pl.min("UTC_DateTime")).collect().item()).alias("RealTimestamp"),
                pl.col("UTC_DateTime").sub(pl.min("UTC_DateTime"))
                .dt.total_microseconds().truediv(1 + diff)
                .cast(pl.Int64).cast(pl.String).add("us")
                .alias("Passed")
                )
            lf = lf.with_columns(
                pl.col("RealTimestamp")
                .dt.offset_by(pl.col("Passed").alias("UTC_DateTime"))
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
        
        # Determines where d/dt is an outlier
        outliers = lf.drop_nulls().with_columns(
            pl.col("ddt").rolling_mean_by(
                (cs.contains("FTC") & ~cs.contains("Stop")),
                window_size="30m"
                ).alias("mean"),
            pl.col("ddt").rolling_std_by(
                (cs.contains("FTC") & ~cs.contains("Stop")),
                window_size="30m"
                ).alias("std"),
            ).filter(
                ~pl.col("ddt").is_between(
                    pl.col("mean").sub(pl.col("std").mul(2)),
                    pl.col("mean").add(pl.col("std").mul(2)))
                ).collect()
        # Strategy that will be used to select d/dt outliers that indicate
        # start/stop of zero measurement; Should be forward if timestamps are
        # aligned, but is backward if Picarro is too far ahead
        if inst == "Picarro_G2307":
            strategy = "forward"
        else:
            strategy = "backward"
        # First time column
        right_on = outliers.columns[0]
        # Determines time of first UZA measurement corresponding to each valve
        # opening
        uza_starts[inst][source] = valve_open.join_asof(
            outliers,
            left_on="UTC_Start",
            right_on=right_on,
            strategy=strategy,
            coalesce=False,
            tolerance="6m"
            ).drop_nulls()
        # Determines time of last UZA measurement corresponding to each valve
        # closing
        uza_stops[inst][source] = valve_closed.join_asof(
            outliers,
            left_on="UTC_Stop",
            right_on=right_on,
            strategy=strategy,
            coalesce=False,
            tolerance="6m"
            ).drop_nulls()
        # Partitions data by week
        by_week = {
            key[0]: df for key, df in lf.collect().partition_by(
                    "Week", as_dict=True, include_key=False
                    ).items()
                    }
        all_weeks += list(by_week.keys())
        data[inst][source] = by_week
        
all_weeks = list(set(all_weeks))
all_weeks.sort()

# %%
# Names the measurement start column by instrument
for inst, starts in uza_starts.items():
    for source, df in starts.items():
        uza_starts[inst][source] = df.select(
            pl.col("UTC_Start"),
            cs.contains("UTC_DateTime", "_right").alias(inst + "_UTC_Start"),
            cs.contains("UTC_Stop").alias(inst + "_UTC_Stop")
            )
for inst, stops in uza_stops.items():
    for source, df in stops.items():
        uza_stops[inst][source] = df.select(
            pl.col("UTC_Stop"),
            cs.contains("UTC_Start").alias(inst + "_UTC_Start"),
            cs.contains("UTC_DateTime", "_right").alias(inst + "_UTC_Stop")
            )
# Joins the UZA measurement starts/stops for all instruments
uza_starts_joined = uza_starts["Picarro_G2307"]["Logger"].join(
    uza_starts["ThermoScientific_42i-TL"]["Logger"],
    on="UTC_Start",
    how="full",
    coalesce=True
    ).join(
        uza_starts["2BTech_205_A"]["SD"],
        on="UTC_Start",
        how="full",
        coalesce=True
        )
uza_stops_joined = uza_stops["Picarro_G2307"]["Logger"].join(
    uza_stops["ThermoScientific_42i-TL"]["Logger"],
    on="UTC_Stop",
    how="full",
    coalesce=True
    ).join(
        uza_stops["2BTech_205_A"]["SD"],
        on="UTC_Stop",
        how="full",
        coalesce=True
        )

9.476 / (datetime(2024, 6, 30, 23, 44, 1) - datetime(2024, 6, 25, 13, 44, 17)).total_seconds()

for week in all_weeks:
    week_plots = []
    if week < 23 or week > 33: continue
    for inst, sources in data.items():
        if inst == "ThermoScientific_42i-TL": break
        for source, dfs in sources.items():
            if week not in dfs.keys(): continue
            df = dfs[week]
            cols = df.columns
            time_col = [col for col in cols if col.find("FTC") != -1][0]
            for var in ["O3_ppb", "NO2_ppb", "CH2O_ppb"]:
                if var in cols:
                    break
            if var not in cols:
                continue
            week_plots.append(
                df.hvplot.scatter(
                    x=time_col,
                    y=var,
                    ylim=(-10, 150),
                    shared_axes=True
                    )
                )
    for i, plot in enumerate(week_plots):
        if i == 0:
            week_plot = plot
        else:
            week_plot = week_plot * plot
            
    week_uza_starts = uza_starts_joined.with_columns(
        cs.contains("UTC").dt.convert_time_zone("America/Denver").name.map(lambda name: name.replace("UTC", "FTC"))
        ).filter(
            pl.col("FTC_Start").dt.week().eq(week)
            ).select(
                cs.contains("FTC")
                ).select(
                    pl.exclude("FTC_Start")
                    )
                    
    week_uza_stops = uza_stops_joined.with_columns(
         cs.contains("UTC").dt.convert_time_zone("America/Denver").name.map(lambda name: name.replace("UTC", "FTC"))
         ).filter(
             pl.col("FTC_Stop").dt.week().eq(week)
             ).select(
                 cs.contains("FTC")
                 ).select(
                     pl.exclude("FTC_Stop")
                     )
    diff_plot = week_uza_starts.with_columns(
        cs.contains("2BTech").sub(pl.col("Picarro_G2307_FTC_Start")).dt.total_microseconds().truediv(1e6).name.suffix("_Diff")
        ).rename(
            {"2BTech_205_A_FTC_Start": "FTC_Start"}).hvplot.scatter(
                x="FTC_Start",
                y=["2BTech_205_A_FTC_Start_Diff", "2BTech_205_A_FTC_Stop_Diff"],
                shared_axes=True
                ) * week_uza_stops.with_columns(
                    cs.contains("2BTech").sub(pl.col("Picarro_G2307_FTC_Stop")).dt.total_microseconds().truediv(1e6).name.suffix("_Diff")
                    ).rename(
                        {"2BTech_205_A_FTC_Start": "FTC_Start"}).hvplot.scatter(
                            x="FTC_Start",
                            y=["2BTech_205_A_FTC_Start_Diff", "2BTech_205_A_FTC_Stop_Diff"],
                            shared_axes=True)
                
    hvplot.show(
        (week_plot + diff_plot).cols(1)
        )


hvplot.show(
    uza_starts_joined.with_columns(
        cs.contains("2BTech").sub(pl.col("Picarro_G2307_UTC_Start"))
        ).hvplot.scatter(
        x="Picarro_G2307_UTC_Start",
        y=["2BTech_205_A_UTC_Start", "2BTech_205_A_UTC_Stop"]
        )
    )
uza_starts_joined.select(
    cs.contains("2BTech").sub(pl.col("Picarro_G2307_UTC_Start")).median()
    )
uza_starts_joined.with_columns(
    pl.exclude("UTC_Start").sub(pl.col("UTC_Start"))
    )
uza_stops_joined.with_columns(
    pl.exclude("UTC_Stop").sub(pl.col("UTC_Stop")).median()
    )
        
# %% Plotting

for week in all_weeks:
    week_plots = []
    ddt_plots = []
    if week < 23 or week > 33: continue
    # fig, ax = plt.subplots(figsize=(8, 6))
    # ax.set_title(str(week))
    for inst, sources in data.items():
        # if inst != "2BTech_205_A":
        #     continue
        for source, dfs in sources.items():
            if week not in dfs.keys():
                continue
            df = dfs[week]
            cols = df.columns
            time_col = [col for col in cols if col.find("FTC") != -1][0]
            for var in ["O3_ppb", "NO2_ppb", "CH2O_ppb"]:
                if var in cols:
                    break
            if var not in cols:
                continue
            
            
            week_uza_starts = uza_starts[inst][source].filter(
                pl.col(time_col).is_between(
                    df[time_col].min(), df[time_col].max()
                    )
                )
            week_uza_stops = uza_stops[inst][source].filter(
                pl.col(time_col).is_between(
                    df[time_col].min(), df[time_col].max()
                    )
                )
            week_plots.append(
                df.hvplot.scatter(
                    x=time_col,
                    y=var,
                    ylim=(-10, 150)
                    )
                )
            week_plots.append(
                week_uza_starts.hvplot.scatter(
                    x=time_col,
                    y=var
                    )
                )
            week_plots.append(
                week_uza_stops.hvplot.scatter(
                    x=time_col,
                    y=var
                    )
                )
            # ddt_plots.append(
            #     uza_start.hvplot.scatter(
            #         x=time_col,
            #         y="d" + var + "/dt"
            #         )
                # )
    for i, plot in enumerate(week_plots):
        if i == 0:
            week_plot = plot
        else:
            week_plot = week_plot * plot
    # for i, plot in enumerate(ddt_plots):
    #     if i == 0:
    #         ddt_plot = plot
    #     else:
    #         ddt_plot = ddt_plot * plot
    hvplot.show(week_plot)
    # hvplot.show((week_plot + ddt_plot).cols(1))
    # break
    #         ax.plot(
    #             df[time_col],
    #             df[var],
    #             label=var
    #             )
    # ax.legend()
    # ax.set_ylim(-10, 100)
            
            
