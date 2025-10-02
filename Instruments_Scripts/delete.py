# -*- coding: utf-8 -*-
"""
Created on Wed Aug 13 15:18:45 2025

@author: agsthk
"""



import os
import polars as pl
import pytz
from datetime import datetime
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
# Full path to directory containing all clean raw data
CLEAN_DATA_DIR = os.path.join(data_dir, "Instruments_CleanData")
# Creates Instruments_CleanData/ directory if needed
if not os.path.exists(CLEAN_DATA_DIR):
    os.makedirs(CLEAN_DATA_DIR)
# Full path to automated addition times
ADD_TIMES_PATH = os.path.join(data_dir, "Instruments_ManualData", "Instruments_ManualExperiments", "ManualAdditionTimes - Copy.csv")
add_times = pl.read_csv(ADD_TIMES_PATH).with_columns(
    pl.selectors.contains("UTC").str.to_datetime(time_zone="America/Denver").dt.convert_time_zone("UTC")
    )
add_times = {key[0]: df for key, df in 
             add_times.partition_by(
                 "Species", as_dict=True, include_key=False
                 ).items()}

AUTO_ADD_TIMES_PATH = os.path.join(data_dir, "Instruments_DerivedData", "AdditionValves_DerivedData", "AdditionValves_AutomatedAdditionTimes.csv")
auto_add_times = pl.read_csv(AUTO_ADD_TIMES_PATH).with_columns(
    pl.selectors.contains("UTC").str.to_datetime()
    )
auto_add_times = {key[0]: df for key, df in 
             auto_add_times.partition_by(
                 "Species", as_dict=True, include_key=False
                 ).items()}
for key, df in add_times.items():
    add_times[key] = pl.concat([df, auto_add_times[key]])

DOOR_STATUS_PATH = os.path.join(data_dir, "Instruments_DerivedData", "TempRHDoor_DerivedData", "TempRHDoor_DoorStatus.csv")
door_times = pl.read_csv(DOOR_STATUS_PATH).with_columns(
    pl.selectors.contains("UTC").str.to_datetime()
    )
door_times = {key[0]: df for key, df in 
             door_times.partition_by(
                 "DoorStatus", as_dict=True, include_key=False
                 ).items()}
valve_states = pl.read_csv(
    os.path.join(
        data_dir,
        "Instruments_DerivedData",
        "Picarro_G2307_DerivedData",
        "Picarro_G2307_SolenoidValveStates.csv"
        )
    ).with_columns(
        pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC").dt.convert_time_zone(time_zone="America/Denver")
        ).rename({"UTC_Start": "FTC_Start", "UTC_Stop": "FTC_Stop"})
        
insts = ["2BTech_202",
         "2BTech_205_A",
         "2BTech_205_B",
         "2BTech_405nm",
         # "Aranet4_1F16F",
         # "Aranet4_1FB20",
         # "LI-COR_LI-840A_A",
         # "LI-COR_LI-840A_B",
         "Picarro_G2307",
         # "TempRHDoor",
         "ThermoScientific_42i-TL"]

data = {inst: {} for inst in insts}

for root, dirs, files in tqdm(os.walk(CLEAN_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        if path.find("DAQ") != -1:
            continue
        for inst in insts:
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
      
        df = lf.collect()
        
        if df.is_empty():
            continue
        data[inst][file.rsplit("_", 1)[-1][:-4]] = df
        # _, source = file[:-17].split("_Structured")
        # f_name = inst + "_Clean" + source + "Data_" + path[-12:-4] + ".csv"
        # f_dir = os.path.join(CLEAN_DATA_DIR,
        #                      inst + "_CleanData",
        #                      inst + "_Clean" + source + "Data")
        # if not os.path.exists(f_dir):
        #     os.makedirs(f_dir)
        # path = os.path.join(f_dir,
        #                     f_name)
        # df.write_csv(path)
all_cols = []
for inst in insts:
    for df in data[inst].values():
        all_cols += df.columns
        break

# %% 
for date, df in data["2BTech_205_A"].items():
    if date.find("202406") == -1:
        continue
    date_plots = df.hvplot.scatter(
        x="UTC_Start",
        y="O3_ppb",
        by="SamplingLocation",
        title=date
        )
    hvplot.show(date_plots)

# %% Check separate datasets against each other
for date, df in data["2BTech_205_A"].items():
    date_plots = df.filter(
        pl.col("SamplingLocation").is_not_null()
        ).hvplot.scatter(
            x="FTC_Start",
            y="O3_ppb"
            )
    plot = False
    if date in data["Picarro_G2307"].keys():
        date_plots = date_plots * data["Picarro_G2307"][date].filter(
            pl.col("SamplingLocation").is_not_null()
            ).hvplot.scatter(
                x="FTC_DateTime",
                y="CH2O_ppb"
                )
        plot = True
    if date in data["ThermoScientific_42i-TL"].keys():
        date_plots = date_plots * data["ThermoScientific_42i-TL"][date].filter(
            pl.col("SamplingLocation").is_not_null()
            ).hvplot.scatter(
                x="FTC_Start",
                y="NO2_ppb"
                )
    if plot:
        hvplot.show(date_plots)
    

#%%
for inst, dfs in tqdm(data.items()):
    if inst != "2BTech_205_A": continue
    # if inst != "Picarro_G2307": continue
    for date, df in tqdm(dfs.items()):
        if "UTC_DateTime" in df.columns:
            tcol1 = tcol2 = "FTC_DateTime"
            join_tcol1 = "FTC_Start"
            join_tcol2 = "FTC_Stop"
            strat = "forward"
        else:
            tcol1 = "FTC_Start"
            tcol2 = "FTC_Stop"
            join_tcol1 = "FTC_Start_right"
            join_tcol2 = "FTC_Stop_right"
            strat = "backward"

        var = "O3_ppb"
        # var = "CH2O_ppb"
        if var not in df.columns:
            continue
        if date.find("202501") == -1:
            continue
        df = df.filter(
            pl.col(var).is_between(-20, 300)
            ).with_columns(
                pl.col(var).sub(pl.col(var).shift(1)).alias("d"),
                pl.col(tcol1).sub(pl.col(tcol1).shift(1)).alias("dt")
                ).with_columns(
                    pl.col("d")
                    .truediv(pl.col("dt").dt.total_microseconds())
                    .mul(1e6)
                    .alias("d/dt")
                    ).drop_nulls().with_columns(
                        pl.col("d").rolling_mean_by(by=tcol1, window_size="360m").alias("mean"),
                        pl.col("d").rolling_std_by(by=tcol1, window_size="360m")
                        .alias("std")
                        ).with_columns(
                            pl.col("mean").add(pl.col("std").mul(2)).alias("ulim"),
                            pl.col("mean").sub(pl.col("std").mul(2)).alias("llim")
                            )

        df = df.with_columns(
            pl.when(
                pl.col("d").is_between(pl.col("llim"), pl.col("ulim"))
                )
            .then(pl.lit(False))
            .otherwise(pl.lit(True))
            .alias("Outlier")
            )
        
        if tcol1 != tcol2:
            df = df.with_columns(
                pl.col(tcol2).sub(pl.col(tcol1)).truediv(2).alias("t_Unc")
                ).with_columns(
                    pl.col(tcol1).add(pl.col("t_Unc")).alias("FTC_Mid")
                    )
            tcolplot = "FTC_Mid"
        else:
            tcolplot = tcol1
        
        tmin = df[tcol1].min()
        tmax = df[tcol2].max()
        date_valve_states = valve_states.filter(
            pl.col("FTC_Stop").is_between(tmin, tmax)
            | pl.col("FTC_Start").is_between(tmin, tmax)
            )
        uza_starts = date_valve_states.filter(pl.col("SolenoidValves").eq(1)).select(pl.col("FTC_Start").alias("UZA_Start"))
        uza_stops = date_valve_states.filter(pl.col("SolenoidValves").eq(1)).select(pl.col("FTC_Stop").alias("UZA_Stop"))
        
        outliers = df.filter(
            pl.col("Outlier")
            ).filter(
                pl.col(tcol1).sub(pl.col(tcol1).shift(1)).dt.total_seconds().gt(120))
        
        uza_starts = uza_starts.join_asof(
            outliers,
            left_on="UZA_Start",
            right_on=tcol1,
            strategy="backward",
            coalesce=False
            ).with_columns(
                pl.col(tcol1).sub(pl.col("UZA_Start")).dt.total_microseconds().truediv(1e6).sub(15).alias("l1"),
                pl.col(tcol2).sub(pl.col("UZA_Start")).dt.total_microseconds().truediv(1e6).sub(15).alias("l2"),
                )
        uza_stops = uza_stops.join_asof(
            outliers,
            left_on="UZA_Stop",
            right_on=tcol1,
            strategy="backward",
            coalesce=False
            ).with_columns(
                pl.col(tcol1).sub(pl.col("UZA_Stop")).dt.total_microseconds().truediv(1e6).sub(15).alias("l1"),
                pl.col(tcol2).sub(pl.col("UZA_Stop")).dt.total_microseconds().truediv(1e6).sub(15).alias("l2"),
                )
        
        uza_starts = uza_starts.with_columns(pl.col("UZA_Start").dt.replace_time_zone(None))
        uza_stops = uza_stops.with_columns(pl.col("UZA_Stop").dt.replace_time_zone(None))
        # if uza_starts.is_empty(): continue
        scatter = df.hvplot.scatter(
            x=var,
            y=tcolplot,
            title=date,
            width=1800,
            height=500,
            invert=True,
            s=100,
            by="SamplingLocation"
            )
        scatter2 = outliers.hvplot.scatter(
            x=var,
            y=tcolplot,
            invert=True,
            s=100)
        lines = hv.HLines(uza_starts["UZA_Start"]) * hv.HLines(uza_stops["UZA_Stop"])
        # line = df.hvplot.line(
        #     x="d/dt",
        #     y=tcolplot,
        #     invert=True)
        gap1 = uza_starts.hvplot.scatter(
            x="l1",
            y=tcolplot,
            width=1800,
            height=300,
            invert=True,
            s=100)
        gapx = uza_starts.with_columns(
            )
        gap2 = uza_starts.hvplot.scatter(
            x="l2",
            y=tcolplot,
            width=1800,
            height=300,
            invert=True,
            s=100)
        gap3 = uza_stops.hvplot.scatter(
            x="l1",
            y=tcolplot,
            width=1800,
            height=300,
            invert=True,
            s=100)
        gap4 = uza_stops.hvplot.scatter(
            x="l2",
            y=tcolplot,
            width=900,
            height=300,
            invert=True,
            s=100)
        gap5 = uza_starts.hvplot.scatter(
            x="l1",
            y="UZA_Start",
            width=1800,
            height=300,
            invert=True,
            s=100)
        gap6 = uza_starts.hvplot.scatter(
            x="l2",
            y="UZA_Start",
            width=1800,
            height=300,
            invert=True,
            s=100)
        gap7 = uza_stops.hvplot.scatter(
            x="l1",
            y="UZA_Stop",
            width=1800,
            height=300,
            invert=True,
            s=100)
        gap8 = uza_stops.hvplot.scatter(
            x="l2",
            y="UZA_Stop",
            width=1800,
            height=300,
            invert=True,
            s=100)
        gaps = gap1 * gap2 * gap3 * gap4 * gap5 * gap6 * gap7 * gap8
        if tcolplot != tcol1:
            errorbars = df.hvplot.errorbars(
                x=var,
                y=tcolplot,
                yerr1="t_Unc",
                invert=True
                )
            plot = (scatter * scatter2 * errorbars * lines) + gaps
        else:
            plot = (scatter * lines) + gaps
            
        hvplot.show(plot.cols(1))

#%%
# for inst, dfs in tqdm(data.items()):
#     if inst != "2BTech_205_A": continue
#     for date, df in tqdm(dfs.items()):
#         if "UTC_DateTime" in df.columns:
#             tcol1 = tcol2 = "FTC_DateTime"
#             join_tcol1 = "FTC_Start"
#             join_tcol2 = "FTC_Stop"
#             strat = "forward"
#         else:
#             tcol1 = "FTC_Start"
#             tcol2 = "FTC_Stop"
#             join_tcol1 = "FTC_Start_right"
#             join_tcol2 = "FTC_Stop_right"
#             strat = "backward"
#         # if date not in ["20240117", "20240201", "20240206", "20240224", "20240415", "20240625", "20240625", "20240702", "20240702", "20241216", "20241217"]:
#         #     continue
#         # if date in ["20231209", "20240119", "20240327", "20240422", "20240502", "20240605", "20240606", "20240607", "20240610", "20240611"]: continue
#         # ["O3_ppb", "CO2_ppm", "RoomRH_percent", "H2O_ppt",
#         #             "NO_ppb", "NO2_ppb", "NOx_ppb", "CH2O_ppb", "CH4_ppm",
#         #             "H20_percent", "DoorStatus", "RoomTemp_C", "SolenoidValves",
#         #             "CellTemp_C", "CellPressure_mbar", "SampleFlow_ccm",
#         #             "O3Flow_ccm", "PhotodiodeVoltage_V", "O3Voltage_V",
#         #             "ScrubberTemp_C", "AlarmStatus", "InstrumentStatus",
#         #             "CavityPressure_Torr", "CavityTemp_C", "DASTemp_C",
#         #             "EtalonTemp_C", "WarmBoxTemp_C", "Species",
#         #             "MPVPosition", "OutletValve_DN", "SolenoidValves",
#         #             "ChamberPressure_mmHg", "PMTTemp_C", "InternalTemp_C",
#         #             "ChamberTemp_C", "NO2ConverterTemp_C", "SampleFlow_LPM",
#         #             "O3Flow_LPM", "PMTVoltage_V"]:
#         var = "O3_ppb"
#         if var not in df.columns:
#             continue
#         df = df.with_columns(
#             pl.col(var).sub(pl.col(var).shift(1)).alias("d")
#             )
#         med = df["d"].median()
#         iqr = df["d"].quantile(0.75) - df["d"].quantile(0.25)
#         llim = med - 1.5 * iqr
#         ulim = med + 1.5 * iqr
        
#         uza_meas = df.filter(
#             ~pl.col("d").is_between(llim, ulim)
#             )
#         uza_meas = uza_meas.filter(
#             pl.col(tcol1).sub(pl.col(tcol1).shift(1)).dt.total_seconds().gt(65)
#             )
#         uza_meas_start = uza_meas.filter(
#             pl.col("d").lt(0)
#             )
#         uza_meas_stop = uza_meas.filter(
#             pl.col("d").gt(0)
#             )
        
#         tmin = df[tcol1].min()
#         tmax = df[tcol2].max()
#         date_valve_states = valve_states.filter(
#             pl.col("FTC_Stop").is_between(tmin, tmax)
#             | pl.col("FTC_Start").is_between(tmin, tmax)
#             )
        
#         uza_starts = date_valve_states.filter(pl.col("SolenoidValves").eq(1)).select(pl.col("FTC_Start"))
#         uza_stops = date_valve_states.filter(pl.col("SolenoidValves").eq(1)).select(pl.col("FTC_Stop"))
        
#         uza_meas_start = uza_starts.join_asof(
#             uza_meas_start,
#             left_on="FTC_Start",
#             right_on=tcol1,
#             strategy=strat,
#             coalesce=False
#             ).with_columns(
#                 pl.col("FTC_Start").sub(pl.col(join_tcol1)).dt.total_microseconds().truediv(1e6).add(15).alias("ulim"),
#                 pl.col("FTC_Start").sub(pl.col(tcol2)).dt.total_microseconds().truediv(1e6).alias("llim")
#                 )
                
#         uza_meas_stop = uza_stops.join_asof(
#             uza_meas_stop,
#             left_on="FTC_Stop",
#             right_on=tcol2,
#             strategy=strat,
#             coalesce=False
#             ).with_columns(
#                 pl.col("FTC_Stop").sub(pl.col(tcol1)).dt.total_microseconds().truediv(1e6).add(15).alias("ulim"),
#                 pl.col("FTC_Stop").sub(pl.col(join_tcol2)).dt.total_microseconds().truediv(1e6).alias("llim")
#                 )
        
#         # uza_meas_start = uza_meas_start.join_asof(
#         #     uza_starts,
#         #     left_on=tcol1,
#         #     right_on="FTC_Start",
#         #     strategy=strat,
#         #     coalesce=False
#         #     ).select(
#         #         pl.selectors.contains("FTC")
#         #         ).rename(
#         #             {join_tcol1: "UZA_Start"}
#         #             ).with_columns(
#         #                 pl.col("UZA_Start").sub(pl.col(tcol1)).dt.total_microseconds().truediv(1e6).alias("ulim"),
#         #                 pl.col("UZA_Start").sub(pl.col(tcol2)).dt.total_microseconds().truediv(1e6).alias("llim")
#         #                 ).filter(
#         #                     pl.col("ulim").lt(300)
#         #                     & pl.col("llim").lt(300)
#         #                     )
#         # uza_meas_stop = uza_meas_stop.join_asof(
#         #     uza_stops,
#         #     left_on=tcol1,
#         #     right_on="FTC_Stop",
#         #     strategy="forward",
#         #     coalesce=False
#         #     ).select(
#         #         pl.selectors.contains("FTC")
#         #         ).rename(
#         #             {join_tcol2: "UZA_Stop"}
#         #             ).with_columns(
#         #                 pl.col("UZA_Stop").sub(pl.col(tcol1)).dt.total_microseconds().truediv(1e6).alias("ulim"),
#         #                 pl.col("UZA_Stop").sub(pl.col(tcol2)).dt.total_microseconds().truediv(1e6).alias("llim")
#         #                 ).filter(
#         #                     pl.col("ulim").lt(300)
#         #                     & pl.col("llim").lt(300)
#         #                     )
#         uza_starts = uza_starts.with_columns(pl.col("FTC_Start").dt.replace_time_zone(None))
#         uza_stops = uza_stops.with_columns(pl.col("FTC_Stop").dt.replace_time_zone(None))
#         plot = uza_meas_start.hvplot.scatter(
#             x=join_tcol1,
#             y="llim",
#             title=date,
#             width=900,
#             height=500,
#             ylim=(0, 150))
#         plot2 = uza_meas_start.hvplot.scatter(
#             x=join_tcol1,
#             y="ulim",
#             title=date,
#             width=900,
#             height=500)
#         plot3 = uza_meas_stop.hvplot.scatter(
#             x=tcol1,
#             y="llim",
#             title=date,
#             width=900,
#             height=500)
#         plot4 = uza_meas_stop.hvplot.scatter(
#             x=tcol1,
#             y="ulim",
#             title=date,
#             width=900,
#             height=500)
#         plot5 = df.hvplot.line(
#             x=tcol1,
#             y=var,
#             # by="outlier",
#             title=date,
#             width=900,
#             height=500,
#             ylim=(None, 120)
#             )
#         if not date_valve_states.is_empty():
#             plot6 = hv.VLines(uza_starts)
#             plot7 = hv.VLines(uza_stops)
#             # hvplot.show(plot5 * plot6 * plot7 * plot * plot2 * plot3 * plot4)
#             hvplot.show(plot * plot2 * plot3 * plot4)
#             # hvplot.show((plot5 * plot * plot2 * plot3 * plot4).opts(multi_y=True))


        # if not date_valve_states.is_empty():
        #     plot5 = hv.VLines(uza_starts)
        #     plot6 = hv.VLines(uza_stops)
        #     hvplot.show(plot * plot2 * plot3 * plot4 * plot5 * plot6)
        #     break
        # if date in data["Picarro_G2307"].keys():
        #     plot2 = data["Picarro_G2307"][date].hvplot.scatter(
        #         x="FTC_DateTime",
        #         y="SolenoidValves")
        #     hvplot.show((plot * plot2))
        #     break
        # else:
        #     continue
        #     hvplot.show(plot)
        # break

            # if "WarmUp" in df.columns:
            #     warm = df.filter(pl.col("WarmUp").eq(0))
            #     not_warm = df.filter(pl.col("WarmUp").ne(0))
            #     fig, ax = plt.subplots(figsize=(6, 4))
            #     ax.scatter(
            #         not_warm[tcol1],
            #         not_warm[var],
            #         color="#D9782D",
            #         zorder=3
            #         )
            #     ax.plot(
            #         warm[tcol1],
            #         warm[var],
            #         color="#1E4D2B",
            #         zorder=5
            #         )
                
            #     plot = df.hvplot.line(
            #         x=tcol1,
            #         y=var,
            #         title=date
            #         )
            #     hvplot.show(plot)
                
            # else:
            #     fig, ax = plt.subplots(figsize=(6, 4))
            #     ax.plot(
            #         df[tcol1],
            #         df[var],
            #         color="#D9782D",
            #         zorder=5
            #         )
            # low = df[var].min() - 5
            # up = df[var].max() + 5
            # if up > 200:
            #     up = 200
            # if low < -10:
            #     low = -10
            # ax.set_ylim(low, up)
            # ax.set_ylabel(var)
            # ax.xaxis.set_major_locator(
            #     mdates.AutoDateLocator(tz=pytz.timezone("America/Denver"))
            #     )
            # # ax.xaxis.set_minor_locator(
            # #     mdates.HourLocator(interval=1, tz=pytz.timezone("America/Denver"))
            # #     )
            # ax.xaxis.set_major_formatter(
            #     mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
            #     )
            # ax.tick_params(axis="x", labelrotation=90)
            # ax.tick_params(axis="y", color="#1E4D2B")
            # ax.set_title(date)
            # ax.grid(zorder=0)
            # ax.grid(which="minor", linestyle=":", zorder=0)
            # ax.set_title(inst + " " + var + " " + date)
    
    # # if datetime.strptime(date,"%Y%m%d") > datetime(2024, 8, 15):
    # #     continue
    # # if date not in ["20240311", "20240416", "20240417", "20240509", "20240514",
    # #                 "20240604", "20240605", "20240606", "20240610", "20240612",
    # #                 "20240617", "20240620", "20240625", "20240713"]:
    # #     continue
    # datetime.strptime(date,"%Y%m%d")
    # if "UTC_DateTime" in df.columns:
    #     tcol1 = tcol2 = "UTC_DateTime"
    # else:
    #     tcol1 = "UTC_Start"
    #     tcol2 = "UTC_Stop"
        
    # if "O3_ppb" in df.columns:
    #     var = "O3_ppb"
    # else:
    #     var = "CO2_ppm"
    # tmin = df[tcol1].min()
    # tmax = df[tcol2].max()
    # date_adds = add_times[var.split("_")[0]].filter(
    #     pl.col("UTC_Start").is_between(tmin, tmax)
    #     | pl.col("UTC_Stop").is_between(tmin, tmax))
    # date_doors = door_times[1].filter(
    #     pl.col("UTC_Start").is_between(tmin, tmax)
    #     | pl.col("UTC_Stop").is_between(tmin, tmax))
    # date_bounds = date_adds.with_columns(
    #     pl.col("UTC_Start").dt.offset_by("-5m").alias("Start_Bound"),
    #     pl.col("UTC_Stop").dt.offset_by("15m").alias("Stop_Bound")
    #     )
    # for row in date_bounds.iter_rows(named=True):
    #     temp_df = df.filter(
    #         pl.col(tcol1).is_between(row["Start_Bound"], row["Stop_Bound"])
    #         | pl.col(tcol2).is_between(row["Start_Bound"], row["Stop_Bound"])
    #         )
    #     fig, ax = plt.subplots(figsize=(6, 4))
    #     ax.plot(
    #         df[tcol1],
    #         df[var],
    #         color="#D9782D",
    #         zorder=5
    #         )
    #     # ax.set_ylim(0, 5000)
    #     low = temp_df[var].min() - 5
    #     up = temp_df[var].max() + 5
    #     if up > 200:
    #         up = 200
    #     if low < -10:
    #         low = -10
    #     ax.set_ylim(low, up)
    #     ax.set_xlim(row["Start_Bound"], row["Stop_Bound"])
    #     ax.set_ylabel(var)
    #     ax.vlines(row["UTC_Start"], low, up, color="#1E4D2B", zorder=10)
    #     ax.vlines(row["UTC_Stop"], low, up, color="#1E4D2B", zorder=10)
    #     # ax.vlines(date_doors["UTC_Start"], 400, 3000, color="gray", zorder=1)
    #     # ax.vlines(date_doors["UTC_Stop"], 400, 3000, color="gray", zorder=1)
    #     ax.xaxis.set_major_locator(
    #         mdates.AutoDateLocator(tz=pytz.timezone("America/Denver"))
    #         )
    #     # ax.xaxis.set_minor_locator(
    #     #     mdates.HourLocator(interval=1, tz=pytz.timezone("America/Denver"))
    #     #     )
    #     ax.xaxis.set_major_formatter(
    #         mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
    #         )
    #     ax.tick_params(axis="x", labelrotation=90)
    #     ax.tick_params(axis="y", color="#1E4D2B")
    #     ax.set_title(date)
    #     ax.grid(zorder=0)
    #     ax.grid(which="minor", linestyle=":", zorder=0)

