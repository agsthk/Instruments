# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 14:43:32 2025

@author: agsthk
"""

import os
import polars as pl
import pytz
from datetime import datetime, timedelta
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import hvplot.polars
import holoviews as hv
import polars.selectors as cs

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all structured raw data
CAL_DATA_DIR = os.path.join(data_dir, "Instruments_CalibratedData")
# Full path to automated addition times
ADD_TIMES_PATH = os.path.join(data_dir, "Instruments_ManualData", "Instruments_ManualExperiments", "ManualAdditionTimes - Copy.csv")
add_times = pl.read_csv(ADD_TIMES_PATH).with_columns(
    pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC")
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
    add_times[key] = pl.concat([df, auto_add_times[key]]).sort(by="UTC_Start")

DOOR_STATUS_PATH = os.path.join(data_dir, "Instruments_DerivedData", "TempRHDoor_DerivedData", "TempRHDoor_DoorStatus.csv")
door_times = pl.read_csv(DOOR_STATUS_PATH).with_columns(
    pl.selectors.contains("UTC").str.to_datetime()
    )
door_times = {key[0]: df for key, df in 
             door_times.partition_by(
                 "DoorStatus", as_dict=True, include_key=False
                 ).items()}
# %% Calibrated 2024 data

cal_dates_2024 = {"2BTech_205_A": "20240604",
                  "2BTech_205_B": "20240604",
                  "Picarro_G2307": "20250625",
                  "ThermoScientific_42i-TL": "20240708"}

cal_dates_2025 = {"2BTech_205_A": "20250115",
                  "2BTech_205_B": "20250115",
                  "Picarro_G2307": "20250625",
                  "ThermoScientific_42i-TL": "20241216"}

data_2024 = {inst: [] for inst in cal_dates_2024.keys()}
data_2025 = {inst: [] for inst in cal_dates_2025.keys()}

for root, dirs, files in tqdm(os.walk(CAL_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        for inst in cal_dates_2024.keys():
            if path.find(inst) != -1:
                break
        if path.find(inst) == -1:
            continue
        
        if path.find("DAQ") != -1:
            if path.find(cal_dates_2025[inst] + "Calibration") != -1:
                data_2025[inst].append(
                    pl.scan_csv(path)
                    )
        else:
            if path.find(cal_dates_2024[inst] + "Calibration") != -1:
                data_2024[inst].append(
                    pl.scan_csv(path)
                    )
            
for inst, lfs in data_2024.items():
    data_2024[inst] = {key[0]: df for key, df in pl.concat(lfs).with_columns(
            cs.contains("UTC").str.to_datetime(time_zone="UTC"),
            cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
            ).sort(
                by=cs.contains("UTC")
                ).with_columns(
                    (cs.contains("FTC") & ~cs.contains("Stop"))
                    .dt.week().alias("Week")
                    ).collect().partition_by(
                        by="Week", include_key=False, as_dict=True
                        ).items()}

                    
for inst, lfs in data_2025.items():
    data_2025[inst] = {key[0]: df for key, df in pl.concat(lfs).with_columns(
            cs.contains("UTC").str.to_datetime(time_zone="UTC"),
            cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
            ).sort(
                by=cs.contains("UTC")
                ).with_columns(
                    (cs.contains("FTC") & ~cs.contains("Stop"))
                    .dt.week().alias("Week")
                    ).collect().partition_by(
                        by="Week", include_key=False, as_dict=True
                        ).items()}
# %%

data_2025["2BTech_205_A_BG"] = {}

for week, df in data_2025["2BTech_205_A"].items():
    week_start = df["UTC_Start"].min()
    week_stop = df["UTC_Stop"].max()
    week_o3_adds = add_times["O3"].filter(
        pl.col("UTC_Stop").ge(week_start)
        & pl.col("UTC_Start").le(week_stop)
        ).with_columns(
            pl.col("UTC_Stop").dt.offset_by("2h")
            )
    if not week_o3_adds.is_empty():
        data_2025["2BTech_205_A_BG"][week] = df.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).join_asof(
                week_o3_adds,
                on="UTC_Start",
                strategy="backward",
                suffix="_Add"
                ).with_columns(
                    pl.when(
                        pl.col("UTC_Start").le(pl.col("UTC_Stop_Add"))
                        )
                    .then(pl.lit(None))
                    .otherwise(pl.col("O3_ppb"))
                    .alias("O3_ppb")
                    ).select(
                        pl.exclude("UTC_Stop_Add")
                        )

data_2025["2BTech_205_A_BG"][15]
# %%

for week, df in data_2025["2BTech_205_A_BG"].items():
    
    bg_o3 = df.filter(
        pl.col("O3_ppb").ge(pl.col("O3_ppb_LOD"))
        ).group_by_dynamic(
            "FTC_Start", every="10m"
            ).agg(
                pl.col("O3_ppb").mean()
                )
    
    plot = df.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).with_columns(
                pl.col("O3_ppb").interpolate_by("FTC_Start")
                ).hvplot.scatter(
                    x="FTC_Start",
                    y="O3_ppb",
                    title=str(week)
                    )
    bg_sub = data_2025["2BTech_205_A"][week].join(
        df.with_columns(
            pl.col("O3_ppb").interpolate_by("FTC_Start")
            ),
        on="FTC_Start",
        suffix="_Background"
        ).with_columns(
            pl.col("O3_ppb").sub(pl.col("O3_ppb_Background")).alias("BG_Sub_O3_ppb")
            )
    plot = plot * bg_sub.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).hvplot.scatter(
                x="FTC_Start",
                y="BG_Sub_O3_ppb"
                )
    if week in data_2025["2BTech_205_B"].keys():
        vent_o3 = data_2025["2BTech_205_B"][week].filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).group_by_dynamic(
                "FTC_Start", every="10m"
                ).agg(
                    pl.col("O3_ppb").mean()
                    )
        io_o3 = bg_o3.join(
            vent_o3,
            on="FTC_Start",
            suffix="_Vent"
            ).with_columns(
                pl.col("O3_ppb").truediv(pl.col("O3_ppb_Vent")).alias("IO")
                )
        # plot = ((plot * data_2025["2BTech_205_B"][week].filter(
        #     pl.col("SamplingLocation").str.contains("C200")
        #     ).hvplot.scatter(
        #         x="FTC_Start",
        #         y="O3_ppb",
        #         title=str(week)
        #         )) + io_o3.hvplot.scatter(
        #             x="FTC_Start",
        #             y="IO"
        #             )).cols(1)
    if week in data_2025["ThermoScientific_42i-TL"].keys():
        plot = (plot + data_2025["ThermoScientific_42i-TL"][week].filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).hvplot.scatter(
                x="FTC_Start",
                y=["NO_ppb", "NO2_ppb"]
                )
                ).cols(1)
        
    hvplot.show(plot)
    
# %% Identifies additions under normal NOx just before and after leak fixed
leak_fixed = datetime(2025, 1, 27, 12, 0, 0, tzinfo=pytz.timezone("America/Denver"))
pre_leak_adds = []
post_leak_adds = []
for add in add_times["O3"]["UTC_Start"]:
    if add < leak_fixed:
        pre_leak_adds.append(add)
    else:
        post_leak_adds.append(add)
pre_leak_adds.sort()
post_leak_adds.sort()
pre_leak_adds = pre_leak_adds[-6:-1]
post_leak_adds = post_leak_adds[0:2] + [post_leak_adds[3]]
# %%
pre_leak_conds = []
post_leak_conds = []
pre_plots = []
post_plots = []
for week, df in data_2025["2BTech_205_A"].items():
    week_start = df["UTC_Start"].min()
    week_stop = df["UTC_Stop"].max()
    if week_stop < pre_leak_adds[0] or week_start > post_leak_adds[-1]:
        continue
    for add in pre_leak_adds:
        pre_plot = df.filter(
            pl.col("UTC_Stop").ge(add - timedelta(minutes=5))
            & pl.col("UTC_Start").le(add + timedelta(hours=2))
            ).with_columns(
                pl.col("UTC_Start").sub(add).dt.total_microseconds().truediv(1e6)
                )
        pre_plots.append(
            pre_plot.hvplot.scatter(
                x="UTC_Start",
                y="O3_ppb",
                xlabel="Seconds from addition start",
                ylabel="[O3] (ppb)",
                title="Before leak fixed",
                width=400,
                height=600,
                grid=True
                )
            )
        pre_add = df.filter(
            pl.col("UTC_Stop").ge(add - timedelta(minutes=5))
            & pl.col("UTC_Start").le(add)
            )
        if pre_add.is_empty():
            continue
        pre_conc = pre_add["O3_ppb"].mean()
        post_add = df.filter(
            pl.col("UTC_Stop").ge(add)
            & pl.col("UTC_Start").le(add + timedelta(minutes=10))
            )
        post_conc = post_add["O3_ppb"].max()
        pre_leak_conds.append([pre_conc, post_conc, post_conc - pre_conc])
    for add in post_leak_adds:
        post_plot = df.filter(
            pl.col("UTC_Stop").ge(add - timedelta(minutes=5))
            & pl.col("UTC_Start").le(add + timedelta(hours=2))
            ).with_columns(
                pl.col("UTC_Start").sub(add).dt.total_microseconds().truediv(1e6)
                )
        post_plots.append(
            post_plot.hvplot.scatter(
                x="UTC_Start",
                y="O3_ppb",
                xlabel="Seconds from addition start",
                ylabel="[O3] (ppb)",
                title="After leak fixed",
                width=400,
                height=600,
                grid=True
                )
            )
        pre_add = df.filter(
            pl.col("UTC_Stop").ge(add - timedelta(minutes=5))
            & pl.col("UTC_Start").le(add)
            )
        if pre_add.is_empty():
            continue
        pre_conc = pre_add["O3_ppb"].mean()
        post_add = df.filter(
            pl.col("UTC_Stop").ge(add)
            & pl.col("UTC_Start").le(add + timedelta(minutes=10))
            )
        post_conc = post_add["O3_ppb"].max()
        post_leak_conds.append([pre_conc, post_conc, post_conc - pre_conc])

for i, plot in enumerate(pre_plots):
    if i == 0:
        pre_plot = plot
    else:
        pre_plot = pre_plot * plot
        
for i, plot in enumerate(post_plots):
    if i == 0:
        post_plot = plot
    else:
        post_plot = post_plot * plot

hvplot.show(
    (pre_plot + post_plot).cols(2)
    )
        
# %% Background comparisons
check_start = leak_fixed - timedelta(days=2)
check_stop = leak_fixed + timedelta(days=2)
pre_leak = []
post_leak = []

for week, df in data_2025["2BTech_205_A"].items():
    week_start = df["FTC_Start"].min()
    week_stop = df["FTC_Stop"].max()
    if week_stop < check_start or week_start > check_stop:
        continue
    
    week_o3_adds = add_times["O3"].filter(
        pl.col("UTC_Stop").dt.convert_time_zone("America/Denver").ge(week_start)
        & pl.col("UTC_Start").dt.convert_time_zone("America/Denver").le(week_stop)
        ).with_columns(
            pl.col("UTC_Stop").dt.offset_by("2h")
            )
    if not week_o3_adds.is_empty():
        df = df.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).join_asof(
                week_o3_adds,
                on="UTC_Start",
                strategy="backward",
                suffix="_Add"
                ).with_columns(
                    pl.when(
                        pl.col("UTC_Start").le(pl.col("UTC_Stop_Add"))
                        )
                    .then(pl.lit(None))
                    .otherwise(pl.col("O3_ppb"))
                    .alias("O3_ppb")
                    ).select(
                        pl.exclude("UTC_Stop_Add")
                        )
    pre_leak.append(
        df.filter(
            pl.col("FTC_Stop").is_between(check_start, leak_fixed)
            )
        )
    post_leak.append(
        df.filter(
            pl.col("FTC_Start").is_between(leak_fixed, check_stop)
            )
        )
# pre_leak = pl.concat(pre_leak).drop_nulls()
# post_leak = pl.concat(post_leak).drop_nulls()


pre_leak = pl.concat(pre_leak).with_columns(
    pl.col("FTC_Start").dt.time().alias("FTC_Time"),
    pl.col("FTC_Start").dt.date().alias("FTC_Date")
    )
# .filter(
#         pl.col("O3_ppb").ge(pl.col("O3_ppb_LOD"))
#         )
post_leak = pl.concat(post_leak).with_columns(
    pl.col("FTC_Start").dt.time().alias("FTC_Time"),
    pl.col("FTC_Start").dt.date().alias("FTC_Date")
    )
# .filter(
#         pl.col("O3_ppb").ge(pl.col("O3_ppb_LOD"))
#         )

pre_leak_plot = pre_leak.hvplot.scatter(
    x="FTC_Time",
    y="O3_ppb",
    by="FTC_Date",
    xlabel="Local time",
    ylabel="Background [O3] (ppb)",
    title="Before leak fixed",
    width=600,
    height=400,
    grid=True,
    xlim=(pre_leak["FTC_Time"].min(), pre_leak["FTC_Time"].max()),
    ylim=(post_leak["O3_ppb"].min()-5, pre_leak["O3_ppb"].max()+5),
    shared_axes=False
    )
pre_leak_lod_plot = pre_leak.hvplot.line(
    x="FTC_Time",
    y="O3_ppb_LOD",
    xlabel="Local time",
    ylabel="Background [O3] (ppb)",
    title="Before leak fixed",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(post_leak["O3_ppb"].min()-5, pre_leak["O3_ppb"].max()+5),
    shared_axes=False,
    by="FTC_Date")

post_leak_plot = post_leak.hvplot.scatter(
    x="FTC_Time",
    y="O3_ppb",
    by="FTC_Date",
    xlabel="Local time",
    ylabel="Background [O3] (ppb)",
    title="After leak fixed",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(pre_leak["O3_ppb"].min()-5, pre_leak["O3_ppb"].max()+5),
    shared_axes=False
    )
post_leak_lod_plot = post_leak.hvplot.line(
    x="FTC_Time",
    y="O3_ppb_LOD",
    xlabel="Local time",
    ylabel="Background [O3] (ppb)",
    title="After leak fixed",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(pre_leak["O3_ppb"].min()-5, pre_leak["O3_ppb"].max()+5),
    shared_axes=False,
    by="FTC_Date")



hvplot.show(
    ((pre_leak_plot * pre_leak_lod_plot)
     + (post_leak_plot * post_leak_lod_plot)).cols(2)
     # + (pre_leak_nox_plot)
     # + (post_leak_nox_plot)
    )

# hvplot.show(
#     ((pre_leak.hvplot.scatter(
#         x="FTC_Start",
#         y="O3_ppb",
#         xlabel="Local time",
#         ylabel="Background [O3] (ppb)",
#         title="Before leak fixed",
#         width=600,
#         height=400,
#         grid=True,
#         xlim=(pre_leak["FTC_Start"].min(), pre_leak["FTC_Start"].max()),
#         ylim=(post_leak["O3_ppb"].min()-5, pre_leak["O3_ppb"].max()+5),
#         shared_axes=False
#         ))+ (post_leak.hvplot.scatter(
#             x="FTC_Start",
#             y="O3_ppb",
#             xlabel="Local time",
#             ylabel="Background [O3] (ppb)",
#             title="After leak fixed",
#             width=600,
#             height=400,
#             grid=True,
#             xlim=(post_leak["FTC_Start"].min(), post_leak["FTC_Start"].max()),
#             ylim=(post_leak["O3_ppb"].min()-5, pre_leak["O3_ppb"].max()+5),
#             shared_axes=False
#             ) * post_leak.hvplot.line(
#                 x="FTC_Start",
#                 y="O3_ppb_LOD",
#                 xlabel="Local time",
#                 ylabel="Background [O3] (ppb)",
#                 title="After leak fixed",
#                 width=600,
#                 height=400,
#                 grid=True,
#                 xlim=(post_leak["FTC_Start"].min(), post_leak["FTC_Start"].max()),
#                 ylim=(post_leak["O3_ppb"].min()-5, pre_leak["O3_ppb"].max()+5),
#                 shared_axes=False,
#                 color="Black"))).cols(2)
#     )


# %%

data_2024["2BTech_205_A_BG"] = {}

for week, df in data_2024["2BTech_205_A"].items():
    week_start = df["UTC_Start"].min()
    week_stop = df["UTC_Stop"].max()
    week_o3_adds = add_times["O3"].filter(
        pl.col("UTC_Stop").ge(week_start)
        & pl.col("UTC_Start").le(week_stop)
        ).with_columns(
            pl.col("UTC_Stop").dt.offset_by("2h")
            )
    if not week_o3_adds.is_empty():
        data_2024["2BTech_205_A_BG"][week] = df.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).join_asof(
                week_o3_adds,
                on="UTC_Start",
                strategy="backward",
                suffix="_Add"
                ).with_columns(
                    pl.when(
                        pl.col("UTC_Start").le(pl.col("UTC_Stop_Add"))
                        )
                    .then(pl.lit(None))
                    .otherwise(pl.col("O3_ppb"))
                    .alias("O3_ppb")
                    ).select(
                        pl.exclude("UTC_Stop_Add")
                        )

# %%
for week, df in data_2024["2BTech_205_A"].items():
    week_start = df["UTC_Start"].min()
    week_stop = df["UTC_Stop"].max()
    week_o3_adds = add_times["O3"].filter(
        pl.col("UTC_Stop").ge(week_start)
        & pl.col("UTC_Start").le(week_stop)
        ).with_columns(
            pl.col("UTC_Stop").dt.offset_by("2h")
            )
    if not week_o3_adds.is_empty():
        df = df.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).join_asof(
                week_o3_adds,
                on="UTC_Start",
                strategy="backward",
                suffix="_Add"
                ).with_columns(
                    pl.when(
                        pl.col("UTC_Start").le(pl.col("UTC_Stop_Add"))
                        )
                    .then(pl.lit(None))
                    .otherwise(pl.col("O3_ppb"))
                    .alias("O3_ppb")
                    ).select(
                        pl.exclude("UTC_Stop_Add")
                        )
    hvplot.show(
        df.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).with_columns(
                pl.col("FTC_Start").dt.time().alias("FTC_Time")
                ).hvplot.scatter(
                    x="FTC_Start",
                    y="O3_ppb",
                    title=str(week)
                    )
        )
# %%
# Possible dates leak was introduced
leak_intro = datetime(2024, 6, 13, 9, 0, 0, tzinfo=pytz.timezone("America/Denver"))
# leak_intro = datetime(2024, 6, 24, 15, 0, 0, tzinfo=pytz.timezone("America/Denver"))
# Start of pre leak intro comparison
comp_start = (leak_intro - timedelta(days=3)).replace(hour=0)
# End of post leak intro comparison
comp_stop = (leak_intro + timedelta(days=4)).replace(hour=0)

pre_leak = []
post_leak = []

pre_leak_nox = []
post_leak_nox = []

for week, df in data_2024["2BTech_205_A"].items():
    week_start = df["UTC_Start"].min()
    week_stop = df["UTC_Stop"].max()
    if week_stop < comp_start or week_start > comp_stop:
        continue
    week_o3_adds = add_times["O3"].filter(
        pl.col("UTC_Stop").ge(week_start)
        & pl.col("UTC_Start").le(week_stop)
        ).with_columns(
            pl.col("UTC_Stop").dt.offset_by("2h")
            )
    if not week_o3_adds.is_empty():
        df = df.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).join_asof(
                week_o3_adds,
                on="UTC_Start",
                strategy="backward",
                suffix="_Add"
                ).with_columns(
                    pl.when(
                        pl.col("UTC_Start").le(pl.col("UTC_Stop_Add"))
                        )
                    .then(pl.lit(None))
                    .otherwise(pl.col("O3_ppb"))
                    .alias("O3_ppb")
                    ).select(
                        pl.exclude("UTC_Stop_Add")
                        )
    pre_leak.append(df.filter(
        pl.col("FTC_Stop").ge(comp_start) & pl.col("FTC_Start").le(leak_intro)
        ))
    post_leak.append(df.filter(
        pl.col("FTC_Stop").ge(leak_intro) & pl.col("FTC_Start").le(comp_stop)
        ))
    
    if week in data_2024["ThermoScientific_42i-TL"].keys():
        nox_df = data_2024["ThermoScientific_42i-TL"][week]
        pre_leak_nox.append(nox_df.filter(
            pl.col("FTC_Stop").ge(comp_start) & pl.col("FTC_Start").le(leak_intro)
            & pl.col("SamplingLocation").str.contains("B203")
            ).with_columns(
                pl.col("NO_ppb").add(pl.col("NO2_ppb")).alias("NOx_ppb")
                ))
        post_leak_nox.append(nox_df.filter(
            pl.col("FTC_Stop").ge(leak_intro) & pl.col("FTC_Start").le(comp_stop)
            & pl.col("SamplingLocation").str.contains("B203")
            ).with_columns(
                pl.col("NO_ppb").add(pl.col("NO2_ppb")).alias("NOx_ppb")
                ))
    # hvplot.show(
    #     df.filter(
    #         pl.col("SamplingLocation").str.contains("C200")
    #         ).with_columns(
    #             pl.col("FTC_Start").dt.time().alias("FTC_Time")
    #             ).hvplot.scatter(
    #                 x="FTC_Time",
    #                 y="O3_ppb",
    #                 title=str(week)
    #                 )
    #     )

pre_leak_nox = pl.concat(pre_leak_nox).with_columns(
    pl.col("FTC_Start").dt.time().alias("FTC_Time"),
    pl.col("FTC_Start").dt.date().alias("FTC_Date")
    )
# .filter(
#         pl.col("NO_ppb").ge(pl.col("NO_ppb_LOD"))
#         )
post_leak_nox = pl.concat(post_leak_nox).with_columns(
    pl.col("FTC_Start").dt.time().alias("FTC_Time"),
    pl.col("FTC_Start").dt.date().alias("FTC_Date")
    )
# .filter(
#         pl.col("NO_ppb").ge(pl.col("NO_ppb_LOD"))
#         )

pre_leak = pl.concat(pre_leak).with_columns(
    pl.col("FTC_Start").dt.time().alias("FTC_Time"),
    pl.col("FTC_Start").dt.date().alias("FTC_Date")
    ).filter(
        pl.col("O3_ppb").ge(pl.col("O3_ppb_LOD"))
        )
post_leak = pl.concat(post_leak).with_columns(
    pl.col("FTC_Start").dt.time().alias("FTC_Time"),
    pl.col("FTC_Start").dt.date().alias("FTC_Date")
    ).filter(
        pl.col("O3_ppb").ge(pl.col("O3_ppb_LOD"))
        )

pre_leak_plot = pre_leak.hvplot.scatter(
    x="FTC_Time",
    y="O3_ppb",
    by="FTC_Date",
    xlabel="Local time",
    ylabel="Background [O3] (ppb)",
    title="Before leak introduced",
    width=600,
    height=400,
    grid=True,
    xlim=(pre_leak["FTC_Time"].min(), pre_leak["FTC_Time"].max()),
    ylim=(pre_leak["O3_ppb"].min()-5, post_leak["O3_ppb"].max()+5),
    shared_axes=False
    )
pre_leak_lod_plot = pre_leak.hvplot.line(
    x="FTC_Time",
    y="O3_ppb_LOD",
    xlabel="Local time",
    ylabel="Background [O3] (ppb)",
    title="Before leak introduced",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(pre_leak["O3_ppb"].min()-5, post_leak["O3_ppb"].max()+5),
    shared_axes=False,
    by="FTC_Date")

post_leak_plot = post_leak.hvplot.scatter(
    x="FTC_Time",
    y="O3_ppb",
    by="FTC_Date",
    xlabel="Local time",
    ylabel="Background [O3] (ppb)",
    title="After leak introduced",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(pre_leak["O3_ppb"].min()-5, post_leak["O3_ppb"].max()+5),
    shared_axes=False
    )
post_leak_lod_plot = post_leak.hvplot.line(
    x="FTC_Time",
    y="O3_ppb_LOD",
    xlabel="Local time",
    ylabel="Background [O3] (ppb)",
    title="After leak introduced",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(pre_leak["O3_ppb"].min()-5, post_leak["O3_ppb"].max()+5),
    shared_axes=False,
    by="FTC_Date")

pre_leak_nox_plot = pre_leak_nox.hvplot.scatter(
    x="FTC_Time",
    y="NOx_ppb",
    by="FTC_Date",
    xlabel="Local time",
    ylabel="[NO] (ppb) in instrument room",
    title="Before leak introduced",
    width=600,
    height=400,
    grid=True,
    xlim=(pre_leak_nox["FTC_Time"].min(), pre_leak_nox["FTC_Time"].max()),
    ylim=(pre_leak_nox["NOx_ppb"].min()-5, post_leak_nox["NOx_ppb"].max()+5),
    shared_axes=False
    )

post_leak_nox_plot = post_leak_nox.hvplot.scatter(
    x="FTC_Time",
    y="NOx_ppb",
    by="FTC_Date",
    xlabel="Local time",
    ylabel="[NO] (ppb) in instrument room",
    title="After leak introduced",
    width=600,
    height=400,
    grid=True,
    xlim=(pre_leak_nox["FTC_Time"].min(), pre_leak_nox["FTC_Time"].max()),
    ylim=(pre_leak_nox["NOx_ppb"].min()-5, post_leak_nox["NOx_ppb"].max()+5),
    shared_axes=False
    )

hvplot.show(
    ((pre_leak_plot * pre_leak_lod_plot)
     + (post_leak_plot * post_leak_lod_plot)).cols(2)
     # + (pre_leak_nox_plot)
     # + (post_leak_nox_plot)
    )

        
# hvplot.show(
#     ((pre_leak.hvplot.scatter(
#         x="FTC_Time",
#         y="O3_ppb",
#         by="FTC_Date",
#         xlabel="Local time",
#         ylabel="Background [O3] (ppb)",
#         title="Before leak introduced",
#         width=600,
#         height=400,
#         grid=True,
#         xlim=(pre_leak["FTC_Time"].min(), pre_leak["FTC_Time"].max()),
#         ylim=(post_leak["O3_ppb"].min()-5, pre_leak["O3_ppb"].max()+5),
#         shared_axes=False
#         ) * pre_leak.hvplot.line(
#                 x="FTC_Time",
#                 y="O3_ppb_LOD",
#                 xlabel="Local time",
#                 ylabel="Background [O3] (ppb)",
#                 title="Before leak introduced",
#                 width=600,
#                 height=400,
#                 grid=True,
#                 xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
#                 ylim=(post_leak["O3_ppb"].min()-5, pre_leak["O3_ppb"].max()+5),
#                 shared_axes=False,
#                 by="FTC_Date")) + (post_leak.hvplot.scatter(
#                     x="FTC_Time",
#                     y="O3_ppb",
#                     by="FTC_Date",
#                     xlabel="Local time",
#                     ylabel="Background [O3] (ppb)",
#                     title="After leak introduced",
#                     width=600,
#                     height=400,
#                     grid=True,
#                     xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
#                     ylim=(post_leak["O3_ppb"].min()-5, pre_leak["O3_ppb"].max()+5),
#                     shared_axes=False
#                     ) * post_leak.hvplot.line(
#                         x="FTC_Time",
#                         y="O3_ppb_LOD",
#                         xlabel="Local time",
#                         ylabel="Background [O3] (ppb)",
#                         title="After leak introduced",
#                         width=600,
#                         height=400,
#                         grid=True,
#                         xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
#                         ylim=(post_leak["O3_ppb"].min()-5, pre_leak["O3_ppb"].max()+5),
#                         shared_axes=False,
#                         by="FTC_Date"))).cols(2)
#     )


# %% CH2O
# Possible dates leak was introduced
leak_intro = datetime(2024, 6, 13, 9, 0, 0, tzinfo=pytz.timezone("America/Denver"))
# leak_intro = datetime(2024, 6, 24, 15, 0, 0, tzinfo=pytz.timezone("America/Denver"))
# Start of pre leak intro comparison
comp_start = (leak_intro - timedelta(days=3)).replace(hour=0)
# End of post leak intro comparison
comp_stop = (leak_intro + timedelta(days=4)).replace(hour=0)

pre_leak = []
post_leak = []

pre_leak_nox = []
post_leak_nox = []

for week, df in data_2024["Picarro_G2307"].items():
    week_start = df["FTC_DateTime"].min()
    week_stop = df["FTC_DateTime"].max()
    if week_stop < comp_start or week_start > comp_stop:
        continue
    df = df.filter(
        pl.col("SamplingLocation").str.contains("C200")
        )
    pre_leak.append(
        df.filter(
            pl.col("FTC_DateTime").is_between(comp_start, leak_intro)
            )
        )
    post_leak.append(
        df.filter(
            pl.col("FTC_DateTime").is_between(leak_intro, comp_stop)
            )
        )
# pre_leak = pl.concat(pre_leak).drop_nulls()
# post_leak = pl.concat(post_leak).drop_nulls()


pre_leak = pl.concat(pre_leak).with_columns(
    pl.col("FTC_DateTime").dt.time().alias("FTC_Time"),
    pl.col("FTC_DateTime").dt.date().alias("FTC_Date")
    ).filter(
        pl.col("CH2O_ppb").ge(pl.col("CH2O_ppb_LOD"))
        )
post_leak = pl.concat(post_leak).with_columns(
    pl.col("FTC_DateTime").dt.time().alias("FTC_Time"),
    pl.col("FTC_DateTime").dt.date().alias("FTC_Date")
    ).filter(
        pl.col("CH2O_ppb").ge(pl.col("CH2O_ppb_LOD"))
        )

pre_leak_plot = pre_leak.hvplot.scatter(
    x="FTC_Time",
    y="CH2O_ppb",
    by="FTC_Date",
    xlabel="Local time",
    ylabel="Background [CH2O] (ppb)",
    title="Before leak introduced",
    width=600,
    height=400,
    grid=True,
    xlim=(pre_leak["FTC_Time"].min(), pre_leak["FTC_Time"].max()),
    ylim=(pre_leak["CH2O_ppb"].min()-5, post_leak["CH2O_ppb"].max()+5),
    shared_axes=False
    )
pre_leak_lod_plot = pre_leak.hvplot.line(
    x="FTC_Time",
    y="CH2O_ppb_LOD",
    xlabel="Local time",
    ylabel="Background [CH2O] (ppb)",
    title="Before leak introduced",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(pre_leak["CH2O_ppb"].min()-5, post_leak["CH2O_ppb"].max()+5),
    shared_axes=False,
    by="FTC_Date")

post_leak_plot = post_leak.hvplot.scatter(
    x="FTC_Time",
    y="CH2O_ppb",
    by="FTC_Date",
    xlabel="Local time",
    ylabel="Background [CH2O] (ppb)",
    title="After leak introduced",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(pre_leak["CH2O_ppb"].min()-5, post_leak["CH2O_ppb"].max()+5),
    shared_axes=False
    )
post_leak_lod_plot = post_leak.hvplot.line(
    x="FTC_Time",
    y="CH2O_ppb_LOD",
    xlabel="Local time",
    ylabel="Background [CH2O] (ppb)",
    title="After leak introduced",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(pre_leak["CH2O_ppb"].min()-5, post_leak["CH2O_ppb"].max()+5),
    shared_axes=False,
    by="FTC_Date")


hvplot.show(
    ((pre_leak_plot * pre_leak_lod_plot)
     + (post_leak_plot * post_leak_lod_plot)).cols(2)
     # + (pre_leak_nox_plot)
     # + (post_leak_nox_plot)
    )


# %%
for week, df in data_2024["2BTech_205_A_BG"].items():
    
    bg_o3 = df.filter(
        pl.col("O3_ppb").ge(pl.col("O3_ppb_LOD"))
        ).group_by_dynamic(
            "FTC_Start", every="10m"
            ).agg(
                pl.col("O3_ppb").mean()
                )
    
    plot = df.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).with_columns(
                pl.col("O3_ppb").interpolate_by("FTC_Start")
                ).hvplot.scatter(
                    x="FTC_Start",
                    y="O3_ppb",
                    title=str(week)
                    )
    bg_sub = data_2024["2BTech_205_A"][week].join(
        df.with_columns(
            pl.col("O3_ppb").interpolate_by("FTC_Start")
            ),
        on="FTC_Start",
        suffix="_Background"
        ).with_columns(
            pl.col("O3_ppb").sub(pl.col("O3_ppb_Background")).alias("BG_Sub_O3_ppb")
            )
    plot = plot * bg_sub.filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).hvplot.scatter(
                x="FTC_Start",
                y="BG_Sub_O3_ppb"
                )
    if week in data_2024["2BTech_205_B"].keys():
        vent_o3 = data_2024["2BTech_205_B"][week].filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).group_by_dynamic(
                "FTC_Start", every="10m"
                ).agg(
                    pl.col("O3_ppb").mean()
                    )
        io_o3 = bg_o3.join(
            vent_o3,
            on="FTC_Start",
            suffix="_Vent"
            ).with_columns(
                pl.col("O3_ppb").truediv(pl.col("O3_ppb_Vent")).alias("IO")
                )
        # plot = ((plot * data_2025["2BTech_205_B"][week].filter(
        #     pl.col("SamplingLocation").str.contains("C200")
        #     ).hvplot.scatter(
        #         x="FTC_Start",
        #         y="O3_ppb",
        #         title=str(week)
        #         )) + io_o3.hvplot.scatter(
        #             x="FTC_Start",
        #             y="IO"
        #             )).cols(1)
    if week in data_2024["ThermoScientific_42i-TL"].keys():
        plot = (plot + data_2024["ThermoScientific_42i-TL"][week].filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).hvplot.scatter(
                x="FTC_Start",
                y=["NO_ppb", "NO2_ppb"]
                )
                ).cols(1)
        
    hvplot.show(plot)
# %% Background comparisons - NOx
check_start = leak_fixed - timedelta(days=2)
check_stop = leak_fixed + timedelta(days=2)
pre_leak = []
post_leak = []

for week, df in data_2025["ThermoScientific_42i-TL"].items():
    week_start = df["FTC_Start"].min()
    week_stop = df["FTC_Stop"].max()
    if week_stop < check_start or week_start > check_stop:
        continue
    df = df.filter(
        pl.col("SamplingLocation").str.contains("C200")
        )
    pre_leak.append(
        df.filter(
            pl.col("FTC_Start").is_between(check_start, leak_fixed)
            )
        )
    post_leak.append(
        df.filter(
            pl.col("FTC_Stop").is_between(leak_fixed, check_stop)
            )
        )
# pre_leak = pl.concat(pre_leak).drop_nulls()
# post_leak = pl.concat(post_leak).drop_nulls()


pre_leak = pl.concat(pre_leak).with_columns(
    pl.col("FTC_Start").dt.time().alias("FTC_Time"),
    pl.col("FTC_Start").dt.date().alias("FTC_Date")
    )
# .filter(
#         pl.col("NO_ppb").ge(pl.col("NO_ppb_LOD"))
#         )
post_leak = pl.concat(post_leak).with_columns(
    pl.col("FTC_Start").dt.time().alias("FTC_Time"),
    pl.col("FTC_Start").dt.date().alias("FTC_Date")
    )
# .filter(
#         pl.col("NO_ppb").ge(pl.col("NO_ppb_LOD"))
#         )

pre_leak_plot = pre_leak.hvplot.scatter(
    x="FTC_Time",
    y="NO_ppb",
    by="FTC_Date",
    xlabel="Local time",
    ylabel="Background [NO] (ppb)",
    title="Before leak fixed",
    width=600,
    height=400,
    grid=True,
    xlim=(pre_leak["FTC_Time"].min(), pre_leak["FTC_Time"].max()),
    ylim=(post_leak["NO_ppb"].min()-5, pre_leak["NO_ppb"].max()+5),
    shared_axes=False
    )
pre_leak_lod_plot = pre_leak.hvplot.line(
    x="FTC_Time",
    y="NO_ppb_LOD",
    xlabel="Local time",
    ylabel="Background [NO] (ppb)",
    title="Before leak fixed",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(post_leak["NO_ppb"].min()-5, pre_leak["NO_ppb"].max()+5),
    shared_axes=False,
    by="FTC_Date")

post_leak_plot = post_leak.hvplot.scatter(
    x="FTC_Time",
    y="NO_ppb",
    by="FTC_Date",
    xlabel="Local time",
    ylabel="Background [NO] (ppb)",
    title="After leak fixed",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(pre_leak["NO_ppb"].min()-5, pre_leak["NO_ppb"].max()+5),
    shared_axes=False
    )
post_leak_lod_plot = post_leak.hvplot.line(
    x="FTC_Time",
    y="NO_ppb_LOD",
    xlabel="Local time",
    ylabel="Background [NO] (ppb)",
    title="After leak fixed",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(pre_leak["NO_ppb"].min()-5, pre_leak["NO_ppb"].max()+5),
    shared_axes=False,
    by="FTC_Date")



hvplot.show(
    ((pre_leak_plot * pre_leak_lod_plot)
     + (post_leak_plot * post_leak_lod_plot)).cols(2)
     # + (pre_leak_nox_plot)
     # + (post_leak_nox_plot)
    )
    
# %% Background comparisons - HCHO
check_start = leak_fixed - timedelta(days=2)
check_stop = leak_fixed + timedelta(days=2)
pre_leak = []
post_leak = []

for week, df in data_2025["Picarro_G2307"].items():
    week_start = df["FTC_DateTime"].min()
    week_stop = df["FTC_DateTime"].max()
    if week_stop < check_start or week_start > check_stop:
        continue
    df = df.filter(
        pl.col("SamplingLocation").str.contains("C200")
        )
    pre_leak.append(
        df.filter(
            pl.col("FTC_DateTime").is_between(check_start, leak_fixed)
            )
        )
    post_leak.append(
        df.filter(
            pl.col("FTC_DateTime").is_between(leak_fixed, check_stop)
            )
        )
# pre_leak = pl.concat(pre_leak).drop_nulls()
# post_leak = pl.concat(post_leak).drop_nulls()


pre_leak = pl.concat(pre_leak).with_columns(
    pl.col("FTC_DateTime").dt.time().alias("FTC_Time"),
    pl.col("FTC_DateTime").dt.date().alias("FTC_Date")
    ).filter(
        pl.col("CH2O_ppb").ge(pl.col("CH2O_ppb_LOD"))
        )
post_leak = pl.concat(post_leak).with_columns(
    pl.col("FTC_DateTime").dt.time().alias("FTC_Time"),
    pl.col("FTC_DateTime").dt.date().alias("FTC_Date")
    ).filter(
        pl.col("CH2O_ppb").ge(pl.col("CH2O_ppb_LOD"))
        )

pre_leak_plot = pre_leak.hvplot.scatter(
    x="FTC_Time",
    y="CH2O_ppb",
    by="FTC_Date",
    xlabel="Local time",
    ylabel="Background [CH2O] (ppb)",
    title="Before leak fixed",
    width=600,
    height=400,
    grid=True,
    xlim=(pre_leak["FTC_Time"].min(), pre_leak["FTC_Time"].max()),
    ylim=(post_leak["CH2O_ppb"].min()-5, pre_leak["CH2O_ppb"].max()+5),
    shared_axes=False
    )
pre_leak_lod_plot = pre_leak.hvplot.line(
    x="FTC_Time",
    y="CH2O_ppb_LOD",
    xlabel="Local time",
    ylabel="Background [CH2O_ppb] (ppb)",
    title="Before leak fixed",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(post_leak["CH2O_ppb"].min()-5, pre_leak["CH2O_ppb"].max()+5),
    shared_axes=False,
    by="FTC_Date")

post_leak_plot = post_leak.hvplot.scatter(
    x="FTC_Time",
    y="CH2O_ppb",
    by="FTC_Date",
    xlabel="Local time",
    ylabel="Background [CH2O_ppb] (ppb)",
    title="After leak fixed",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(pre_leak["CH2O_ppb"].min()-5, pre_leak["CH2O_ppb"].max()+5),
    shared_axes=False
    )
post_leak_lod_plot = post_leak.hvplot.line(
    x="FTC_Time",
    y="CH2O_ppb_LOD",
    xlabel="Local time",
    ylabel="Background [O3] (ppb)",
    title="After leak fixed",
    width=600,
    height=400,
    grid=True,
    xlim=(post_leak["FTC_Time"].min(), post_leak["FTC_Time"].max()),
    ylim=(pre_leak["CH2O_ppb"].min()-5, pre_leak["CH2O_ppb"].max()+5),
    shared_axes=False,
    by="FTC_Date")



hvplot.show(
    ((pre_leak_plot * pre_leak_lod_plot)
     + (post_leak_plot * post_leak_lod_plot)).cols(2)
     # + (pre_leak_nox_plot)
     # + (post_leak_nox_plot)
    )