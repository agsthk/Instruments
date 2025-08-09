# -*- coding: utf-8 -*-
"""
Created on Mon Jul 21 16:52:19 2025

@author: agsthk
"""

import os
import polars as pl
import pytz
from datetime import datetime
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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

insts = ["2BTech_202",
         "2BTech_205_A",
         "2BTech_205_B",
         "2BTech_405nm",
         "Aranet4_1F16F",
         "Aranet4_1FB20",
         "LI-COR_LI-840A_A",
         "LI-COR_LI-840A_B",
         "Picarro_G2307",
         "TempRHDoor",
         "ThermoScientific_42i-TL"]

sampling_locs = {
    "2BTech_202" : [
        ["2024-01-18T13:39:00-0700", "Calibration Source"],
        ["2024-01-18T17:14:00-0700", "B213"]
        ],
    "2BTech_205_A": [
        ["2025-01-15T09:03:00-0700", "Calibration Source"],
        ["2025-01-17T10:00:00-0700", "B211"],
        ["2025-01-17T19:08:00-0700", "TG_Line"],
        ],
    "2BTech_205_B": [
        ["2025-01-15T09:18:00-0700", "B211"],
        ["2025-01-15T10:03:00-0700", "Calibration Source"],
        ["2025-01-17T10:00:00-0700", "B211"],
        ["2025-01-20T17:11:00-0700", "C200_Vent"],
        ["2025-02-12T09:21:00-0700", None], #?
        ["2025-02-12T09:22:00-0700", "C200_Vent"],
        ["2025-02-28T17:35:00-0700", None], #?
        ["2025-02-28T17:37:00-0700", "C200_Vent"], #?
        ["2025-03-03T07:05:00-0700", None], #?
        ["2025-03-03T07:07:00-0700", "C200_Vent"], #?
        ],
    "2BTech_405nm" : [
        ["2024-12-16T15:59:00-0700", "Calibration Source"]
        ],
    "Aranet4_1F16F": [
        ["2025-03-20T20:13:00-0600", "C200"]
        ],
    "Aranet4_1FB20": [
        ["2025-03-20T20:15:00-0600", "C200"]
        ],
    "LI-COR_LI-840A_A": [
        ["2025-01-10T10:10:01-0700", "C200"]
        ],
    "LI-COR_LI-840A_B": [
        ["2025-02-28T17:34:00-0700", "B203"],
        ["2025-02-28T17:37:00-0700", "C200_Vent"]
        ],
    "Picarro_G2307": [
        ["2025-01-08T17:49:00-0700", "B211"],
        ["2025-01-09T14:55:00-0700", "Calibration Source"],
        ["2025-01-09T18:12:00-0700", "B211"],
        ["2025-01-20T16:29:00-0700", "TG_Line"],
        ["2025-01-27T11:31:00-0700", "B203"],
        ["2025-01-27T11:33:00-0700", "B203/Drierite"],
        ["2025-01-27T16:30:00-0700", "B203"],
        ["2025-01-27T17:17:00-0700", "TG_Line"]
        ],
    "ThermoScientific_42i-TL": [
        ["2025-01-15T09:33:00-0700", "B213"],
        ["2025-01-20T16:29:00-0700", "TG_Line"],
        ["2025-01-20T17:11:00-0700", "B203"],
        ["2025-01-20T17:30:00-0700", "TG_Line"],
        ["2025-01-27T11:25:00-0700", "B203"],
        ["2025-01-27T17:15:00-0700", "TG_Line"],
        ["2025-02-04T14:55:00-0700", "B203"],
        ["2025-02-06T15:49:00-0700", "TG_Line"],
        ["2025-02-06T16:54:00-0700", "B203"],
        ["2025-02-07T08:07:00-0700", "TG_Line"]
        ],
    "TG_Line": [
        ["2025-01-17T19:08:00-0700", "C200/B203/Exhaust"],
        ["2025-01-20T16:20:00-0700", None],
        ["2025-01-20T16:29:00-0700", "C200/B203/Exhaust"],
        ["2025-01-20T17:11:00-0700", None],
        ["2025-01-20T17:30:00-0700", "C200/B203"],
        ["2025-01-27T10:37:00-0700", None],
        ["2025-01-27T12:01:00-0700", "C200"],
        ["2025-01-27T17:07:00-0700", None],
        ["2025-01-27T17:45:00-0700", "C200"],
        ["2025-01-28T01:44:08-0700", "UZA"],
        ["2025-01-28T01:54:05-0700", "C200"],
        ["2025-01-28T05:44:08-0700", "UZA"],
        ["2025-01-28T05:54:05-0700", "C200"],
        ["2025-01-28T09:44:08-0700", "UZA"],
        ["2025-01-28T09:54:05-0700", "C200"],
        ["2025-02-03T09:45:00-0700", None],
        ["2025-02-03T12:07:00-0700", "C200"],
        ["2025-02-03T13:55:00-0700", None], #?
        ["2025-02-03T13:56:00-0700", "C200"], #?
        ["2025-02-03T14:35:00-0700", None], #?
        ["2025-02-03T14:37:00-0700", "C200"], #?
        ["2025-02-03T16:22:00-0700", None],
        ["2025-02-03T17:30:00-0700", "C200"],
        ["2025-02-04T09:00:00-0700", None], #?
        ["2025-02-04T09:01:00-0700", "C200"], #?
        ["2025-02-06T14:22:00-0700", None], #?
        ["2025-02-06T14:23:00-0700", "C200"], #? 
        ["2025-02-06T15:17:00-0700", None], #?
        ["2025-02-06T16:22:00-0700", "C200"], #?
        ["2025-02-06T16:54:00-0700", None], #?
        ["2025-02-06T16:57:00-0700", "C200"], #?
        ["2025-02-07T08:06:00-0700", None], #?
        ["2025-02-07T08:07:00-0700", "C200"], #?
        ["2025-02-10T10:05:00-0700", None],
        ["2025-02-10T11:37:00-0700", "C200"],
        ["2025-02-12T08:02:00-0700", None], #?
        ["2025-02-12T08:03:00-0700", "C200"], #?
        ["2025-02-12T08:13:00-0700", None], #?
        ["2025-02-12T08:14:00-0700", "C200"], #?
        ["2025-02-12T09:21:00-0700", None], #?
        ["2025-02-12T09:23:00-0700", "C200"], #?
        ["2025-02-12T15:30:00-0700", None], #?
        ["2025-02-13T09:20:00-0700", "C200"], #?
        ["2025-02-17T12:33:00-0700", "UZA"],
        ["2025-02-17T12:35:00-0700", "C200"],
        ["2025-03-06T07:55:00-0700", None], #?
        ["2025-03-06T07:57:00-0700", "C200"], #?
        ["2025-03-06T08:52:00-0700", None], #?
        ["2025-03-06T08:53:00-0700", "C200"], #?
        ["2025-03-17T09:10:00-0600", None],
        ["2025-03-17T10:38:00-0600", "C200"],
        ["2025-03-18T08:55:15-0600", "B203"],
        ["2025-03-18T09:06:05-0600", "C200"],
        ["2025-03-18T09:15:17-0600", "B203"],
        ["2025-03-18T09:24:45-0600", "C200"],
        ["2025-03-18T09:34:30-0600", "B203"],
        ["2025-03-18T09:45:05-0600", "C200"],
        ["2025-03-20T08:32:15-0600", "B203"],
        ["2025-03-20T08:42:55-0600", "C200"],
        ["2025-03-20T08:50:44-0600", "B203"],
        ["2025-03-20T09:03:55-0600", "C200"],
        ["2025-03-20T09:13:15-0600", "B203"],
        ["2025-03-20T09:23:10-0600", "C200"],
        ["2025-05-05T12:47:00-0600", None]
        ]
    }
sampling_locs = {
    inst: pl.DataFrame(
        locs, schema=["UTC_Start", "SamplingLocation"], orient="row"
        ).with_columns(
            pl.col("UTC_Start").str.to_datetime()
            ).select(
                pl.col("UTC_Start"),
                pl.col("UTC_Start").shift(-1).alias("UTC_Stop"),
                pl.col("SamplingLocation")
                ).with_columns(
                    pl.col("UTC_Stop").fill_null(
                        pl.col("UTC_Start").dt.offset_by("1y")
                        )
                    )
    for inst, locs in sampling_locs.items()
    }

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
        
valve_states = valve_states.select(
    pl.selectors.contains("UTC"),
    pl.when(
        pl.col("UTC_Start").gt(datetime(2025, 3, 20, 15, 57, tzinfo=pytz.UTC))
        & pl.col("UTC_Stop").lt(datetime(2025, 3, 20, 20, 35, tzinfo=pytz.UTC))
        & pl.col("SolenoidValves").eq(1))
    .then(pl.lit("B203"))
    .when(pl.col("SolenoidValves").eq(1))
    .then(pl.lit("UZA"))
    .alias("SamplingLocation")
    )
# Intervals that Picarro G2307 is on the Trace Gas Line
pic_on_tg = sampling_locs["Picarro_G2307"].filter(
    pl.col("SamplingLocation").eq("TG_Line")
    ).select(
        pl.exclude("SamplingLocation")
        )
# Intervals that Picarro G2307 is not on the Trace Gas Line
pic_off_tg = pl.concat(
    [pic_on_tg.with_columns(
        pl.col("UTC_Start").alias("UTC_Stop"),
        pl.col("UTC_Stop").shift(1).alias("UTC_Start")
        ),
    pic_on_tg.with_columns(
        pl.col("UTC_Start").shift(-1).alias("UTC_Stop"),
        pl.col("UTC_Stop").alias("UTC_Start")
        )]
    ).unique(maintain_order=True).with_columns(
        pl.col("UTC_Start").fill_null(
            pl.col("UTC_Stop").dt.offset_by("-1y")
            ),
        pl.col("UTC_Stop").fill_null(
            pl.col("UTC_Start").dt.offset_by("1y")
            )
        )

# Valve states while Picarro G2307 is on the Trace Gas Line
valve_on_tg = valve_states.join(pic_on_tg, on=None, how="cross").filter(
    pl.col("UTC_Start").lt(pl.col("UTC_Stop_right"))
    & pl.col("UTC_Start_right").lt(pl.col("UTC_Stop"))
    ).select(
        pl.when(pl.col("UTC_Start").lt(pl.col("UTC_Start_right")))
        .then(pl.col("UTC_Start_right"))
        .otherwise(pl.col("UTC_Start"))
        .alias("UTC_Start"),
        pl.when(pl.col("UTC_Stop").gt(pl.col("UTC_Stop_right")))
        .then(pl.col("UTC_Stop_right"))
        .otherwise(pl.col("UTC_Stop"))
        .alias("UTC_Stop"),
        pl.col("SamplingLocation")
        )
# Assigns sampling locations
valve_on_tg = valve_on_tg.join(
    sampling_locs["TG_Line"], on=None, how="cross"
    ).filter(
        pl.col("UTC_Start").lt(pl.col("UTC_Stop_right"))
        & pl.col("UTC_Start_right").lt(pl.col("UTC_Stop"))
        ).filter(
            pl.col("SamplingLocation_right").str.contains("C200")
            ).select(
                pl.when(pl.col("UTC_Start").lt(pl.col("UTC_Start_right")))
                .then(pl.col("UTC_Start_right"))
                .otherwise(pl.col("UTC_Start"))
                .alias("UTC_Start"),
                pl.when(pl.col("UTC_Stop").gt(pl.col("UTC_Stop_right")))
                .then(pl.col("UTC_Stop_right"))
                .otherwise(pl.col("UTC_Stop"))
                .alias("UTC_Stop"),
                pl.when(pl.col("SamplingLocation").is_null())
                .then(pl.col("SamplingLocation_right"))
                .otherwise(pl.col("SamplingLocation"))
                .alias("SamplingLocation")
                )

valve_off_tg = pic_off_tg.join(
    sampling_locs["TG_Line"], on=None, how="cross"
    ).filter(
        pl.col("UTC_Start").lt(pl.col("UTC_Stop_right"))
        & pl.col("UTC_Start_right").lt(pl.col("UTC_Stop"))
        ).filter(
            pl.col("SamplingLocation").str.contains("C200")
            ).select(
                pl.when(pl.col("UTC_Start").lt(pl.col("UTC_Start_right")))
                .then(pl.col("UTC_Start_right"))
                .otherwise(pl.col("UTC_Start"))
                .alias("UTC_Start"),
                pl.when(pl.col("UTC_Stop").gt(pl.col("UTC_Stop_right")))
                .then(pl.col("UTC_Stop_right"))
                .otherwise(pl.col("UTC_Stop"))
                .alias("UTC_Stop"),
                pl.col("SamplingLocation")
                )

sampling_locs["TG_Line"] = pl.concat([
    valve_on_tg,
    valve_off_tg,
    sampling_locs["TG_Line"].filter(
        ~pl.col("SamplingLocation").str.contains("C200")
        )
    ]).sort(by="UTC_Start")

for inst, df in sampling_locs.items():
    temp_locs = pl.concat(
        [sampling_locs[inst].filter(
            ~pl.col("SamplingLocation").eq("TG_Line")
            | pl.col("SamplingLocation").is_null()
            ),
        df.join(
            sampling_locs["TG_Line"], on=None, how="cross").filter(
                (pl.col("UTC_Start") < pl.col("UTC_Stop_right")) &
                (pl.col("UTC_Start_right") < pl.col("UTC_Stop"))
                ).select(
                    pl.when(pl.col("UTC_Start").lt(pl.col("UTC_Start_right")))
                    .then(pl.col("UTC_Start_right"))
                    .otherwise(pl.col("UTC_Start"))
                    .alias("UTC_Start"),
                    pl.when(pl.col("UTC_Stop").gt(pl.col("UTC_Stop_right")))
                    .then(pl.col("UTC_Stop_right"))
                    .otherwise(pl.col("UTC_Stop"))
                    .alias("UTC_Stop"),
                    pl.when(pl.col("SamplingLocation").eq("TG_Line"))
                    .then(pl.col("SamplingLocation_right"))
                    .otherwise(pl.col("SamplingLocation"))
                    .alias("SamplingLocation")
                    )]
        ).sort(by="UTC_Start")
    sampling_locs[inst] = temp_locs.with_columns(
        pl.col("UTC_Start").dt.offset_by("60s"),
        pl.col("UTC_Stop").dt.offset_by("-60s")
        )

data = {inst: {} for inst in insts}

for root, dirs, files in tqdm(os.walk(STRUCT_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        for inst in insts:
            if path.find(inst) != -1:
                break
        if inst == "ThermoScientific_42i-TL" and path.find(inst) == -1:
            continue
        if inst == "2BTech_405nm":
            lf = pl.scan_csv(path, infer_schema_length=None)
        else:
            lf = pl.scan_csv(path)
        lf = lf.with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC"),
            pl.selectors.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        if inst in sampling_locs.keys():
            if "UTC_DateTime" in lf.collect_schema().names():
                on = "UTC_DateTime"
                compare = on
                locs = sampling_locs[inst].rename(
                    {"UTC_Start": "UTC_DateTime",
                     "UTC_Stop": "Sampling_Stop"}
                    ).lazy()
            else:
                on = "UTC_Start"
                compare = "UTC_Stop"
                locs = sampling_locs[inst].rename(
                    {"UTC_Stop": "Sampling_Stop"}
                    ).lazy()
            lf = lf.join_asof(
                locs, on=on, strategy="backward", coalesce=True
                )
            lf = lf.with_columns(
                pl.when(pl.col(compare).gt(pl.col("Sampling_Stop")))
                .then(pl.lit(None))
                .otherwise(pl.col("SamplingLocation"))
                .alias("SamplingLocation")
                )
        if "WarmUp" in lf.collect_schema().names():
            lf = lf.filter(
                pl.col("WarmUp").eq(0)
                ).select(
                    pl.exclude("WarmUp"))
        if inst == "ThermoScientific_42i-TL":
            lf = lf.filter(
                pl.col("SampleFlow_LPM").gt(0.5)
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

# for date, df in tqdm(data["2BTech_205_B"].items()):
#     df = df.filter(pl.col("SamplingLocation").eq("C200_Vent"))
#     if df.is_empty():
#         continue
#     fig, (ax1, ax2) = plt.subplots(2, 1,figsize=(8, 8))
#     ax1.plot(df["UTC_Start"], df["O3_ppb"], color="#1E4D2B")
#     ax1.set_ylabel("O3_ppb", color="#1E4D2B")
#     ax1.spines["left"].set_color("#1E4D2B")
#     ax1.xaxis.set_major_locator(
#         mdates.AutoDateLocator(tz=pytz.timezone("America/Denver"),)
#         )
#     ax1.xaxis.set_major_formatter(
#         mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
#         )
#     ax1.tick_params(axis="x", labelrotation=90)
#     ax1.tick_params(axis="y", color="#1E4D2B", labelcolor="#1E4D2B")
#     ax1.set_title(date)
#     ax1.grid(axis="x")
#     og = df.height
#     var = "O3_ppb"
#     pts_removed = []
#     absolute = [0.1 + (i / 100) for i in range(20)][::-1]
#     for a in absolute:
#         df2 = df.with_columns(
#             pl.col(var).shift(-1).sub(pl.col(var)).abs().alias("diff"),
#             pl.col("UTC_Start").shift(-1).sub(pl.col("UTC_Start")).dt.total_microseconds().truediv(1e6).alias("dt"),
#             ).with_columns(
#                 pl.col("diff").truediv(pl.col("dt")).alias("d/dt"),
#                 ).filter(
#                     pl.col("d/dt").abs().lt(a)
#                     )
#         pts_removed.append((og - df2.height) / og)
#     ax1.plot(df2["UTC_Start"], df2["O3_ppb"], color="#D9782D")
#     ax2.plot(absolute, pts_removed, color="#D9782D")
#     ax2.set_xlabel("Absolute d/dt Cutoff")
#     ax2.set_ylabel("Fraction of data removed")


# for date, df in tqdm(data["2BTech_205_B"].items()):
#     df = df.filter(pl.col("SamplingLocation").eq("C200_Vent"))
#     if df.is_empty():
#         continue
#     fig, (ax1, ax2) = plt.subplots(2, 1,figsize=(8, 8))
#     ax1.plot(df["UTC_Start"], df["O3_ppb"], color="#1E4D2B")
#     ax1.set_ylabel("O3_ppb", color="#1E4D2B")
#     ax1.spines["left"].set_color("#1E4D2B")
#     ax1.xaxis.set_major_locator(
#         mdates.AutoDateLocator(tz=pytz.timezone("America/Denver"),)
#         )
#     ax1.xaxis.set_major_formatter(
#         mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
#         )
#     ax1.tick_params(axis="x", labelrotation=90)
#     ax1.tick_params(axis="y", color="#1E4D2B", labelcolor="#1E4D2B")
#     ax1.set_title(date)
#     ax1.grid(axis="x")
#     og = df.height
#     var = "O3_ppb"
#     pts_removed = []
#     absolute = [0.1 + (i / 10) for i in range(5)][::-1]
#     relative = [0.01 + (i / 100) for i in range(10)][::-1]
#     for a in absolute:
#         a_rmvd = []
#         for r in relative:
#             df2 = df.with_columns(
#                 pl.col(var).shift(-1).sub(pl.col(var)).abs().alias("diff"),
#                 pl.col("UTC_Start").shift(-1).sub(pl.col("UTC_Start")).dt.total_microseconds().truediv(1e6).alias("dt"),
#                 ).with_columns(
#                     pl.col("diff").truediv(pl.col(var)).truediv(pl.col("dt")).abs().alias("rel d/dt"),
#                     pl.col("diff").truediv(pl.col("dt")).alias("d/dt"),
#                     ).filter(
#                         pl.col("d/dt").lt(a) & pl.col("rel d/dt").lt(r)
#                         )
#             a_rmvd.append((og - df2.height))
#         pts_removed.append(a_rmvd)
#     ax1.plot(df2["UTC_Start"], df2["O3_ppb"], color="#D9782D")
#     # ax3 = ax1.twinx()
#     # ax3.plot(df2["UTC_Start"], df2["rel d/dt"])
#     cmap = plt.colormaps['GnBu']
#     # fig, ax = plt.subplots(figsize=(8, 4))
#     cs = ax2.pcolormesh(relative, absolute, pts_removed, cmap=cmap, shading="auto")
#     ax2.set_xlabel("Relative d/dt Cutoff")
#     ax2.set_ylabel("Absolute d/dt Cutoff")
#     cbar = fig.colorbar(cs)
#     cbar.ax.set_ylabel("# of outliers")
#     # ax.set_title(date)

# for date, df in tqdm(data["2BTech_205_B"].items()):
#     df = df.filter(pl.col("SamplingLocation").eq("C200_Vent"))
#     if df.is_empty():
#         continue
#     fig, (ax1, ax2) = plt.subplots(2, 1,figsize=(8, 8))
#     ax1.plot(df["UTC_Start"], df["O3_ppb"], color="#1E4D2B")
#     ax1.set_ylabel("O3_ppb", color="#1E4D2B")
#     ax1.spines["left"].set_color("#1E4D2B")
#     ax1.xaxis.set_major_locator(
#         mdates.AutoDateLocator(tz=pytz.timezone("America/Denver"),)
#         # mdates.HourLocator(byhour=[h * 3 for h in range(8)], tz=pytz.timezone("America/Denver"))
#         )
#     ax1.xaxis.set_major_formatter(
#         mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
#         )
#     ax1.tick_params(axis="x", labelrotation=90)
#     ax1.tick_params(axis="y", color="#1E4D2B", labelcolor="#1E4D2B")
#     ax1.set_title(date)
#     ax1.grid(axis="x")
#     og = df.height
#     var = "O3_ppb"
#     pts_removed = []
#     windows = [5 * (i + 1) for i in range(6)]
#     sigmas = [2 + (i / 4) for i in range(5)][::-1]
#     for m in windows:
#         m_rmvd = []
#         for s in sigmas:
#             df2 = df.with_columns(
#                 pl.col(var).shift(-1).sub(pl.col(var)).abs().alias("diff"),
#                 pl.col("UTC_Start").shift(-1).sub(pl.col("UTC_Start")).dt.total_microseconds().truediv(1e6).alias("dt"),
#                 pl.col(var).rolling_median_by("UTC_Start", str(m) + "m").alias("med")
#                 ).with_columns(
#                     pl.col("diff").truediv(pl.col(var)).truediv(pl.col("dt")).abs().alias("rel d/dt"),
#                     pl.col("diff").truediv(pl.col("dt")).alias("d/dt"),
#                     (pl.col(var).sub(pl.col("med"))).abs().alias("abs_diff")
#                     ).with_columns(
#                         pl.col("abs_diff").rolling_median_by("UTC_Start", str(m) + "m").mul(1.4826).alias("mad")
#                         ).with_columns(
#                             pl.col("mad").mul(s).alias("thresh")
#                             ).filter(
#                                 (pl.col(var).sub(pl.col("med")).abs().lt(pl.col("thresh"))
#                                 | (pl.col("d/dt").lt(0.25) & pl.col("rel d/dt").lt(0.005)))
#                                 & (pl.col("d/dt").lt(1) & pl.col("rel d/dt").lt(0.02))
#                                 )
                                
#             m_rmvd.append((og - df2.height))
#         pts_removed.append(m_rmvd)
#     ax1.plot(df2["UTC_Start"], df2["O3_ppb"], color="#D9782D")
#     cmap = plt.colormaps['GnBu']
#     # fig, ax = plt.subplots(figsize=(8, 4))
#     cs = ax2.pcolormesh(sigmas, windows, pts_removed, cmap=cmap, shading="auto")
#     ax2.set_xlabel("sigmas")
#     ax2.set_ylabel("window size (minutes)")
#     cbar = fig.colorbar(cs)
#     cbar.ax.set_ylabel("# of data points removed by hampel filter")
#     # ax.set_title(date)

# IQR of STD of d/dt
for date, df in tqdm(data["2BTech_205_B"].items()):
    df = df.filter(pl.col("SamplingLocation").eq("C200_Vent"))
    if df.is_empty():
        continue
    var = "O3_ppb"
    i = 0
    fig, axs = plt.subplots(2, 2, figsize=(9, 6), sharex=True, sharey=True)
    for window in ["10m", "20m"]:
        j = 0
        df2 = df.with_columns(
            pl.col(var).shift(-1).sub(pl.col(var)).alias("diff"),
            pl.col("UTC_Start").shift(-1).sub(pl.col("UTC_Start")).dt.total_microseconds().truediv(1e6).alias("dt"),
            ).with_columns(
                pl.col("diff").truediv(pl.col("dt")).fill_null(strategy="forward").fill_null(strategy="backward").alias("d/dt"),
                ).with_columns(
                    pl.col("d/dt").rolling_median_by("UTC_Start", window).alias("med_d/dt"),                    
                    pl.col("d/dt").rolling_quantile_by(by="UTC_Start", quantile=0.25, interpolation="linear", window_size=window).alias("lq_d/dt"),
                    pl.col("d/dt").rolling_quantile_by(by="UTC_Start", quantile=0.75, interpolation="linear", window_size=window).alias("uq_d/dt")
                    ).with_columns(
                        pl.col("uq_d/dt").sub(pl.col("lq_d/dt")).alias("iqr_d/dt")
                        )
        for iqrs in [5, 6]:
            df3 = df2.filter(
                pl.col("d/dt").sub(pl.col("med_d/dt")).abs().gt(pl.col("iqr_d/dt").mul(iqrs))
                )
            axs[i][j].scatter(df["UTC_Start"], df[var], color="#D9782D", s=10)
            axs[i][j].scatter(df3["UTC_Start"], df3[var], color="#1E4D2B", s=10)
            axs[i][j].grid(axis="x")
            axs[i][j].text(1.1, 1, str(df3.height), horizontalalignment='right',
                           verticalalignment='top', transform=axs[i][j].transAxes, fontsize="medium")
            if i == 0:
                axs[i][j].set_title(str(iqrs) + " IQRs/sigmas", fontsize="medium")
            if j == 0:
                axs[i][j].set_ylabel(window + " window", fontsize="medium")
            j += 1
        i += 1
    for ax in axs[-1]:
        ax.xaxis.set_major_locator(
            mdates.AutoDateLocator(tz=pytz.timezone("America/Denver"))
            )
        ax.xaxis.set_major_formatter(
            mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
            )
        ax.tick_params(axis="x", labelrotation=90, labelsize="medium")
    fig.suptitle(date, size="xx-large")
    fig.supylabel(var, size="large")

# Hampel + IQR visualization
for date, df in tqdm(data["2BTech_205_B"].items()):
    df = df.filter(pl.col("SamplingLocation").eq("C200_Vent"))
    if df.is_empty():
        continue
    var = "O3_ppb"
    i = 0
    fig, axs = plt.subplots(4, 4, figsize=(9, 6), sharex=True, sharey=True)
    for window in ["10m", "20m", "30m", "40m"]:
        j = 0
        df2 = df.with_columns(
            pl.col(var).rolling_median_by("UTC_Start", window).alias("median"),
            pl.col(var).rolling_quantile_by(by="UTC_Start", quantile=0.25, interpolation="linear", window_size=window).alias("lq"),
            pl.col(var).rolling_quantile_by(by="UTC_Start", quantile=0.75, interpolation="linear", window_size=window).alias("uq")
            ).with_columns(
                pl.col("uq").sub(pl.col("lq")).alias("iqr")
                ).with_columns(
                    (pl.col(var).sub(pl.col("median"))).abs().alias("abs_diff")
                    ).with_columns(
                        pl.col("abs_diff").rolling_median_by("UTC_Start", window).mul(1.4826).alias("mad")
                        )
        for iqrs in [5, 6, 7, 8]:
            df3 = df2.filter(
                pl.col(var).sub(pl.col("median")).abs().gt(pl.col("iqr").mul(iqrs))
                & pl.col(var).sub(pl.col("median")).abs().gt(pl.col("mad").mul(iqrs)))
            axs[i][j].scatter(df["UTC_Start"], df[var], color="#D9782D", s=10)
            axs[i][j].scatter(df3["UTC_Start"], df3[var], color="#1E4D2B", s=10)
            axs[i][j].grid(axis="x")
            axs[i][j].text(1.1, 1, str(df3.height), horizontalalignment='right',
                           verticalalignment='top', transform=axs[i][j].transAxes, fontsize="medium")
            if i == 0:
                axs[i][j].set_title(str(iqrs) + " IQRs/sigmas", fontsize="medium")
            if j == 0:
                axs[i][j].set_ylabel(window + " window", fontsize="medium")
            j += 1
        i += 1
    for ax in axs[-1]:
        ax.xaxis.set_major_locator(
            mdates.AutoDateLocator(tz=pytz.timezone("America/Denver"))
            )
        ax.xaxis.set_major_formatter(
            mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
            )
        ax.tick_params(axis="x", labelrotation=90, labelsize="medium")
    fig.suptitle(date, size="xx-large")
    fig.supylabel(var, size="large")

# Hampel visualization
for date, df in tqdm(data["2BTech_205_B"].items()):
    df = df.filter(pl.col("SamplingLocation").eq("C200_Vent"))
    if df.is_empty():
        continue
    var = "O3_ppb"
    i = 0
    fig, axs = plt.subplots(4, 4, figsize=(9, 6), sharex=True, sharey=True)
    for window in ["5m", "10m", "60m", "120m"]:
        j = 0
        df2 = df.with_columns(
            pl.col(var).rolling_median_by("UTC_Start", window).alias("med")
            ).with_columns(
                (pl.col(var).sub(pl.col("med"))).abs().alias("abs_diff")
                ).with_columns(
                    pl.col("abs_diff").rolling_median_by("UTC_Start", window).mul(1.4826).alias("mad")
                    ).with_columns(
                        pl.col("mad").mul(6).alias("thresh")
                        )
        for sigma in [6, 7, 8, 9]:
            df3 = df2.filter(
                pl.col(var).sub(pl.col("med")).abs().gt(pl.col("mad").mul(sigma))
                )
            axs[i][j].scatter(df["UTC_Start"], df[var], color="#D9782D", s=10)
            axs[i][j].scatter(df3["UTC_Start"], df3[var], color="#1E4D2B", s=10)
            axs[i][j].grid(axis="x")
            axs[i][j].text(1.1, 1, str(df3.height), horizontalalignment='right',
                           verticalalignment='top', transform=axs[i][j].transAxes, fontsize="medium")
            if i == 0:
                axs[i][j].set_title(str(sigma) + " sigmas", fontsize="medium")
            if j == 0:
                axs[i][j].set_ylabel(window + " window", fontsize="medium")
            j += 1
        i += 1
    for ax in axs[-1]:
        ax.xaxis.set_major_locator(
            mdates.AutoDateLocator(tz=pytz.timezone("America/Denver"))
            )
        ax.xaxis.set_major_formatter(
            mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
            )
        ax.tick_params(axis="x", labelrotation=90, labelsize="medium")
    fig.suptitle(date, size="xx-large")
    fig.supylabel(var, size="large")

# Hampel visualization
for date, df in tqdm(data["2BTech_205_B"].items()):
    df = df.filter(pl.col("SamplingLocation").eq("C200_Vent"))
    if df.is_empty():
        continue
    var = "O3_ppb"
    i = 0
    fig, axs = plt.subplots(4, 4, figsize=(9, 6), sharex=True, sharey=True)
    for window in ["10m", "20m", "30m", "40m"]:
        j = 0
        df2 = df.with_columns(
            pl.col(var).shift(-1).sub(pl.col(var)).abs().alias("diff"),
            pl.col("UTC_Start").shift(-1).sub(pl.col("UTC_Start")).dt.total_microseconds().truediv(1e6).alias("dt"),
            pl.col(var).rolling_median_by("UTC_Start", window).alias("med")
            ).with_columns(
                (pl.col(var).sub(pl.col("med"))).abs().alias("abs_diff"),
                pl.col("diff").truediv(pl.col("dt")).alias("d/dt"),
                ).with_columns(
                    pl.col("abs_diff").rolling_median_by("UTC_Start", window).mul(1.4826).alias("mad")
                    )
        for sigma in [6, 7, 8, 9]:
            df3 = df2.filter(
                pl.col(var).sub(pl.col("med")).abs().gt(pl.col("mad").mul(sigma))
                )
            axs[i][j].scatter(df["UTC_Start"], df[var], color="#D9782D", s=10)
            axs[i][j].scatter(df3["UTC_Start"], df3[var], color="#1E4D2B", s=10)
            axs[i][j].grid(axis="x")
            axs[i][j].text(1.1, 1, str(df3.height), horizontalalignment='right',
                           verticalalignment='top', transform=axs[i][j].transAxes, fontsize="medium")
            ax2 = axs[i][j].twinx()
            ax2.scatter(df3["UTC_Start"], df3["d/dt"], color="gray", alpha=0.5, s=10)
            if i == 0:
                axs[i][j].set_title(str(sigma) + " sigmas", fontsize="medium")
            if j == 0:
                axs[i][j].set_ylabel(window + " window", fontsize="medium")
            j += 1
        i += 1
    for ax in axs[-1]:
        ax.xaxis.set_major_locator(
            mdates.AutoDateLocator(tz=pytz.timezone("America/Denver"))
            )
        ax.xaxis.set_major_formatter(
            mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
            )
        ax.tick_params(axis="x", labelrotation=90, labelsize="medium")
    fig.suptitle(date, size="xx-large")
    fig.supylabel(var, size="large")

# Hampel + d/dt filter number of points removed
for date, df in tqdm(data["2BTech_205_B"].items()):
    df = df.filter(pl.col("SamplingLocation").eq("C200_Vent"))
    if df.is_empty():
        continue
    fig, (ax1, ax2) = plt.subplots(2, 1,figsize=(8, 8))
    ax1.plot(df["UTC_Start"], df["O3_ppb"], color="#1E4D2B")
    ax1.set_ylabel("O3_ppb", color="#1E4D2B")
    ax1.spines["left"].set_color("#1E4D2B")
    ax1.xaxis.set_major_locator(
        mdates.AutoDateLocator(tz=pytz.timezone("America/Denver"),)
        # mdates.HourLocator(byhour=[h * 3 for h in range(8)], tz=pytz.timezone("America/Denver"))
        )
    ax1.xaxis.set_major_formatter(
        mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
        )
    ax1.tick_params(axis="x", labelrotation=90)
    ax1.tick_params(axis="y", color="#1E4D2B", labelcolor="#1E4D2B")
    ax1.set_title(date)
    ax1.grid(axis="x")
    og = df.height
    var = "O3_ppb"
    pts_removed = []
    perc_removed = []
    absolute = [0.025 + (i / 40) for i in range(5)][::-1]
    sigmas = [3 + (i / 8) for i in range(16)][::-1]
    for a in absolute:
        a_rmvd = []
        a_perc_rmvd = []
        for s in sigmas:
            df2 = df.with_columns(
                pl.col(var).shift(-1).sub(pl.col(var)).abs().alias("diff"),
                pl.col("UTC_Start").shift(-1).sub(pl.col("UTC_Start")).dt.total_microseconds().truediv(1e6).alias("dt"),
                pl.col(var).rolling_median_by("UTC_Start", "30m").alias("med")
                ).with_columns(
                    pl.col("diff").truediv(pl.col("dt")).alias("d/dt"),
                    (pl.col(var).sub(pl.col("med"))).abs().alias("abs_diff")
                    ).with_columns(
                        pl.col("abs_diff").rolling_median_by("UTC_Start", "30m").mul(1.4826).alias("mad")
                        ).with_columns(
                            pl.col("mad").mul(s).alias("thresh")
                            ).filter(
                                pl.col(var).sub(pl.col("med")).abs().lt(pl.col("thresh"))
                                | pl.col("d/dt").abs().lt(a)
                                )
                                
            a_rmvd.append((og - df2.height))
            a_perc_rmvd.append((og - df2.height) / og)
        pts_removed.append(a_rmvd)
        perc_removed.append(a_perc_rmvd)
    vmin = 0#min(min(x) for x in pts_removed)
    vmax = 0.03 * og#max(max(x) for x in pts_removed)
    vmin2 = 0#min(min(x) for x in perc_removed)
    vmax2 = 0.03#max(max(x) for x in perc_removed)
    ax1.plot(df2["UTC_Start"], df2["O3_ppb"], color="#D9782D")
    cmap = plt.colormaps['GnBu']
    cs = ax2.pcolormesh(sigmas, absolute, pts_removed, cmap=cmap, shading="auto", vmin=vmin, vmax=vmax)
    ax2.set_xlabel("sigmas")
    ax2.set_ylabel("absolute derivative cutoff")
    cbar = fig.colorbar(cs)
    pos = cbar.ax.get_position()
    cax2 = cbar.ax.twinx()
    cax2.set_ylim([vmin2, vmax2])
    pos.x0 += 0.05
    cbar.ax.set_position(pos)
    cax2.set_position(pos)
    cbar.ax.set_ylabel("# of data points removed")
    cbar.ax.yaxis.set_label_position("left")
    cax2.set_ylabel("fraction of data removed")
    cax2.yaxis.set_label_position("right")

# Hampel + d/dt visualization
for date, df in data["2BTech_205_B"].items():
    df = df.filter(pl.col("SamplingLocation").eq("C200_Vent"))
    if df.is_empty():
        continue
    var = "O3_ppb"
    df = df.with_columns(
        pl.col(var).shift(-1).sub(pl.col(var)).abs().alias("diff"),
        pl.col("UTC_Start").shift(-1).sub(pl.col("UTC_Start")).dt.total_microseconds().truediv(1e6).alias("dt"),
        pl.col(var).rolling_median_by("UTC_Start", "10m").alias("med")
        ).with_columns(
            pl.col("diff").truediv(pl.col(var)).truediv(pl.col("dt")).abs().alias("rel d/dt"),
            pl.col("diff").truediv(pl.col("dt")).alias("d/dt"),
            (pl.col(var).sub(pl.col("med"))).abs().alias("abs_diff")
            ).with_columns(
                pl.col("abs_diff").rolling_median_by("UTC_Start", "10m").mul(1.4826).alias("mad")
                ).with_columns(
                    pl.col("mad").mul(6).alias("thresh")
                    )
    df2 = df.filter(
        (pl.col(var).sub(pl.col("med"))).abs().lt(pl.col("thresh"))
        & ((pl.col("d/dt").lt(0.5))
        | (pl.col("rel d/dt").lt(0.01)))
        )
        # pl.col("O3_ppb").is_between(pl.col("med_llim"), pl.col("med_ulim"))
        # & pl.col("O3_ppb").is_between(pl.col("mean_llim"), pl.col("mean_ulim"))
        # )
    df3 = df.filter(
        (pl.col(var).sub(pl.col("med"))).abs().ge(pl.col("thresh"))
         & (pl.col("d/dt").lt(0.5))
         & (pl.col("rel d/dt").lt(0.01))
        )
    fig, ax = plt.subplots()
    ax2 = ax.twinx()
    ax.plot(df["UTC_Start"], df["O3_ppb"], color="#1E4D2B")
    
    ax.plot(df2["UTC_Start"], df2["O3_ppb"], color="#D9782D")
    ax.set_ylabel("O3_ppb", color="#1E4D2B")
    ax.spines["left"].set_color("#1E4D2B")
    ax.xaxis.set_major_locator(
        mdates.AutoDateLocator(tz=pytz.timezone("America/Denver"),)
        # mdates.HourLocator(byhour=[h * 3 for h in range(8)], tz=pytz.timezone("America/Denver"))
        )
    ax.xaxis.set_major_formatter(
        mdates.DateFormatter("%H:%M", tz=pytz.timezone("America/Denver"))
        )
    ax.tick_params(axis="x", labelrotation=90)
    ax.tick_params(axis="y", color="#1E4D2B", labelcolor="#1E4D2B")

    # ax2.scatter(df3["UTC_Start"], df3["rel d/dt"], color="black", alpha=0.5)
    # ax.plot(df["UTC_Start"], df["llim"], color="#D9782D")
    # ax.fill_between(df["UTC_Start"], df["ulim"], df["llim"], color="#D9782D", alpha=0.5)
    # ax.plot(df["UTC_Start"], df["ulim"], color="#D9782D")
    # ax.set_ylabel("med", color="#D9782D")
    ax2.spines["right"].set_color("black")
    ax.spines["left"].set_color("#1E4D2B")
    ax.tick_params(axis="y", colors="#D9782D")
    ax.set_title(date)
    ax.grid(axis="x")
    

