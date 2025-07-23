# -*- coding: utf-8 -*-
"""
Created on Mon Jul 21 16:52:19 2025

@author: agsthk
"""

import os
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pytz

# Declares full path to ResearchInstruments_Data/ directory
data_dir = os.getcwd()
# Starts in ResearchInstruments/ directory
if "ResearchInstruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "ResearchInstruments_Data")

# Full path to directory containing all structured raw data
STRUCT_DATA_DIR = os.path.join(data_dir, "ResearchInstruments_StructuredData")

insts = ["2BTech_202",
         "2BTech_205_A",
         "2BTech_205_B",
         "2BTech_405nm",
         "LI-COR_LI-840A_A",
         "LI-COR_LI-840A_B",
         "Picarro_G2307",
         "TempRHDoor",
         "ThermoScientific_42i-TL"]

# tg_line = ["2BTech_205_A",
#            "Picarro_G2307",
#            "ThermoScientific_42i-TL"]

data = {inst: {} for inst in insts}

for root, dirs, files in os.walk(STRUCT_DATA_DIR):
    for file in files:
        path = os.path.join(root, file)
        for inst in insts:
            if path.find(inst) != -1:
                break
        lf = pl.scan_csv(path, try_parse_dates=True, infer_schema_length=None)
        data[inst][path[-12:-4]] = lf
        # try:
        #     df = pl.read_csv(path, try_parse_dates=True)
        # except pl.exceptions.ComputeError:
        #     df = pl.read_csv(path, try_parse_dates=True,
        #                      infer_schema_length=None)
        # finally:
        #     data[inst].append(df)

# for inst, dfs in data.items():
#     if len(dfs) == 0:
#         continue
#     df = pl.concat(dfs)
#     if inst in tg_line:
#         df = df.with_columns(
#             pl.lit(inst).alias("Instrument")
#             )
#         if "UTC_DateTime" in df.columns:
#             df = df.with_columns(
#                 pl.col("UTC_DateTime").alias("UTC_Start")
#                 )
#     data[inst] = df

# tg_line_data = pl.concat([data[inst] for inst in tg_line],
#                          how="align").sort(by="UTC_Start")

# tg_line_data = tg_line_data.with_columns(
#     pl.when(pl.col("SolenoidValves").fill_null(strategy="forward").eq(1))
#     .then(True)
#     .otherwise(False)
#     .alias("Zeroing")
#     )

# tg_line_data = {
#     key[0]: df for key, df in tg_line_data.partition_by(
#         "Instrument", include_key=False, as_dict=True
#         ).items()
#     }

# for inst, df in tg_line_data.items():
#     tg_line_data[inst] = df.with_columns(
#         pl.col(
#             [col for col in tg_line_data[inst].columns
#              if col in data[inst].columns] + ["Zeroing"]
#             )
#         )
