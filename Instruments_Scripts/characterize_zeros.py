# -*- coding: utf-8 -*-
"""
Created on Mon Aug 18 11:48:33 2025

@author: agsthk
"""

import os
import polars as pl
import polars.selectors as cs
from tqdm import tqdm

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all clean raw data
CLEAN_DATA_DIR = os.path.join(data_dir, "Instruments_CleanData")
# Full path to directory containing zero results
ZERO_RESULTS_DIR = os.path.join(data_dir, "Instruments_DerivedData")


insts = ["2BTech_205_A",
         "2BTech_405nm",
         "Picarro_G2307",
         "ThermoScientific_42i-TL"]

uza_stats = {inst: [] for inst in insts}

for root, dirs, files in tqdm(os.walk(CLEAN_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        for inst in insts:
            if path.find(inst) != -1:
                break
        if path.find(inst) == -1:
            continue
        try:
            df = pl.read_csv(path)
        except pl.exceptions.ComputeError:
            df = pl.read_csv(path, infer_schema_length=None)
        df = df.with_columns(
            cs.contains("UTC").str.to_datetime(),
            intv=pl.col("SamplingLocation").rle_id()
            )
        df = df.filter(
            pl.col("SamplingLocation").eq("UZA")
            )
        if df.height == 0:
            continue
        uza_stats[inst].append(df)
        
for inst, dfs in uza_stats.items():
    if len(dfs) == 0:
        continue
    df = pl.concat(
        dfs,
        how="diagonal_relaxed"
        ).sort(
            by=cs.contains("UTC")
            ).with_columns(
                pl.col("intv").rle_id()
                )
    df = df.group_by("intv").agg(
        cs.contains("UTC_Start").min(),
        cs.contains("UTC_Stop").max(),
        cs.contains("UTC_DateTime").min().alias("UTC_Start"),
        cs.contains("UTC_DateTime").max().alias("UTC_Stop"),
        cs.contains("_pp", "_perc").mean().name.suffix("_Mean"),
        cs.contains("_pp", "_perc").std().name.suffix("_STD")
        ).select(
            ~cs.contains("NOx", "intv")
            )
    uza_stats[inst] = df
    
for inst, df in uza_stats.items():
    if isinstance(df, list):
        continue
    folder = inst + "_DerivedData"
    direct = os.path.join(ZERO_RESULTS_DIR, folder)
    if not os.path.exists(direct):
        os.makedirs(direct)
    file = inst + "_UZAStatistics.csv"
    path = os.path.join(direct, file)
    df.write_csv(path)