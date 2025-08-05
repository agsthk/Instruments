# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 13:33:00 2025

@author: agsthk
"""

import os
import polars as pl
from datetime import datetime
import pytz
from tqdm import tqdm

research_project = "KeckO3"
calibrated_sources = ["2BTech_205_A",
                      "2BTech_205_B",
                      "Picarro_G2307",
                      "ThermoScientific_42i-TL"]
clean_sources = ["Aranet4_1F16F",
                 "Aranet4_1FB20",
                 "LI-COR_LI-840A_A",
                 "LI-COR_LI-840A_B",
                 "TempRHDoor"]
derived_sources = ["AdditionValves",
                   "TempRHDoor"]
start_date = datetime(2025, 1, 17)
stop_date = datetime(2025, 5, 6)
# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")
# Full path to directory containing all calibrated clean data
CALIBRATED_DATA_DIR = os.path.join(data_dir, "Instruments_CalibratedData")
# Full path to directory containing all clean raw data
CLEAN_DATA_DIR = os.path.join(data_dir, "Instruments_CleanData")
# Full path to directory containing all derived data
DERIVED_DATA_DIR =  os.path.join(data_dir, "Instruments_DerivedData")
# Full path to Graduate_ResearchProjects directory
PROJECT_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(data_dir)),
    research_project,
    research_project + "_Data"
    )

for root, dirs, files in tqdm(os.walk(CLEAN_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        for inst in clean_sources:
            if path.find(inst) != -1:
                break
        if path.find(inst) == -1:
            continue
        file_date = datetime.strptime(file[-12:-4], "%Y%m%d")
        if file_date < start_date or file_date > stop_date:
            continue
        df = pl.read_csv(path).with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC"),
            pl.selectors.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        if "SamplingLocation" in df.columns:
            df = df.filter(
                pl.col("SamplingLocation").is_not_null()
                )
        if df.is_empty():
            continue
        new_dir = os.path.join(PROJECT_DATA_DIR,
                               inst + "_" + research_project + "Data"
                                )
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        new_name = inst + "_" + research_project + "Data_" + file[-12:]
        new_path = os.path.join(new_dir, new_name)
        df.write_csv(new_path)

for root, dirs, files in tqdm(os.walk(CALIBRATED_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        for inst in calibrated_sources:
            if path.find(inst) != -1:
                break
        if path.find(inst) == -1:
            continue
        file_date = datetime.strptime(file[-12:-4], "%Y%m%d")
        if file_date < start_date or file_date > stop_date:
            continue
        df = pl.read_csv(path).with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC"),
            pl.selectors.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        if "SamplingLocation" in df.columns:
            df = df.filter(
                pl.col("SamplingLocation").is_not_null()
                )
        if df.is_empty():
            continue
        new_dir = os.path.join(PROJECT_DATA_DIR,
                               inst + "_" + research_project + "Data"
                                )
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        new_name = inst + "_" + research_project + "Data_" + file[-12:]
        new_path = os.path.join(new_dir, new_name)
        df.write_csv(new_path)

start_date = start_date.replace(
    tzinfo=pytz.timezone("America/Denver")
    ).astimezone(pytz.UTC)
stop_date = stop_date.replace(
    tzinfo=pytz.timezone("America/Denver")
    ).astimezone(pytz.UTC)
        
for root, dirs, files in tqdm(os.walk(DERIVED_DATA_DIR)):
    for file in tqdm(files):
        if file.startswith("."):
            continue
        path = os.path.join(root, file)
        for inst in derived_sources:
            if path.find(inst) != -1:
                break
        if path.find(inst) == -1:
            continue
        df = pl.read_csv(path).with_columns(
            pl.selectors.contains("UTC").str.to_datetime(time_zone="UTC")
            )
        df = df.filter(
            pl.col("UTC_Stop").gt(start_date)
            & pl.col("UTC_Start").lt(stop_date)
            )
        if df.is_empty():
            continue
        new_dir = os.path.join(PROJECT_DATA_DIR,
                               inst + "_" + research_project + "Data"
                               )
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        new_name = inst + "_" + research_project + "Data_" + file.split("_")[-1]
        new_path = os.path.join(new_dir, new_name)
        df.write_csv(new_path)