# -*- coding: utf-8 -*-
"""
Created on Thu Jul 31 12:23:33 2025

@author: agsthk
"""

import os
import polars as pl
import polars.selectors as cs
import matplotlib.pyplot as plt
from tqdm import tqdm
import scipy as sp
import numpy as np

# Declares full path to ResearchInstruments_Data/ directory
data_dir = os.getcwd()
# Starts in ResearchInstruments/ directory
if "ResearchInstruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "ResearchInstruments_Data")

# Full path to directory containing all structured data
STRUCT_DATA_DIR = os.path.join(data_dir, "ResearchInstruments_StructuredData")
# Full path to calibration data
CAL_DIR = os.path.join(data_dir,
                       "ResearchInstruments_ManualData",
                       "ResearchInstruments_Calibrations")

def linear(B, x):
    return B[0] * x + B[1]


def perform_odr(delivered, measured, unc_delivered, unc_measured):
    model = sp.odr.Model(linear)
    unc_delivered = unc_delivered.replace(0, 1e-15)
    unc_measured = unc_measured.replace(0, 1e-15)
    data = sp.odr.RealData(delivered,
                           measured,
                           sx=unc_delivered,
                           sy=unc_measured)
    output = sp.odr.ODR(data, model, beta0=[1, 0]).run()
    sensitivity, offset = output.beta
    unc_sensitivity, unc_offset = output.sd_beta
    return sensitivity, offset, unc_sensitivity, unc_offset

def calc_r2(delivered, measured, sensitivity, offset):
    ideal_measured = linear([sensitivity, offset], delivered)
    ss_residual = ((measured - ideal_measured) ** 2).sum()
    ss_total = ((measured - measured.mean()) ** 2).sum()
    r2 = 1 - (ss_residual / ss_total)
    return r2

cal_inputs = {}

for root, dirs, files in tqdm(os.walk(CAL_DIR)):
    for file in tqdm(files):
        inst = file.split("_CalibrationInputs.txt")[0]
        inst_cal_inputs = pl.read_csv(
            os.path.join(root, file)
            ).with_columns(
                pl.col("Start", "Stop")
                .str.to_datetime()
                ).with_columns(
                    FTC_Date=pl.col("Start")
                    .dt.convert_time_zone("America/Denver")
                    .dt.strftime("%Y%m%d")
                    )
        cal_inputs[inst] = {date[0]: df.with_columns(
            pl.col("Start").dt.offset_by("2m"),
            pl.col("Stop").dt.offset_by("-2m")
            )
            for date, df in inst_cal_inputs.partition_by(
                    "FTC_Date",
                    include_key=False,
                    as_dict=True
                    ).items()}

cal_factors = {}
for inst, inst_cal_inputs in cal_inputs.items():
    inst_cal_factors = []
    for date, inst_cal_input in inst_cal_inputs.items():
        cal_vars = inst_cal_input.select(
            ~cs.contains("Unc") &
            ~cs.datetime()
            ).columns
        for root, dirs, files in tqdm(os.walk(STRUCT_DATA_DIR)):
            for file in tqdm(files):
                if file.find(inst) == -1 or file.find(date) == -1:
                    continue
                try:
                    cal_data = pl.read_csv(os.path.join(root, file))
                except pl.exceptions.ComputeError:
                    cal_data = pl.read_csv(os.path.join(root, file),
                                           infer_schema_length=None)
                if "UTC_Start" in cal_data.columns:
                    left_on = "UTC_Start"
                    stop_compare = "UTC_Stop"
                else:
                    left_on = stop_compare = "UTC_DateTime"
                cal_plot_data = cal_data.with_columns(
                            cs.contains("UTC").str.to_datetime()
                            ).join_asof(inst_cal_input,
                                        left_on=left_on,
                                        right_on="Start",
                                        strategy="backward",
                                        suffix="_Delivered").with_columns(
                                            pl.col("Start")
                                            .add(
                                                pl.col("Stop")
                                                .sub(pl.col("Start"))
                                                .truediv(2)
                                                )
                                            .alias("Mid"),
                                            ).fill_null(np.nan).filter(
                                                pl.col(stop_compare).gt(pl.min("Start"))
                                                & pl.col(left_on).lt(pl.max("Stop"))
                                                )
                cal_data = cal_plot_data.filter(
                    pl.col(stop_compare).lt(pl.col("Stop"))
                    )
                
                for var in cal_vars:
                    var_nounits = var.split("_")[0]
                    fig, ax = plt.subplots()
                    ax.scatter(cal_plot_data[left_on], cal_plot_data[var])
                    ax.errorbar(cal_data["Mid"], cal_data[var + "_Delivered"],
                                xerr=(cal_data["Mid"] - cal_data["Start"]),
                                yerr=cal_data["Unc_" + var],
                                linestyle="",
                                color="black"
                                )
                    ax.set_ylabel(var)
                    ax.set_title(inst + " " + date)
                cal_data = cal_data.group_by(
                    cs.ends_with("Delivered")
                    ).agg(
                        cs.ends_with("Delivered"),
                        cs.starts_with("Unc").mean()
                        .name.suffix("_Delivered"),
                        cs.by_name(cal_vars).mean()
                        .name.suffix("_Measured"),
                        cs.by_name(cal_vars).std()
                        .name.map(lambda c: "Unc_" + c + "_Measured")
                        )
                for var in cal_vars:
                    if var == "NO_ppb":
                        odr_cal_data = cal_data.filter(
                            pl.col("NO2_ppb_Delivered").eq(0)
                            )
                    elif var == "NO2_ppb":
                        odr_cal_data = cal_data.filter(
                            (
                                pl.col("NO_ppb_Delivered").eq(0)
                                & pl.col("NO2_ppb_Delivered").eq(0)
                            )
                            | pl.col("NO2_ppb_Delivered").ne(0)
                            )
                    elif var == "NOx_ppb":
                        continue
                    else:
                        odr_cal_data = cal_data
                    sens, off, unc_sens, unc_off = perform_odr(
                        odr_cal_data[var + "_Delivered"],
                        odr_cal_data[var + "_Measured"],
                        odr_cal_data["Unc_" + var + "_Delivered"],
                        odr_cal_data["Unc_" + var + "_Measured"]
                        )
                    r2 = calc_r2(
                        odr_cal_data[var + "_Delivered"],
                        odr_cal_data[var + "_Measured"],
                        sens,
                        off
                        )
                    inst_cal_factors.append(
                        [date, var_nounits, sens, unc_sens, off, unc_off, r2])
                    
                    fig, ax = plt.subplots(figsize=(5, 5))
                    ax.errorbar(odr_cal_data[var + "_Delivered"],
                                odr_cal_data[var + "_Measured"],
                                xerr=odr_cal_data["Unc_" + var + "_Delivered"],
                                yerr=odr_cal_data["Unc_" + var + "_Measured"],
                                linestyle="")
                    ax.plot(odr_cal_data[var + "_Delivered"],
                            odr_cal_data[var + "_Delivered"] * sens + off)
                    ax.set_title(inst + " " + date)
                    ax.set_xlabel(var + "_Delivered")
                    ax.set_ylabel(var + "_Measured")
                cal_factors[inst] = inst_cal_factors
