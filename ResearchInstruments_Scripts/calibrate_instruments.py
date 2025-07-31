# -*- coding: utf-8 -*-
"""
Created on Thu Jul 31 12:23:33 2025

@author: agsthk
"""

import os
import polars as pl
import matplotlib.pyplot as plt
from tqdm import tqdm
import scipy as sp


# Declares full path to ResearchInstruments_Data/ directory
data_dir = os.getcwd()
# Starts in ResearchInstruments/ directory
if "ResearchInstruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "ResearchInstruments_Data")

# Full path to directory containing all structured data
STRUCT_DATA_DIR = os.path.join(data_dir, "ResearchInstruments_StructuredData")

def linear(B, x):
    return B[0] * x + B[1]

cal_info = {
    "2BTech_202": {
        "20240118": [
            ["2024-01-18T13:42:00-0700", "2024-01-18T14:42:00-0700", 0, 2],
            ["2024-01-18T14:42:00-0700", "2024-01-18T15:12:00-0700", 30, 2],
            ["2024-01-18T15:12:00-0700", "2024-01-18T15:42:00-0700", 60, 2],
            ["2024-01-18T15:42:00-0700", "2024-01-18T16:12:00-0700", 90, 2],
            ["2024-01-18T16:12:00-0700", "2024-01-18T16:42:00-0700", 120, 2.4],
            ["2024-01-18T16:42:00-0700", "2024-01-18T17:12:00-0700", 150, 3]
            ]
        },
    "2BTech_205_A": {
        "20240119": [
            ["2024-01-19T10:21:00-0700", "2024-01-19T11:21:00-0700", 0 , 2],
            ["2024-01-19T11:21:00-0700", "2024-01-19T11:51:00-0700", 30, 2],
            ["2024-01-19T11:51:00-0700", "2024-01-19T12:21:00-0700", 60, 2],
            ["2024-01-19T12:21:00-0700", "2024-01-19T12:51:00-0700", 90, 2],
            ["2024-01-19T12:51:00-0700", "2024-01-19T13:21:00-0700", 120, 2.4],
            ["2024-01-19T13:21:00-0700", "2024-01-19T13:51:00-0700", 150, 3]
            ],
        "20240604": [
            ["2024-06-04T10:28:00-0600", "2024-06-04T10:48:00-0600", 30, 2],
            ["2024-06-04T10:48:00-0600", "2024-06-04T11:08:00-0600", 60, 2],
            ["2024-06-04T11:08:00-0600", "2024-06-04T11:28:00-0600", 90, 2],
            ["2024-06-04T11:28:00-0600", "2024-06-04T11:48:00-0600", 120, 2.4],
            ["2024-06-04T11:48:00-0600", "2024-06-04T12:08:00-0600", 150, 3],
            ["2024-06-04T12:08:00-0600", "2024-06-04T12:49:00-0600", 0 , 2],
            ],
        "20250115": [
            ["2025-01-15T09:03:00-0700", "2025-01-15T09:23:00-0700", 0 , 2],
            ["2025-01-15T09:23:00-0700", "2025-01-15T09:33:00-0700", 30, 2],
            ["2025-01-15T09:33:00-0700", "2025-01-15T09:43:00-0700", 60, 2],
            ["2025-01-15T09:43:00-0700", "2025-01-15T09:53:00-0700", 90, 2],
            ["2025-01-15T09:53:00-0700", "2025-01-15T10:03:00-0700", 120, 2.4],
            ["2025-01-15T10:03:00-0700", "2025-01-15T10:13:00-0700", 150, 3]
            ]
        },
    "2BTech_205_B": {
        "20240604": [
            ["2024-06-04T13:00:00-0600", "2024-06-04T13:27:00-0600", 0, 2],
            ["2024-06-04T13:27:00-0600", "2024-06-04T13:47:00-0600", 30, 2],
            ["2024-06-04T13:47:00-0600", "2024-06-04T14:07:00-0600", 60, 2],
            ["2024-06-04T14:07:00-0600", "2024-06-04T14:27:00-0600", 90, 2.4],
            ["2024-06-04T14:27:00-0600", "2024-06-04T14:47:00-0600", 120, 3],
            ["2024-06-04T14:47:00-0600", "2024-06-04T15:07:00-0600", 150, 2],
            ],
        "20250115": [
            ["2025-01-15T10:17:00-0700", "2025-01-15T10:37:00-0700", 0 , 2],
            ["2025-01-15T10:37:00-0700", "2025-01-15T10:47:00-0700", 30, 2],
            ["2025-01-15T10:47:00-0700", "2025-01-15T10:57:00-0700", 60, 2],
            ["2025-01-15T10:57:00-0700", "2025-01-15T11:07:00-0700", 90, 2],
            ["2025-01-15T11:07:00-0700", "2025-01-15T11:17:00-0700", 120, 2.4],
            ["2025-01-15T11:17:00-0700", "2025-01-15T11:27:00-0700", 150, 3]
            ]
        }
        
    }

for inst, dic in cal_info.items():
    for date, concs in dic.items():
        conc = pl.DataFrame(
            concs,
            schema={"Start": pl.String(),
                    "Stop": pl.String(),
                    "O3_ppb": pl.Float64(),
                    "Uncertainty_O3_ppb": pl.Float64()},
            orient="row"
            ).with_columns(
                pl.col("Start", "Stop").str.to_datetime()
                )
        conc = conc.with_columns(
            pl.col("Start").dt.offset_by("2m"),
            pl.col("Stop").dt.offset_by("-2m")
            )
        dic[date] = conc
    cal_info[inst] = dic

for inst, datecal in cal_info.items():
    for date, cal in datecal.items():
        for root, dirs, files in tqdm(os.walk(STRUCT_DATA_DIR)):
            for file in tqdm(files):
                if file.find(inst) == -1 or file.find(date) == -1:
                    continue
                cal_data = pl.read_csv(
                    os.path.join(root, file)
                    ).with_columns(
                        pl.selectors.contains("UTC").str.to_datetime()
                        )
                
                calib = cal_data.join_asof(cal,
                                           left_on="UTC_Start",
                                           right_on="Start",
                                           strategy="backward",
                                           suffix="_delivered").filter(
                                               pl.col("UTC_Stop").lt(pl.col("Stop"))
                                               )
                calib = calib.with_columns(
                    pl.col("Start").add(pl.col("Stop").sub(pl.col("Start")).truediv(2))
                    .alias("Mid")
                    )
                fig, ax = plt.subplots()
                ax.scatter(calib["UTC_Start"], calib["O3_ppb"])
                ax.errorbar(calib["Mid"], calib["O3_ppb_delivered"],
                            xerr=(calib["Mid"] - calib["Start"]),
                            yerr=calib["Uncertainty_O3_ppb"],
                            linestyle="",
                            color="black"
                            )
                calib = calib.group_by("O3_ppb_delivered").agg(
                    pl.col("Uncertainty_O3_ppb").mean(),
                    pl.col("O3_ppb").mean().alias("O3_ppb_measured"),
                    pl.col("O3_ppb").std().alias("O3_ppb_measured_uncertainty")
                    )
                ax.set_title(inst + " " + date)

                model = sp.odr.Model(linear)
                data = sp.odr.RealData(calib["O3_ppb_delivered"], calib["O3_ppb_measured"],
                                       calib["Uncertainty_O3_ppb"],
                                       calib["O3_ppb_measured_uncertainty"])
                output = sp.odr.ODR(data, model, beta0=[1, 0]).run()
                
                slope, intercept = output.beta
                unc_slope, unc_intercept = output.sd_beta
                
                fig, ax = plt.subplots()
                ax.errorbar(calib["O3_ppb_delivered"], calib["O3_ppb_measured"],
                           xerr=calib["Uncertainty_O3_ppb"],
                           yerr=calib["O3_ppb_measured_uncertainty"],
                           linestyle="")
                ax.plot(calib["O3_ppb_delivered"],
                        calib["O3_ppb_delivered"] * slope + intercept)
                ax.set_title(inst + " " + date)