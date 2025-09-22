# -*- coding: utf-8 -*-
"""
Created on Thu Jul 31 12:23:33 2025

@author: agsthk
"""

import os
import polars as pl
import polars.selectors as cs
import matplotlib.pyplot as plt
import scipy as sp
import numpy as np
import matplotlib.dates as mdates
from matplotlib import ticker
import pytz

# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")

# Full path to directory containing all structured data
STRUCT_DATA_DIR = os.path.join(data_dir, "Instruments_StructuredData")
# Full path to calibration data
CAL_DIR = os.path.join(data_dir,
                       "Instruments_ManualData",
                       "Instruments_Calibrations")

def linear(B, x):
    return B[0] * x + B[1]

def linear_zero(B, x):
    return B[0] * x


def perform_odr(delivered, measured, unc_delivered, unc_measured):
    model = sp.odr.Model(linear)
    unc_delivered = unc_delivered.replace(0, 1e-15)
    unc_delivered = unc_delivered.replace(None, 1e-15)
    unc_measured = unc_measured.replace(0, 1e-15)
    unc_measured = unc_measured.replace(None, 1e-15)
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

def calc_no2_delivered(nox_delivered,
                       no_measured,
                       no_sensitivity,
                       no_offset,
                       unc_nox_delivered,
                       unc_no_measured,
                       unc_no_sensitivity,
                       unc_no_offset):
    
    no_meas_sub_off = no_measured - no_offset
    unc_no_meas_sub_off = (
        (unc_no_measured ** 2) + (unc_no_offset ** 2)
        ) ** 0.5
    no2_delivered = (no_meas_sub_off / no_sensitivity).rename("NO2_ppb_Delivered")
    unc_no2_delivered = (no2_delivered * (
        (
            ((unc_no_meas_sub_off / no_meas_sub_off) ** 2)
            + ((unc_no_sensitivity / no_sensitivity) ** 2)
        ) ** 0.5
        # SOURCE OF THE DIVIDE BY ZERO ERROR I BELIEVE
        )).rename("Unc_NO2_ppb_Delivered")
    return no2_delivered, unc_no2_delivered

# Defines functions for plotting outputs
def set_ax_ticks(ax, ts=False, round_to=5):
    '''
    Locates and formats x- and y-axis tick labels and draws gridlines

    Parameters
    ----------
    ax : matplotlib.axes._axes.Axes
        Axes of subplot to set tick labels for
    ts : bool, optional
        Used to determine if the x-axis is a time series axis. The default is
        False.
    round_to : int, optional
        Value used for rounding. Rounds the lower bound down to the nearest
        value evenly divisible by round_to and the upper bound up to the
        nearest value evenly divisible by round_to. For time series axis, this
        is in units of minutes. The default is 5.

    Returns
    -------
    None.

    '''
    # Gets default x and y limits
    xlims = np.array(ax.get_xlim())
    ylims = np.array(ax.get_ylim())
    # Rounds x and y limits to nearest 'round_to' value (rounds down for lower
    # limits and up for upper limits)
    for lims in [xlims, ylims]:
        # Rounds to nearest 'round_to' minutes for time series axis
        if ts and (lims == xlims).all():
            lims[0] = np.floor(lims[0] / (round_to / 1440)) * (round_to / 1440)
            lims[1] = np.ceil(lims[1] / (round_to / 1440)) * (round_to / 1440)
            continue
        lims[0] = np.floor(lims[0] / round_to) * round_to
        lims[1] = np.ceil(lims[1] / round_to) * round_to
    # Sets tick locations (and formatting as appropriate) for x and y axes
    for axis in [ax.xaxis, ax.yaxis]:
        if ts and axis == ax.xaxis:
            # axis.set_major_locator(mdates.MinuteLocator(byminute=[0, 30]))

            axis.set_major_locator(mdates.AutoDateLocator(tz=pytz.timezone("America/Denver")))
            # axis.set_minor_locator(mdates.MinuteLocator(interval=10))
            axis.set_major_formatter(mdates.DateFormatter('%H:%M', tz=pytz.timezone("America/Denver")))
            # Sets number of minor tick intervals for time series
            n = 3
        else:
            axis.set_major_locator(ticker.AutoLocator())
            # Does not set a fixed number of minor tick intervals for non-time
            # series axes
            n = None
        axis.set_minor_locator(ticker.AutoMinorLocator(n=n))
    # Turns on major and minor gridlines
    ax.grid(which='major')
    ax.grid(which='minor', linestyle=':')

# Defines list of Colorado State University brand colors
csu_colors = {'csu_green': '#1E4D2B',
              'csu_gold': '#C8C372',
              'aggie_orange': '#D9782D',
              '80_black': '#59595B',
              'white': '#FFFFFF',
              'oval_green': '#006144',
              'lovers_lane': '#82C503',
              'energy_green': '#CFFC00',
              'flower_trial_red': '#E56A54',
              'powdered_purple': '#7E5475',
              'horsetooth_blue': '#008FB3',
              'stalwart_slate': '#105456',
              'sunshine': '#FFC038',
              'black': '#000000',
              'gray': '#CCCCCC',
              'tan': '#E3CDB1'}
# Defines dictionaries for keyword arguments for plotting
# Error bars
ebar_kwargs = {'fmt': 'o', # Marker style
               'linestyle': '', # Turns off lines between points
               'linewidth': 3, # Width of error bars
               'capsize': 3, # Length of error bar caps
               'barsabove': True} # Error bars drawn on top of markers
# Scatter points
scatter_kwargs = {'s': 25, # Marker size
                  'color': csu_colors['csu_gold']}
# Lines
line_kwargs = {'linewidth': 3,
               'color': csu_colors['aggie_orange']}

cal_inputs = {}
os.path.exists(CAL_DIR)
for root, dirs, files in (os.walk(CAL_DIR)):
    for file in (files):
        inst = file.split("_CalibrationInputs.txt")[0]
        if inst == file:
            continue
        inst_cal_inputs = pl.read_csv(
            os.path.join(root, file), comment_prefix="#"
            ).with_columns(
                pl.col("Start", "Stop")
                .str.to_datetime()
                ).with_columns(
                    FTC_Date=pl.col("Start")
                    .dt.convert_time_zone("America/Denver")
                    .dt.strftime("%Y%m%d")
                    )
        cal_inputs[inst] = {date[0]: df
            for date, df in inst_cal_inputs.partition_by(
                    "FTC_Date",
                    include_key=False,
                    as_dict=True
                    ).items()}

cal_factors = {}
for inst, inst_cal_inputs in cal_inputs.items():
    inst_cal_factors = {}
    inst_cal_fig_dir = os.path.join(CAL_DIR,
                                    inst + "_Calibrations",
                                    inst + "_CalibrationFigures")
    if not os.path.exists(inst_cal_fig_dir):
        os.makedirs(inst_cal_fig_dir)
    for date, inst_cal_input in inst_cal_inputs.items():
        date_cal_factors = {}
        cal_vars = inst_cal_input.select(
            ~cs.contains("Unc") &
            ~cs.datetime()
            ).columns
        for root, dirs, files in (os.walk(STRUCT_DATA_DIR)):
            for file in (files):
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
                                            pl.col("Stop")
                                            .sub(pl.col("Start"))
                                            .truediv(2)
                                            .alias("DeltaT")
                                            ).filter(
                                                pl.col(stop_compare).gt(pl.min("Start"))
                                                & pl.col(left_on).lt(pl.max("Stop"))
                                                )
                cal_input_data = cal_plot_data.filter(
                    pl.col(stop_compare).lt(pl.col("Stop"))
                    )
                cal_input_data.columns
                cal_data = cal_input_data.group_by(
                    cs.ends_with("Delivered"),
                    pl.col("Mid"),
                    pl.col("DeltaT")
                    ).agg(
                        cs.ends_with("Delivered"),
                        cs.starts_with("Unc").mean()
                        .name.suffix("_Delivered"),
                        cs.by_name(cal_vars).mean()
                        .name.suffix("_Measured"),
                        cs.by_name(cal_vars).std()
                        .name.map(lambda c: "Unc_" + c + "_Measured"),
                        pl.len().alias("N")
                        )
                avg_t = (cal_plot_data.select(
                    pl.col("UTC_DateTime")
                    .sub(pl.col("UTC_DateTime").shift(1))
                    .dt.total_seconds().cast(pl.String)
                    ).filter(
                        pl.col("UTC_DateTime").ne("0")
                        )["UTC_DateTime"].mode().item() + "s")
                if inst == "Picarro_G2307":
                    avg_t = "1s"
                date_cal_factors["AveragingTime"] = avg_t
                        
                # Identifies instrument temperature data
                temp_data = cal_plot_data.select(
                    cs.contains("UTC", "CellTemp", "InternalTemp", "CavityTemp")
                    )
                # Identifies the column name for temperature data
                temp_col = temp_data.columns[-1]
                # Assigns unique ID every time instrument temperature changes
                temp_data = temp_data.with_columns(
                    pl.col(temp_col).rle_id().alias("ID")
                    )
                # Calculates d(Temp)/dt
                ddt_data = temp_data.filter(
                        pl.col("ID").is_first_distinct()
                        ).with_columns(
                            pl.col(temp_col).shift(-1).sub(pl.col(temp_col)).alias("dTemp"),
                            pl.col(left_on).shift(-1).sub(pl.col(left_on)).dt.total_microseconds().truediv(1e6).alias("dt")
                            ).select(
                                pl.col("ID"),
                                pl.col("dTemp").truediv(pl.col("dt")).alias("d/dt")
                                )
                temp_data = temp_data.join(ddt_data, on="ID", how="left")
                date_cal_factors[temp_col] = {
                    "Min": temp_data[temp_col].min(),
                    "Max": temp_data[temp_col].max(),
                    "MedianDerivative": ddt_data["d/dt"].median()
                    }
                # Plots temperature data over the calibration
                temp_fig, temp_ax = plt.subplots(figsize=(8, 6))
                change_ax = temp_ax.twinx()
                change_ax.scatter(
                    temp_data[left_on],
                    temp_data["d/dt"],
                    s=25,
                    color="#D9782D")
                temp_ax.scatter(
                    temp_data[left_on],
                    temp_data[temp_col],
                    s=25,
                    color="#1E4D2B"
                    )
                temp_ax.set_zorder(change_ax.get_zorder() + 1)
                temp_ax.patch.set_visible(False)
                set_ax_ticks(temp_ax, ts=True)
                temp_ax.set_title(date)
                temp_ax.set_ylabel(temp_col.replace("_", " (") + ")", color="#1E4D2B")
                change_ax.set_ylabel("d(" + temp_col.split("_")[0] + ")/dt (C/s)", color="#D9782D")
                temp_fig.suptitle(inst.replace("_", " ") + " Instrument Temperature Calibration Time Series",
                                  size=15)
                temp_fig_name = inst + "_TemperatureCalibrationTimeSeries_" + date + ".png"
                temp_fig.savefig(os.path.join(
                    inst_cal_fig_dir,
                    temp_fig_name
                    ))
                plt.close()
                        
                for var in cal_vars:
                    if var == "NO_ppb":
                        odr_cal_data = cal_data.filter(
                            pl.col("NO2_ppb_Delivered").eq(0)
                            )
                    elif var == "NO2_ppb":
                        no2_delivered, unc_no2_delivered = calc_no2_delivered(
                            cal_data["NOx_ppb_Delivered"],
                            cal_data["NO_ppb_Measured"],
                            date_cal_factors["NO_ppb"]["Sensitivity"],
                            date_cal_factors["NO_ppb"]["Offset"],
                            cal_data["Unc_NOx_ppb_Delivered"],
                            cal_data["Unc_NO_ppb_Measured"],
                            date_cal_factors["NO_ppb"]["Sensitivity_Uncertainty"],
                            date_cal_factors["NO_ppb"]["Offset_Uncertainty"]
                            )
                        cal_data = cal_data.with_columns(
                            pl.when(pl.col("NO2_ppb_Delivered").is_null())
                            .then(no2_delivered)
                            .otherwise(pl.col("NO2_ppb_Delivered"))
                            .alias("NO2_ppb_Delivered"),
                            pl.when(pl.col("Unc_NO2_ppb_Delivered").is_null())
                            .then(unc_no2_delivered)
                            .otherwise(pl.col("Unc_NO2_ppb_Delivered"))
                            .alias("Unc_NO2_ppb_Delivered"),
                            )
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
                    if odr_cal_data.is_empty():
                        continue
                    var_name, var_units = var.split("_")
                    var_nounits = var_name
                    for i, char in enumerate(var_name):
                        if char.isnumeric():
                            var_name = var_name[:i] + "$_" + char + "$" + var_name[i + 1:]
                            
                    cal_plot_data = cal_plot_data.filter(
                        pl.col(var).gt(-2000)
                        )
                    
                    ts_fig, ts_ax = plt.subplots(figsize=(8, 6))
                    ts_ax.scatter(cal_plot_data[left_on],
                                  cal_plot_data[var],
                                  label="Measured [" + var_name + "]",
                                  **scatter_kwargs)
                    ts_ax.errorbar(odr_cal_data["Mid"],
                                   odr_cal_data[var + "_Delivered"],
                                   xerr=odr_cal_data["DeltaT"],
                                   yerr=odr_cal_data["Unc_" + var + "_Delivered"],
                                   label="Delivered [" + var_name + "]",
                                   color="#D9782D",
                                   **ebar_kwargs)
                    ts_ax.errorbar(odr_cal_data["Mid"],
                                   odr_cal_data[var + "_Measured"],
                                   xerr=odr_cal_data["DeltaT"],
                                   yerr=odr_cal_data["Unc_" + var + "_Measured"],
                                   label="Mean Measured [" + var_name + "]",
                                   color="#1E4D2B",
                                   **ebar_kwargs)
                    
                    set_ax_ticks(ts_ax, ts=True)
                    ts_ax.legend(framealpha=1, edgecolor='black')
                    ts_ax.set_title(date)
                    ts_ax.set_ylabel('[' + var_name + '] (' + var_units + ")")

                    ts_fig.suptitle(inst.replace("_", " ") + " " + var_name + " Calibration Time Series",
                                    size=15)
                    ts_fig_name = inst + "_" + var_nounits + "_CalibrationTimeSeries_" + date + ".png"

                    ts_fig.savefig(os.path.join(
                        inst_cal_fig_dir,
                        ts_fig_name
                        ))
                    
                    plt.close()
                    
                    # Identifies delivery concentrations that were repeated
                    duplicates = odr_cal_data.filter(
                        pl.col(var + "_Delivered").is_duplicated()
                        ).unique(
                            subset=var + "_Delivered"
                            )[var + "_Delivered"]
                    # For repeated delivery concentrations, calculates pooled
                    # statistics and replaces original rows with one combined
                    # row
                    for conc in duplicates:
                        conc_data = odr_cal_data.filter(
                            pl.col(var + "_Delivered").eq(conc)
                            ).select(
                                cs.contains("Measured", "Delivered"),
                                pl.col("N"),
                                pl.col("N").sub(1).alias("DOF")
                                ).with_columns(
                                    (cs.contains("Unc").pow(2))
                                    .mul(pl.col("DOF")),
                                    (cs.contains("Measured", "Delivered")
                                     & ~cs.contains("Unc")).mul(pl.col("N"))
                                    ).with_columns(
                                        pl.all().sum(),
                                        ).with_columns(
                                            (cs.contains("Unc")
                                             .truediv(pl.col("DOF"))).sqrt(),
                                            (cs.contains("Measured",
                                                         "Delivered")
                                             & ~cs.contains("Unc"))
                                            .truediv(pl.col("N"))
                                            ).select(
                                                pl.exclude("DOF")
                                                ).unique()
                        # Combines existing DataFrame with current delivery
                        # concentrations excluded with combined row for current
                        # delivery concentration
                        odr_cal_data = pl.concat(
                            [odr_cal_data.filter(
                                pl.col(var + "_Delivered").ne(conc)
                                ),
                            conc_data],
                            how="diagonal_relaxed")

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
                    date_cal_factors[var] = {
                        "Sensitivity": sens,
                        "Offset": off,
                        "Sensitivity_Uncertainty": unc_sens,
                        "Offset_Uncertainty": unc_off,
                        "R2": r2
                        }
                    fit_label = ('['
                     + var_name
                     + ']$_{measured}$ = '
                     + f'{sens:.3f}'
                     + r'$\cdot$ ['
                     + var_name
                     + ']$_{delivered}$ ')
                    if off > 0: # Add zero
                        fit_label += f'+ {off:.3f}\nR' + '$^2$ = ' + f'{r2:.4f}'
                    else: # Subtract negative zero
                        fit_label += f'\u2212 {-off:.3f}\nR' + '$^2$ = ' + f'{r2:.4f}'
                    fit_fig, fit_ax = plt.subplots(figsize=(6, 6))
                    fit_ax.errorbar(
                        odr_cal_data[var + "_Delivered"],
                        odr_cal_data[var + "_Measured"],
                        xerr=odr_cal_data["Unc_" + var + "_Delivered"],
                        yerr=odr_cal_data["Unc_" + var + "_Measured"],
                        color=csu_colors['csu_green'],
                        **ebar_kwargs)
                    fit_ax.plot(odr_cal_data[var + "_Delivered"], odr_cal_data[var + "_Delivered"] * sens + off,
                                **line_kwargs)
                    
                    fit_ax.text(0.05, 0.95, fit_label, va='top',
                                transform=fit_ax.transAxes, bbox={'facecolor': 'white',
                                                                  'alpha': 1})
                    set_ax_ticks(fit_ax)
                    fit_ax.set_xlabel('[' + var_name + '] Delivered (' + var_units + ")")
                    fit_ax.set_ylabel('[' + var_name + '] Measured (' + var_units + ")")
                    fit_ax.set_title(date)
                    fit_fig.suptitle(inst.replace("_", " ") + " " + var_name + " Calibration Fit",
                                    size=15)
                    
                    fit_fig.savefig(os.path.join(
                        inst_cal_fig_dir,
                        inst + "_" + var_nounits + "_CalibrationODR_" + date + ".png"
                        ))
                    
                    plt.close()

                    model = sp.odr.Model(linear)
                    data = sp.odr.RealData(
                        odr_cal_data[var + "_Measured"],
                        odr_cal_data["Unc_" + var + "_Measured"]
                        )
                    output = sp.odr.ODR(data, model, beta0=[0.005, 0]).run()
                    ns_slope, ns_off = output.beta
                    ns_slope_unc, ns_off_unc = output.sd_beta
                    
                    ns_r2 = calc_r2(
                        odr_cal_data[var + "_Measured"],
                        odr_cal_data["Unc_" + var + "_Measured"],
                        ns_slope,
                        ns_off
                        )
                    
                    date_cal_factors[var]["NoiseSignal_Slope"] = ns_slope
                    date_cal_factors[var]["NoiseSignal_Offset"] = ns_off
                    date_cal_factors[var]["NoiseSignal_Slope_Uncertainty"] = ns_slope_unc
                    date_cal_factors[var]["NoiseSignal_Offset_Uncertainty"] = ns_off_unc
                    date_cal_factors[var]["NoiseSignal_R2"] = ns_r2

                    fit_label = (var_name
                                 + " Noise = "
                                 + f'{ns_slope:.5f}'
                                 + r' $\cdot$ ('
                                 + var_name
                                 + " Signal)"
                                 )
                    if ns_off > 0: # Add zero
                        fit_label += f'+ {ns_off:.3f}\nR' + '$^2$ = ' + f'{ns_r2:.4f}'
                    else: # Subtract negative zero
                        fit_label += f'\u2212 {-ns_off:.3f}\nR' + '$^2$ = ' + f'{ns_r2:.4f}'
                    
                    if var.find("NO") != -1:
                        odr_cal_data = odr_cal_data.filter(
                            odr_cal_data[var + "_Delivered"].lt(70)
                            )
                    
                    ns_fig, ns_ax = plt.subplots(figsize=(6, 6))
                    ns_ax.errorbar(
                        odr_cal_data[var + "_Measured"],
                        odr_cal_data["Unc_" + var + "_Measured"],
                        color=csu_colors['csu_green'],
                        **ebar_kwargs)
                    ns_ax.plot(odr_cal_data[var + "_Measured"], odr_cal_data[var + "_Measured"] * ns_slope + ns_off,
                                **line_kwargs)
                    ns_ax.text(0.01, 1.06, fit_label, va='top', ha="left",
                                transform=ns_ax.transAxes, bbox={'facecolor': 'white',
                                                                  'alpha': 1})
                    set_ax_ticks(ns_ax)
                    ns_ax.set_xlabel(var_name + ' Signal (' + var_units + ")")
                    ns_ax.set_ylabel(var_name + ' Noise (' + var_units + ")")
                    ns_ax.set_title(date, loc="right")
                    ns_fig.suptitle(inst.replace("_", " ") + " " + var_name + " Signal to Noise Fit",
                                    size=15)
                    
                    ns_fig.savefig(os.path.join(
                        inst_cal_fig_dir,
                        inst + "_" + var_nounits + "_CalibrationSNR_" + date + ".png"
                        ))
                    plt.close()
                    
                    # Uses the standard deviation of the zero measurements to
                    # calculate a "default" limit of detection
                    zero_data = odr_cal_data.filter(
                        pl.col(var + "_Delivered").eq(0)
                        )
                    if len(zero_data) == 0:
                        continue
                    lod = zero_data["Unc_" + var + "_Measured"].item() * 3
                    date_cal_factors[var]["LOD"] = lod
              
                inst_cal_factors[date] = date_cal_factors
    cal_factors[inst] = inst_cal_factors

#%%
for inst, factors in cal_factors.items():
    inst_cal_results = []
    for date, results in factors.items():
        date_results = []
        for species, result in results.items():
            if type(result) is str:
                date_results.append(
                    pl.DataFrame([result], schema=[species]).with_columns(
                        pl.lit(date).alias("CalDate")
                        )
                    )
                continue
            date_results.append(
                pl.DataFrame(result).select(
                    pl.lit(date).alias("CalDate"),
                    pl.all().name.prefix(species + "_")
                    )
                )
        inst_cal_results.append(pl.concat(date_results, how="align"))
    inst_cal_results = pl.concat(inst_cal_results, how="diagonal").sort(by="CalDate").select(
        pl.col("CalDate"),
        cs.contains("pp", "perc"),
        ~cs.contains("pp", "perc", "CalDate")
        )
    inst_cal_results.write_csv(os.path.join(CAL_DIR,
                                            inst + "_Calibrations",
                                            inst + "_CalibrationResults.txt"),
                               float_precision=6)
          
