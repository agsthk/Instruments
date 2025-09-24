# -*- coding: utf-8 -*-
"""
Created on Mon Aug 18 11:48:33 2025

@author: agsthk
"""

import os
import polars as pl
import polars.selectors as cs
from tqdm import tqdm
import matplotlib.pyplot as plt
from matplotlib import ticker
import scipy as sp
import hvplot.polars

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

def linear(B, x):
    return B[0] * x + B[1]

def perform_odr(x, y, unc_x, unc_y, beta0):
    model = sp.odr.Model(linear)
    if unc_x is not None:
        unc_x = unc_x.replace(0, 1e-15)
        unc_x = unc_x.replace(None, 1e-15)
    if unc_y is not None:
        unc_y = unc_y.replace(0, 1e-15)
        unc_y = unc_y.replace(None, 1e-15)
    data = sp.odr.RealData(x,
                           y,
                           sx=unc_x,
                           sy=unc_y)
    output = sp.odr.ODR(data, model, beta0=beta0).run()
    sensitivity, offset = output.beta
    unc_sensitivity, unc_offset = output.sd_beta
    return sensitivity, offset, unc_sensitivity, unc_offset

def calc_r2(x, y, sensitivity, offset):
    ideal_y = linear([sensitivity, offset], x)
    ss_residual = ((y - ideal_y) ** 2).sum()
    ss_total = ((y - y.mean()) ** 2).sum()
    r2 = 1 - (ss_residual / ss_total)
    return r2

ebar_kwargs = {'fmt': 'o', # Marker style
               'linestyle': '', # Turns off lines between points
               'linewidth': 3, # Width of error bars
               'capsize': 3, # Length of error bar caps
               'barsabove': False,
               "mec": "black"} # Error bars drawn on top of markers
line_kwargs = {'linewidth': 3,
               'color': "#D9782D"}

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
            )
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
                intv=pl.col("SamplingLocation").rle_id()
                ).filter(
                    pl.col("SamplingLocation").eq("UZA")
                    )
    length = df.group_by("intv").len()
    df = df.group_by("intv").agg(
        cs.contains("UTC_Start").min(),
        cs.contains("UTC_Stop").max(),
        cs.contains("UTC_DateTime").min().alias("UTC_Start"),
        cs.contains("UTC_DateTime").max().alias("UTC_Stop"),
        cs.contains("_pp", "_perc").mean().name.suffix("_Mean"),
        cs.contains("_pp", "_perc").std().name.suffix("_STD"),
        cs.contains("_C").mean().name.suffix("_Mean"),
        cs.contains("_C").std().name.suffix("_STD")
        ).join(
            df.group_by("intv").len(),
            on="intv",
            how="full",
            coalesce=True,
            validate="1:1",
            maintain_order="left"
            ).filter(
                pl.col("len").gt(2)
                ).select(
                    ~cs.contains("NOx", "30", "2min", "5min", "intv", "len")
                    )
    uza_stats[inst] = df

hvplot.show(
    uza_stats["ThermoScientific_42i-TL"].hvplot.scatter(
        x="UTC_Start",
        y="NO_ppb_Mean"
        )
    * uza_stats["ThermoScientific_42i-TL"].hvplot.scatter(
        x="UTC_Stop",
        y="NO_ppb_Mean"
        )
    )

temps = ["CellTemp_C", "CavityTemp_C", "InternalTemp_C", "ChamberTemp_C"]

temp_corr = {}
lod_corr = {}
for inst, df in uza_stats.items():
    inst_temp_corr = {}
    year = 2025
    df = df.filter(
        pl.col("UTC_Start").dt.year().eq(year)
        )
    if df.is_empty():
        continue
    for temp in ["CellTemp_C", "InternalTemp_C"]:
        tvar = temp + "_Mean"
        tvar_unc = temp + "_STD"
        if tvar in df.columns:
            break
    if tvar not in df.columns:
        continue
    for var in ["O3_ppb", "NO_ppb", "NO2_ppb", "NOx_ppb"]:
        dvar = var + "_Mean"
        dvar_unc = var + "_STD"
        if dvar not in df.columns:
            continue
        x=df[tvar]
        y=df[dvar]
        x_unc=df[tvar_unc]
        y_unc=df[dvar_unc]
        
        xname, xunit, _ = tvar.split("_")
        yname, yunit, _ = dvar.split("_")
        
        linreg = sp.stats.linregress(x, y)
        sens, off, unc_sens, unc_off = perform_odr(x, y, x_unc, y_unc, [linreg.slope, linreg.intercept])
        r2 = calc_r2(x, y, sens, off)
        
        if inst not in temp_corr.keys():
            temp_corr[inst] = {}
        
        temp_corr[inst][var] = {"Slope": sens,
                                "Intercept": off,
                                "Slope_Uncertainty": unc_sens,
                                "Intercept_Uncertainty": unc_off,
                                "R2": r2}
        fit_label = ('['
                     + yname
                     + ']$_{UZA}$ = '
                     + f'{sens:.3f}'
                     + r'$\cdot$ '
                     + xname
                     + '$_{UZA}$ ')
        if off > 0: # Add zero
            fit_label += f'+ {off:.3f}\nR' + '$^2$ = ' + f'{r2:.4f}'
        else: # Subtract negative zero
            fit_label += f'\u2212 {-off:.3f}\nR' + '$^2$ = ' + f'{r2:.4f}'
        fit_fig, fit_ax = plt.subplots(figsize=(8, 8))
        fit_ax.errorbar(
            x,
            y,
            xerr=x_unc,
            yerr=y_unc,
            color="#1E4D2B",
            **ebar_kwargs)
        fit_ax.plot(x, x * sens + off,
                    **line_kwargs,
                    zorder=10,
                    label=fit_label)
        
        fit_ax.legend()
        fit_ax.set_xlabel(xname + " (" + xunit + ")")
        fit_ax.set_ylabel(yname + " Offset (" + yunit + ")")
        fit_ax.set_title(inst + " Mean " + yname + " Offset vs. Temperature")
        fit_ax.xaxis.set_major_locator(ticker.AutoLocator())
        fit_ax.xaxis.set_minor_locator(ticker.AutoMinorLocator())
        fit_ax.yaxis.set_major_locator(ticker.AutoLocator())
        fit_ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
        # Turns on major and minor gridlines
        fit_ax.grid(which='major')
        fit_ax.grid(which='minor', linestyle=':')
        
        folder = inst + "_DerivedData"
        direct = os.path.join(ZERO_RESULTS_DIR, folder, folder + "_Figures")
        if not os.path.exists(direct):
            os.makedirs(direct)
        fit_fig.savefig(os.path.join(
            direct,
            inst + "_" + yname + "_OffsetTemperatureCorrelation_" + str(year) + ".png"
            ))
        plt.close()

        
        lod = y_unc * 3
        lodreg = sp.stats.linregress(x, lod)
        lodsens, lodoff, unc_lodsens, lodunc_off = perform_odr(x, lod, x_unc, None, [lodreg.slope, lodreg.intercept])
        lodr2 = calc_r2(x, lod, lodsens, lodoff)
        
        if inst not in lod_corr.keys():
            lod_corr[inst] = {}
        
        lod_corr[inst][var] = {"Slope": lodsens,
                               "Intercept": lodoff,
                               "Slope_Uncertainty": unc_lodsens,
                               "Intercept_Uncertainty": lodunc_off,
                               "R2": lodr2}
        
        fit_label = ('['
                     + yname
                     + ']$_{LOD}$ = '
                     + f'{lodsens:.3f}'
                     + r'$\cdot$ '
                     + xname)
        if lodoff > 0: # Add zero
            fit_label += f' + {lodoff:.3f}\nR' + '$^2$ = ' + f'{lodr2:.4f}'
        else: # Subtract negative zero
            fit_label += f' \u2212 {-lodoff:.3f}\nR' + '$^2$ = ' + f'{lodr2:.4f}'
        
        lod_fig, lod_ax = plt.subplots(figsize=(8, 8))
        lod_ax.errorbar(
            x,
            lod,
            xerr=x_unc,
            color="#1E4D2B",
            **ebar_kwargs)
        lod_ax.plot(x, x * lodsens + lodoff,
                    **line_kwargs,
                    zorder=10,
                    label=fit_label)
        lod_ax.legend()
        lod_ax.set_xlabel(xname + " (" + xunit + ")")
        lod_ax.set_ylabel(yname + " LOD (" + yunit + ")")
        lod_ax.set_title(inst + " " + yname + " LOD vs. Temperature")
        lod_ax.xaxis.set_major_locator(ticker.AutoLocator())
        lod_ax.xaxis.set_minor_locator(ticker.AutoMinorLocator())
        lod_ax.yaxis.set_major_locator(ticker.AutoLocator())
        lod_ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
        # Turns on major and minor gridlines
        lod_ax.grid(which='major')
        lod_ax.grid(which='minor', linestyle=':')
        
        lod_fig.savefig(os.path.join(
            direct,
            inst + "_" + yname + "_LODTemperatureCorrelation_" + str(year) + ".png"
            ))
        plt.close()

for inst, df in uza_stats.items():
    if isinstance(df, list):
        continue
    df = df.select(
        ~cs.contains("_C")
        )
    folder = inst + "_DerivedData"
    direct = os.path.join(ZERO_RESULTS_DIR, folder)
    if not os.path.exists(direct):
        os.makedirs(direct)
    file = inst + "_UZAStatistics.csv"
    path = os.path.join(direct, file)
    df.write_csv(path)
    if inst in temp_corr.keys():
        inst_corrs = []
        for var, corr in temp_corr[inst].items():
            inst_corrs.append(
                pl.DataFrame(corr).select(
                    pl.lit(var.split("_")[0]).alias("Species"),
                    pl.all()
                    )
                )
        inst_corrs = pl.concat(inst_corrs)
        file = inst + "_OffsetTemperatureCorrelation.csv"
        path = os.path.join(direct, file)
        inst_corrs.write_csv(path,
                             float_precision=6)
    if inst in lod_corr.keys():
        inst_corrs = []
        for var, corr in lod_corr[inst].items():
            inst_corrs.append(
                pl.DataFrame(corr).select(
                    pl.lit(var.split("_")[0]).alias("Species"),
                    pl.all()
                    )
                )
        inst_corrs = pl.concat(inst_corrs)
        file = inst + "_LODTemperatureCorrelation.csv"
        path = os.path.join(direct, file)
        inst_corrs.write_csv(path,
                             float_precision=6)
