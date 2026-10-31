# -*- coding: utf-8 -*-
"""
Created on Tue Sep  1 13:56:26 2026

@author: agsthk
"""
# %% Imports
import os
import numpy as np
import lmfit
import polars as pl
import polars.selectors as cs
from polars.exceptions import ComputeError
from datetime import datetime
from pytz import utc
import hvplot.polars
import holoviews as hv
import matplotlib.pyplot as plt
# %% User-defined functions
def read_ict(path):
    '''
    Reads ICARTT files

    Parameters
    ----------
    path : str
        Absolute or relative path to ICARTT file.

    Returns
    -------
    df : polars.DataFrame
        DataFrame containing ICARTT data with missing values removed and time 
        converted from seconds since midnight to polars.datatypes.Datetime
        object.

    '''
    with open(path, "r") as f:
        # Number of lines in header
        lines = int(f.readline().split(",")[0])
        # Start and stop date
        for i in range(6):
            date = f.readline()
    y1, m1, d1, y2, m2, d2 = (int(x) for x in date.split(","))
    # Midnight of start date
    start = datetime(year=y1, month=m1, day=d1, hour=0, minute=0, second=0)
    # Reads ICARTT file, skipping header
    try:
        df = pl.read_csv(path, skip_lines=lines - 1)
    except ComputeError:
        df = pl.read_csv(path, skip_lines=lines - 1, infer_schema_length=None)
    df = df.with_columns(
        # Converts from seconds since midnight to polars.datatypes.Datetime
        cs.contains("UTC").mul(1e6)
        .cast(pl.Duration()).add(start).dt.replace_time_zone("UTC")
        ).with_columns(
            cs.contains("UTC").dt.convert_time_zone("America/Denver")
            .name.map(lambda x: x.replace("UTC", "FTC"))
            ).with_columns(
                # Removes missing values
                pl.all().replace(-9999, None)
                )
    return df
# %% Path definitions
# Absolute path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")
# Absolute path to Instruments_ICARTTData/ directory
ICARTT_DIR = os.path.join(data_dir, "Instruments_ICARTTData")
# %% Reads ICARTT data
# Names of files containing CO2 data
licor_files = ["Keck-CO2_C200_20231214_R1.ict",
               "Keck-CO2_C200_20250109_R2.ict"]
aranet_files = ["Keck-AranetCO2_C200_20250321_R1.ict"]

licor_data = []
aranet_data = []
occ_data = []
for root, dirs, files in os.walk(ICARTT_DIR):
    for file in files:
        if file in licor_files:
            licor_data.append(read_ict(os.path.join(root, file)))
        if file in aranet_files:
            aranet_data.append(read_ict(os.path.join(root, file)))
        if file.find("Occupancy") != -1:
            occ_data.append(read_ict(os.path.join(root, file)))
# Concatenates data for all phases
licor_data = pl.concat(licor_data).sort(by="UTC_DateTime")
aranet_data = pl.concat(aranet_data).sort(by="UTC_DateTime")
occ_data = pl.concat(occ_data).sort(by="UTC_Start")
# Calculates uncertainty in CO2 measurements
licor_data = licor_data.with_columns(
    CO2_ppm_Unc=pl.col("CO2_ppm").mul(0.015)
    )
aranet_data = aranet_data.with_columns(
    cs.contains("CO2_ppm").mul(0.03).add(30).name.suffix("_Unc")
    )
# %% Averages Aranet data across sensors
aranet_data = aranet_data.group_by_dynamic(
    "UTC_DateTime", every="1m"
    ).agg(
        ~cs.contains("DateTime")
        ).with_columns(
            CO2_ppm=pl.concat_list(cs.contains("CO2")
                                   & ~(cs.contains("Unc"))),
            CO2_ppm_Unc=pl.concat_list(cs.contains("CO2") 
                                       & cs.contains("Unc")),
            Temp_C=pl.concat_list(cs.contains("Temp_C")),
            RH_percent=pl.concat_list(cs.contains("RH")),
            Pressure_hPa=pl.concat_list(cs.contains("Pressure_hPa"))
            ).select(
                pl.col("UTC_DateTime").dt.replace_time_zone("UTC"),
                pl.col("UTC_DateTime")
                .dt.convert_time_zone("America/Denver")
                .alias("FTC_DateTime"),
                pl.col("CO2_ppm").list.mean(),
                pl.col("CO2_ppm").list.std().alias("CO2_ppm_STD"),
                pl.col("CO2_ppm_Unc").list.drop_nulls()
                .list.eval(pl.element() ** 2).list.sum()
                .sqrt().truediv(pl.col("CO2_ppm_Unc").list.drop_nulls()
                                .list.len()),
                pl.col("Temp_C").list.mean(),
                pl.col("Temp_C").list.std().alias("Temp_C_STD"),
                pl.col("RH_percent").list.mean(),
                pl.col("RH_percent").list.std().alias("RH_percent_STD"),
                pl.col("Pressure_hPa").list.mean(),
                pl.col("Pressure_hPa").list.std().alias("Pressure_hPa_STD")
                ).select(
                    ~cs.contains("CO2_ppm_"),
                    # Uncertainty as max of STD between monitors and
                    # propagated manufacturer reported uncertainty
                    CO2_ppm_Unc=pl.max_horizontal(
                        pl.col("CO2_ppm_STD"),
                        pl.col("CO2_ppm_Unc")
                        )
                    )
# %% Reads automated and manual addition times 
add_times = pl.read_csv(
    os.path.join(data_dir,
                 "Instruments_DerivedData",
                 "AdditionValves_DerivedData",
                 "AdditionValves_AutomatedAdditionTimes.csv")
    ).with_columns(
        pl.selectors.contains("UTC").str.to_datetime()
        )
add_times = {key[0]: df for key, df in 
             add_times.partition_by(
                 "Species", as_dict=True, include_key=False
                 ).items()}
man_add_times = pl.read_csv(
    os.path.join(data_dir,
                 "Instruments_ManualData",
                 "Instruments_ManualExperiments",
                 "ManualAdditionTimes.csv")
    ).with_columns(
        pl.selectors.contains("UTC").str.to_datetime()
        ).with_columns(
            # Defines missing stop times
            UTC_Stop=pl.when(pl.col("UTC_Stop").is_null())
            .then(pl.col("UTC_Start").dt.offset_by("20m"))
            .otherwise(pl.col("UTC_Stop"))
            )
man_add_times = {key[0]: df for key, df in 
             man_add_times.partition_by(
                 "Species", as_dict=True, include_key=False
                 ).items()}
# Combines manual and automated additions
for spec, df in add_times.items():
    if spec in man_add_times.keys():
        add_times[spec] = pl.concat(
            [df, man_add_times[spec]]
            ).sort(by="UTC_Start")
# %% Defines times that CO2 is perturbed
co2_pert = pl.concat(
    [add_times["CO2"].select(
        pl.col("UTC_Start").dt.offset_by("-2m"),
        UTC_Stop=pl.when(
            pl.any_horizontal(
                cs.contains("UTC").is_between(
                    datetime(2025, 1, 18, tzinfo=utc),
                    datetime(2025, 1, 28, tzinfo=utc)
                    )
                )
            | pl.any_horizontal(
                cs.contains("UTC").is_between(
                    datetime(2025, 1, 18, tzinfo=utc),
                    datetime(2025, 1, 28, tzinfo=utc)
                    )
                )
            )
            .then(pl.col("UTC_Stop").dt.offset_by("2h"))
            .otherwise(pl.col("UTC_Stop").dt.offset_by("4h45m"))
        ),
    occ_data.filter(
        pl.col("OccupancyStatus").eq(1)
        ).select(
            pl.col("UTC_Start").dt.offset_by("-10m"),
            UTC_Stop=pl.col("UTC_Stop").dt.offset_by("10m")
            )]
            ).sort(by="UTC_Start")
# Combines overlapping intervals
co2_pert = co2_pert.with_columns(
    # Identifies as new interval if start time is after any previous stop time
    (pl.col("UTC_Start").ge(
        pl.col("UTC_Stop").cum_max().shift(1, fill_value=0)
        ))
    .cum_sum()
    .alias("IntvID")
    ).group_by(
        # Identifies first start and last stop of overlapping intervals
        "IntvID"
        ).agg(
            pl.min("UTC_Start"),
            pl.max("UTC_Stop")
            ).sort("UTC_Start").drop("IntvID")
# 
licor_bg = licor_data.join_asof(
    co2_pert,
    left_on="UTC_DateTime",
    right_on="UTC_Stop",
    strategy="forward" # For CO2 datetime, IDs next perturbed time end (could
    # be in perturbed period if after start but before end)
    ).filter(
        # Keeps only CO2 measured in unperturbed times
        pl.col("UTC_DateTime").lt(pl.col("UTC_Start"))
        )
licor_bymonth = licor_data.with_columns(
    Year=pl.col("FTC_DateTime").dt.year(),
    Month=pl.col("FTC_DateTime").dt.week()
    ).partition_by("Year", "Month", as_dict=True, include_key=False)

licor_bg_bymonth = licor_bg.with_columns(
    Year=pl.col("FTC_DateTime").dt.year(),
    Month=pl.col("FTC_DateTime").dt.week()
    ).partition_by("Year", "Month", as_dict=True, include_key=False)
for (y, m), df in licor_bymonth.items():
    
    fig, ax = plt.subplots()
    ax.plot(df["FTC_DateTime"], df["CO2_ppm"])
    try:
        df_bg = licor_bg_bymonth[(y, m)]
    except: pass
    ax.plot(df_bg["FTC_DateTime"], df_bg["CO2_ppm"])

# %% Defines background CO2 for background subtraction
# 10 minutes before CO2 additions
bg_begin = add_times["CO2"].select(
    pl.col("UTC_Start").dt.offset_by("-10m"),
    UTC_Stop=pl.col("UTC_Start")
    ).with_row_index()
# 10 minutes after 5.5 hours following CO2 addition end
bg_end = add_times["CO2"].select(
    UTC_Start=pl.col("UTC_Stop").dt.offset_by("5h30m"),
    UTC_Stop=pl.col("UTC_Stop").dt.offset_by("5h40m"),
    ).with_row_index(offset=1)
# Removes end background times if next CO2 addition occurs < 5.5 hours after
# previous end (covered by bg_begin)
bg_end = bg_end.join(
    bg_begin,
    on="index",
    suffix="_Next",
    how="left"
    ).filter(
        pl.col("UTC_Stop").lt(pl.col("UTC_Start_Next"))
        ).drop(cs.contains("Next"))
# Combines all times to consider as background
bg_times = pl.concat(
    [bg_begin, bg_end]
    ).select(
        pl.exclude("index")
        ).sort(
            by="UTC_Start"
            )
# Calculates LI-COR CO2 during background times
licor_bg = licor_data.join_asof(
    bg_times,
    left_on="UTC_DateTime",
    right_on="UTC_Stop",
    strategy="forward" # For CO2 datetime, IDs next BG time 
    ).filter(
        # Removes data outside of background times
        pl.col("UTC_DateTime").gt(pl.col("UTC_Start"))
        ).group_by(
            # Groups by background interval
            "UTC_Start"
            ).agg(
                pl.max("UTC_Stop"),
                BG_CO2_ppm=pl.mean("CO2_ppm"),
                BG_CO2_ppm_Unc=pl.max_horizontal(
                    pl.std("CO2_ppm"),
                    (pl.col("CO2_ppm_Unc").pow(2)).sum().sqrt().truediv(
                        pl.col("CO2_ppm_Unc").drop_nulls().len()
                        )
                    )
                ).sort(by="UTC_Start")
# Calculates Aranet CO2 during background times
aranet_bg = aranet_data.join_asof(
    bg_times,
    left_on="UTC_DateTime",
    right_on="UTC_Stop",
    strategy="forward" # For CO2 datetime, IDs next BG time 
    ).filter(
        # Removes data outside of background times
        pl.col("UTC_DateTime").gt(pl.col("UTC_Start"))
        ).group_by(
            # Groups by background interval
            "UTC_Start"
            ).agg(
                pl.max("UTC_Stop"),
                BG_CO2_ppm=pl.mean("CO2_ppm"),
                BG_CO2_ppm_Unc=pl.max_horizontal(
                    pl.std("CO2_ppm"),
                    (pl.col("CO2_ppm_Unc").pow(2)).sum().sqrt().truediv(
                        pl.col("CO2_ppm_Unc").drop_nulls().len()
                        )
                    )
                ).sort(by="UTC_Start")
# %% Adds background CO2 to LI-COR and Aranet datasets
# Adds BG CO2 to LI-COR
licor_w_bg = licor_data.join_asof(
    licor_bg,
    left_on="UTC_DateTime",
    right_on="UTC_Stop",
    strategy="forward"
    ).with_columns(
        # Time, CO2, and uncertainty of next true BG
        x1=pl.col("UTC_Start"),
        y1=pl.col("BG_CO2_ppm"),
        sy1=pl.col("BG_CO2_ppm_Unc"),
        # Removes background outside of background times
        BG_CO2_ppm=pl.when(
            pl.col("UTC_DateTime").gt(pl.col("UTC_Start"))
            )
        .then(pl.col("BG_CO2_ppm"))
        .otherwise(pl.lit(None)),
        # Removes uncertainty outside of background times
        BG_CO2_ppm_Unc=pl.when(
            pl.col("UTC_DateTime").gt(pl.col("UTC_Start"))
            )
        .then(pl.col("BG_CO2_ppm_Unc"))
        .otherwise(pl.lit(None))
        ).drop(
            # Drops BG start and stop column
            cs.contains("Start", "Stop")
            ).join_asof(
                # Time, CO2, and uncertainty of previous true BG
                licor_bg.select(
                    pl.col("UTC_Start"),
                    x0=pl.col("UTC_Stop"),
                    y0=pl.col("BG_CO2_ppm"),
                    sy0=pl.col("BG_CO2_ppm_Unc"),
                    ),
                left_on="UTC_DateTime",
                right_on="UTC_Start",
                strategy="backward"
                ).drop(
                    # Drops BG start column
                    "UTC_Start"
                    ).with_columns(
                        # Interpolates missing BG CO2
                        pl.col("BG_CO2_ppm").interpolate_by(
                            "UTC_DateTime"
                            ).forward_fill().backward_fill()
                        )
# Determines uncertainty in interpolated background CO2 for LI-COR
licor_w_bg = licor_w_bg.with_columns(
    # Calculates necessary differences
    pl.col("UTC_DateTime").sub(pl.col("x0")).dt.total_seconds()
    .alias("x-x0"),
    pl.col("x1").sub(pl.col("x0")).dt.total_seconds()
    .alias("x1-x0"),
    pl.col("y1").sub(pl.col("y0"))
    .alias("y1-y0")
    ).with_columns(
        # Calculates partial derivative of interpolated measurement
        # with respect to boundary measurements
        ddy0=(1 - (pl.col("x-x0").truediv(pl.col("x1-x0")))),
        ddy1=pl.col("x-x0").truediv(pl.col("x1-x0"))
        ).with_columns(
            # Calculates uncertainty from partial derivatives and
            # boundary uncertainties
            (pl.col("dd" + var).mul(pl.col("s" + var))).pow(2)
            .alias("(dd" + var + "*s" + var +")^2")
            for var in ["y0", "y1"]
            ).with_columns(
                (pl.sum_horizontal(cs.contains("^2"))).sqrt()
                .alias("BG_CO2_ppm_InterpolatedUnc")
                ).select(
                    cs.contains("DateTime"),
                    pl.col("CO2_ppm", "CO2_ppm_Unc", "BG_CO2_ppm"),
                    # Uses propagated uncertainty for interpolated
                    # values
                    pl.when(pl.col("BG_CO2_ppm_Unc").is_not_null())
                    .then(pl.col("BG_CO2_ppm_Unc"))
                    .otherwise(pl.col("BG_CO2_ppm_InterpolatedUnc"))
                    ).with_columns(
                        # Calculates BG subtracted CO2
                        BGSub_CO2_ppm=pl.col("CO2_ppm")
                        .sub(pl.col("BG_CO2_ppm")),
                        BGSub_CO2_ppm_Unc=pl.sum_horizontal(
                            cs.contains("Unc").pow(2)
                            ).sqrt()
                        )
# Adds BG CO2 to Aranet
aranet_w_bg = aranet_data.join_asof(
    aranet_bg,
    left_on="UTC_DateTime",
    right_on="UTC_Stop",
    strategy="forward"
    ).with_columns(
        # Time, CO2, and uncertainty of next true BG
        x1=pl.col("UTC_Start"),
        y1=pl.col("BG_CO2_ppm"),
        sy1=pl.col("BG_CO2_ppm_Unc"),
        # Removes background outside of background times
        BG_CO2_ppm=pl.when(
            pl.col("UTC_DateTime").gt(pl.col("UTC_Start"))
            )
        .then(pl.col("BG_CO2_ppm"))
        .otherwise(pl.lit(None)),
        # Removes uncertainty outside of background times
        BG_CO2_ppm_Unc=pl.when(
            pl.col("UTC_DateTime").gt(pl.col("UTC_Start"))
            )
        .then(pl.col("BG_CO2_ppm_Unc"))
        .otherwise(pl.lit(None))
        ).drop(
            # Drops BG start and stop column
            cs.contains("Start", "Stop")
            ).join_asof(
                # Time, CO2, and uncertainty of previous true BG
                aranet_bg.select(
                    pl.col("UTC_Start"),
                    x0=pl.col("UTC_Stop"),
                    y0=pl.col("BG_CO2_ppm"),
                    sy0=pl.col("BG_CO2_ppm_Unc"),
                    ),
                left_on="UTC_DateTime",
                right_on="UTC_Start",
                strategy="backward"
                ).drop(
                    # Drops BG start column
                    "UTC_Start"
                    ).with_columns(
                        # Interpolates missing BG CO2
                        pl.col("BG_CO2_ppm").interpolate_by(
                            "UTC_DateTime"
                            ).forward_fill().backward_fill()
                        )
# Determines uncertainty in interpolated background CO2
aranet_w_bg = aranet_w_bg.with_columns(
    # Calculates necessary differences
    pl.col("UTC_DateTime").sub(pl.col("x0")).dt.total_seconds()
    .alias("x-x0"),
    pl.col("x1").sub(pl.col("x0")).dt.total_seconds()
    .alias("x1-x0"),
    pl.col("y1").sub(pl.col("y0"))
    .alias("y1-y0")
    ).with_columns(
        # Calculates partial derivative of interpolated measurement
        # with respect to boundary measurements
        ddy0=(1 - (pl.col("x-x0").truediv(pl.col("x1-x0")))),
        ddy1=pl.col("x-x0").truediv(pl.col("x1-x0"))
        ).with_columns(
            # Calculates uncertainty from partial derivatives and
            # boundary uncertainties
            (pl.col("dd" + var).mul(pl.col("s" + var))).pow(2)
            .alias("(dd" + var + "*s" + var +")^2")
            for var in ["y0", "y1"]
            ).with_columns(
                (pl.sum_horizontal(cs.contains("^2"))).sqrt()
                .alias("BG_CO2_ppm_InterpolatedUnc")
                ).select(
                    cs.contains("DateTime"),
                    pl.col("CO2_ppm", "CO2_ppm_Unc", "BG_CO2_ppm"),
                    # Uses propagated uncertainty for interpolated
                    # values
                    pl.when(pl.col("BG_CO2_ppm_Unc").is_not_null())
                    .then(pl.col("BG_CO2_ppm_Unc"))
                    .otherwise(pl.col("BG_CO2_ppm_InterpolatedUnc"))
                    ).with_columns(
                        # Calculates BG subtracted CO2
                        BGSub_CO2_ppm=pl.col("CO2_ppm")
                        .sub(pl.col("BG_CO2_ppm")),
                        BGSub_CO2_ppm_Unc=pl.sum_horizontal(
                            cs.contains("Unc").pow(2)
                            ).sqrt()
                        )
# %% Splits CO2 data by addition-decay experiment
licor_decay = licor_w_bg.join_asof(
    # IDs most recent CO2 addition start
    add_times["CO2"],
    left_on="UTC_DateTime",
    right_on="UTC_Stop",
    strategy="backward"
    ).join_asof(
        # IDs closest following occupant entering
        occ_data.filter(
            pl.col("OccupancyStatus").eq(1)
            ).sort(by="UTC_Start"),
        left_on="UTC_DateTime",
        right_on="UTC_Start",
        strategy="forward", # For CO2 datetime, IDs next occupied time
        suffix="_Occ"
        ).filter(
            # For each addition, selects only data between addition end and the
            # soonest of 5.5h after addition end, next addition start, and next
            # occupant entering
            pl.col("UTC_DateTime").gt(pl.col("UTC_Stop"))
            & pl.col("UTC_DateTime").lt(
                pl.col("UTC_Stop").dt.offset_by("5h30m")
                )
            & pl.col("UTC_DateTime").lt(pl.col("UTC_Start_Occ"))
            ).select(
                # Defines string IDing addition start
                pl.col("UTC_Start").dt.convert_time_zone("America/Denver")
                .dt.strftime("%Y-%m-%d %H:%M").alias("AdditionID"),
                # Calculates time from addition end in hours
                pl.col("UTC_DateTime").sub(pl.col("UTC_Stop"))
                .dt.total_microseconds().truediv(3600e6).alias("Time_h"),
                # Keeps CO2 measurements
                cs.contains("BGSub")
                ).drop_nulls().partition_by(
                    # Splits CO2 measurements by addition-decay experiment
                    "AdditionID", 
                    include_key=False,
                    as_dict=True
                    )
occ_data.filter(
    pl.col("OccupancyStatus").eq(1)
    & pl.col("UTC_Start").dt.day().eq(28)
    ).sort(by="UTC_Start")
aranet_decay = aranet_w_bg.join_asof(
    # IDs most recent CO2 addition start
    add_times["CO2"],
    left_on="UTC_DateTime",
    right_on="UTC_Stop",
    strategy="backward"
    ).join_asof(
        # IDs closest following occupant entering
        occ_data.filter(
            pl.col("OccupancyStatus").eq(1)
            ).sort(by="UTC_Start"),
        left_on="UTC_DateTime",
        right_on="UTC_Start",
        strategy="forward", # For CO2 datetime, IDs next occupied time
        suffix="_Occ"
        ).filter(
            # For each addition, selects only data between addition end and the
            # soonest of 5.5h after addition end, next addition start, and next
            # occupant entering
            pl.col("UTC_DateTime").gt(pl.col("UTC_Stop"))
            & pl.col("UTC_DateTime").lt(
                pl.col("UTC_Stop").dt.offset_by("5h30m")
                )
            & pl.col("UTC_DateTime").lt(pl.col("UTC_Start_Occ"))
            ).select(
                # Defines string IDing addition start
                pl.col("UTC_Start").dt.convert_time_zone("America/Denver")
                .dt.strftime("%Y-%m-%d %H:%M").alias("AdditionID"),
                # Calculates time from addition end in hours
                pl.col("UTC_DateTime").sub(pl.col("UTC_Stop"))
                .dt.total_microseconds().truediv(3600e6).alias("Time_h"),
                # Keeps CO2 measurements
                cs.contains("BGSub")
                ).drop_nulls().partition_by(
                    # Splits CO2 measurements by addition-decay experiment
                    "AdditionID", 
                    include_key=False,
                    as_dict=True
                    )
# %% Fits CO2 decays
acrs = []
for (add_id,), add_df in licor_decay.items():
    # if "2024-03" in add_id:
    #     continue
    if "2024-06" in add_id:
        break
    # Skips fitting if CO2 range is small (no addition?)
    if (add_df["BGSub_CO2_ppm"].max() 
        - add_df["BGSub_CO2_ppm"].min()) < 200:
        continue
    # Skips fitting if time range is small (insufficient time?)
    if add_df["Time_h"].max() < 1 or add_df["Time_h"].min() > 1:
        continue
    # Highest post addition end CO2 concentration
    max_co2 = add_df.filter(
        # Requires it to actually be after addition
        pl.col("Time_h").is_between(1/120, 1)
        )["BGSub_CO2_ppm"].max()
    if max_co2 is None: continue
    
    # CO2 concentration to start decay fit at
    thresh_co2 = 0.8 * max_co2
    # Identifies time CO2 decays below 80% of max
    dec_start_t = add_df.filter(
        pl.col("BGSub_CO2_ppm").gt(thresh_co2)
        & pl.col("Time_h").lt(1)
        )["Time_h"].last()
    # Determines true CO2 concentration at decay start
    dec_start_co2 = add_df.filter(
        pl.col("Time_h").eq(dec_start_t)
        )["BGSub_CO2_ppm"].first()
    # Determines CO2 concentration to end decay fit at (2 e-folds)
    dec_end_co2 = dec_start_co2 / (np.e ** 2)
    # Identifies last time CO2 is above 2 e-folds decay
    dec_end_t = add_df.filter(
        pl.col("BGSub_CO2_ppm").lt(dec_end_co2)
        )["Time_h"].first()
    if dec_end_t is None:
        print(add_id)
    # continue
    # Selects data to fit
    fit_co2 = add_df.filter(
        pl.col("Time_h").is_between(dec_start_t, dec_end_t)
        ).with_columns(
            # Weights for fit as 1/sigma
            Weights=pl.lit(1).truediv(pl.col("BGSub_CO2_ppm_Unc")),
            # Log-transforms background subtracted CO2 for rough fitting
            # parameter estimates
            LogCO2=pl.col("BGSub_CO2_ppm").log()
            )
    # Plots decay fit to visually check fit
    fig, ax = plt.subplots()
    # Full decay data with uncertainty
    ax.errorbar(add_df["Time_h"],
                add_df["BGSub_CO2_ppm"],
                yerr=add_df["BGSub_CO2_ppm_Unc"],
                linestyle="",
                marker=".",
                label="Decay Data"
                )
    # Portion of decay fit
    ax.scatter(fit_co2["Time_h"],
               fit_co2["BGSub_CO2_ppm"],
               color="orange",
               zorder=20,
               s=1,
               label="Fit Data"
               )
    ax.axhline(y=max_co2)
    ax.axhline(y=thresh_co2)
    ax.axvline(x=dec_start_t)
    ax.axvline(x=dec_end_t)
    # Figure formatting
    ax.set_title(spec + " " + add_id)
    ax.legend()
    ax.grid()
    ax.set_xlabel("Hours from CO$_2$ Addition End")
    ax.set_ylabel("CO$_2$ (ppm)")
    ax.set_xlim(0, add_df["Time_h"].max())
    # %%
    if fit_co2.height < 5:
        print(add_id, thresh_co2, dec_end_co2)
        continue
    lin_model = lmfit.models.LinearModel()
    # Linear regression of log-transformed data to estimate initial
    # exponential parameters
    log_lin_result = lin_model.fit(fit_co2.drop_nans("LogCO2")["LogCO2"],
                                   x=fit_co2.drop_nans("LogCO2")["Time_h"])
    # Initial exponential parameter guesses
    exp_params = lmfit.Parameters()
    exp_params.add("A", value=(
        np.e ** log_lin_result.params["intercept"].value
        ))
    exp_params.add("k", value=-log_lin_result.params["slope"].value)
    exp_params.add("c0", value=0, min=-3, max=3)
    # First order exponential decay model
    exp_model = lmfit.models.ExpressionModel(
        "A * exp(-k * t) + c0",
        independent_vars=["t"]
        )
    try:
        # Fits data to exponential model
        exp_result = exp_model.fit(
            fit_co2.drop_nans("BGSub_CO2_ppm")["BGSub_CO2_ppm"],
            **exp_params,
            t=fit_co2["Time_h"],
            weights=np.array(fit_co2.drop_nans("BGSub_CO2_ppm")["Weights"])
            )
    # Moves on when fit fails
    except ValueError:
        continue
    acr = exp_result.params["k"].value
    acr_unc = exp_result.params["k"].stderr
    if acr_unc is None:
        continue
    if acr_unc / acr > 0.2:
        continue
    # Saves calculated ACR and associated uncertainty
    acrs.append(
        [add_id, acr, acr_unc]
        )
    # Adds modeled fit results
    fit_co2 = fit_co2.with_columns(
        Fit=exp_result.best_fit
        )

    # Portion of decay fit
    ax.scatter(fit_co2["Time_h"],
               fit_co2["BGSub_CO2_ppm"],
               color="orange",
               zorder=20,
               label="Fit Data"
               )
    # Best fit line
    ax.plot(fit_co2["Time_h"],
            fit_co2["Fit"],
            color="black",
            zorder=30,
            label="Best Fit"
            )


# %%

        # Calculates air change rates
        phase_acr = []
        for (add_id,), add_df in decay_data.items():
            # Skips fitting if decay data covers less than 3.5 hours
            # if (add_df["Time_h"].max() - add_df["Time_h"].min()) < 3.5:
            #     continue
            # Skips fitting if CO2 range is small (no addition?)
            if (add_df["BGSub_CO2_ppm"].max() 
                - add_df["BGSub_CO2_ppm"].min()) < 200:
                continue
            # Highest post addition end CO2 concentration
            max_co2 = add_df["BGSub_CO2_ppm"].max()
            # CO2 concentration to start decay fit at
            thresh_co2 = 0.8 * max_co2
            # Identifies time CO2 decays below 80% of max
            dec_start_t = add_df.filter(
                pl.col("BGSub_CO2_ppm").lt(thresh_co2)
                )["Time_h"].first()
            # Determines true CO2 concentration at decay start
            dec_start_co2 = add_df.filter(
                pl.col("Time_h").eq(dec_start_t)
                )["BGSub_CO2_ppm"].first()
            # Determines CO2 concentration to end decay fit at (2 e-folds)
            dec_end_co2 = dec_start_co2 / (np.e ** 2)
            # Identifies last time CO2 is above 2 e-folds decay
            dec_end_t = add_df.filter(
                pl.col("BGSub_CO2_ppm").lt(dec_end_co2)
                )["Time_h"].first()
            if dec_end_t is None:
                continue
            # Selects data to fit
            fit_co2 = add_df.filter(
                pl.col("Time_h").is_between(dec_start_t, dec_end_t)
                ).with_columns(
                    # Weights for fit as 1/sigma
                    Weights=pl.lit(1).truediv(pl.col("BGSub_CO2_ppm_Unc")),
                    # Log-transforms background subtracted CO2 for rough fitting
                    # parameter estimates
                    LogCO2=pl.col("BGSub_CO2_ppm").log()
                    )
            lin_model = lmfit.models.LinearModel()
            # Linear regression of log-transformed data to estimate initial
            # exponential parameters
            log_lin_result = lin_model.fit(fit_co2.drop_nans("LogCO2")["LogCO2"],
                                           x=fit_co2.drop_nans("LogCO2")["Time_h"])
            # Initial exponential parameter guesses
            exp_params = lmfit.Parameters()
            exp_params.add("A", value=(
                np.e ** log_lin_result.params["intercept"].value
                ))
            exp_params.add("k", value=-log_lin_result.params["slope"].value)
            exp_params.add("c0", value=0, min=-3, max=3)
            # First order exponential decay model
            exp_model = lmfit.models.ExpressionModel(
                "A * exp(-k * t) + c0",
                independent_vars=["t"]
                )
            try:
                # Fits data to exponential model
                exp_result = exp_model.fit(
                    fit_co2.drop_nans("BGSub_CO2_ppm")["BGSub_CO2_ppm"],
                    **exp_params,
                    t=fit_co2["Time_h"],
                    weights=np.array(fit_co2.drop_nans("BGSub_CO2_ppm")["Weights"])
                    )
            # Moves on when fit fails
            except ValueError:
                continue
            acr = exp_result.params["k"].value
            acr_unc = exp_result.params["k"].stderr
            if acr_unc is None:
                continue
            if acr_unc / acr > 0.2:
                continue
            # Saves calculated ACR and associated uncertainty
            phase_acr.append(
                [add_id, acr, acr_unc]
                )
            # Adds modeled fit results
            fit_co2 = fit_co2.with_columns(
                Fit=exp_result.best_fit
                )
            # Plots decay fit to visually check fit
            fig, ax = plt.subplots()
            # Full decay data with uncertainty
            ax.errorbar(add_df["Time_h"],
                        add_df["BGSub_CO2_ppm"],
                        yerr=add_df["BGSub_CO2_ppm_Unc"],
                        linestyle="",
                        marker=".",
                        label="Decay Data"
                        )
            # Portion of decay fit
            ax.scatter(fit_co2["Time_h"],
                       fit_co2["BGSub_CO2_ppm"],
                       color="orange",
                       zorder=20,
                       label="Fit Data"
                       )
            # Best fit line
            ax.plot(fit_co2["Time_h"],
                    fit_co2["Fit"],
                    color="black",
                    zorder=30,
                    label="Best Fit"
                    )
            # Figure formatting
            ax.set_title(spec + " " + add_id)
            ax.legend()
            ax.grid()
            ax.set_xlabel("Hours from CO$_2$ Addition End")
            ax.set_ylabel("CO$_2$ (ppm)")
            ax.set_xlim(0, add_df["Time_h"].max())
        # Converts list of ACRs
        phase_acr = pl.DataFrame(phase_acr, orient="row", 
                     schema=["AddTime", "ACR_h", "ACR_h_Unc"])
        acrs[spec][phase] = phase_acr
        # Exports calculated ACRs
        phase_fname = spec + "_ACRs_" + phase + ".txt"
        phase_path = os.path.join(DERIVED_DIR, phase_fname)
        phase_acr.write_csv(phase_path)
# %% Phase comparison
compare = acrs["CO2"]["Phase2"].join(
    acrs["AranetCO2"]["Phase2"],
    on="AddTime",
    coalesce=True,
    suffix="Aranet",
    how="inner"
    )
fig, ax = plt.subplots()
ax.scatter(
    compare["ACR_h"],
    compare["ACR_hAranet"]
    )
compare_relat = lin_model.fit(compare["ACR_h"],
                              x=compare["ACR_hAranet"])
compare_relat.params["slope"].stderr
print(compare_relat.params["slope"].value,
      compare_relat.params["slope"].stderr)
compare_relat.params["intercept"].value
