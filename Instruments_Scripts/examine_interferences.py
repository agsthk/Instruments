# -*- coding: utf-8 -*-
"""
Created on Wed Aug 12 17:05:26 2026

@author: agsthk
"""

# %% Package imports
import os
import polars as pl
import polars.selectors as cs
from datetime import datetime, timedelta, date
import hvplot.polars
import holoviews as hv
import pytz
import yaml
import re
import requests
from bs4 import BeautifulSoup
# User defined function
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
    df = pl.read_csv(path, skip_lines=lines - 1)
    df = df.with_columns(
        # Converts from seconds since midnight to polars.datatypes.Datetime
        cs.contains("time", "UTC", "FTC").mul(1e6).cast(pl.Duration()).add(start)
        ).with_columns(
            # Removes missing values
            pl.all().replace(-9999, None)
            )
    return df
# %% Path definitions
# Declares full path to Instruments_Data/ directory
data_dir = os.getcwd()
# Starts in Instruments/ directory
if "Instruments" in os.path.dirname(data_dir):
    data_dir = os.path.dirname(data_dir)
data_dir = os.path.join(data_dir, "Instruments_Data")
# Full path to Vocus ICARTT file
VOCUS_PATH = os.path.join(data_dir, "Instruments_ICARTTData",
                          "Vocus_ICARTTData", "Vocus_ICARTTData_RC",
                          "Keck-VocusCIMS_C200_20260323_RC.ict")
O3_DIR = os.path.join(data_dir, "Instruments_CleanData",
                      "2BTech_205_A_CleanData", "2BTech_205_A_CleanHubData")
VENT_DIR = os.path.join(data_dir, "Instruments_CleanData",
                      "2BTech_202_CleanData", "2BTech_202_CleanHubData")

# %% Addition times
sab_adds = pl.DataFrame(
    [datetime(2026, 4, 2, 7, 50, tzinfo=pytz.timezone("America/Denver")),
     datetime(2026, 4, 2, 15, 55, tzinfo=pytz.timezone("America/Denver")),
     datetime(2026, 4, 6, 15, 55, tzinfo=pytz.timezone("America/Denver")),
     datetime(2026, 4, 9, 7, 55, tzinfo=pytz.timezone("America/Denver"))],
    schema=["Start"]).with_columns(
        Stop=pl.col("Start").dt.offset_by("4h")
        )
oc_adds = pl.DataFrame(
    [datetime(2026, 4, 1, 7, 57, tzinfo=pytz.timezone("America/Denver")),
    datetime(2026, 4, 1, 15, 55, tzinfo=pytz.timezone("America/Denver")),
    datetime(2026, 4, 3, 15, 55, tzinfo=pytz.timezone("America/Denver")),
    datetime(2026, 4, 8, 7, 55, tzinfo=pytz.timezone("America/Denver")),
    datetime(2026, 4, 8, 15, 55, tzinfo=pytz.timezone("America/Denver"))],
    schema=["Start"]).with_columns(
        Stop=pl.col("Start").dt.offset_by("4h")
        )
bpin_adds = pl.DataFrame(
    [datetime(2026, 4, 3, 7, 55, tzinfo=pytz.timezone("America/Denver"))],
    schema=["Start"]).with_columns(
        Stop=pl.col("Start").dt.offset_by("4h")
    )
        
# Full path to automated addition times
ADD_TIMES_PATH = os.path.join(data_dir,
                              "Instruments_DerivedData",
                              "AdditionValves_DerivedData",
                              "AdditionValves_AutomatedAdditionTimes.csv")
add_times = pl.read_csv(ADD_TIMES_PATH).select(
    pl.selectors.contains("UTC").str.to_datetime()
    .dt.convert_time_zone("America/Denver")
    .name.map(lambda x: x.replace("UTC_", "")),
    pl.col("Species")
    )
add_times = {key[0]: df for key, df in 
             add_times.partition_by(
                 "Species", as_dict=True, include_key=False
                 ).items()}
# Full path to manual addition times
MAN_ADDS_PATH = os.path.join(data_dir,
                             "Instruments_ManualData",
                             "Instruments_ManualExperiments",
                             "ManualAdditionTimes - Copy.csv")
man_add_times = pl.read_csv(MAN_ADDS_PATH).select(
    pl.selectors.contains("UTC").str.to_datetime()
    .dt.convert_time_zone("America/Denver")
    .name.map(lambda x: x.replace("UTC_", "")),
    pl.col("Species")
    )
man_add_times = {key[0]: df for key, df in 
             man_add_times.partition_by(
                 "Species", as_dict=True, include_key=False
                 ).items()}
for spec, df in add_times.items():
    if spec in man_add_times.keys():
        add_times[spec] = pl.concat(
            [df, man_add_times[spec]]
            ).sort(by="Start")
        
man_o3 = pl.DataFrame(
    [datetime(2026, 1, 13, 13, 5, tzinfo=pytz.timezone("America/Denver")),
     datetime(2026, 1, 13, 15, 5, tzinfo=pytz.timezone("America/Denver")),
     datetime(2026, 1, 13, 17, 5, tzinfo=pytz.timezone("America/Denver")),
     datetime(2026, 1, 13, 21, 5, tzinfo=pytz.timezone("America/Denver")),
     datetime(2026, 1, 13, 23, 5, tzinfo=pytz.timezone("America/Denver"))],
    schema=["Start"]).with_columns(
        Stop=pl.col("Start").dt.offset_by("5m")
        )
add_times["O3"] = pl.concat(
    [add_times["O3"],
     man_o3]
    ).sort(by="Start")


# %% Scrapes CDPHE database for outdoor O3 measurements

aqsids = {"FTC": "080691004",
          "FOS": "080690015"}

scrape_start = date(2026, 4, 1)
scrape_end = date(2026, 4, 11)
scrape_delta = timedelta(days=1)

scrape_dates = []
while scrape_start <= scrape_end:
    scrape_dates.append(scrape_start.strftime("%m-%d-%y"))
    scrape_start += scrape_delta

aqsid = "080691004"

data_list = []
for scrape_date in scrape_dates:
    url = ("https://www.colorado.gov/airquality/site.aspx?aqsid="
           + aqsid
           + "&seeddate="
           + scrape_date)
    req = requests.get(url)
    page = BeautifulSoup(req.content, "html.parser")
    table = page.find("table", {"class": "output1"})
    rows = table.find_all("tr")

    header, *table_data = [
        [col.text.strip() for col in row.find_all("td")]
        for row in table.find_all("tr")
        ]
    header = (["date"]
              + [re.sub(r"\s+", "_", col).lower() for col in header])

    table_data = [[scrape_date] + row for row in table_data]
    data_list += (table_data)
temp_data = pl.DataFrame(data_list, orient="row", schema=header)
temp_data = temp_data.with_columns(
    mst_datetime=pl.concat_str(
        [pl.col("date"), pl.col("*hour(mst)")], separator=" "
        ).str.to_datetime("%m-%d-%y %-I:%M %p").dt.replace_time_zone("MST")
    )
temp_data = temp_data.with_columns(
    Start=pl.col("mst_datetime").dt.convert_time_zone("America/Denver")
    )
save_cols = [col for col in ["Start", "o3_ppb"]
             if col in temp_data.columns]

out_o3 = temp_data.select(pl.col(save_cols)).with_columns(
    pl.when(pl.col(pl.String).str.len_chars() == 0)
    .then(None)
    .otherwise(pl.col(pl.String))
    .name.keep()
    ).filter(
        ~pl.all_horizontal(pl.col(save_cols[1:]).is_null())
        ).select(
            pl.col("Start"),
            pl.col("o3_ppb").cast(pl.Float64).alias("O3_ppb")
            )
# %% Reads ICARTT data
vocus = read_ict(VOCUS_PATH).select(
    cs.contains("UTC").dt.convert_time_zone("America/Denver")
    .dt.offset_by("8m")
    .name.map(
        lambda name: name.replace("_UTC", "")
        ),
    pl.col("C10H16(NH4)+_cps").alias("terpene")
    ).drop_nulls()
o3 = []
for file in os.listdir(O3_DIR):
    path = os.path.join(O3_DIR, file)
    o3.append(
        pl.read_csv(path).with_columns(
            cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        )
o3 = pl.concat(o3).sort(by="FTC_Start").select(
    (~cs.contains("UTC")).name.map(lambda x: x.replace("FTC_", ""))
    ).filter(
        pl.col("SamplingLocation").eq("C200")
        & ~pl.any_horizontal(
            pl.col("Start", "Stop").is_between(
                datetime(2026, 3, 31, 12, 40, tzinfo=pytz.timezone("America/Denver")),
                datetime(2026, 3, 31, 12, 44, tzinfo=pytz.timezone("America/Denver"))
                )
            )
        & pl.col("Start").lt(datetime(2026, 4, 14, 20, tzinfo=pytz.timezone("America/Denver")))
        )
vent_o3 = []
for file in os.listdir(VENT_DIR):
    path = os.path.join(VENT_DIR, file)
    vent_o3.append(
        pl.read_csv(path).with_columns(
            cs.contains("FTC").str.to_datetime(time_zone="America/Denver")
            )
        )
vent_o3 = pl.concat(vent_o3).sort(by="FTC_Start").select(
    (~cs.contains("UTC")).name.map(lambda x: x.replace("FTC_", ""))
    ).filter(
        pl.col("SamplingLocation").eq("C200_Vent")
        )
# %% Defines BG room O3 at 30 minute time resolution
# Times to consider ozone perturbed
perturbed_o3_t = pl.concat(
    [add_times["O3"].select(
        pl.col("Start").dt.offset_by("-2m"),
        pl.col("Stop").dt.offset_by("1h45m")
        ),
    sab_adds,
    oc_adds,
    bpin_adds]
    ).sort(by="Start")
# Combines overlapping perturbed O3 intervals
perturbed_o3_t = perturbed_o3_t.with_columns(
    # Identifies as new interval if start time is after any previous stop time
    (pl.col("Start").ge(
        pl.col("Stop").cum_max().shift(1, fill_value=0)
        ))
    .cum_sum()
    .alias("IntvID")
    ).group_by(
        # Identifies first start and last stop of overlapping intervals
        "IntvID"
        ).agg(
            pl.min("Start"),
            pl.max("Stop")
            ).sort("Start").drop("IntvID")

bg_o3 = o3.join_asof(
    perturbed_o3_t,
    on="Start"
    ).filter(
        # Keeps only ozone measured in unperturbed times
        pl.col("Start").gt(pl.col("Stop_right"))
        ).select(
            pl.exclude("Stop_right")
            ).group_by_dynamic(
                "Start",
                every="30m"
                ).agg(
                    BG=pl.mean("O3_ppb"),
                    BG_STD=pl.std("O3_ppb"))
vent_resamp = vent_o3.group_by_dynamic(
    "Start",
    every="30m"
    ).agg(
        V=pl.mean("O3_ppb"),
        V_STD=pl.std("O3_ppb")
        )
# %%
io = bg_o3.join(
    vent_resamp,
    on="Start",
    how="full",
    coalesce=True
    ).with_columns(
        rel_i=pl.col("BG_STD").truediv(pl.col("BG")),
        rel_o=pl.col("V_STD").truediv(pl.col("V"))
        ).with_columns(
            io=pl.col("BG").truediv(pl.col("V")),
            rel_io=(pl.col("rel_i").pow(2).add(pl.col("rel_o").pow(2))).sqrt()
            ).select(
                pl.col("Start", "io"),
                io_unc=pl.col("rel_io").mul(pl.col("io"))
                ).sort(by="Start")
# %%
hvplot.show(
    o3.hvplot.scatter(
        x="Start",
        y="O3_ppb"
        ) * bg_o3.hvplot.scatter(
            x="Start",
            y="O3_ppb"
            )
    )

# %%

for start in sab_adds:
    stop = start + timedelta(hours=6)
    o3_adds = add_times["O3"].filter(
        pl.any_horizontal(
            pl.col("Start", "Stop").is_between(start - timedelta(minutes=10),
                                               stop + timedelta(minutes=10))
            )
        ).with_columns(
            pl.col("Start", "Stop").dt.replace_time_zone("UTC")
            )
    add_o3 = o3.filter(
        pl.any_horizontal(
            pl.col("Start", "Stop").is_between(start - timedelta(minutes=10),
                                               stop + timedelta(minutes=10))
            )
        )
    add_vent = vent_o3.filter(
        pl.any_horizontal(
            pl.col("Start", "Stop").is_between(start - timedelta(minutes=10),
                                               stop + timedelta(minutes=10))
            )
        )
    add_out = out_o3.filter(
        pl.col("Start").is_between(start - timedelta(minutes=10),
                                   stop + timedelta(minutes=10))
        )
    add_vocus = vocus.filter(
        pl.any_horizontal(
            pl.col("Start", "Stop").is_between(start - timedelta(minutes=10),
                                               stop + timedelta(minutes=10))
            )
        & (
           pl.any_horizontal(
               pl.col("Start", "Stop").dt.hour().mod(2).eq(1)
               )
           | ~pl.any_horizontal(
            pl.col("Start", "Stop").dt.minute().is_between(36, 46)
            )
           )
        )
    add_bg = bg_o3.filter(
        pl.any_horizontal(
            pl.col("Start", "Stop").is_between(start - timedelta(minutes=10),
                                               stop + timedelta(minutes=10))
        )
    )
    hvplot.show(
        (add_vocus.hvplot.scatter(
            x="Start",
            y="terpene"
            ) + add_o3.hvplot.scatter(
            x="Start",
            y="O3_ppb"
            ) * hv.VLines(o3_adds["Start"]) + (add_vent.hvplot.scatter(
                    x="Start",
                    y="O3_ppb"
                    ) * add_out.hvplot.scatter(
                        x="Start",
                        y="O3_ppb"
                        ))).cols(1)
        
        )
    
# %%

for start in sab_adds:
    stop = start + timedelta(hours=6)
    add_o3 = o3.filter(
        pl.any_horizontal(
            pl.col("Start", "Stop").is_between(start - timedelta(minutes=10),
                                               stop + timedelta(minutes=10))
            )
        ).group_by_dynamic(
            "Start",
            every="10m"
            ).agg(
                pl.mean("O3_ppb")
                )
    add_vocus = vocus.filter(
        pl.any_horizontal(
            pl.col("Start", "Stop").is_between(start - timedelta(minutes=10),
                                               stop + timedelta(minutes=10))
            )
        & (
           pl.any_horizontal(
               pl.col("Start", "Stop").dt.hour().mod(2).eq(1)
               )
           | ~pl.any_horizontal(
            pl.col("Start", "Stop").dt.minute().is_between(36, 46)
            )
           )
        ).group_by_dynamic(
            "Start",
            every="10m"
            ).agg(
                pl.mean("terpene")
                )
    bg_o3 = add_o3.filter(
        ~pl.col("Start").is_between(start, stop)
        ).upsample(
            "Start",
            every="10m"
            ).with_columns(
                pl.col("O3_ppb").interpolate_by("Start")
                )
    bg_sub_o3 = add_o3.join(
        bg_o3,
        on="Start",
        suffix="_BG"
        ).select(
            pl.col("Start"),
            pl.col("O3_ppb").sub(pl.col("O3_ppb_BG"))
            )
    comb = bg_sub_o3.join(
        add_vocus,
        on="Start"
        ).with_columns(
            pl.col("O3_ppb").truediv(pl.col("terpene")).alias("Factor")
            )
            
    hvplot.show(
        (comb.hvplot.scatter(
            x="Start",
            y="terpene"
            ) * comb.hvplot.scatter(
                x="Start",
                y="O3_ppb"
                ) * out_o3.filter(
                        pl.col("Start").is_between(start, stop)
                        ).hvplot.scatter(
                            x="Start",
                            y="O3_ppb"
                            ) + (comb.hvplot.scatter(
                                x="Start",
                                y="Factor"
                                ))).cols(1)
    )