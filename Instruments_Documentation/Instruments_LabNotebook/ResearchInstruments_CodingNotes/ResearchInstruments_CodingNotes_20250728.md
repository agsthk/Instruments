# clean_rawdata.py
- First checking that the code that worked on my MacBook will also work on Windows PC
	- It does
- Now that I have successfully handled the zeros, I need to consider what else is necessary to generate "clean" data
	- For the Thermo NOx monitor
		- Remove data transmitted when pulling no flow
			- Flagging data when flow is < 0.5 LPM (seems like a decent cutoff?)
		- Remove data transmitted during instrument warmup period
			- First addressing DAQ data, then will handle the data with no "WarmUp" column
- 20250203 has data that is flagged as UZA but should be None
- Based on Thermo NOx, need to extend time on either side of solenoid valve switching
	- May revise this?
- I'm considering creating a separate script to determine Picarro zeroing intervals, as then I won't have to redetermine them every time
	- Created a new DerivedData folder where I can store this
		- I will also be able to get the door open/door closed and store it in here
		- I will also be able to get the addition times and store in here
- After creating an external valve state file, I revised the manner in which I assigned sampling locations
	- Required some troubleshooting but I eventually got there
- Offset the sampling interval start/stop times by 30 seconds either way to cut out non-real data transmitted during a change in location
- On 20250128, there are what appear to be zeroing periods just before 2, 6, and 10 that are not identified as zeroing periods
	- It seems the Picarro instrument wasn't logging at these times, so I manually added them to TG_Line
- For the Thermo NOx instrument, I may need to cut out more than 30 seconds on either side of any location switches
	- Changed the offset to 1 minute on either side for all instruments
		- I can't imagine that this would be a problem
- Now that the location switches have been taken care off, need to move forward with other cleaning methods
	- WarmUp
		- Non-zero for the first 10 minutes after instrument starts transmitting
		- Not sure that I need to remove the full 10 minutes - 5 would probably be fine
			- Seems like an arbitrary decision - let's just go with the full 10 minutes - and do it for all instruments
	- Then, filtered out when sample flow was below 0.5 LPM
- I'm attempting to use a hampel filter to clean up some noise from the ozone instrument in particular, but this is made challenging by the intentional ozone spikes
	- Perhaps I can filter only outside of these spikes - how so?
# interpret_solenoidvalve.py
- Copying over a fair bit of code from clean_rawdata.py
- Revising how I determine intervals - no need for a separate gap DataFrame when that information is given already by valve state being zero
- Rather than counting any non-zero value as open, I will only consider when the value is exactly 0 or exactly 1
	- Anything in between will be dropped
- Exported to DerivedData folder

C200 occupied
1/20 16:37 - 17:05
1/23 17:58 - 17:59
1/24 08:37
1/24 09:15
1/24 15:35
1/24 15:50 - 15:50:25
1/27
08:31 - 08:33
1/28
08:22 - 08:29
1/29
13:27 - 13:34 (?)
1/30
14:20 - 14:23 (?)
2/3
08:24 - 08:31
10:43
2/4
08:44 - 08:45
12:50 - 12:51
12:55 -12:57
13:10 - 13:12
13:14 - 13:20
13:33 - 13:47
17:55 - 17:56
2/5
14:00  - 14:01
14:30
17:01
17:31
17:41
2/6?????
10:00 ?
16:52
2/7
08:30 - 08:32
11:15 - 11:42
2/14
09:59 - 10:01
16:55
2/18
10:01 - 10:05
17:09
2/19
09:34 (?)
09:52 - 11:02
17:13
17:15
2/20
10:00
11:03
13:29
15:40

2/21
10:00 ?
12:26?
13:17?
13:34

2/24
10:57
11:57 - 17:40 ?

2/26
08:48 - 08:51

2/27
09:58 - 10:00
16:31 - 16:32

3/3
11:01

3/6
09:59 - 10:00
19:38

3/7
09:59 - 10:00
atomizer cart removed when??

3/11
09:59 - 10:00
17:40

3/12
09:00 - 09:10

3/13
08:59 - ?

3/14
08:59 - ?

3/17 
08:53 - 09:10
09:38 - 10:08 (not occupied but open? actually i think occupied)
10:38 - 10:57
10:55

3/20
09:07 - 09:16
09:16 - 09:40 ?
14:30 - 14:37
15:29
17:10 - 17:15

3/21
09:00
13:51 - 13:55
14:59 - 15:02 ?
16:07 - 16:14
atomizer removed?


3/22
09:10 - 09:17

3/24
08:09 - 08:17

3/26
07:37 - 07:47 ?
09:02 - 09:04
10:00 ?
12:54 - 12:55
17:01 - 17:06

3/28
09:01 - 09:04
09:30?
12:30 - 12:51
13:40?

3/29
09:21 - 09:25

4/7
09:14 - 09:16
09:31 - 09:33
09:50
12:45
11:05 - 11:10
11:19 - 11:20

4/14
10:56 - 10:57

4/21
11:46 - 11:50



valve_states = valve_states.select(
    pl.exclude("SolenoidValves"),
    pl.when(
        pl.col("UTC_Start").gt(datetime(2025, 3, 20, 15, 57, tzinfo=pytz.UTC))
        & pl.col("UTC_Stop").lt(datetime(2025, 3, 20, 20, 35, tzinfo=pytz.UTC))
        & pl.col("SolenoidValves").eq(1)
        )
    .then(pl.lit("B203"))
    .when(pl.col("SolenoidValves").eq(1))
    .then(pl.lit("UZA"))
    .alias("SamplingLocation")
    ).collect()
pic_on_tg = sampling_locs["Picarro_G2307"].filter(
    pl.col("SamplingLocation").eq("TG_Line")
    ).select(
        pl.exclude("SamplingLocation")
        )
valve_on_tg = valve_states.join(pic_on_tg, on=None, how="cross").filter(
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
        pl.col("SamplingLocation")
        )

valve_on_tg = valve_on_tg.join(
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
            pl.when(pl.col("SamplingLocation").is_null())
            .then(pl.col("SamplingLocation_right"))
            .otherwise(pl.col("SamplingLocation"))
            .alias("SamplingLocation")
            )

sampling_locs["TG_Line"] = pl.concat(
    [valve_on_tg,
     sampling_locs["TG_Line"].filter(
         ~pl.col("SamplingLocation").str.contains("C200")
         | pl.col("SamplingLocation").is_null()
         )]
    ).unique().sort(by="UTC_Start")

for inst, df in sampling_locs.items():
     temp_locs = pl.concat(
        [sampling_locs[inst].filter(
            ~pl.col("SamplingLocation").eq("TG_Line")
            | pl.col("SamplingLocation").is_null()
            ),
        df.join(
            sampling_locs["TG_Line"], on=None, how="cross").filter(
                (pl.col("UTC_Start").lt(pl.col("UTC_Stop_right"))) &
                (pl.col("UTC_Start_right").lt(pl.col("UTC_Stop")))
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
                    )]
     )
     temp_locs = temp_locs.with_columns(
         pl.col("UTC_Start").dt.offset_by("30s"),
         pl.col("UTC_Stop").dt.offset_by("30s")
         )
     sampling_locs[inst] = temp_locs