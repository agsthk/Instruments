# Addition Valve data
- Collected missing addition valve state data this morning, so running structure_rawdata.py, interpret_additionvalves.py, and select_projectdata.py
# calibrate_instruments.py
- Created new script that I will use to run calibrations
	- Will use external input files, but for testing will hard-code calibration information
- Using polars and scipy ODR, managed calibration for all ozone monitors
	- Not forcing zero
- To move away from hardcoding, I am thinking about where to put the file information
- Created new folder ReserachInstruments_ManualData that will contain the data I have to manually define
	- Calibration inputs
	- Times that the room is occupied?
	- Non-automated additions
- I'm trying to consider what format I want this calibration information in
	- A separate input file for each calibration?
	- I think I want all outputs of the calibration in one file, with information on the date the calibration was done included
		- This will allow me to easily read in all calibrations and check them against one another
		- Separate files for each instrument
	- All calibration inputs will go in one file, even those occurring on different days
		- On read in, I will split them into different calibration days since they have their full DateTime written
	- Turned all of the separate calibration input files I have into one file for each instrument with timestamps now in ISO format and including timezone information
- Revised code to get calibration inputs from an external file, but it isn't running the regression for the Picarro instrument
	- I will need to resolve this
	- I also need to check my notes about the NOx SDC calibrations - want a point of comparison



Removed 2BTech_405nm from Keck room 20240625 AM
2BTech_405nm running on lab room air from ? to 20240702 08:30??
5s averaging starting 9:40:30 20240702
Back on room air 14:13 and changed avgng time back to 1 minute

Thermo turned on 20241216 11:13
14:37 - 14L38

20240201 2B Nox & thermo
20240206 thermo


20240612 Thermo turned on 16:20 to pull room air (which room?)

20240206 Thermo connected to cal source at 12:02, sequence start 12:05
20240206 2B Nox connected to cal source 09:41, sequence started same time
1/19/24 both nox monitors synched and on farmer lab room air at 16:03 w/ avg time of 1 minute

1/8/24 thermo nox was on NY time (2 hours ahead of FTC time)
???
20240108T12:42:00-0700,20240108T13:21:00-0700,0,0,0,0,0,0,
20240108T13:21:00-0700,20240108T13:49:00-0700,22.58,,0,0,22.58,
20240108T13:49:00-0700,?,50.35,,0,0,50.35,
???
20240118T14:09:00-0700,20240118T14:15:00-0700,0,0,0,0,0,0
20240118T14:15:00-0700,20240118T14:49:00-0700,
20240118T14:49:00-0700,20240118T15:11:00-0700,
20240118T15:11:00-0700,?
???
20240119T13:23:00-0700,20240119T13:29:00-0700,0,0,0,0,0,0
20240119T14:51:00-0700,20240119T14:57:00-0700,0,0,0,0,0,0
20240119T15:00:00-0700,20240119T15:06:00-0700,
20240119T15:07:00-0700,20240119T15:10:00-0700
20240119T15:10:00-0700,20240119T15:15:00-0700
20240119T15:15:00-0700,20240119T15:18:00-0700
20240119T15:18:00-0700,20240119T15:25:00-0700
20240119T15:25:00-0700,20240119T15:40:00-0700
20240119T15:40:00-0700,20240119T16:21:00-0700
20240119T16:21:00-0700,20240119T16:30:00-0700


20240201T11:00:00-0700,20240201T11:09:00-0700,0,0,0,0,0,0
20240201T11:12:00-0700,20240201T11:16:00-0700,0,0,0,0,0,0
20240201T11:16:00-0700,20240201T11:28:00-0700,39.14,,0,,39.14,
20240201T11:34:00-0700,20240201T11:44:00-0700,39.14,,0,,39.14,
20240201T11:47:00-0700,20240201T11:57:00-0700,81.38,,0,,81.38,
20240201T12:02:00-0700,20240201T12:12:00-0700,65.03,,0,,65.03
20240201T12:16:00-0700,20240201T12:26:00-0700,31.77,,0,,31.77
20240201T12:26:00-0700,20240201T13:56:00-0700,0,0,0,0,0,0
20240201T14:00:00-0700,20240201T14:13:00-0700,,,,,84.98,
20240201T14:17:00-0700,20240201T14:28:00-0700,,,,,42.11,
20240201T14:34:00-0700,20240201T14:46:00-0700,,,,,110.58,
20240201T14:50:00-0700,20240201T15:01:00-0700,,,,,53.53,
20240201T15:01:00-0700,20240201T15:11:00-0700,53.53,,0,,53.53,
20240201T15:14:00-0700,20240201T15:26:00-0700,106.67,,0,,106.67,