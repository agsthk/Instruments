# Git repository
- Due to the path length issues, I need to come up with a structure that has a shorter path length
- Considering moving my working project repositories outside of my OneDrive
	- Need to think about how this might work with lab notebooks - I want those backed up, and really I need to be able to document on my MacBook
		- I could use windows remote desktop, that would likely be fine
	- Should I have my documentation backed up by git?
		- I could since it's all markdown files - think about it
- Recreated ResearchInstruments project in agsthk directory on Windows PC and now am going to assess if this will fix the path length issue
	- Renamed "ResearchInstruments" to "Instruments" - I suspect that will do in combination with the change in location
	- Will be trying to use git to backup documentation
- After a lot of kind of difficult renaming and such, got git repo up to date (I think)
- Now, need to go into my scripts and change the paths
# calibrate_instruments.py
- After changing the paths and corresponding folder names, calibrate_instruments ran and exported the figures with no issue
	- Changed the time series plotting for NO2 to only show errorbars for data used to fit ODR
- Removed outlier points for NO2 calibration of Thermo instrument done with 2BTech cal source (100 ppb 2/6, 75 ppb others for some reason) and reran calibrations
	- Actually removed all that were too high - 75 and 100 ppb NO2
# calibrate_cleandata.py
- Since I have rerun the calibrations without forcing zeros, I want to apply those calibrations to the clean data
	- Which I will then use to check zeros
- Replaced explicit declaration of calibration factors with read-in of calibration results and declaration of date to use
- Encountering some issues with Picarro data having formaldehyde labeled as ppm instead of ppb
	- Rerunning structure_rawdata.py
		- It doesn't run for the 2025-02-15 file?
		- Unsure of the source of this issue, going to manually change the units in the affected files
- After resolving the units, the calibration script worked fine
- Revised file naming to include the calibration date
- When going from Clean -> Calibrated, it seems that the actual data was removed from the files - why?
	- No it wasn't lol I changed the script without realizing (in Keck O3) and was plotting the actual data instead of the zeros - my bad!
# select_projectdata.py
- Because I have moved this project, I need to move the KeckO3 project to my PC folder
- After doing so, the script runs with no issue (I think)