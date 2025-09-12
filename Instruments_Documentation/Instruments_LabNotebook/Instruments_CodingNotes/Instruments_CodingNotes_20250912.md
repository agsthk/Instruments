# calibrate_instruments.py
- Want to characterize instrument temperature and the rate of change of temperature during calibrations
- Beginning by simply plotting time series of temperatures during calibrations
	- For many of the cals, the temperature is actively ramping up during the calibration - problem?
- Checking the change in temperature with respect to time during the calibration
	- End up with a lot of zeros that I don't want because temperature does remain "constant" for some time
	- Instead of determining d/dt everywhere, identifying constant temperature intervals, taking the first time that temperature is registered, and dividing it by the time difference between two consecutive new temperatures
		- Justification is that the temperature is definitely changing over the interval, just not enough for the change to record at the instrument's precision
		- Was previously getting too many 0 values
- Added the minimum and maximum instrument temperature and median d(Temp)/dt to calibration outputs
- Revised averaging time to only be in there once
- Reordered calibration output to have extra information after
# characterize_zeros.py
- Want to check for any correlation between instrument temperature and UZA standard deviation (LOD)
- LOD is 3 times the uncertainty in y
- From here, it's just following the same procedure as with the offset
- Added code to export the LOD correlation information
	- Only significant for Thermo I think
- Quickly plotting LOD vs offset - I bet there is a correlation for Thermo
	- Not interested in keeping these plots, doesn't look informative
- Revised to export figures
- Want to investigate month-to-month change in correlation
	- Ozone monitor definitely does not show a clear correlation between LOD and cell temperature for any month
		- For offset vs cell temperature, strongest correlation is in April
		- Month to month change isn't clear enough - how would I apply it anyway?
	- Thermo
		- Feb, March, April show clearest correlations, April is a bit different than all together (weaker correlation), but Feb and march basically the same
	- Definitely not significant enough to account for - full Phase II measurements works just fine if not better
- Interested if the range of temperatures has a correlation with noise
	- As in, what if the temperature is changing significantly during zeroing period?
	- No clear relationship visually, not even going to attempt to fit it
- Calling script complete for now, moving on
# calibrate_instruments.py
- Thought of a way to get a more representative median d(Temp)/dt - fill the ones that would be zero with the one after, that way if it's maintaining that rate of change for a while, a representative number of points are contributed to the median
# calibrate_cleandata.py
- First things first, want to visualize cell temperature and rates of change, see what we get
	- Adding the calibration minimum and maximum temperatures and median dTemp/dT to figures as horizontal lines to visualize difference between calibration conditions and measurement conditions
	- Saving figures in here, but not necessarily long term - depends on how useful they end up being
		- Insane number of figures - by week would likely be better, but that would require reading in all files at once which I don't want to do
		- Basically, temperature is nearly always out of the calibration range
# structure_rawdata.py
- Not the point right now, but I do have more calibrations from Audrey to run
	- Downloaded the data to raw data folders, now going to run structure_rawdata.py and hope it works like it should
		- Well Picarro isn't an HDF file so that obviously threw an error
		- Revising to just add a csv reading code to get data from Logger that isn't H5 file - would prefer H5 file if I can get my hands on it, but hopefully this is fine
		- The DATE_TIME column is error in a lot of places?
			- Going to use Epoch time as the datetime instead and see what that does, and set the real date time column type to be a string
			- Threw an error because one of the other files is open somewhere idk
				- Adding a line to not write the new file if it already exists, so hopefully it'll just do the new ones and then I can delete that
				- It ran! With no error! Let's see if it even worked
					- It did! Picarro has the stupid dumb repeating times tho :/
- Revising sampling locations to be calibration source between the end of Phase II and the calibration for the respective instruments
	- Not running any additional scripts at this time because I don't have any calibration inputs!
# calibrate_cleandata.py
- Transitioning back to comparing the different offsets - fixed, true/interpolated, and predicted from temperature correlation
	- First, just visually examining the different offsets (which I did last time - what am I doing?)
	- I actually think this should be its own script
		- Will do similar things to calibrate_cleandata.py, but will have an easier time if I can read in all the data
		- I have a headache :( and it's 5:40, so come back to this another day