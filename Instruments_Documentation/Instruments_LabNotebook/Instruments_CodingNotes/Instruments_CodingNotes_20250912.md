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