# Picking up from last session
- Revise the sampling location assignment, ensure non-logged zeros are included, check data filtering
- Record all events over both campaigns to use with filtering
	- For now, can focus on ozone, CO2, and citrus peeling
- Prepare Phase II ICARTT files
	- Do not upload - will need to change a couple of things first
- Compare pre- and post-fixing leak measurements to establish a systematic relative error
	- Phase II files
# Sampling locations
- Going to go through Phase I and identify sampling locations for all instruments so as to complete this work
- Creating folder in ManualData called Instruments_SamplingLocations
	- Each instrument will have its own text file with all the sampling location information in it
		- FTC_Start,SamplingLocation,Comments will be the header
		- When I read it in, I will convert from Fort Collins time to UTC time, but it will be overly difficult if I try to manually convert every time to UTC
		- Comments column may end up being removed, but I think it could prove useful
	- When I read in, I may wish to cut off a period of time surrounding location switches - think about if that would be useful
	- Will be using "delete.py" to quickly visualize data I think as a way to check against my notes
	- Beginning with 2BTech 202 - only ever calibrated this, so should just be calibration source and B213