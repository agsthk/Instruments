# Picking up
- Need to handle averaging times
	- Declare by date?
- In the future, revise the timezone assignment to go by date
# structure_rawdata.py
- Running through all the DataFrames to establish averaging times for given DateTimes
	- Creating dictionaries containing averaging times keyed by start date
	- Averaging times frequently repeat
	- Not sure how to handle variable averaging time of Teledyne N300, but for now I will not worry about that since the data is invalid anyway
	- I am not positive, but I believe that the Thermo 42i-TL data is still averaged over a minute, even though it transmits every second with the DAQ
- Using the dictionary, I will establish start and mid times for the instruments collecting at less than 1 Hz frequency
# clean_rawdata.py
- Created new script which will take the raw (structured) instrument data and clean it
- Will also apply calibration factors at least for the moment
	- May revise later on to do this in a different script