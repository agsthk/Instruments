# icartt_data.py
- Continuing work to fix precision, but first going to fix timestamp precision because I expect that to be relatively straightforward
- Added rounding to 1 decimal place when converting from datetime to seconds since midnight, and that worked
- Precision
	- Testing that it works for data with time-based uncertainty columns
		- Generated new ICARTT input files for 2BTech_205_A, 2BTech_205_B, ThermoScientific_42i-TL, AirChangeRates (although ACR I probably would like to fix to include ACRs calculated using Aranet sensors, which are currently omitted due to higher uncertainty relative to LI-COR)
		- For all of these, passed ICARTT scanner, quick looks show they did what I wanted
	- Challenge is now what to do with files that don't have (or don't only have) time-based uncertainty columns - Picarro, Aranet, LI-COR, TempRHDoor
		- Best bet is to probably put it in the ICARTT input file somewhere
		- Adding precision key to dependent variables where it'll need to be manual
		- Straightforward application if the uncertainty is constant, for percentage based, may need something else
		- Good on TempRHDoor, Picarro, just need to figure out LI-COR with the percentage and Aranet with no reported uncertainty (maybe Aranet doesn't need that?)
			- Aranet doesn't need a revision, no calibrations and it reports the precision it has
		- Briefly, decided to update header files after ICARTT files have been created such that complete == True and I don't have to change it manually
		- Using percent uncertainty to calculate an uncertainty column, which will be dropped after rounding
		- Realized that the rounding is also rounding missing values, so moved missing values filling after precision functions
		- I don't necessarily know that the LI-COR data actually got changed in any way, I think it was already reporting to appropriate precision most likely - but that's fine, I'll upload them anyway
	- Corrected slight issue with the fixed uncertainty rounding that was causing a random 3 to appear as the 17th digit after the decimal (after real rounding, rounded to 10 decimal places)
# structure_rawdata.py
- Revised not to overwrite existing structured files
- Added raw input parameters for new instruments and hub instruments (under Hub key)
- Created a read_hubdata function that is essentially the same as read_DAQ data except it doesn't need to handle extra header lines and it adds a warm up column that is all zeros (not strictly good, but can't think of a great way to determine when warm up starts and also I don't care that much)
- The hub data is offset by an hour after structuring? Has to do with reading in string values
	- Changed hub read in to read FTC_DateTime as a string first, then convert
	- It was still doing it because it was overwriting the timestamp with the monitor date and time since there was no UTC_DateTime column
		- Manually added a UTC_DateTime column in the read in function by converting the FTC_DateTime to get around this
# icartt_data.py
- Back to this to try to handle clean data from trace gas hub
- If there aren't any calibrations in the header (there aren't), if campaign starts in 2026, then try Hub first and if that doesn't exist then try Logger
	- This made it work (at least for now)