# characterize_zeros.py
- Noticed yesterday that there were some zeros with a null standard deviation - assume that means not enough points
- Going to revise to not calculate mean/std if number of points is too small (2 or less?)
- Joined the grouped dataframe with a dataframe that counts the number of items in each group, then filtered out zero intervals with 2 or fewer rows
	- Ensures all zero intervals have a standard deviation calculated
- I want to look at the cell temperature versus zero - won't want to keep this, but do want figures
- Creating a branch uza_temps
- Going to plot the mean zero value against the instrument temperature and see if there is a correlation
	- Absolutely there is!
	- Positive for Thermo, negative for O3
	- Will run an ODR to get an idea of the correlation slope
- Have ODR results for Thermo and 2B 205A
	- Picarro no clear correlation, but also never deployed without zeroing so not a concern
- Only using the 2025 zeros because I know that my sampling locations aren't all correct for 2024
- Would it be useful to export these results? Probably
- Revised code to create a correlation file to export
	- Same location as the UZA statistics files
# calibrate_instruments.py
- I want to take the S/N that I determined and add it to calibration results output
- Straightforward given it was already calculated, I just needed to add it to the output files
	- Checked and doing so was successful
# calibrate_cleandata.py
- Need to carefully think about how I am going to determine uncertainty
- Also need to think about how I am going to get estimated zero offset from the correlation plots
- I think it might be useful to go back to calibrate_instruments.py and get another entry that is the averaging time
	- Actually, I have the averaging times stored in external files - use those?
	- This might work, but the challenge is that the averaging time can change in a day and I only have the date of the calibration recorded, not the time
	- I think I will revise the calibration script again
# calibrate_instruments.py
- Revising to have an averaging time determined
- Using the gap between consecutive UTC_DateTimes
- Convert time differences to total seconds
	- Automatically rounds
- Get the mode
	- With zero second averaging time filtered out (for Picarro)
- Cast the mode to a string and add "s" to get the averaging time over the calibration
- Add it to the file to be exported
# calibrate_cleandata.py
- Now I have the averaging times for during the calibration in the cal_factors files