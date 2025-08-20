# calibrate_cleandata.py
- Intending to incorporate zeroing/variable LOD into this script rather than having it run in the KeckO3 project
	- The challenge here is that I need the zeros for the whole time frame to allow for interpolation, but I don't want to read in and concatenate all of the data to do that
	- Would be useful to have a separate script to get the zeros from the clean raw data - then I can just read in the zeroing information and use that without reading all of the data in
	- Creating a new script to handle this
# characterize_zeros.py
- How do I handle calibrations? I suspect the best method would be to characterize without any calibration, then I can just apply after?
	- Actually I'm unsure if this would give the desired result
	- Did the math and figured out the appropriate transformation
		- Let $x_{cal}=\dfrac{x_{init} - off}{sens}$
		- $\mu_{cal}=\dfrac{\mu_{init} - off}{sens}$
		- $\sigma_{cal}=\dfrac{\sigma_{init}}{sens}$
		- Tested this with various randomly generated integer arrays and it always worked
- What I want to do is go through all of the clean data I have, skip through the ones where there is no UZA sampling, and take the mean/standard deviation of the zeros and put them in a DataFrame to export
	- To derived data
- Have a loop that looks through all of the files, keeps the zero data
- Then, the zero data from different dates is concatenated
	- Doing it this way in case a zeroing interval carries over between dates, although this situation is not accounted for
- Then, statistics are run on all zeroing intervals
- Then, exporting the zero statistics in appropriate DerivedData folders
# calibrate_cleandata.py
- Now that I have the zero statistics, I can just pull those without having to read in and concatenate all files
- Adding lines to get all of the zeros read in
- Having trouble with joining the data frames
	- Previously I used the interval, but I don't see a way I can make that work in a separate script
	- Difficult to determine what I am doing as well