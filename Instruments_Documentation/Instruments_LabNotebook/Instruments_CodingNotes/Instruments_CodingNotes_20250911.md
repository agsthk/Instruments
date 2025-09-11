# calibrate_cleandata.py
- First order of business is fixing the zero correction during the UZA active periods
	- I do not need to be applying the calibration offset at all
	- Apply the UZA offset first, then just apply sensitivity
	- Ran into some trouble, turns out it was because it was treating the offset and sensitivity as an array instead of a single value
		- Changed assignment to include .item()