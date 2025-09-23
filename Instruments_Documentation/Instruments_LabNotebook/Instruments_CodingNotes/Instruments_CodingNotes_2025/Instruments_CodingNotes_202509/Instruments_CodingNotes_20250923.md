# evaluate_offset.py
- Picking up, need to apply the calibration LOD and noise-to-signal regression-based uncertainty, both of which have averaging time dependences
	- From cal_factors\[inst\], selecting the columns that contain LOD and NoiseSignal information, keeping CalDate and AveragingTime
	- Trying to think about how to add information for ALL cal dates, given the previously I only was joining with one of each averaging time selected
	- How do I select which one to use?
		- NoiseSignal - based on highest R2
		- LODs
			- By caldate when available, otherwise by highest value
			- Don't need to do LOD for ALL cals, can just use the date that I actually will use
	- Finished selecting appropriate values