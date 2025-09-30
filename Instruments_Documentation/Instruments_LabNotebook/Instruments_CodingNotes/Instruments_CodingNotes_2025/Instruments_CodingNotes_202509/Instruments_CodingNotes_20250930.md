# align_timestamps.py
- Trying to correct Picarro time drift - wasn't working yesterday
	- Brought the Picarro data out of the loop to check that datetime offset was happening as I wanted
		- At this stage, it is
		- Based on a quick look at the figures, it now seems that I have corrected timestamp drift well enough
			- Method is correct even if value will later vary
- Now, want to go back into the UZA assignment and see if I can't figure out how to identify the first O3 point that measures UZA instead of the 2nd/3rd/whatever it was doing
	- Got it to identify only the first outlier as an outlier if there are consecutive outliers
	- But the second I bring it in to the actual processing loop it fails to identify outliers for 2BTech_205_A - this is the problem I was encountering yesterday