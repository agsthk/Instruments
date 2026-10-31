# calculate_acrs.py
- Continuing with this script, first will be checking background
	- Making weekly plots to see if I can define a background easily enough - looks fairly reasonable for the time periods where I do have perturbation times defined (aka not 2023-early 2024)
		- Also missing a week in March 2024? Whatever
	- The background definition works well enough, my issue now is that I'm torn between using data from tail end of decay or missing regular backgrounds when the additions are very close
		- Perhaps can set offset depending on time period? It seems to be 1/18 1/28 2025 ish that I need to use closer ones, everything else appears fine at 4h(?)
	- There's something wrong with my occupancy file - going to try to fix if I can? have to go into derive_occupancy.py
# derive_occupancy.py
- Trying to fix whatever is wrong with my occupancy file that has the room occupied for a week straight despite that not being in the source file
- Revised how I combine occupied periods that are less than 10 minutes apart
- Double checked times being determined and they look good at first glance
- Revised ICARTT generation to be R1
# calculate_acrs.py
- Now that I fixed occupancy files, looks much better
- Finally decided the time interval to cut out after additions and occupancy, varies with time in the campaign a little bit but mostly consistent, need to stop it with the background and move on
- Resampled background at 10 minute time resolution
- Applied background modeling method to Aranet data