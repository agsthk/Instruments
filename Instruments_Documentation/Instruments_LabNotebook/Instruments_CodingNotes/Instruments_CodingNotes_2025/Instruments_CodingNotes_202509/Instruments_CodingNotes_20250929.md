# align_timestamps.py
- Prior to checking the offset of the other instruments, need to apply the known timestamp drift correction to 2BTech_205_A
	- Tried a few different methods to do this, but ultimately:
		- Declared the timestamps known to be correct
		- Used the known correct timestamps and offset just before second correction (15 s ahead) to determine extra time added by instrument
			- ~5 us/s
		- Knowing approximate offset before first correction (~1 minute), used known drift to determine an approximate third (/first) "real" timestamp
		- Joined the real timestamps with the DataFrame and determined how many seconds the instrument said passed since the most recent known correct timestamp
		- Converted from instrument seconds passed to real seconds passed, then used that offset from the most recent correct timestamp to determine the corrected value
		- Seems to work well!