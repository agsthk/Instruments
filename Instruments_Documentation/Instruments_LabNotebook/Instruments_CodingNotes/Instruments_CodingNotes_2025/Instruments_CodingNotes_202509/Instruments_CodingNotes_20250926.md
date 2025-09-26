# icartt_data.py
- Trying to figure out the errors that are causing the ICARTT scanner to fail
- First, found that I didn't actually store the file with the updated start times, so fixed that
- Campaign dates are off for the YAML files - accidentally extended the room ozone to start too early instead of the vent ozone - fixed
- Other issue is that the number of normal comments is off - needs to increase by 1 - fixed
	- No clue why this is, maybe the header counts as a normal comment?