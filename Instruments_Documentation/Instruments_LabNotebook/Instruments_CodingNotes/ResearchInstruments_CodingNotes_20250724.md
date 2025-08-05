# clean_rawdata.py
- Revised code to assign the appropriate sampling location to each data point rather than removing "invalid" ones, as that will keep all valid data even if measurements aren't where desired
- Need to determine sampling location for given times - considering only 2025 for now
	- For the instruments on the trace gas line, I will just note when they were connected to the line, and then the line itself will have sampling locations that will be carried through
	- During setups/takedowns/configuring of lines, sampling location will be None
		- For the most part, have this documented, aside from short times where lines are disconnected - return to this
- Able to use a single TG_Line DataFrame to make it so that the instruments on a common line can be declared as sampling from "TG_Line"
- While this has mostly worked, I do need to modify it to identify when sampling from UZA
- I successfully created a DataFrame that has the start and stop times for UZA measurement intervals, it is now a matter of applying that to previous data
	- Given this information uses Picarro G2307 data, I will have to revise to assign sampling locations in a separate for loop, rather than in the read-in loop