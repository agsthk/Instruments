# Virtual Environment
- Used conda-forge to install tqdm so as to be able to monitor progress
# structure_rawdata.py
- Still working in combine-fxns branch
- Checked to make sure that all 2BTech data can be successfully read in with functions in current state, and they can
- Revised Temp/RH/Door read-in to first check the first line of the file and determine number of rows to skip based on whether the first element of the first line can be converted to an integer
	- Significantly speeds up run time as read-in is not required
	- Also changed read-in to be via polars rather than pandas, with the end of line character specified as "\r"
	- May wish to do something similar for 2BTech later
		- Temp/RH/Door files are much larger and most of them do skip one line
		- Most 2BTech files do not need to skip lines, so defaulting to skip 0 lines is likely acceptable
- Defined a function to read Picarro HDF5 files, as renaming the columns makes it enough steps to do so
- With a working function to read in the raw data, I can shift my focus to writing a function to handle DateTime determination
	- Defined the datetime formats of the strings made by concatenating date and time together joined by a space