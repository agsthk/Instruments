# Virtual Environment
- Created new virtual environment in Anaconda Navigator called inst_venv with python version 3.11.13 installed
- Installed Spyder version 6.0.7 via Anaconda Navigator
- Used conda-forge channel in Anaconda Powershell Prompt to install cython, matplotlib, numpy, pandas, scipy, sympy, polars, altair-all, altair_viewer, and hvplot
# Spyder Project
- Initialized ResearchInstruments as a new Spyder project
# Git Repository
- Created new private "ResearchInstruments" repository in github
	- Initialized with a README file, Python templated .gitignore file, and no license
- In Git Bash terminal, navigated to ~/OneDrive - Colostate/Graduate/Graduate_ResearchProjects/ResearchInstruments and initialized as a local git repository using ```git init```
- Linked local and remote git repositories using ```git remote add origin git@github.com:agsthk/ResearchInstruments.git```
- Fetched all remote files and merged remote and local repositories
	- ```git fetch --all```
	- ```git merge origin/main```
- Added "ResearchInstruments_Data/" and "ResearchInstruments_Documentation/" directories to .gitignore
	- Staged edited .gitignore file and pushed to github
# structure_rawdata.py
- The aim of this script is to structure the raw collected data into a standard format
- Each instrument will have its own function, and each data source for each instrument may also have its own function
	- May make the data source a function argument instead
- Set directories
- Defined function read_2b202(raw_dir, struct_dir) to read in and structure data in 2BTech_202_RawData
	- The directory arguments are the overarching directories, not the instrument specific ones
	- May revise this function to handle 2BTech ozone monitor data from different sources since the read in should be the same if the source is the same?
- Changed read in function to be based on 2B ozone monitor data sources
	- Logger and SD should be the same read in I believe
- Defined function structure_2b_o3(raw_dir, struct_dir, inst, source)
	- Capable of reading either SD or Logger data
	- May extend to all 2B instruments
	- Changed name of function to structure_2btech and added schema for NOx monitor
	- After a couple revisions, no errors raised on any of the raw data files
- Defined function structure_thermo(raw_dir, struct_dir)
	- Straightforward, but uses pandas for initial read in due to non-specific whitespace separation
	- Removes columns that aren't transmitted to the DAQ
- Defined function structure_licor(raw_dir, struct_dir, inst)
	- Like with Thermo, reading in using pandas initially due to tab separation
	- Additional hurdle based on observation of daylight savings
		- Created workaround