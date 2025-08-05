# Virtual Environment
- Replicated inst_venv on MacBook since I was unable to remotely connect to my work computer
	- Python version 3.11.13, Spyder version 6.0.7, conda install -c conda-forge cython matplotlib numpy pandas scipy sympy polars altair-all altair_viewer hvplot pytables tqdm
- Kernel wouldn't connect, so ended up completely uninstallying and reinstalling conda
	- After doing so, it worked
# Git Repository
- Had to mess around with creating an ssh key to be able to push to Git
	- Eventually figured it out
# clean_rawdata.py
- First tried to run the script as I left it yesterday and received an error
	- Upon examination, appears to be an issue with the ".DS_Store" files that Mac creates
	- Obviously these files can't be read in properly
	- Added line to skip these files
	- Also added these files to .gitignore
- Split the location assignments from the instrument read in loop
- Moved zeroing determination after instruments read in and before location assignments
- Moved replacement of TG_Line to after TG_Line was replaced with zeroing information
- Reassigning sampling location now takes significantly longer - is there a way to speed it up?
- Instead of doing it the way I originally coded, I am concatenating the sampling locations DataFrame with the data ones, sorting by time, and then forward filling location values