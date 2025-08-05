# combine-fxns branch
- Created and switched to new branch combine-fxns using command ```git checkout -b combine-fxns```
# structure_rawdata.py
- Combining/restructuring the functions that structure raw data will make revisions easier, as there is a lot in common with processing
- I will create schemas for all data sources to standardize CSV read-in
- ~~Created function read_polars() which uses the declared schemas to scan data to LazyFrame~~
- ~~Created function read_pandas() which uses pandas to read in data then converts to LazyFrame~~
- Created function read_daqdata(path, schema) that can handle all DAQ files, even those with errors
- Created smaller specific functions to make read_daqdata readible
	- Any read-ins that aren't one function only have homemade functions
