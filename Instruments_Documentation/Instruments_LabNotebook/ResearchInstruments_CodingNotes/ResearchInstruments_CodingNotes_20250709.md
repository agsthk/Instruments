# Virtual Environment
- Used conda-forge to install pytables
# structure_rawdata.py
- Overnight, I remembered that these files should be split by UTC date, so I will need to revise the code to do that
- Created function structure_picarro(raw_dir, struct_dir, source) to structure data collected by Picarro G2307
	- Like other functions, names based on start and stop in local time
- Will not be working on structuring Teledyne data at the moment considering none of the data are real
- For the moment, I am going to hold off on dealing with the temperature/RH/door status and addition valve state data, but I would like that included in this script
- Over the different functions I have defined in this script, there are a lot of repeats happening
	- I would like to revise this script to have one main "structure_rawdata" function that then accesses data from user-defined instrument/source
- I would also like to have simple "read" functions
	- Currently, I feel like the functions do too much to be only one function
	- I will return to this later, as it is functional in its current form, even if it isn't beautiful
- Defined function structure_temprhdoor(raw_dir, struct_dir)
	- Working in general, although fails on 20240109_Housekeeping_00.txt (no header), and TempRH_20240411_00.txt (Last line is messed up so getting mixed data type warning)