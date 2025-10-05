# clean_rawdata.py
- After realizing that the measurements for the Aranet sensors were not averaged, I want to delete the averaging times for them to prevent calculation of false averaging times
- Ran script after deleting averaging time files and going to check that the clean data looks how it should
	- It does, slay
# combine_aranet.py
- Since I changed to not determine averaging times (there isn't any), want to modify this script to work with that
- Removing rounding
- Also changing the temperature to use Fahrenheit if available since that apparently gives better precision?
- 