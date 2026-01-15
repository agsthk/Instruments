# calibrate_cleandata.py
- After running calibration script on the cals Morgan did, wanted to calibrate them for the ICARTT files
- Changed year to calibrate data for to 2026 and appropriate cal dates to 20260112
# icartt_data.py
- Revised to select Hub data for 2026 campaign start if calibrated, so I could create ICARTT files with preliminary calibrations applied
# derive_occupancy.py
- Creating this script to try to transform door status into occupancy status - use a combination of the door sensor, CO2 measurements, and lab notes
- Read in DoorStatus file, LI-COR files, and Aranet files