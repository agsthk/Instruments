# evaluate_leak.py
- Want to get an idea of the background before and after the leak fixed
	- With both possible dates that leak was introduced
	- Going to take within 2 days of before and after leak fixed to visually compare
		- Actually doing within 3 days
	- After looking at June 13 vs June 24 date, leak was definitely introduced June 13 - satisfied with that!
- Want to look at the NOx as well just to have an idea of what effect that might have
	- Only have NOx measurements from B203, but NO is basically zero in that room - presumably similar?
- Did same background comparison with Phase II data (3 days difference)
# ICARTT files
 - Going to create the ICARTT header files for the Phase I data, though waiting to put the bias info in there until I talk to Megan
 - First, created the Phase II R1 headers that I'll use to share the actually background corrected data
 - Then, created the Phase I R2 header for O3 and R1 headers for other instruments
 - Running icartt_data.py to get the R1 phase II data and updated phase I data (most of which will be overwritten after I comment on the leak)
	 - Didn't work lol
	 - Did work for vent CO2, only ran for uncalibrated data
	 - But then for vent CO2 needed to add a comment about the time offset correction - did that and re-ran
# icartt_data.py
- Need to fix directory identification for calibrated data to not look in DAQ folder for phase I
- Moving determination of campaign start and stop to be earlier to use for selection of directories
- Revised to look in DAQ directory for 2025 data, and for 2024 data look in SD if it exists and Logger if it doesn't