# evaluate_offsets.py
- Left off with all possible offsets added to concatenated 2025 data
- Now, will actually calibrate the data using all possible offsets and sensitivities (where appropriate)
	- For fixed offset, apply sensitivity of the same calibration
	- For other offsets, check all sensitivities to see
- Added sensitivity columns to make it easier to calculate without having to go back into the calibration data
- First, uses fixed calibration offsets to calibrate data
- Then, for each calibration date, uses the UZA and Temperature correlation offsets to calibrate