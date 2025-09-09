# characterize_zeros.py
- Noticed yesterday that there were some zeros with a null standard deviation - assume that means not enough points
- Going to revise to not calculate mean/std if number of points is too small (2 or less?)
- Joined the grouped dataframe with a dataframe that counts the number of items in each group, then filtered out zero intervals with 2 or fewer rows
	- Ensures all zero intervals have a standard deviation calculated