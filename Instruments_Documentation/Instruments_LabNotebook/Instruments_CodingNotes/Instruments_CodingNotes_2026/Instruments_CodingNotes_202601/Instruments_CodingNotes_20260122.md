# derive_occupancy.py
- Want to update the occupancy file to group occupancies if they are less than 5 minutes apart
- Took out the code that splits by week
- Wrote code to merge consecutive rows if 10 or fewer minutes pass between start and stop
- Quick visual check seems to indicate it worked