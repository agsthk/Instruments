# delete.py
- Confirming that the lab notes are representative
	- Picarro_G2307
		- What's happening June 14? 10:00, 13:00, 16:00 - CO2 addition????
			- New regulator - some kind of interference?
		- What's happening July 8? 19:21 ???
		- What's happening July 29? 12:00 - 20:00 ish - air flow measurements? CO2 addition? - I think it just has to do with people coming in/out of the space
		- What happened July 31 ~13:00? - GUV? someone in the room
		- What happened August 5? ~13:00? someone in the room
	- ThermoScientific_42i-TL
		- Add zeros 7/21 - 7/22
	- Satisfied with the interferences labeled, need to add the zeros
		- Done, now need to remove the zeros from June when the UZA was empty adjust clean_rawdata.py
# clean_rawdata.py
- Revised valve_states to not count solenoid valve states of 1 during period where UZA tank was empty
	- After checking, looks good!