# icartt_data.py
- Now that I have calculated the air change rates, creating an ICARTT input to get it to ICARTT format
- To get script to work, need to revise to say that if the "directory" in the clean data is a csv file, don't add it to the directory part
- Also need to only filter by sampling location if its in the file
- I also had an idea to put another parameter in the YAML files "complete" so that I don't have to move the header inputs out of the folder and can skip the headers I don't need