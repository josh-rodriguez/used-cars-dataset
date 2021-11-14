# =============================================================================
# Authors: Cruz-Tokar, Rodriguez, Warren
# Course: BAN 707 Big Data
# Created Date: August 15, 2021
# =============================================================================

import csv
import json
from datetime import datetime

# Script timer start
startTime = datetime.now()

# DB & Collection name
database = "jrodriguez23_FinalProject"
collection = "cityData"
filename_prefix = "insertCity_"

# Create the preamble to the MongoDB insertMany command
importString = "use "+database+"\n"
importString += "db."+collection+".drop()\n"

count = 0
fileVer = 1

with open('cal_cities_lat_long.csv') as file:
    
    cal_cities = csv.DictReader(file)
    importString += "db."+collection+".insertMany(["

    for i,row in enumerate(cal_cities):
        
        # Data type transformation
        for k in row:
            if k == 'Longitude':
                row[k] = None if row[k] == "" else float(row[k])
            if k == 'Latitude':
                row[k] = None if row[k] == "" else float(row[k])
        
        importString += json.dumps(row)
        importString += ",\n"
        
        count += 1
        
        # Break the write file loop after x documents for testing imports.
        # if (i >= 99):
        #     break

        # Create a new insertMany() command for every 50 documents.
        if(count % 50 == 0):
            importString = importString[:-2]
            importString += "],{writeConcern: { w: 0}, ordered: false})"

            if(count % 50000 == 0):

                with open(filename_prefix+str(fileVer)+".js","w+") as outfile:
                    outfile.write(importString)
                
                # Print file created success message
                runtime = datetime.now() - startTime
                print(f"Successfully created file: {filename_prefix}{str(fileVer)}.js in {runtime}")

                importString = "use "+database+"\n"
                
                fileVer += 1
                        
            importString += "\n"
            importString += "db."+collection+".insertMany(["


importString = importString[:-2]
importString += "],{writeConcern: { w: 0}})"

# Create JavaScript file

with open(filename_prefix+str(fileVer)+".js","w+") as outfile:
    outfile.write(importString)

runtime = datetime.now() - startTime
print(f"Successfully created file: {filename_prefix}{str(fileVer)}.js in {runtime}")
print(f"All files created successfully.")