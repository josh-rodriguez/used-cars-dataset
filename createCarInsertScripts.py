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
collection = "carData"
filename_prefix = "insertCars_"

# Data clean-up list
dropColumns = ['','url','region_url','image_url','description']

# Create the preamble to the MongoDB insertMany command
importString = "use "+database+"\n"
importString += "db."+collection+".drop()\n"

count = 0
fileVer = 1

with open('vehicles.csv', encoding='utf-8') as file:
    vehicles = csv.DictReader(file)

    # importString += "db."+collection+".createIndex(\"_id\":1)"

    importString += "db."+collection+".insertMany(["

    for i,row in enumerate(vehicles):
        
        # Drop the columns we don't use
        for x in dropColumns:
            row.pop(x,None)
        
        # Data type transformation
        for k in row:

            if k == 'price':
                row[k] = None if row[k] == "" else float(row[k])
            if k == 'year':
                row[k] = None if row[k] == "" else float(row[k])
            if k == 'odometer':
                row[k] = None if row[k] == "" else float(row[k])
            if k == 'lat':
                row[k] = None if row[k] == "" else float(row[k])
            if k == 'long':
                row[k] = None if row[k] == "" else float(row[k])
            
            if k == 'posting_date':
                row[k] = "ISODate('{}')".format(row[k])
        
        # Delete keys with no values
        for k in list(row.items()):
            if k[1] == "":
                row.pop(k[0])

        # Set the doc _id to the existing car id, and format date
        importString += json.dumps(row).replace('"id":','"_id":').replace('"ISODate','ISODate').replace(')"}',')}')
        # Add comma before next object.
        importString += ",\n"

        # Break the write file loop after x documents for testing imports.
        # if (i >= 5000):
        #     break

        # Keep count of documents written to insertMany()
        count += 1

        # Create a new insertMany() command for every 50 documents.
        if(count % 50 == 0):

            # Remove last comma and close the insertmany() command.
            importString = importString[:-2]
            importString += "],{writeConcern: { w: 0}, ordered: false})"

            # Write new file every 50,000 documents
            if(count % 50000 == 0):

                with open(filename_prefix+str(fileVer)+".js","w+") as outfile:
                    outfile.write(importString)

                # Print file created success message
                runtime = datetime.now() - startTime
                print(f"Successfully created file: {filename_prefix}{str(fileVer)}.js in {runtime}")

                # Reset the importString to the use database command
                importString = "use "+database+"\n"
                
                # Increment the filename suffix.
                fileVer += 1
            
            # Create the preamble to the MongoDB insertMany command for next batch.
            importString += "\n"
            importString += "db."+collection+".insertMany(["  


# Remove last comma and close the insertmany() command.
importString = importString[:-2]
importString += "],{writeConcern: { w: 0}, ordered: false})"

# Write final javascript file.
with open(filename_prefix+str(fileVer)+".js","w+") as outfile:
    outfile.write(importString)

# Print file created success message
runtime = datetime.now() - startTime
print(f"Successfully created file: {filename_prefix}{str(fileVer)}.js in {runtime}")
print(f"All files created successfully.")