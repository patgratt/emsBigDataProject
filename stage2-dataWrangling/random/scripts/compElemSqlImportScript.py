import csv
import sqlite3
import os
import time
from humanfriendly import format_timespan

t_start_script = time.time()
print("Establishing connection to sqlite3 database...")

connection = sqlite3.connect('/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/emsData.db')
print("Successfully connected to sqlite3 database!")
print(f"Time to connect = {format_timespan(time.time() - t_start_script)}")

t_create_table = time.time()
print("Creating table now...")
# Creating a cursor object to execute
# SQL queries on a database table
cursor = connection.cursor()

# Define SQL statement to create table
create_table = '''
CREATE TABLE IF NOT EXISTS computedElements (
	PcrKey INT PRIMARY KEY NOT NULL,  
	USCensusRegion TEXT NOT NULL, 
	USCensusDivision TEXT NOT NULL,  
	NasemsoRegion TEXT NOT NULL,  
	Urbanicity TEXT NOT NULL,  
	ageinyear INT NOT NULL,  
	EMSDispatchCenterTimeSec INT NOT NULL,  
	EMSChuteTimeMin FLOAT NOT NULL,  
	EMSSystemResponseTimeMin FLOAT NOT NULL,  	
	EMSSceneResponseTimeMin FLOAT NOT NULL,  
	EMSSceneTimeMin FLOAT NOT NULL,  
	EMSSceneToPatientTimeMin FLOAT NOT NULL,  
	EMSTransportTimeMin FLOAT NOT NULL,  
	EMSTotalCallTimeMin FLOAT NOT NULL
);
'''

# Execute create table statement
cursor.execute(create_table)
print("Table successfully created!")
print(f"Time to create table = {format_timespan(time.time() - t_create_table)}")
# Define path of chunk folder
# define path of csv file
chunkfolder = '/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/cleaned_big/computedelements_chunks/'

# loop thru the chunked csv files 
for chunk in os.listdir(chunkfolder):

    t_chunk = time.time()
    print(f"Initializing reading {chunk} and inserting into db...")
    # Read contents
    with open(chunkfolder + chunk, "r") as csvfile:
        contents = csv.reader(csvfile)
        # skip over the header row
        next(contents)

        # Define SQL statement to insert data into the computedElements table
        insert = '''
        INSERT INTO computedElements (
            PcrKey,
            USCensusRegion,
            USCensusDivision,
            NasemsoRegion,
            Urbanicity,
            ageinyear,
            EMSDispatchCenterTimeSec,
            EMSChuteTimeMin,
            EMSSystemResponseTimeMin,
            EMSSceneResponseTimeMin,
            EMSSceneTimeMin,
            EMSSceneToPatientTimeMin,
            EMSTransportTimeMin,
            EMSTotalCallTimeMin
            )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        '''

        # Execute insert statement
        cursor.executemany(insert, contents)
        print(f"Successfully inserted {chunk} into db!")
        print(f"Time to read and insert {chunk} = {format_timespan(time.time() - t_chunk)}")


# Committing the changes
connection.commit()
 
# closing the database connection
connection.close()

print("Successfully inserted all csv files into sqlite3 database")
print(f"Time for entire script to run = {format_timespan(time.time() - t_start_script)}")