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
# Use integer for now even though some of these are really booleans
create_table = '''
CREATE TABLE IF NOT EXISTS raceTable (
    PcrKey INT PRIMARY KEY NOT NULL,
    black INTEGER,  
    white INTEGER,  
    hispanic/latino INTEGER,  
    asian INTEGER,  
    americanIndian/alaskaNative INTEGER,  
    nativeHawaiian/otherPacificIslander INTEGER
    );
'''

# Execute create table statement
cursor.execute(create_table)
print("Table successfully created!")
print(f"Time to create table = {format_timespan(time.time() - t_create_table)}")

inputpath = ('/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/raceInProgress/raceV3.csv')

t_start_read = time.time()
print("Reading csv file...")
with open(inputpath, "r") as csvfile:
        contents = csv.reader(csvfile)
        # skip over the header row
        next(contents)

        # Define SQL statement to insert data into the computedElements table
        insert = '''
        INSERT INTO raceTable (PcrKey,
            black,
            white,
            hispanic/latino,
            asian,
            americanIndian/alaskaNative,
            nativeHawaiian/otherPacificIslander)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        '''

        # Execute insert statement
        cursor.executemany(insert, contents)
        print(f"Successfully inserted {inputpath} into db!")


# Committing the changes
connection.commit()
 
# closing the database connection
connection.close()

print("Successfully inserted csv file into sqlite3 database")
print(f"Time for entire script to run = {format_timespan(time.time() - t_start_script)}")