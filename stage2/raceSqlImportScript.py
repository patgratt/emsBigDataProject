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
CREATE TABLE IF NOT EXISTS pcrpatientracegroup (
    PcrPatientRaceGroupKey INT PRIMARY KEY NOT NULL,
    PcrKey INT NOT NULL,
    ePatient_14 INT,
    FOREIGN KEY(PcrKey) REFERENCES computedElements(PcrKey)
);
'''

# Execute create table statement
cursor.execute(create_table)
print("Table successfully created!")
print(f"Time to create table = {format_timespan(time.time() - t_create_table)}")

inputpath = ('/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/cleaned_small/pcrpatientracegroup.csv')

t_start_read = time.time()
print("Reading csv file...")
with open(inputpath, "r") as csvfile:
        contents = csv.reader(csvfile)
        # skip over the header row
        next(contents)

        # Define SQL statement to insert data into the computedElements table
        insert = '''
        INSERT INTO pcrpatientracegroup (PcrPatientRaceGroupKey,
            PcrKey,
            ePatient_14)
        VALUES (?, ?, ?);
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