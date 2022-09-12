import csv
import sqlite3

connection = sqlite3.connect('/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/emsData.db')

# Creating a cursor object to execute
# SQL queries on a database table
cursor = connection.cursor()

# Define SQL statement to create table
create_table = '''
CREATE TABLE IF NOT EXISTS computedElements (
	PcrKey INT PRIMARY KEY,  
	USCensusRegion TEXT, 
	USCensusDivision TEXT,  
	NasemsoRegion TEXT,  
	Urbanicity TEXT,  
	ageinyear INT,  
	EMSDispatchCenterTimeSec INT,  
	EMSChuteTimeMin FLOAT,  
	EMSSystemResponseTimeMin FLOAT,  	
	EMSSceneResponseTimeMin FLOAT,  
	EMSSceneTimeMin FLOAT,  
	EMSSceneToPatientTimeMin FLOAT,  
	EMSTransportTimeMin FLOAT,  
	EMSTotalCallTimeMin FLOAT
);
'''

# Execute create table statement
cursor.execute(create_table)

# define path of csv file
filepath = '/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/cleaned_big/computedelements_chunks/computedelements_chunk1.csv'

# Read contents
with open(filepath, "r") as csvfile:
    contents = csv.reader(csvfile)
    next(contents)

# Define SQL statement to insert data into the computedElements table
insert = '''
INSERT INTO computedElements (PcrKey,
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
    EMSTotalCallTimeMin)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
'''

# Execute insert statement
cursor.executemany(insert, contents)

# SQL query to retrieve first 50 rows from the computedElements table to verify that the
# data from the csv file has been successfully inserted into the table
cursor.execute('.mode table')
select100 = 'SELECT * FROM ComputedElements LIMIT 50;'
rows = cursor.execute(select100).fetchall()

# Output to the console screen
for r in rows:
    print(r)

# Committing the changes
connection.commit()
 
# closing the database connection
connection.close()