# Emergency Medical Services Data Analysis Project

## Stage 1: Data Preprocessing

- This stage consisted of:
    - Obtaining the data from NEMSIS
    - Downloading, decrypting and decompressing the dataset
    - Cleaning the data (removing spaces and unnecessary bloat characters)
    - Converting the data from .txt (psv) files to usable .csv files

## Stage 2: Data Cleaning / Wrangling

- This stage consisted of:
    - Exploring variables of interest
    - Further decreasing bloat by declaring types for variables
    - Throwing out observations with missing data
    - Aggregating the tables that I had to use chunks to split into multiple csv files into a local SQLite3 database
    - Joining variables from multiple tables into one csv that is usable for stats

## Stage 3: Stats