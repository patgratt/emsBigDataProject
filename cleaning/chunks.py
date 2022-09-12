import pandas as pd
import os
import os.path
import time
import gc

input_folder = '/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/decompressed/'
output_folder = '/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/factpcrvital_chunks/'

file_name = 'FACTPCRVITAL.txt'
new_file_name = file_name.replace(".txt","").lower()

# 5 million rows per chunk
chunk_size = 5000000
chunk_no=1

for chunk in pd.read_csv(input_folder + file_name, sep='|',chunksize=chunk_size):
    chunk.to_csv(output_folder + new_file_name + '_chunk' + str(chunk_no)+ '.csv',index=False)
    chunk_no+=1