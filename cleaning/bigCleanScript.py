import pandas as pd
import os
import os.path
import time
import gc


def main():

    input_folder = '/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/decompressed/'
    output_folder = '/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/cleaned_big/'

    input_file_counter = 0

    # loop thru folder of raw files
    for file_name in os.listdir(input_folder):

        # ignore ds store
        if file_name == ".DS_Store":
            input_file_counter += 1
            continue

        # ignore if file has already been cleaned
        completed = os.listdir(output_folder)
        if file_name.replace('.txt','_chunks/').lower() in completed:
            input_file_counter += 1
            continue

        print(f"Analyzing {file_name}")

        # create chunk folder
        chunk_folder = os.mkdir(input_folder.replace('decompressed/',file_name.replace('.txt','_chunks/')).lower())
        print(f"Succesfully created {chunk_folder}")

        raw_file_size = convert_bytes(os.path.getsize(input_folder + file_name))
        print(f"Raw size of {file_name} = {raw_file_size}")

        # 5 million rows per chunk
        chunk_size = 5000000
        chunk_no=1

        loop_start = time.time()
        print("Initialzing reading chunk {chunk_no} of {file_name} into pandas dataframe.")
        
        # loop thru file by chunks
        for chunk in pd.read_csv(input_folder + file_name, sep='|',chunksize=chunk_size):
            iteration_start = time.time()

            print("Chunk {} of {} successfully read into pandas dataframe.".format(chunk_no, file_name))
            if chunk_no == 1:
                print(f"Time to load chunk {chunk_no} = {iteration_start - loop_start} seconds")
            else:
                print(f"Time to load chunk {chunk_no} = {iteration_start - iteration_end} seconds")

            # clean data
            print(f"Initializing data cleaning on chunk {chunk_no}")
            clean_start_time = time.time()
            chunk.columns = chunk.columns.str.strip("~'")
            chunk = chunk.apply(lambda x: x.str.strip("~ ") if x.dtype == "object" else x)
            clean_end_time = time.time()
            print(f"Time to clean this chunk = {clean_end_time -  clean_start_time} seconds")

            # export
            cleaned_chunk_name = file_name.replace(".txt",f"_chunk{chunk_no}.csv").lower()
            chunk.to_csv(output_folder + chunk_folder + cleaned_chunk_name, index=False)
            print(f"{cleaned_chunk_name} succesfully exported to {output_folder}")

            # analyze cleaned chunk
            cleaned_chunk_size = convert_bytes(os.path.getsize(output_folder + cleaned_chunk_name))
            print(f"Size of chunk after cleaning = {cleaned_chunk_size}")

            # free up memory
            gc.collect()

            chunk_no += 1
            iteration_end = time.time()


        # free up memory
            gc.collect()

        # iteration end
        loop_end = time.time()
        print(f"{file_name} succesfully broken up into chunks, cleaned, and exported")
        print(f"Time to clean entire file = {loop_end - loop_start} seconds")


# calculate file size in KB, MB, GB
def convert_bytes(size):
    """ Convert bytes to KB, or MB or GB"""
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return "%3.1f %s" % (size, x)
        size /= 1024.0


if __name__ == '__main__':
	main()


