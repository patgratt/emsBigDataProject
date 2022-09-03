import pandas as pd
import os
import time

def main():

    # start timer on whole script
    script_start = time.time()

    input_folder = '/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/decompressed/'
    output_folder = '/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/cleaned_big/'

    # chunksize = 10 million rows
    chunksize = 10 * (10**6)

    file_counter = 0

    for file_name in os.listdir(input_folder):

        # ignore ds store
        if file_name == ".DS_Store":
            counter += 1
            continue

        # check size of file
        raw_size = os.path.getsize(input_folder + file_name)
        converted_size = convert_bytes(raw_size)

        # initialize the file
        file_start = time.time()
        print("Initialzing reading {} into pandas dataframe.".format(file_name))
        print("Size of {} = {}".format(file_name, converted_size))
        total_chunks = chunksize // converted_size

        # load the file
        with pd.read_csv(input_folder + file_name, sep='|', chunksize=chunksize) as reader:
            for i, chunk in enumerate(reader):
                
                print("Initialzing cleaning chunk number {} of {}".format(i+1, total_chunks))
                chunk_start = time.time()

                chunk.columns = chunk.columns.str.strip("~'")
                chunk = chunk.apply(lambda x: x.str.strip("~ ") if x.dtype == "object" else x)

                print("Finished cleaning chunk number {}".format(i+1))
                chunk_end = time.time()
                print("Time to clean chunk = {} seconds".format(chunk_end - chunk_start))



                new_file_name = file_name.replace(".txt",".csv").lower() + str(i+1)
                chunk.to_csv(output_folder + new_file_name, index=False)

        # file finished
        file_finish = time.time()







# calculate file size in KB, MB, GB
def convert_bytes(size):
    """ Convert bytes to KB, or MB or GB"""
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return "%3.1f %s" % (size, x)
        size /= 1024.0






if __name__ == '__main__':
	main()
