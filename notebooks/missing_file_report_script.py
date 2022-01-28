"""
Author: @SirenaYu

Script to generate csv files for list of missing, empty, corrupted files.

In missing_file_report, the following definition is used:

* A file is defined as missing if the filepath does not exist in the directory.
* A file is define as empty if the filepath exists but the file is empty.
* A file is defined as corrupted if the number of posts it contains is less than the lower of 1) 10,000 or 2) the bottom 10th percentile number of posts from that year.
"""

import sys
import os
import numpy as np
import pandas as pd
import gzip
from script import days_in_month, hours_in_day

def saving_list_of_files(year, folder, geo_dir, sent_dir, out_dir):
    """
    @param year: int, year
    @param folder: str, "geography" for geography folder, "sentiment" for sentiment folder
    @param geo_dir: directory under which geography data is stored
    @param sent_dir: directory under which sentiment data is stored
    @param out_dir: directory to which the csv files will be saved
    
    Save lists of missing, empty, and corrupted files from given year in csv format.
    """
    missing_files = []
    empty_files = []
    num_posts = []
    if folder == "geography":
        path = geo_dir
        prefix = "geography"
    else:
        path = sent_dir
        prefix = "bert_sentiment"
    file_name_to_num_post = dict()
    
    for month in range(1, 13):
        for day in range(1, days_in_month(month, year)+1):
            for hour in range(24):
                file_name = ''.join([path, str(year), "/", prefix, "_", str(year), "_", str(month), "_", str(day).zfill(2), "_", str(hour).zfill(2), ".csv.gz"])
                if not os.path.exists(file_name):
                    missing_files.append(file_name)
                else:
                    try: 
                        with gzip.open(file_name) as f:
                            posts = pd.read_csv(f, sep="\t")
                            num_posts.append(len(posts))
                            file_name_to_num_post[file_name] = len(posts)
                    except pd.errors.EmptyDataError:
                        empty_files.append(file_name)
                        continue
                        
    bottom_10_percentile = pd.Series(num_posts).quantile(0.1)
    threshold = min(10000, bottom_10_percentile)
    
    corrupted_files = []
    for month in range(1, 13):
        for day in range(1, days_in_month(month, year)+1):
            for hour in range(24):
                file_name = ''.join([path, str(year), "/", prefix, "_", str(year), "_", str(month), "_", str(day).zfill(2), "_", str(hour).zfill(2), ".csv.gz"])
                if file_name in file_name_to_num_post:
                    if file_name_to_num_post[file_name] < threshold:
                        corrupted_files.append(file_name)
    missing_files_df = pd.DataFrame(data=pd.Series(missing_files),
                               columns=["missing_files"])
    empty_files_df = pd.DataFrame(data=pd.Series(empty_files),
                               columns=["empty_files"])
    corrupted_files_df = pd.DataFrame(data=pd.Series(corrupted_files),
                               columns=["corrupted_files"])
    missing_files_df.to_csv(''.join([out_dir, "missing_files_", str(year), "_", folder,".csv"]))
    empty_files_df.to_csv(''.join([out_dir, "empty_files_", str(year), "_", folder, ".csv"]))
    corrupted_files_df.to_csv(''.join([out_dir, "corrupted_files_", str(year), "_", folder, ".csv"]))