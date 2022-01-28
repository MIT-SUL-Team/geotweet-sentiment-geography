"""
Author: @SirenaYu

Script to 1) generate dataframes which includes daily number of geotagged, sentiment, and common posts for any given year and to 2) generate corresponding line graphs.
"""

import sys
import os
import numpy as np
import pandas as pd
import gzip
from script import days_in_month, hours_in_day, leap_year
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import date, timedelta
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


def daily_num_posts_two_df(path_1, prefix_1, path_2, prefix_2, year, month, day):
    """
    @param path_n: str, file path (directory) to df n
    @param prefix_n: str, file prefix, "geography" for geography files, "bert_sentiment" for sentiment files
    @param year: int, year
    @param month: int, month
    @param day: int, day
    
    returns: number of geotagged posts, number of sentiment posts, number of common posts on this day
    """
    num_posts_1 = 0
    num_posts_2 = 0
    num_posts_common = 0
    day_path_1 = ''.join([path_1, prefix_1, "_", str(year), "_", str(month), "_", str(day).zfill(2)])
    day_path_2 = ''.join([path_2, prefix_2, "_", str(year), "_", str(month), "_", str(day).zfill(2)])
    for hour in hours_in_day():
        try:
            with gzip.open(''.join([day_path_1, "_", hour, ".csv.gz"])) as f:
                posts_1 = pd.read_csv(f, sep="\t")
                num_posts_1 += len(posts_1)
        except FileNotFoundError:
            print(''.join([day_path_1, "_", hour, ".csv.gz"]), "does not exist.")
            continue
        except pd.errors.EmptyDataError:
            print(''.join([day_path_1, "_", hour, ".csv.gz"]), "is empty.")
            continue
        try:
            with gzip.open(''.join([day_path_2, "_", hour, ".csv.gz"])) as f:
                posts_2 = pd.read_csv(f, sep="\t")
                num_posts_2 += len(posts_2)
        except FileNotFoundError:
            print(''.join([day_path_2, "_", hour, ".csv.gz"]), "does not exist.")
            continue
        except pd.errors.EmptyDataError:
            print(''.join([day_path_2, "_", hour, ".csv.gz"]), "is empty.")
            continue
        num_posts_common += len(pd.merge(posts_1, posts_2, on="message_id", how="inner"))
    return num_posts_1, num_posts_2, num_posts_common


def generate_daily_num_posts_df_year(year, geo_dir, sent_dir, out_dir):
    """
    @param year: int, year to generate the dataframe for
    @param geo_dir: directory under which the geography data files are stored
    @param sent_dir: directory under which the sentiment data files are stored
    @param out_dir: directory to which the dataframe will be stored as csv file
    
    Generates a csv file which includes the number of geotagged posts, sentiment posts, common psets by day for the given year
    """
    geo_path = ''.join([geo_dir, str(year), "/"])
    sent_path = ''.join([sent_dir, str(year), "/"])
    data = []
    for month in range(1, 13):
        for day in range(1, days_in_month(month, year) + 1):
            data.append([year, month, day] + list(daily_num_posts_two_df(geo_path, "geography", sent_path, "bert_sentiment", year, month, day)))
    df = pd.DataFrame(data=data,    # values 
             columns=["year", "month", "day", "num_geo_posts", "num_sent_posts", "num_common_posts"]) 
    df.to_csv(''.join([out_dir, "num_posts_summary_", str(year), ".csv"]))


def generate_daily_num_posts_graph_year(year, in_dir, out_dir):
    """
    @param year: int, year
    @param in_dir: str, directory to which num_post_summary csv files are stored
    @param out_dir: str, directory to which the graph will be stored
    """
    def get_date_list(year):
        base = date(year, 1, 1)
        numdays = 365 + int(leap_year(year))*1
        return [base + timedelta(days=x) for x in range(numdays)]
    
    df = pd.read_csv("".join([in_dir,"num_posts_summary_", str(year), ".csv"]))

    x = get_date_list(year)
    y_geo = df['num_geo_posts']
    y_sent = df['num_sent_posts']
    y_common = df['num_common_posts']

    plt.plot(x, y_geo, color='silver', label="Geotagged Posts")
    plt.plot(x, y_sent, color='grey', label="Posts with Sentiment Scores")
    plt.plot(x, y_common, color='green', label="Common Posts")

    plt.title("".join(["Number of Posts by Day in ", str(year)]))
    plt.legend(bbox_to_anchor=(1.6, 1.0), loc='upper right')

    plt.savefig("".join([out_dir, "daily_num_posts_graph_", str(year)]), bbox_inches = 'tight')