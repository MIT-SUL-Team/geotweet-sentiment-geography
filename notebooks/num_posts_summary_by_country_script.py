import sys
import os
import numpy as np
import pandas as pd
import gzip
from script import days_in_month, hours_in_day, leap_year
import time
from datetime import date, timedelta


def daily_num_posts_by_country(path_1, prefix_1, path_2, prefix_2, year, month, day):
    """
    @param path_n: str, file path (directory) to df n
    @param prefix_n: str, file prefix, "geography" for geography files, "bert_sentiment" for sentiment files
    @param year: int, year
    @param month: int, month
    @param day: int, day

    returns: a df of size (1, num_countries) representing number of common posts by countries on this day
    """
    day_path_1 = ''.join([path_1, prefix_1, "_", str(year), "_", str(month), "_", str(day).zfill(2)])
    day_path_2 = ''.join([path_2, prefix_2, "_", str(year), "_", str(month), "_", str(day).zfill(2)])
    num_posts_by_country = pd.DataFrame()
    for hour in hours_in_day():
        try:
            with gzip.open(''.join([day_path_1, "_", hour, ".csv.gz"])) as f:
                posts_1 = pd.read_csv(f, sep="\t")
        except FileNotFoundError:
            print(''.join([day_path_1, "_", hour, ".csv.gz"]), "does not exist.")
            continue
        except pd.errors.EmptyDataError:
            print(''.join([day_path_1, "_", hour, ".csv.gz"]), "is empty.")
            continue
        try:
            with gzip.open(''.join([day_path_2, "_", hour, ".csv.gz"])) as f:
                posts_2 = pd.read_csv(f, sep="\t")
        except FileNotFoundError:
            print(''.join([day_path_2, "_", hour, ".csv.gz"]), "does not exist.")
            continue
        except pd.errors.EmptyDataError:
            print(''.join([day_path_2, "_", hour, ".csv.gz"]), "is empty.")
            continue
        common_posts = pd.merge(posts_1, posts_2, on="message_id", how="inner")
        num_posts_by_country_this_hour = common_posts.groupby(['NAME_0']).size().to_frame().transpose()
        num_posts_by_country = pd.concat([num_posts_by_country, num_posts_by_country_this_hour], join="outer",
                                             sort=True)
        # print(num_posts_by_country.fillna(0).sum(axis=0).astype(int))
    return num_posts_by_country.fillna(0).sum(axis=0).astype(int)


def generate_daily_num_posts_by_country_df_year(year, geo_dir, sent_dir, out_dir):
    """
    @param year: int, year to generate the dataframe for
    @param geo_dir: directory under which the geography data files are stored
    @param sent_dir: directory under which the sentiment data files are stored
    @param out_dir: directory to which the dataframe will be stored as csv file

    Generates
    1) a csv file which includes the number of geotagged posts, sentiment posts, common psets by day for the givern year;
    2) missing file, empty file, corrupt file reports

    """
    geo_path = ''.join([geo_dir, str(year), "/"])
    sent_path = ''.join([sent_dir, str(year), "/"])
    data = []
    for month in range(1, 13):
        for day in range(1, days_in_month(month, year) + 1):
            print(year, month, day)
            data.append(pd.Series([year, month, day], index=["year", "month", "day"]).append(
                daily_num_posts_by_country(geo_path, "geography", sent_path, "bert_sentiment", year, month, day)))
    df = pd.DataFrame(data=data).fillna(0).astype(int)
    df = pd.melt(df, id_vars=["year", "month", "day"],
                 value_vars=df.drop(columns=["year", "month", "day"]).columns.tolist())
    df = df.rename(columns={"variable": "country", "value": "num_posts"})
    df.to_csv(''.join([out_dir, "num_posts_summary_by_country_", str(year), ".csv"]))