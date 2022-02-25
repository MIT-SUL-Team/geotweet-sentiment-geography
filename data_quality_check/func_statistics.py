# -*- coding: utf-8 -*-
# @File       : func_statistics.py
# @Author     : Yuchen Chai
# @Date       : 2022-02-25 16:33
# @Description:

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


def generate_daily_num_posts_df_year(year=None):
    """
    如果year是None，就跑全部。
    如果year是具体值，就跑那一年
    @param year: int, year to generate the dataframe for
    @param geo_dir: directory under which the geography data files are stored
    @param sent_dir: directory under which the sentiment data files are stored
    @param out_dir: directory to which the dataframe will be stored as csv file

    Generates a csv file which includes the number of geotagged posts, sentiment posts, common psets by day for the given year
    """
    geo_dir=""
    sent_dir=""
    out_dir=""
    raise Exception("Change geo dir")

    if year is None:
        pass
    else:
        pass





    geo_path = ''.join([geo_dir, str(year), "/"])
    sent_path = ''.join([sent_dir, str(year), "/"])
    data = []
    for month in range(1, 13):
        for day in range(1, days_in_month(month, year) + 1):
            data.append([year, month, day] + list(
                daily_num_posts_two_df(geo_path, "geography", sent_path, "bert_sentiment", year, month, day)))
    df = pd.DataFrame(data=data,  # values
                      columns=["year", "month", "day", "num_geo_posts", "num_sent_posts", "num_common_posts"])
    df.to_csv(''.join([out_dir, "num_posts_summary_", str(year), ".csv"]))


def get_daily_sentiment_avg_and_num_posts(cities, year, month, day):
    """
    @param cities: list of str in the form of "COUNTRY_STATE_CITY" (ex. "United States_California_Los Angeles"), cities that we want to look at
    @param year: int
    @param month: int
    @param day: int

    returns: Pandas DataFrame, contains three columns: city, daily_avg_score (average sentiment scores from the day) and num_posts (number of posts from the day)
    """
    start_time = datetime.now()
    geo_path = "".join(["/srv/data/twitter_geography/", str(year), "/"])
    sent_path = "".join(["/srv/data/twitter_sentiment/", str(year), "/"])
    date = "".join([str(year), "_", str(month), "_", str(day).zfill(2)])

    result_df = None

    for hour in range(0, 24):
        pre_open_time = datetime.now()
        try:
            with gzip.open(''.join([geo_path, "geography_", date, "_", str(hour).zfill(2), ".csv.gz"])) as f:
                geo_posts = pd.read_csv(f, sep="\t")
        except FileNotFoundError:
            print(''.join([geo_path, "geography_", date, "_", str(hour).zfill(2), ".csv.gz"]), "does not exist.")
            continue
        except pd.errors.EmptyDataError:
            print(''.join([geo_path, "geography_", date, "_", str(hour).zfill(2), ".csv.gz"]), "is empty.")
            continue
        try:
            with gzip.open(''.join([sent_path, "bert_sentiment_", date, "_", str(hour).zfill(2), ".csv.gz"])) as f:
                sent_posts = pd.read_csv(f, sep="\t")
        except FileNotFoundError:
            print(''.join([sent_path, "bert_sentiment_", date, "_", str(hour).zfill(2), ".csv.gz"]), "does not exist.")
            continue
        except pd.errors.EmptyDataError:
            print(''.join([sent_path, "bert_sentiment_", date, "_", str(hour).zfill(2), ".csv.gz"]), "is empty.")
            continue

        common_posts = pd.merge(geo_posts, sent_posts, on="message_id", how="inner")

        common_posts["COUNTRY_STATE_CITY"] = common_posts['NAME_0'].astype(str) + '_' + common_posts['NAME_1'].astype(
            str) + '_' + common_posts['NAME_2']

        post_in_cities = common_posts[common_posts["COUNTRY_STATE_CITY"].isin(cities)]
        city_result = post_in_cities.groupby(["COUNTRY_STATE_CITY"]).agg(
            {"score": np.sum, "message_id": len}).reset_index()
        city_result.rename(columns={"COUNTRY_STATE_CITY": "city", "score": "total_score", "message_id": "num_posts"},
                           inplace=True)
        if result_df is None:
            result_df = city_result
        else:
            result_df = result_df.merge(city_result, on="city", how="outer", suffixes=('_x', '_y'))
            result_df["total_score"] = result_df["total_score_x"].fillna(0) + result_df["total_score_y"].fillna(0)
            result_df["num_posts"] = result_df["num_posts_x"].fillna(0) + result_df["num_posts_y"].fillna(0)
            result_df.drop(columns=["total_score_x", "total_score_y", "num_posts_x", "num_posts_y"], inplace=True)

    if result_df is None:
        result_data = np.array([
            cities,
            [0] * len(cities),
            [np.nan] * len(cities)
        ])
        result_df = pd.DataFrame(data=result_data.T,
                                 columns=["city", "num_posts", "daily_avg_score"])
        result_df = result_df.astype({'num_posts': 'int64'})
    else:
        result_df["daily_avg_score"] = result_df["total_score"] / result_df["num_posts"]
        result_df.drop(columns=["total_score"], inplace=True)

    end_time = datetime.now()
    print("get_daily_sent_avg_and_num_posts took", end_time - start_time, "seconds.")
    return result_df


def generate_daily_sentiment_avg_and_num_posts_by_month_csv(cities, year, month, city_group, out_dir):
    """
    @param cities: list of str in the form of "COUNTRY_STATE_CITY" (ex. "United States_California_Los Angeles"), cities that we want to look at
    @param year: int, desired year
    @param month: int, desired month
    @param city_group: name of group of the cities (ex. "top_5_us_cities")
    @param out_dir: str, output directory

    Generates csv files for average daily sentiment and daily number of posts for given year and month and cities.
    Save csv file under the name "CITY_GROUP_sentiment_avg_YEAR_MONTH.csv" or "CITY_GROUP_num_posts_YEAR_MONTH.csv"

    returns: nothing
    """
    sentiment_avg_df = None
    num_post_df = None
    for day in range(1, days_in_month(month, year) + 1):
        date = "".join([str(year), "_", str(month), "_", str(day).zfill(2)])
        daily_df = get_daily_sentiment_avg_and_num_posts(cities, year, month, day)
        daily_sentiment_avg_df = daily_df.drop(columns=["num_posts"])
        daily_sentiment_avg_df = daily_sentiment_avg_df.rename(columns={"daily_avg_score": date})
        daily_num_post_df = daily_df.drop(columns=["daily_avg_score"])
        daily_num_post_df = daily_num_post_df.rename(columns={"num_posts": date})
        if sentiment_avg_df is None:
            sentiment_avg_df = daily_sentiment_avg_df
        else:
            sentiment_avg_df = sentiment_avg_df.merge(daily_sentiment_avg_df, on="city", how="outer")
        if num_post_df is None:
            num_post_df = daily_num_post_df
        else:
            num_post_df = num_post_df.merge(daily_num_post_df, on="city", how="outer")

    sentiment_avg_by_month_df = sentiment_avg_df.T
    sentiment_avg_by_month_df.rename(columns=sentiment_avg_by_month_df.iloc[0], inplace=True)
    sentiment_avg_by_month_df.drop(sentiment_avg_by_month_df.index[0], inplace=True)
    display(sentiment_avg_by_month_df)
    sentiment_avg_by_month_df.to_csv(
        "".join([out_dir, city_group, "_sentiment_avg_", str(year), "_", str(month).zfill(2), ".csv"]))

    num_post_by_month_df = num_post_df.T
    num_post_by_month_df.rename(columns=num_post_by_month_df.iloc[0], inplace=True)
    num_post_by_month_df.drop(num_post_by_month_df.index[0], inplace=True)
    display(num_post_by_month_df)
    num_post_by_month_df.to_csv(
        "".join([out_dir, city_group, "_num_posts_", str(year), "_", str(month).zfill(2), ".csv"]))


def months_in_between(start, end):
    """
    @param start: datetime object, start date
    @param end: datetime object, end date

    returns: months in between the start and the end as a list (Ex. ["2008_08", "2008_09"])
    """
    delta = end - start  # as timedelta
    months_with_duplicate = [f"{start + timedelta(days=i):%Y_%m}" for i in range(delta.days + 1)]
    months = pd.Series(months_with_duplicate).unique()
    return months


def get_corrupted_dates(start, end, file_dir):
    """
    @param start: datetime object, start date
    @param end: datetime object, end date
    @param file_dir: file directory under which corrupted file reports are stored

    returns: corrupted dates in between the start and the end as a list
    """

    def get_date(file_name, category):
        if category == "geography":
            return "_".join(file_name.split("/")[5].split(".")[0].split("_")[1:4])
        else:
            return "_".join(file_name.split("/")[5].split(".")[0].split("_")[2:5])

    corrupted_dates = set()
    for year in range(start.year, end.year + 1):
        try:
            corrupted_files_geo = pd.read_csv("".join([file_dir, "corrupted_files_", str(year), "_geography.csv"]))
        except FileNotFoundError:
            raise Exception("".join(["Corrupted file report for geography data in ", year, " does not exist"]))
        try:
            corrupted_files_sent = pd.read_csv("".join([file_dir, "corrupted_files_", str(year), "_sentiment.csv"]))
        except FileNotFoundError:
            raise Exception("".join(["Corrupted file report for geography data in ", year, " does not exist"]))

        corrupted_files_geo.drop(columns="Unnamed: 0", inplace=True)
        corrupted_files_sent.drop(columns="Unnamed: 0", inplace=True)

        corrupted_files_geo['dates'] = corrupted_files_geo['corrupted_files'].apply(lambda x: get_date(x, "geography"))
        corrupted_files_sent['dates'] = corrupted_files_sent['corrupted_files'].apply(
            lambda x: get_date(x, "sentiment"))

        corrupted_dates_year = set(corrupted_files_sent['dates'].unique()) | set(corrupted_files_geo['dates'].unique())
        corrupted_dates.update(corrupted_dates_year)

    return corrupted_dates


def iterate_files(p_year=None):
    if p_year is None:
        # Get all year data
        file_list = os.listdir("")
        for year in file_list:
            pass

        pass
    else:
        # Get specific data
        pass
