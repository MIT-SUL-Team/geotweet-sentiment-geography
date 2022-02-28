# -*- coding: utf-8 -*-
# @File       : func_statistics.py
# @Author     : Yuchen Chai, Sirena Yu
# @Date       : 2022-02-25 16:33
# @Description:

import os
import numpy as np
import pandas as pd
import gzip
from util import days_in_month
from settings import *
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


def get_daily_num_post_and_sentiment_and_missing_file(year, month, day):
    """

    @param year: int or str, year that we look at
    @param month: int
    @param day: int
    @return:
    1) result_df: Pandas dataframe, a dataframe that includes the following columns: year, month, day, country, state, city, num_posts, avg_sent_score
    2) missing_files: a list of str, file names that do not exist
    3) empty_files: a list of str, files names that are empty
    4) file_name_to_num_post: dict, maps file names to number of posts in that file
    """
    print(year, month, day)
    date = "".join([str(year), "_", str(month), "_", str(day).zfill(2)])
    result_df = None  # sentiment score and num post dataframe

    # num_post and sentiment result
    num_posts_by_city = pd.DataFrame()  # pd dataframe to represent number of posts by city

    # missing data result
    missing_files = []
    empty_files = []

    # getting file name to num post map for corrupt files later
    file_name_to_num_post = dict()

    for hour in range(0, 24):
        has_error = False
        try:
            file_name = ''.join([DIR_GEOGRAPHY, str(year), "/" "geography_", date, "_", str(hour).zfill(2), ".csv.gz"])
            with gzip.open(file_name) as f:
                geo_posts = pd.read_csv(f, sep="\t")
                file_name_to_num_post[file_name] = len(geo_posts)
        except FileNotFoundError:
            print(file_name, "does not exist.")
            missing_files.append(file_name)
            has_error = True
        except pd.errors.EmptyDataError:
            print(file_name, "is empty.")
            empty_files.append(file_name)
            has_error = True
        try:
            file_name = ''.join(
                [DIR_SENTIMENT, str(year), "/", "bert_sentiment_", date, "_", str(hour).zfill(2), ".csv.gz"])
            with gzip.open(file_name) as f:
                sent_posts = pd.read_csv(f, sep="\t")
                file_name_to_num_post[file_name] = len(sent_posts)
        except FileNotFoundError:
            print(file_name, "does not exist.")
            missing_files.append(file_name)
            has_error = True
        except pd.errors.EmptyDataError:
            print(file_name, "is empty.")
            empty_files.append(file_name)
            has_error = True

        if has_error:
            continue

        common_posts = pd.merge(geo_posts, sent_posts, on="message_id", how="inner")

        common_posts["COUNTRY_STATE_CITY"] = common_posts['NAME_0'].astype(str) + '_' + common_posts['NAME_1'].astype(
            str) + '_' + common_posts['NAME_2']

        # num posts calculation
        num_posts_by_city_this_hour = common_posts.groupby(["COUNTRY_STATE_CITY"]).size().to_frame().transpose()
        num_posts_by_city = pd.concat([num_posts_by_city, num_posts_by_city_this_hour], join="outer",
                                      sort=True)

        city_result = common_posts.groupby(["COUNTRY_STATE_CITY"]).agg(
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

    column_names = ["year", "month", "day", "country", "state", "city", "num_posts", "daily_avg_score"]

    if result_df is None:
        data = np.array([[year, month, day, "World", "World", "World",  0, np.nan]])
        result_df = pd.DataFrame(data=data,
                                 columns=column_names)
    else:
        result_df["year"] = [year] * len(result_df)
        result_df["month"] = [month] * len(result_df)
        result_df["day"] = [day] * len(result_df)
        result_df["num_posts"] = result_df["num_posts"].astype(int)
        result_df["country"] = result_df["city"].apply(lambda x: x.split("_")[0])
        result_df["state"] = result_df["city"].apply(lambda x: "_".join(x.split("_")[0:2]))
        result_df["daily_avg_score"] = result_df["total_score"] / result_df["num_posts"]
        result_df.drop(columns=["total_score"], inplace=True)

        result_df = result_df[column_names]

    return result_df, missing_files, empty_files, file_name_to_num_post


def generate_statistic_year(year):
    """

    @param year: int or str, year we look at
    @return: nothing
    """
    missing_files = []
    empty_files = []
    result_df = pd.DataFrame()
    file_name_to_num_post = dict()
    for month in range(1, 2):
        for day in range(1, days_in_month(month, year)+1):
            day_df, day_missing_files, day_empty_files, day_file_name_to_num_post = \
                get_daily_num_post_and_sentiment_and_missing_file(year, month, day)
            missing_files = missing_files + day_missing_files
            empty_files = empty_files + day_empty_files
            result_df = pd.concat([result_df, day_df], ignore_index=True)
            file_name_to_num_post.update(day_file_name_to_num_post)

    bottom_10_percentile = pd.Series(data=list(file_name_to_num_post.values())).quantile(0.1)
    threshold = min(10000, bottom_10_percentile)

    corrupted_files = []
    for file_name in file_name_to_num_post.keys():
        if file_name_to_num_post[file_name] < threshold:
            corrupted_files.append(file_name)

    result_df.to_csv(''.join([DIR_STORE, "num_posts_and_sentiment_summary_", str(year), ".csv"]))
    missing_files_df = pd.DataFrame(data=pd.Series(missing_files),
                                    columns=["missing_files"])
    missing_files_df.to_csv(''.join([DIR_STORE, "missing_files_", str(year), ".csv"]))
    empty_files_df = pd.DataFrame(data=pd.Series(empty_files),
                                    columns=["empty_files"])
    empty_files_df.to_csv(''.join([DIR_STORE, "empty_files_", str(year), ".csv"]))
    corrupted_files_df = pd.DataFrame(data=pd.Series(empty_files),
                                  columns=["corrupted_files"])
    corrupted_files_df.to_csv(''.join([DIR_STORE, "corrupted_files_", str(year), ".csv"]))


def iterate_files(p_year=None):

    if p_year is None:
        # Get all year data
        file_list = os.listdir(DIR_GEOGRAPHY)
        for year in file_list:
            generate_statistic_year(year)
    else:
        print("iterate files", p_year)
        generate_statistic_year(p_year)