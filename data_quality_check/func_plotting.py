# -*- coding: utf-8 -*-
# @File       : func_plotting.py
# @Author     : Yuchen Chai, Sirena Yu
# @Date       : 2022-02-25 16:34
# @Description:

import numpy as np
import pandas as pd
from util import months_in_between, days_in_month, get_date_list
from settings import *
import matplotlib.pyplot as plt
from datetime import timedelta
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


def generate_daily_num_posts_graph_year(year, area_level=None, area=None):
    """
    @param year: int, year
    @param area_level: str, "country", "state" or "city"
    @param area: str, the area that we want to look at
    @return: file name
    """

    df = pd.read_csv("".join([DIR_STORE, "num_posts_and_sentiment_summary_", str(year), ".csv"]))

    x = get_date_list(int(year))

    if area_level is None:
        summed_df = df.groupby(["year", "month", "day"]).sum()
        summed_df.reset_index(inplace=True)
        area = "World"
        y = summed_df["num_posts"]
    else:
        area_summed_df = df.groupby(["year", "month", "day", area_level]).sum()
        area_summed_df.reset_index(inplace=True)
        y = area_summed_df[area_summed_df[area_level] == area].reset_index()["num_posts"]

    if len(y) < len(x):
        y = y.append(pd.Series([np.nan]*(len(x)-len(y))))
    plt.clf()
    plt.plot(x, y, color='green', label="Common Posts")
    plt.xticks(rotation=45)
    plt.title("".join(["Number of Posts by Day in ", area, ", ", str(year)]))
    plt.savefig("".join([DIR_STORE, "daily_num_posts_graph_in_", area, "_", str(year)]), bbox_inches='tight')
    return "".join("".join([DIR_STORE, "daily_num_posts_graph_in_", area, "_", str(year)]))


def generate_daily_sentiment_graph(year, area_level=None, area=None):
    """
    @param year: int, year
    @param area_level: str, "country", "state" or "city"
    @param area: str, the area that we want to look at
    """

    df = pd.read_csv("".join([DIR_STORE, "num_posts_and_sentiment_summary_", str(year), ".csv"]))
    df["daily_total_score"] = df["num_posts"] * df["daily_avg_score"]

    x = get_date_list(int(year))

    if area_level is None:
        summed_df = df.groupby(["year", "month", "day"]).sum()
        area = "World"
    else:
        summed_df = df.groupby(["year", "month", "day", area_level]).sum()

    summed_df.reset_index(inplace=True)
    summed_df["daily_avg_score"] = summed_df["daily_total_score"] / summed_df["num_posts"]
    summed_df.loc[~np.isfinite(summed_df['daily_avg_score']), 'daily_avg_score'] = np.nan

    # remove corrupted dates
    corrupted_dates = get_corrupted_dates(year)
    df['year'] = df['year'].astype(str)
    df['month'] = df['month'].astype(str)
    df['day'] = df['day'].astype(str)
    summed_df['date'] = df[['year', 'month', 'day']].agg('_'.join, axis=1)
    summed_df['daily_avg_score'] = np.where(summed_df['date'].isin(corrupted_dates),
                                            np.nan,
                                            summed_df['daily_avg_score'])

    if area_level is None:
        y = summed_df["daily_avg_score"]
    else:
        y = area_summed_df[area_summed_df[area_level] == area].reset_index()["daily_avg_score"]

    if len(y) < len(x):
        y = y.append(pd.Series([np.nan]*(len(x)-len(y))))
    plt.clf()
    plt.plot(x, y)

    plt.title("".join(["Daily Average Sentiment of Posts in ", area, ", ", str(year)]))
    plt.xticks(rotation=45)
    plt.xlabel("Dates")
    plt.ylabel("Daily Average Sentiment")

    plt.show()
    plt.savefig("".join([DIR_STORE, area, "_daily_sentiment_", str(year), ".png"]),
                bbox_inches='tight')


def get_corrupted_dates(year):
    """
    @param year: int or str, desired year

    returns: corrupted dates in between the start and the end as a list
    """

    def get_date(file_name, is_geo):
        if is_geo:
            return "_".join(file_name.split("/")[5].split(".")[0].split("_")[1:4])
        else:
            return "_".join(file_name.split("/")[5].split(".")[0].split("_")[2:5])

    try:
        corrupted_files = pd.read_csv("".join([DIR_STORE, "corrupted_files_", str(year), ".csv"]))
    except FileNotFoundError:
        raise Exception("".join(["Corrupted file report for ", year, " does not exist"]))

    corrupted_files.drop(columns="Unnamed: 0", inplace=True)

    corrupted_files['dates'] = np.where(corrupted_files["corrupted_files"].str.contains("geography"),
                                        corrupted_files['corrupted_files'].apply(lambda x: get_date(x, True)),
                                        corrupted_files['corrupted_files'].apply(lambda x: get_date(x, False)))

    corrupted_dates = set(corrupted_files['dates'].unique())

    return corrupted_dates


def iterate_plotting(p_year=None, area_level=None, area=None):
    if p_year is None:
        # Make all year plot
        file_list = os.listdir(DIR_GEOGRAPHY)
        for year in file_list:
            generate_daily_num_posts_graph_year(year, area_level, area)
            generate_daily_sentiment_graph(year, area_level, area)
    else:
        generate_daily_num_posts_graph_year(p_year, area_level, area)
        generate_daily_sentiment_graph(p_year, area_level, area)