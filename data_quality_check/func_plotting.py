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

    x = get_date_list(year)

    if area_level is None:
        summed_df = df.groupby(["year", "month", "day"]).sum()
        summed_df.reset_index(inplace=True)
        y = summed_df["num_posts"]
    else:
        area_summed_df = df.groupby(["year", "month", "day", area_level]).sum()
        area_summed_df.reset_index(inplace=True)
        y = area_summed_df[area_summed_df[area_level] == area].reset_index()["num_posts"]

    plt.plot(x, y, color='green', label="Common Posts")

    plt.title("".join(["Number of Posts by Day in ", area, ", ", str(year)]))
    plt.savefig("".join([DIR_STORE, "daily_num_posts_graph_", str(year)]), bbox_inches='tight')
    return "".join([DIR_STORE, "daily_num_posts_graph_", str(year)])


def generate_daily_sentiment_graph(year, area_level=None, area=None):
    """


    Generates sentiment average graphs from cities in city_group from the month of start_date to the month of end_date
    and save it to out_dir
    """

    df = pd.read_csv("".join([DIR_STORE, "num_posts_and_sentiment_summary_", str(year), ".csv"]))
    df["daily_total_score"] = df["num_posts"] * df["daily_avg_score"]

    x = get_date_list()

    if area_level is None:
        summed_df = df.groupby(["year", "month", "day"]).sum()
        summed_df.reset_index(inplace=True)
        summed_df["daily_avg_score"] = summed_df["daily_total_score"] / summed_df["num_posts"]
        summed_df.loc[~np.isfinite(summed_df['daily_avg_score']), 'daily_avg_score'] = np.nan
        y = summed_df["daily_avg_score"]
    else:
        area_summed_df = df.groupby(["year", "month", "day", area_level]).sum()
        area_summed_df.reset_index(inplace=True)
        area_summed_df["daily_avg_score"] = area_summed_df["daily_total_score"] / area_summed_df["num_posts"]
        area_summed_df.loc[~np.isfinite(area_summed_df['daily_avg_score']), 'daily_avg_score'] = np.nan
        y = area_summed_df["daily_avg_score"]

    # generating a dataframe that covers all months between start date and end date




    corrupted_dates = get_corrupted_dates(year)
    for city in cities:
        all_df[city] = np.where(all_df['date'].isin(corrupted_dates), np.nan, all_df[city])

    for city in cities:
        base = datetime(start_date.year, start_date.month, 1)
        numdays = (datetime(end_date.year, end_date.month,
                            days_in_month(end_date.month, end_date.year)) - base).days + 1
        x = [base + timedelta(days=x) for x in range(numdays)]
        y = all_df[city]

        plt.plot(x, y)

        plt.title("".join(["Daily Average Sentiment of Posts in ", city]))
        plt.xticks(rotation=45)
        plt.xlabel("Dates")
        plt.ylabel("Daily Average Sentiment")
        # plt.legend(bbox_to_anchor=(1.6, 1.0), loc='upper right')

        plt.show()
        plt.savefig("".join([out_dir, city, "_daily_sentiment_", months[0], "_to_", months[len(months) - 1], ".png"]),
                    bbox_inches='tight')


def get_corrupted_dates(year):
    """
    @param year: int or str, desired year

    returns: corrupted dates in between the start and the end as a list
    """

    def get_date(file_name, category):
        if category == "geography":
            return "_".join(file_name.split("/")[5].split(".")[0].split("_")[1:4])
        else:
            return "_".join(file_name.split("/")[5].split(".")[0].split("_")[2:5])

    corrupted_dates = set()

    try:
        corrupted_files = pd.read_csv("".join([DIR_STORE, "corrupted_files_", str(year), ".csv"]))
    except FileNotFoundError:
        raise Exception("".join(["Corrupted file report for ", year, " does not exist"]))

    corrupted_files.drop(columns="Unnamed: 0", inplace=True)

    corrupted_files['dates'] = corrupted_files['corrupted_files'].apply(lambda x: get_date(x))

    corrupted_dates = corrupted_files['dates'].unique()


    return corrupted_dates