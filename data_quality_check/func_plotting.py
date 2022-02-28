# -*- coding: utf-8 -*-
# @File       : func_plotting.py
# @Author     : Yuchen Chai
# @Date       : 2022-02-25 16:34
# @Description:

import numpy as np
import pandas as pd
from util import months_in_between, days_in_month, leap_year
from settings import *
import matplotlib.pyplot as plt
from datetime import date, timedelta
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


def generate_daily_num_posts_graph_year(year, in_dir, out_dir):
    """
    @param year: int, year
    @param in_dir: str, directory to which num_post_summary csv files are stored
    @param out_dir: str, directory to which the graph will be stored

    @return: file name
    """

    def get_date_list(year):
        base = date(year, 1, 1)
        numdays = 365 + int(leap_year(year)) * 1
        return [base + timedelta(days=x) for x in range(numdays)]

    df = pd.read_csv("".join([in_dir, "num_posts_summary_", str(year), ".csv"]))

    x = get_date_list(year)
    y_geo = df['num_geo_posts']
    y_sent = df['num_sent_posts']
    y_common = df['num_common_posts']

    plt.plot(x, y_geo, color='silver', label="Geotagged Posts")
    plt.plot(x, y_sent, color='grey', label="Posts with Sentiment Scores")
    plt.plot(x, y_common, color='green', label="Common Posts")

    plt.title("".join(["Number of Posts by Day in ", str(year)]))
    plt.legend(bbox_to_anchor=(1.6, 1.0), loc='upper right')
    plt.savefig("".join([out_dir, "daily_num_posts_graph_", str(year)]), bbox_inches='tight')
    return "".join([out_dir, "daily_num_posts_graph_", str(year)])


def generate_daily_sentiment_graph(start_date, end_date, city_group, in_dir, out_dir, remove_corrupted_data=True,
                                   corrupt_data_dir=None):
    """
    @param start_date: datetime object
    @param end_date: datetime object
    @param city_group: str, used for searching for corresponding city_group csv files
    @param in_dir: directory under which the sentiment score / num post by month csv files are stored
    @param out_dir: directory to which the daily sentiment graphs will be stored
    @param remove_corrupted_data: boolean, True if we want to remove corrupted data points, False if not
    @param corrupt_data_dir: directory under which the corrupt data is

    Generates sentiment average graphs from cities in city_group from the month of start_date to the month of end_date
    and save it to out_dir
    """
    # generating a dataframe that covers all months between start date and end date
    all_df = None

    months = months_in_between(start_date, end_date)
    for month in months:
        try:
            month_df = pd.read_csv("".join([in_dir, city_group, "_sentiment_avg_", month, ".csv"]))
        except FileNotFoundError:
            raise Exception("".join(["csv file for ", month, " in ", city_group, " does not exist"]))
        month_df = month_df.rename(columns={"Unnamed: 0": "date"})
        if all_df is None:
            all_df = month_df
        else:
            all_df = pd.concat([all_df, month_df], ignore_index=True)

    cities = list(all_df.columns)
    cities.remove("date")

    # remove corrupted data points
    if remove_corrupted_data:
        if corrupt_data_dir is None:
            raise Exception("Need to provide corrupt data directory as input.")
        corrupted_dates = get_corrupted_dates(start_date, end_date, corrupt_data_dir)
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
