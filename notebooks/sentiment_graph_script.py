"""
Author: @SirenaYu

Script to generate daily sentiment average graphs within a user-specified timeframe. 
"""

import sys
import os
import numpy as np
import pandas as pd
import gzip
from script import days_in_month
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf
from datetime import timedelta, datetime
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

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
        
        common_posts["COUNTRY_STATE_CITY"] = common_posts['NAME_0'].astype(str) + '_' + common_posts['NAME_1'].astype(str) + '_' + common_posts['NAME_2']
        
        post_in_cities = common_posts[common_posts["COUNTRY_STATE_CITY"].isin(cities)]
        city_result = post_in_cities.groupby(["COUNTRY_STATE_CITY"]).agg({"score": np.sum, "message_id": len}).reset_index()
        city_result.rename(columns={"COUNTRY_STATE_CITY": "city", "score": "total_score", "message_id": "num_posts"}, inplace=True)
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
        result_df["daily_avg_score"] = result_df["total_score"]/result_df["num_posts"]
        result_df.drop(columns=["total_score"], inplace=True)
    
    end_time = datetime.now()
    print("get_daily_sent_avg_and_num_posts took", end_time-start_time, "seconds.")
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
    for day in range(1, days_in_month(month, year)+1):
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
    # display(sentiment_avg_by_month_df)
    sentiment_avg_by_month_df.to_csv("".join([out_dir, city_group, "_sentiment_avg_", str(year),"_", str(month).zfill(2), ".csv"]))
    
    num_post_by_month_df = num_post_df.T
    num_post_by_month_df.rename(columns=num_post_by_month_df.iloc[0], inplace=True)
    num_post_by_month_df.drop(num_post_by_month_df.index[0], inplace=True)
    # display(num_post_by_month_df)
    num_post_by_month_df.to_csv("".join([out_dir, city_group, "_num_posts_", str(year),"_", str(month).zfill(2), ".csv"]))
    
    
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
    for year in range(start.year, end.year+1):
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
        corrupted_files_sent['dates'] = corrupted_files_sent['corrupted_files'].apply(lambda x: get_date(x, "sentiment"))
        
        corrupted_dates_year = set(corrupted_files_sent['dates'].unique()) | set(corrupted_files_geo['dates'].unique())
        corrupted_dates.update(corrupted_dates_year)
    
    return corrupted_dates


def generate_daily_sentiment_graph(start_date, end_date, city_group, in_dir, out_dir, remove_corrupted_data=True, corrupt_data_dir=None):
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
        month_df = month_df.rename(columns={"Unnamed: 0":"date"})
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
        numdays = (datetime(end_date.year, end_date.month, days_in_month(end_date.month, end_date.year)) - base).days + 1
        x = [base + timedelta(days=x) for x in range(numdays)]
        y = all_df[city]

        plt.plot(x, y)

        plt.title("".join(["Daily Average Sentiment of Posts in ", city]))
        plt.xticks(rotation = 45)
        plt.xlabel("Dates")
        plt.ylabel("Daily Average Sentiment")
        # plt.legend(bbox_to_anchor=(1.6, 1.0), loc='upper right')

        plt.show()
        plt.savefig("".join([out_dir, city,"_daily_sentiment_", months[0], "_to_",  months[len(months)-1], ".png"]), bbox_inches = 'tight')
    