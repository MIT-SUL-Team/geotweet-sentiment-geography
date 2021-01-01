#usage: python3 src/main_sentiment_aggregator.py US --data_path /home/sentiment/data_lake/twitter/processed/

import pandas as pd
import numpy as np
import argparse
from tqdm.auto import tqdm

from utils.aggregation_utils import check_args, get_dates, aggregate_sentiment, save_df

def run_aggregation(args):

    check_args(args)

    df = pd.DataFrame()
    for i in tqdm(get_dates(args)):
        temp = aggregate_sentiment(i, args)
        df = pd.concat([df, temp])
        save_df(df, args)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('country', help='Country abbreviation')
    parser.add_argument('--data_path', default='', type=str, help='path to data')
    parser.add_argument('--sentiment_method', default='bert', help='Which sentiment imputation method?')
    parser.add_argument('--geo_level', default='admin1', type=str, help='level of geo granularity')
    parser.add_argument('--by_ind', default=True, type=bool, help='Individual agg or tweet agg?')
    parser.add_argument('--start_date', default='2019-01-01', type=str, help='Start date')
    parser.add_argument('--end_date', default='2020-09-30', type=str, help='End date')
    parser.add_argument('--keywords', nargs='*', default='', help='Which keywords do you want to subset to?')
    parser.add_argument('--text_field', default='tweet_text_stemmed', help='Which text field to use for keyword matching?')
    parser.add_argument('--name_ext', default='', type=str, help='File name extension')
    args = parser.parse_args()

    print("\nRunning for {}".format(args.country))

    run_aggregation(args)
