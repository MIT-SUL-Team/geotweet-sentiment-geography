#usage: python3 src/main_sentiment_aggregator.py US --data_path /home/sentiment/data_lake/twitter/processed/

import pandas as pd
import numpy as np
import glob
import os.path
import argparse
import sys
from datetime import date, timedelta
from tqdm.auto import tqdm

from utils.aggregation_utils import aggregate_sentiment

def run_aggregation(args):

    start_date = date(int(args.start_date[:4]), int(args.start_date[5:7]), int(args.start_date[8:]))
    end_date = date(int(args.end_date[:4]), int(args.end_date[5:7]), int(args.end_date[8:]))

    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days +1)]

    df = pd.DataFrame()
    for i in tqdm(dates):
        temp = aggregate_sentiment(i, args)
        df = pd.concat([df, temp])

    df.to_csv('data/aggregate_sentiment/{}_{}_{}.tsv'.format(args.sentiment_method, args.country.lower(), args.geo_level), sep='\t', index=False)

    return df

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('country', help='Country abbreviation')
    parser.add_argument('--data_path', default='', type=str, help='path to data')
    parser.add_argument('--sentiment_method', default='bert', help='Which sentiment imputation method?')
    parser.add_argument('--geo_level', default='admin1', type=str, help='level of geo granularity')
    parser.add_argument('--by_ind', default=True, type=bool, help='Individual agg or tweet agg?')
    parser.add_argument('--start_date', default='2019-01-01', type=str, help='Start date')
    parser.add_argument('--end_date', default='2020-09-30', type=str, help='End date')
    args = parser.parse_args()

    print("\nRunning for {}".format(args.country))

    run_aggregation(args)
