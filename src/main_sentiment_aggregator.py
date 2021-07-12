#usage: python3 src/main_sentiment_aggregator.py twitter --country USA --tweet_text_path /home/sentiment/data-lake/twitter/processed/ --tweet_geo_path /home/sentiment/data-lake/twitter/geoinfo/

import pandas as pd
import numpy as np
import argparse

from utils.aggregation_utils import run_aggregation

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('platform', help='Which social media data are we using (twitter, weibo)?')
    parser.add_argument('--countries', nargs='*', default='', help='Country abbreviations (3 letter)')
    parser.add_argument('--text_path', default='', type=str, help='path to tweet text data')
    parser.add_argument('--geo_path', default='', type=str, help='path to tweet geography data')
    parser.add_argument('--sentiment_method', default='bert', help='Which sentiment imputation method?')
    parser.add_argument('--geo_level', default='admin1', type=str, help='level of geo granularity')
    parser.add_argument('--time_level', default='day', type=str, help='level of time granularity')
    parser.add_argument('--ind_robust_threshold', default=3, type=int, help='How many tweets for an individual to be considered robust?')
    parser.add_argument('--ind_level', default=False, type=bool, help='Would you like the data aggregated at individual level?  (produces larger files)')
    parser.add_argument('--lang_level', default=False, type=bool, help='Would you like the data aggregated by languages? (produces larger files)')
    parser.add_argument('--start_date', default='2019-01-01', type=str, help='Start date')
    parser.add_argument('--end_date', default='2020-09-30', type=str, help='End date')
    parser.add_argument('--incl_keywords', nargs='*', default='', help='Which keywords do you want to include in the subset?')
    parser.add_argument('--excl_keywords', nargs='*', default='', help='Which keywords do you want to exclude in the subset?')
    parser.add_argument('--text_field', default='text_keywords', help='Which text field to use for keyword matching?')
    parser.add_argument('--name_ext', default='', type=str, help='File name extension')
    args = parser.parse_args()

    run_aggregation(args)
