import pandas as pd
import numpy as np
import glob
import os.path
import sys
from datetime import date, timedelta

def check_args(args):

    if len(args.keywords)>0 and args.name_ext == '':
        raise ValueError("Must provide a name extension (--name_ext) when subsetting by keywords")

def get_dates(args):

    start_date = date(int(args.start_date[:4]), int(args.start_date[5:7]), int(args.start_date[8:]))
    end_date = date(int(args.end_date[:4]), int(args.end_date[5:7]), int(args.end_date[8:]))

    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days +1)]

    return dates

def aggregate_sentiment(date, args):

    try:
        tweets = pd.read_csv(args.data_path+'{}{}{}.tsv'.format(
            date.year, str(date.month).zfill(2), str(date.day).zfill(2)
        ), sep='\t', usecols=['tweet_id', 'sender_id', 'country', 'admin1', 'admin2', 'place_name', args.text_field])
        scores = pd.read_csv('data/sentiment_scores/sentiment_{}{}{}_{}.tsv'.format(
            date.year, str(date.month).zfill(2), str(date.day).zfill(2), args.sentiment_method
        ), sep='\t')
    except:
        print("No sentiment data for {}.".format(date))
        return pd.DataFrame()

    scores = scores[scores['score'].notnull()].reset_index()
    tweets = tweets[tweets['country']==args.country].reset_index()

    for keyword in args.keywords:
        tweets = tweets[tweets[args.text_field].str.contains(keyword)]

    df = pd.merge(tweets, scores, how='inner', on='tweet_id')
    del scores, tweets

    gb_vars = ['country', 'admin1', 'admin2', 'place_name']
    gb_vars = gb_vars[:gb_vars.index(args.geo_level)+1]
    for var in gb_vars:
        df[var].fillna('', inplace=True)

    if args.by_ind == True:
        df = df.groupby(['sender_id']+gb_vars)
        df = pd.DataFrame({
            'count': df['score'].count(),
            'score': df['score'].mean(),
        }).reset_index()
        df['score_robust'] = np.where(df['count']>2, df['score'], np.nan)
    else:
        df['score_robust'] = df['score']

    df = df.groupby(gb_vars)
    df = pd.DataFrame({
        'ind_count': df['score'].count(),
        'count_robust': by_ind['score_robust'].count(),
        'score': df['score'].mean(),
        'score_10q': df['score'].quantile(0.1),
        'score_50q': df['score'].quantile(0.5),
        'score_90q': df['score'].quantile(0.9),
        'score_robust': by_ind['score_robust'].mean(),
        'score_robust_10q': by_ind['score_robust'].quantile(0.1),
        'score_robust_50q': by_ind['score_robust'].quantile(0.5),
        'score_robust_90q': by_ind['score_robust'].quantile(0.9),
    }).reset_index()

    df['date'] = date

    return df

def save_df(df, args):
    df.to_csv('data/aggregate_sentiment/{}_{}_{}{}.tsv'.format(
        args.country.lower(),
        args.geo_level,
        args.sentiment_method,
        args.name_ext
    ), sep='\t', index=False)

    if len(args.keywords)>0:
        f = open('data/aggregate_sentiment/{}_{}_{}{}_README.txt'.format(
            args.country.lower(),
            args.geo_level,
            args.sentiment_method,
            args.name_ext
        ), "w")
        f.write('Subset based on keywords {} in {} field.'.format(args.keywords, args.text_field))
        f.close()
