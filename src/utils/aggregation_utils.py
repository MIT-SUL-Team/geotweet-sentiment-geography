import pandas as pd
import numpy as np
import glob
import os.path
import sys
from datetime import date, timedelta
from tqdm.auto import tqdm
import re

def check_args(args):

    if (len(args.incl_keywords)>0 or len(args.excl_keywords)>0) and args.name_ext == '':
        raise ValueError("Must provide a name extension (--name_ext) when subsetting by keywords")

    if len(args.name_ext)>0 and args.name_ext[0]!='_':
        args.name_ext = "_" + args.name_ext

    if args.geo_level=='admin1':
        args.geo_level='admin1_id'
    if args.geo_level=='admin2':
        args.geo_level='admin2_id'
    geo_vars = ['country', 'admin1_id', 'admin2_id']
    if args.geo_level not in geo_vars:
        raise ValueError("Must provide a valid geo level \('country', 'admin1', or 'admin2'\)")
    args.geo_vars = geo_vars[:geo_vars.index(args.geo_level)+1]
    args.geo_level = args.geo_level.replace("_id", "")

    if args.time_level not in ['day', 'month', 'year', 'all']:
        raise ValueError("Must provide a valid time level \('day', 'month', 'year', 'all'\)")

    time_vars = ['year', 'month', 'day']
    if args.time_level in time_vars:
        args.time_vars = time_vars[:time_vars.index(args.time_level)+1]
    elif args.time_level=='all':
        args.time_vars = []

    args.incl_keywords = list(args.incl_keywords)
    args.excl_keywords = list(args.excl_keywords)
    args.keywords = args.incl_keywords + args.excl_keywords

    return args

def get_dates(args):

    start_date = date(int(args.start_date[:4]), int(args.start_date[5:7]), int(args.start_date[8:]))
    end_date = date(int(args.end_date[:4]), int(args.end_date[5:7]), int(args.end_date[8:]))

    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days +1)]

    return dates

def get_data(date, args):

    try:
        scores = pd.read_csv('data/sentiment_scores/sentiment_{}{}{}_{}.tsv'.format(
            date.year, str(date.month).zfill(2), str(date.day).zfill(2), args.sentiment_method
        ), sep='\t')

        tweet_geo = pd.read_csv(args.tweet_geo_path+'{}-{}-{}.tsv.gz'.format(
            date.year, str(date.month).zfill(2), str(date.day).zfill(2)
        ), sep=',', usecols=['tweet_id', 'sender_id']+args.geo_vars)

        if len(args.keywords) > 0:
            tweet_text = pd.read_csv(args.tweet_text_path+'{}{}{}.tsv.gz'.format(
                date.year, str(date.month).zfill(2), str(date.day).zfill(2)
            ), sep='\t', usecols=['tweet_id', args.text_field])

    except:
        print("\nNo data for {}.".format(date))
        return pd.DataFrame()

    scores = scores[scores['score'].notnull()].reset_index(drop=True)
    tweet_geo = tweet_geo[tweet_geo['country']==args.country].reset_index(drop=True)
    df = pd.merge(tweet_geo, scores, how='inner', on='tweet_id')
    del scores, tweet_geo

    if len(args.keywords) > 0:
        tweet_text = tweet_text[tweet_text[args.text_field].notnull()].reset_index(drop=True)
        if len(args.incl_keywords)>0:
            regex = '|'.join(args.incl_keywords)
            tweet_text['keep'] = [bool(re.search(regex, elem)) for elem in tweet_text[args.text_field].values]
            tweet_text = tweet_text[tweet_text['keep']==True].reset_index(drop=True)
            del tweet_text['keep']
        if len(args.excl_keywords)>0:
            regex = '|'.join(args.excl_keywords)
            tweet_text['drop'] = bool(re.search(regex, elem) for elem in tweet_text[args.text_field].values
            tweet_text = tweet_text[tweet_text['drop']==True].reset_index(drop=True)
            del tweet_text['drop']

        df = pd.merge(df, tweet_text, how='inner', on='tweet_id')
        del tweet_text

    df['date'] = date
    df['day'] = pd.DatetimeIndex(df['date']).day
    df['month'] = pd.DatetimeIndex(df['date']).month
    df['year'] = pd.DatetimeIndex(df['date']).year

    for var in args.geo_vars:
        df[var].fillna(0, inplace=True)

    return df

def groupby(df, gb_vars, prefix=''):
    df = df.groupby(gb_vars)
    df = pd.DataFrame({
        prefix+'count': df['score'].count(),
        prefix+'score': df['score'].mean(),
        prefix+'score_10q': df['score'].quantile(0.1),
        prefix+'score_25q': df['score'].quantile(0.25),
        prefix+'score_50q': df['score'].quantile(0.5),
        prefix+'score_75q': df['score'].quantile(0.75),
        prefix+'score_90q': df['score'].quantile(0.9),
    }).reset_index()
    return df

def aggregate_sentiment(df, args):

    if df.shape[0]==0:
        return df
    elif args.ind_level:
        df = groupby(df, ['sender_id']+args.time_vars+args.geo_vars)
        return df
    else:
        by_tweet = groupby(df, args.time_vars+args.geo_vars, prefix='tweet_')
        df = groupby(df, ['sender_id']+args.time_vars+args.geo_vars)
        by_ind = groupby(df, args.time_vars+args.geo_vars, prefix='ind_')
        df = df[df['count']>args.ind_robust_threshold].reset_index(drop=True)
        by_robust_ind = groupby(df, args.time_vars+args.geo_vars, prefix='robust_ind_')
        df = pd.merge(by_tweet, by_ind, how='left', on=args.time_vars+args.geo_vars)
        df = pd.merge(df, by_robust_ind, how='left', on=args.time_vars+args.geo_vars)
        return df

def save_df(df, args):

    df.to_csv('data/aggregate_sentiment/{}_{}_{}_{}{}.tsv'.format(
        args.country.lower(),
        args.geo_level,
        args.time_level,
        args.sentiment_method,
        args.name_ext
    ), sep='\t', index=False)
    f = open('data/aggregate_sentiment/{}_{}_{}_{}{}_README.txt'.format(
        args.country.lower(),
        args.geo_level,
        args.time_level,
        args.sentiment_method,
        args.name_ext
    ), "w")
    f.write('Run with the following options:\n{}'.format(args))
    f.close()

def last_day(i, args):
    if args.time_level=='day':
        return True
    elif args.time_level=='month':
        return i.month == (i+timedelta(days=1)).month
    elif args.time_level=='year':
        return i.year == (i+timedelta(days=1)).year
    elif args.time_level=='all':
        return i == args.end_date

def run_aggregation(args):

    args = check_args(args)

    df = pd.DataFrame()
    temp = pd.DataFrame()
    dates = get_dates(args)
    for i in tqdm(dates):
        t = get_data(i, args)
        temp = pd.concat([temp, t])
        if last_day(i, args) or i==dates[-1]:
            temp = aggregate_sentiment(temp, args)
            df = pd.concat([df, temp])
            save_df(df, args)
            temp = pd.DataFrame()
