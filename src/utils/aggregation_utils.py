import pandas as pd
import numpy as np
import glob
import os.path
import sys
from datetime import date, timedelta

def check_args(args):

    if (len(args.incl_keywords)>0 or len(args.excl_keywords)>0) and args.name_ext == '':
        raise ValueError("Must provide a name extension (--name_ext) when subsetting by keywords")

    if len(args.name_ext)>0 and args.name_ext[0]!='_':
        args.name_ext = "_" + args.name_ext

    if args.geo_level=='admin1':
        args.geo_level='admin1_id'
    if args.geo_level=='admin2':
        args.geo_level='admin2_id'

    return args

def get_dates(args):

    start_date = date(int(args.start_date[:4]), int(args.start_date[5:7]), int(args.start_date[8:]))
    end_date = date(int(args.end_date[:4]), int(args.end_date[5:7]), int(args.end_date[8:]))

    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days +1)]

    return dates

def groupby(df, gb_vars, prefix=''):
    df = df.groupby(vars)
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

def aggregate_sentiment(date, args):

    geo_vars = ['country', 'admin1_id', 'admin2_id', 'place_name']
    geo_vars = geo_vars[:geo_vars.index(args.geo_level)+1]

    keywords = args.incl_keywords + args.excl_keywords

    try:
        scores = pd.read_csv('data/sentiment_scores/sentiment_{}{}{}_{}.tsv'.format(
            date.year, str(date.month).zfill(2), str(date.day).zfill(2), args.sentiment_method
        ), sep='\t')

        tweet_geo = pd.read_csv(args.tweet_geo_path+'{}-{}-{}.tsv.gz'.format(
            date.year, str(date.month).zfill(2), str(date.day).zfill(2)
        ), sep=',', usecols=['tweet_id', 'sender_id']+geo_vars)

        if len(keywords) > 0:
            tweet_text = pd.read_csv(args.tweet_text_path+'{}{}{}.tsv'.format(
                date.year, str(date.month).zfill(2), str(date.day).zfill(2)
            ), sep='\t', usecols=['tweet_id', args.text_field])

    except:
        print("No data for {}.".format(date))
        return pd.DataFrame()

    scores = scores[scores['score'].notnull()].reset_index(drop=True)
    tweet_geo = tweet_geo[tweet_geo['country']==args.country].reset_index(drop=True)
    df = pd.merge(tweet_geo, scores, how='inner', on='tweet_id')
    del scores, tweet_geo

    if len(keywords) > 0:
        if len(args.incl_keywords)>0:
            regex = '|'.join(args.incl_keywords)
            tweet_text = tweet_text[tweet_text[args.text_field].notnull()].reset_index(drop=True)
            tweet_text = tweet_text[tweet_text[args.text_field].str.contains(regex)].reset_index(drop=True)

        df = pd.merge(df, tweet_text, how='inner', on='tweet_id')
        del tweet_text

    df['date'] = date

    for var in geo_vars:
        df[var].fillna('', inplace=True)

    if args.ind_level:
        df = groupby(df, ['sender_id', 'date']+geo_vars)
        return df
    else:
        by_tweet = groupby(df, ['date']+geo_vars, prefix='tweet_')
        if args.ind_normed:
            df = groupby(df, ['sender_id', 'date']+geo_vars)
            by_ind = groupby(df, ['date']+geo_vars, prefix='ind_')
            df = df[df['count']>args.ind_robust_threshold].reset_index(drop=True)
            by_robust_ind = groupby(df, ['date']+geo_vars, prefix='robust_ind_')
            df = pd.merge(by_tweet, by_ind, how='left', on=['date']+geo_vars)
            df = pd.merge(df, by_robust_ind, how='left', on=['date']+geo_vars)
            return df
        else:
            return by_tweet

def save_df(df, args):

    df.to_csv('data/aggregate_sentiment/{}_{}_{}{}.tsv'.format(
        args.country.lower(),
        args.geo_level,
        args.sentiment_method,
        args.name_ext
    ), sep='\t', index=False)
    f = open('data/aggregate_sentiment/{}_{}_{}{}_README.txt'.format(
        args.country.lower(),
        args.geo_level,
        args.sentiment_method,
        args.name_ext
    ), "w")
    f.write('Run with the following options:\n{}'.format(args))
    f.close()
