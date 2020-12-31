import pandas as pd
import numpy as np
import glob
import os.path
import sys

def aggregate_sentiment(date, args):

    try:
        tweets = pd.read_csv(args.data_path+'{}{}{}.tsv'.format(
            date.year, str(date.month).zfill(2), str(date.day).zfill(2)
        ), sep='\t', usecols=['tweet_id', 'sender_id', 'country', 'admin1', 'admin2', 'place_name'])
        scores = pd.read_csv('data/sentiment_scores/sentiment_{}{}{}_{}.tsv'.format(
            date.year, str(date.month).zfill(2), str(date.day).zfill(2), args.sentiment_method
        ), sep='\t')
    except:
        print("No sentiment data for {}.".format(date))
        return pd.DataFrame()

    scores = scores[scores['score'].notnull()].reset_index()
    tweets = tweets[tweets['country']==args.country].reset_index()

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
            # 'score_50q': df['score'].quantile(0.5)
        }).reset_index()

    df = df.groupby(gb_vars)
    df = pd.DataFrame({
        'ind_count': df['score'].count(),
        # 'count_robust': by_ind['score_robust'].count(),

        'score': df['score'].mean(),
        'score_10q': df['score'].quantile(0.1),
        'score_50q': df['score'].quantile(0.5),
        'score_90q': df['score'].quantile(0.9),
        # 'score_robust': by_ind['score_robust'].mean(),
        # 'score_robust_25q': by_ind['score_robust'].quantile(0.25),
        # 'score_robust_50q': by_ind['score_robust'].quantile(0.5),
        # 'score_robust_75q': by_ind['score_robust'].quantile(0.75),

        # 'med_score': df['score_50q'].mean(),
        # 'med_score_25q': df['score_50q'].quantile(0.25),
        # 'med_score_50q': df['score_50q'].quantile(0.5),
        # 'med_score_75q': df['score_50q'].quantile(0.75),
        # 'med_score_robust': by_ind['score_50q_robust'].mean(),
        # 'med_score_robust_25q': by_ind['score_50q_robust'].quantile(0.25),
        # 'med_score_robust_50q': by_ind['score_50q_robust'].quantile(0.5),
        # 'med_score_robust_75q': by_ind['score_50q_robust'].quantile(0.75),

    }).reset_index()

    df['date'] = date

    return df
