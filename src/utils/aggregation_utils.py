import pandas as pd
import numpy as np
import glob
import os.path
import sys
import datetime
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

    args.other_gb_vars = []
    if args.lang_level:
        args.other_gb_vars.append('lang')
        args.name_ext = "_by_lang" + args.name_ext
    if args.ind_level:
        args.name_ext = "_by_ind" + args.name_ext

    args.incl_keywords = list(args.incl_keywords)
    args.excl_keywords = list(args.excl_keywords)
    args.keywords = args.incl_keywords + args.excl_keywords

    args.filename = "{}_{}_{}_{}{}".format(
        args.sentiment_method,
        'global' if len(args.countries)==0 else "_".join([elem.lower() for elem in args.countries]),
        args.geo_level,
        args.time_level,
        args.name_ext
    )

    if args.subset_usernames_file != '':
        args.usernames = [elem for elem in open(args.subset_usernames_file).read().split("\n") if elem != '']

    return args

def get_dates(args):

    start_date = datetime.date(int(args.start_date[:4]), int(args.start_date[5:7]), int(args.start_date[8:]))
    end_date = datetime.date(int(args.end_date[:4]), int(args.end_date[5:7]), int(args.end_date[8:]))

    dates = [start_date + datetime.timedelta(days=i) for i in range((end_date - start_date).days +1)]

    return dates

def get_data(date, args):

    try:
        scores = pd.read_csv('data/sentiment_scores/{}_sentiment_{}{}{}_{}.tsv'.format(
            args.platform, date.year, str(date.month).zfill(2), str(date.day).zfill(2), args.sentiment_method
        ), sep='\t')

        geo_df = pd.read_csv(args.geo_path+'{}-{}-{}.tsv.gz'.format(
            date.year, str(date.month).zfill(2), str(date.day).zfill(2)
        ), sep=',', usecols=['message_id', 'sender_id']+args.geo_vars).drop_duplicates()

        if len(args.keywords) > 0 or args.lang_level:
            text_df = pd.read_csv(args.text_path+'text_{}{}{}.tsv.gz'.format(
                date.year, str(date.month).zfill(2), str(date.day).zfill(2)
            ), sep='\t', usecols=['message_id', 'lang', args.text_field])

    except:
        print("\nNo data for {}.".format(date))
        return pd.DataFrame({
            'message_id': pd.Series([], dtype='str'),
            'sender_id': pd.Series([], dtype='str'),
            'lang': pd.Series([], dtype='str'),
            'day': pd.Series([], dtype='int'),
            'month': pd.Series([], dtype='int'),
            'year': pd.Series([], dtype='int'),
            'score': pd.Series([], dtype='float'),
            args.text_field: pd.Series([], dtype='str'),
            'country': pd.Series([], dtype='str'),
            'admin1_id': pd.Series([], dtype='str'),
            'admin2_id': pd.Series([], dtype='str')
        })

        pd.DataFrame(columns = ['message_id', 'sender_id', 'lang', 'date', 'day', 'month', 'year', 'score', args.text_field]+args.geo_vars)

    scores = scores[scores['score'].notnull()].reset_index(drop=True)
    if len(args.countries)>0:
        geo_df = geo_df[geo_df['country'].isin([elem.upper() for elem in args.countries])].reset_index(drop=True)
    if args.subset_usernames_file != '':
        geo_df = geo_df[geo_df['sender_id'].isin(args.usernames)].reset_index(drop=True)
    df = pd.merge(geo_df, scores, how='inner', on='message_id')
    del scores, geo_df

    if len(args.keywords) > 0 or args.lang_level:
        if len(args.incl_keywords)>0:
            text_df = text_df[text_df[args.text_field].notnull()].reset_index(drop=True)
            regex = '|'.join(args.incl_keywords)
            text_df['keep'] = [bool(re.search(regex, elem)) for elem in text_df[args.text_field].values]
            text_df = text_df[text_df['keep']==True].reset_index(drop=True)
            del text_df['keep']
        if len(args.excl_keywords)>0:
            text_df = text_df[text_df[args.text_field].notnull()].reset_index(drop=True)
            regex = '|'.join(args.excl_keywords)
            text_df['drop'] = [bool(re.search(regex, elem)) for elem in text_df[args.text_field].values]
            text_df = text_df[text_df['drop']==False].reset_index(drop=True)
            del text_df['drop']

        del text_df[args.text_field]
        df = pd.merge(df, text_df, how='inner', on='message_id')
        del text_df

    df['date'] = date
    dtindex = pd.DatetimeIndex(df['date'])
    df['day'] = dtindex.day
    df['month'] = dtindex.month
    df['year'] = dtindex.year
    del df['date']

    for var in args.geo_vars:
        df[var].fillna(0, inplace=True)

    return df

def groupby_to_ind(df, args):
    df = df.groupby(['sender_id']+args.time_vars+args.geo_vars+args.other_gb_vars)
    df = pd.DataFrame({
        'count': df['message_id'].count(),
        'score': df['score'].mean(),
    }).reset_index()
    return df

def weighted_groupby(df, args, ind_level=True, prefix=""):
    if ind_level:
        vars = ['sender_id']+args.time_vars+args.geo_vars+args.other_gb_vars
    else:
        vars = args.time_vars+args.geo_vars+args.other_gb_vars
    df = df.groupby(vars)
    df = pd.DataFrame({
        prefix+'count': df['count'].sum(),
        prefix+'score': df.apply(lambda x: np.average(x['score'], weights=x['count']))
    }).reset_index()
    df[prefix+'score'] = pd.to_numeric(df[prefix+'score'])
    return df

def quantiles_groupby(df, args, prefix=""):
    vars = args.time_vars+args.geo_vars+args.other_gb_vars
    df = df.groupby(vars)
    df = pd.DataFrame({
        prefix+'count': df['sender_id'].count(),
        prefix+'score': df['score'].mean(),
        prefix+'score_10q': df['score'].quantile(0.1),
        prefix+'score_25q': df['score'].quantile(0.25),
        prefix+'score_50q': df['score'].quantile(0.5),
        prefix+'score_75q': df['score'].quantile(0.75),
        prefix+'score_90q': df['score'].quantile(0.9),
    }).reset_index()
    return df

def aggregate_sentiment(df, args):
    if args.ind_level:
        return df
    else:
        by_post = weighted_groupby(df, args, ind_level=False, prefix='post_')
        by_ind = quantiles_groupby(df, args, prefix='ind_')
        df = df[df['count']>args.ind_robust_threshold].reset_index(drop=True)
        by_robust_ind = quantiles_groupby(df, args, prefix='robust_ind_')
        df = pd.merge(by_post, by_ind, how='left', on=args.time_vars+args.geo_vars+args.other_gb_vars)
        df = pd.merge(df, by_robust_ind, how='left', on=args.time_vars+args.geo_vars+args.other_gb_vars)
        return df

def save_df(df, args):

    df.to_csv('data/aggregate_sentiment/{}.tsv'.format(
        args.filename
    ), sep='\t', index=False)
    f = open('data/aggregate_sentiment/{}_README.txt'.format(
        args.filename
    ), "w")
    f.write('Run with the following options:\n{}'.format(args))
    f.close()

def last_day(i, args):
    if args.time_level=='day':
        return True
    elif args.time_level=='month':
        return i.month == (i+datetime.timedelta(days=1)).month
    elif args.time_level=='year':
        return i.year == (i+datetime.timedelta(days=1)).year
    elif args.time_level=='all':
        return i == args.end_date

def run_aggregation(args):

    args = check_args(args)

    df = pd.DataFrame()
    ind_df = pd.DataFrame()
    dates = get_dates(args)
    for i in tqdm(dates):
        temp = get_data(i, args)
        temp = groupby_to_ind(temp, args)
        ind_df = weighted_groupby(pd.concat([ind_df, temp], axis=0), args)
        if last_day(i, args) or i==dates[-1]:
            ind_df = aggregate_sentiment(ind_df, args)
            df = pd.concat([df, ind_df], axis=0)
            save_df(df, args)
            ind_df = pd.DataFrame()
