import pandas as pd
import numpy as np
import glob
import os.path
import sys
import datetime
import re

from utils.data_read_in import read_in

def check_args(args):

    if (len(args.incl_keywords)>0 or len(args.excl_keywords)>0) and args.name_ext == '':
        raise ValueError("Must provide a name extension (--name_ext) when subsetting by keywords")

    if len(args.name_ext)>0 and args.name_ext[0]!='_':
        args.name_ext = "_" + args.name_ext

    if args.geo_level=='country':
        args.geo_level='id_0'
    if args.geo_level=='admin1' or args.geo_level=='admin1_id':
        args.geo_level='id_1'
    if args.geo_level=='admin2' or args.geo_level=='admin2_id':
        args.geo_level='id_2'
    geo_vars = ['global', 'id_0', 'id_1', 'id_2']
    if args.geo_level not in geo_vars:
        raise ValueError("Must provide a valid geo level \('id_0', 'admin1', or 'admin2'\)")
    args.geo_vars = geo_vars[1:geo_vars.index(args.geo_level)+1]
    if args.geo_level != 'global':
        if args.geo_level == 'id_0':
            args.geo_level = 'country'
        else:
            args.geo_level = "admin" + args.geo_level[-1]

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

def get_daily_data(date, args):

    df_day = pd.DataFrame()

    for i in range(24):

        try:

            try:

                text_df = read_in(
                    file = "{}_{}_{}_{}.csv.gz".format(date.year, date.month, str(date.day).zfill(2), str(i).zfill(2)),
                    path = args.text_path,
                    cols = ["message_id", "user_id", "tweet_lang", "text"]
                )

                geo_df = pd.read_csv(os.path.join(args.geo_path, 'geography_{}_{}_{}_{}.csv.gz'.format(
                    date.year, date.month, str(date.day).zfill(2), str(i).zfill(2)
                )), sep='\t', usecols = ['message_id', 'ID_0', 'ISO', 'ID_1', 'ID_2'])
                geo_df.columns = [elem.lower() for elem in list(geo_df)]

                sent_df = pd.read_csv(os.path.join(args.sent_path, '{}_sentiment_{}_{}_{}_{}.csv.gz'.format(
                    args.sentiment_method, date.year, date.month, str(date.day).zfill(2), str(i).zfill(2)
                )), sep='\t')

            except: # Account for folder structure with years

                text_df = read_in(
                    file = "{}_{}_{}_{}.csv.gz".format(date.year, date.month, str(date.day).zfill(2), str(i).zfill(2)),
                    path = os.path.join(args.text_path, date.year),
                    cols = ["message_id", "user_id", "tweet_lang", "text"]
                )

                geo_df = pd.read_csv(os.path.join(args.geo_path, date.year, 'geography_{}_{}_{}_{}.csv.gz'.format(
                    date.year, date.month, str(date.day).zfill(2), str(i).zfill(2)
                )), sep='\t', usecols = ['message_id', 'ID_0', 'ISO', 'ID_1', 'ID_2'])
                geo_df.columns = [elem.lower() for elem in list(geo_df)]

                sent_df = pd.read_csv(os.path.join(args.sent_path, date.year, '{}_sentiment_{}_{}_{}_{}.csv.gz'.format(
                    args.sentiment_method, date.year, date.month, str(date.day).zfill(2), str(i).zfill(2)
                )), sep='\t')

            sent_df = sent_df[sent_df['score'].notnull()].reset_index(drop=True)

            if len(args.countries)>0:
                geo_df = geo_df[geo_df['iso'].isin([elem.upper() for elem in args.countries])].reset_index(drop=True)

            if args.subset_usernames_file != '':
                geo_df = geo_df[geo_df['user_id'].isin(args.usernames)].reset_index(drop=True)

            df = pd.merge(text_df, geo_df, how='inner', on='message_id')
            df = pd.merge(df, sent_df, how='inner', on='message_id')
            del text_df, geo_df, sent_df

            if len(args.keywords) > 0 or args.lang_level:
                if len(args.incl_keywords)>0:
                    df = df[df['text'].notnull()].reset_index(drop=True)
                    regex = '|'.join(args.incl_keywords)
                    df['keep'] = [bool(re.search(regex, elem)) for elem in df['text'].values]
                    df = df[df['keep']==True].reset_index(drop=True)
                    del df['keep']
                if len(args.excl_keywords)>0:
                    df = df[df['text'].notnull()].reset_index(drop=True)
                    regex = '|'.join(args.excl_keywords)
                    df['drop'] = [bool(re.search(regex, elem)) for elem in df['text'].values]
                    df = df[df['drop']==False].reset_index(drop=True)
                    del df['drop']
            del df['text']


        except:
            print("\nNo data for {}_{}_{}_{}".format(date.year, date.month, str(date.day).zfill(2), str(i).zfill(2)))
            df = pd.DataFrame({
                'message_id': pd.Series([], dtype='str'),
                'lang': pd.Series([], dtype='str'),
                'user_id': pd.Series([], dtype='str'),
                'objectid': pd.Series([], dtype='int'),
                'id_0': pd.Series([], dtype='int'),
                'iso': pd.Series([], dtype='str'),
                'id_1': pd.Series([], dtype='int'),
                'id_2': pd.Series([], dtype='int'),
                'score': pd.Series([], dtype='float')
            })

        df['date'] = date
        dtindex = pd.DatetimeIndex(df['date'])
        df['day'] = dtindex.day
        df['month'] = dtindex.month
        df['year'] = dtindex.year
        del df['date']

        for var in args.geo_vars:
            df[var].fillna(0, inplace=True)

        df = df[['message_id', 'lang', 'user_id', 'score']+args.geo_vars+args.time_vars]

        df_day = pd.concat([df_day, df]).reset_index(drop=True)

    return df_day

def weighted_groupby(df, args, ind_level=True, prefix=""):
    if ind_level:
        vars = ['user_id']+args.time_vars+args.geo_vars+args.other_gb_vars
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
        prefix+'count': df['user_id'].count(),
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

def last_day(i, args):
    if args.time_level=='day':
        return True
    elif args.time_level=='month':
        return i.month == (i+datetime.timedelta(days=1)).month
    elif args.time_level=='year':
        return i.year == (i+datetime.timedelta(days=1)).year
    elif args.time_level=='all':
        return i == args.end_date

def save_df(df, args):

    df.to_csv('data/aggregate_sentiment/{}.tsv'.format(
        args.filename
    ), sep='\t', index=False)
    f = open('data/aggregate_sentiment/{}_README.txt'.format(
        args.filename
    ), "w")
    f.write('Run with the following options:\n{}'.format(args))
    f.close()

def run_aggregation(args):

    args = check_args(args)

    df = pd.DataFrame()
    ind_df = pd.DataFrame()
    dates = get_dates(args)
    for date in dates:
        temp = get_daily_data(date, args)
        temp = temp.groupby(['user_id']+args.time_vars+args.geo_vars+args.other_gb_vars)
        temp = pd.DataFrame({
            'count': temp['message_id'].count(),
            'score': temp['score'].mean(),
        }).reset_index()

        ind_df = weighted_groupby(pd.concat([ind_df, temp], axis=0), args)

        if last_day(date, args) or date==dates[-1]:
            ind_df = aggregate_sentiment(ind_df, args)
            df = pd.concat([df, ind_df], axis=0)
            save_df(df, args)
            ind_df = pd.DataFrame()
