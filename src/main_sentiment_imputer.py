# usage: python src/main_sentiment_imputer.py 2020_11_18_23.csv.gz --data_path /n/holyscratch01/cga/nicogj/main/ --output_path /n/cga/data/geo-tweets/cga-sbg-sentiment/2020 --dict_methods --emb_methods bert

import pandas as pd
import os
import argparse
import multiprocessing

def imputer(args, imputation_method):

    print("\n{}:".format(imputation_method.upper()))

    if imputation_method not in ['liwc', 'emoji', 'hedono', 'bert']:
        print("Not a proper imputation method. Skipping.")

    elif imputation_method in args.dict_methods:
        from utils.dict_sentiment_imputer import parallel_imputation
        df = parallel_imputation(args, imputation_method=imputation_method)

    elif imputation_method in args.emb_methods:
        from utils.emb_sentiment_imputer import embedding_imputation
        df = embedding_imputation(args)

    df.to_csv(
        os.path.join(args.output_path, '{}_sentiment_{}'.format(imputation_method, args.filename)), sep='\t', index=False
    )
    print("Done! Imputed {} scores.".format(df.shape[0]))
    del df

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='What filename do you want to impute sentiment for?')
    parser.add_argument('--platform', default='', help='Which social media data are we using (twitter, weibo)?')
    parser.add_argument('--data_path', default='', type=str, help='path to data')
    parser.add_argument('--output_path', default='data/sentiment_scores/', type=str, help='path to output')
    parser.add_argument('--dict_methods', nargs='*', default='liwc emoji hedono', help='Which dict techniques do you want to use?')
    parser.add_argument('--emb_methods', nargs='*', default='bert', help='Which embedding techniques do you want to use?')
    parser.add_argument('--random_seed', default=123, type=int, help='random seed')
    parser.add_argument('--score_digits', default=6, type=int, help='how many digits to the output score')

    # Emb based parameters
    parser.add_argument('--batch_size', default = 100, type = int, help='batch size')

    # Dict based parameters
    parser.add_argument('--max_rows', default = 2500000, type=int, help='Run by chunks of how many rows')
    parser.add_argument('--nb_cores', default = min(16, multiprocessing.cpu_count()), type=int, help='')

    args = parser.parse_args()

    print("\n\nRunning for {}".format(args.filename))

    for method in args.dict_methods + args.emb_methods:
        imputer(args, method)
