import numpy as np
import pandas as pd
import os
from collections import Counter
from multiprocessing import Pool
import time

from utils.sentiment_imputer_utils import _search_trie, file_to_dict, get_words_simple, get_words_advanced, get_emojis

def imputer(row, imputation_method, sentiment_dict, args):

    message_id = row['message_id']
    lang = row['lang']
    text = str(row['text_clean'])

    if (imputation_method=='liwc') or (imputation_method=='hedono') or (imputation_method=='emoji'):

        if imputation_method=='emoji':
            lang='emoji'

        if lang in sentiment_dict.keys():

            # Get tockens:
            if lang=='emoji':
                words = get_emojis(text)
            elif (lang=='ru') or (lang=='ur') or (lang=='no') or (lang=='ca'):
                words = get_words_simple(text)
            else:
                words = get_words_advanced(text)

            # Evaluate tockens:
            if imputation_method=='liwc':

                def parse_token(token, trie):
                    for category_name in _search_trie(trie, token):
                        yield category_name
                counts = Counter(category for word in words for category in parse_token(word, sentiment_dict[lang]['trie']))

                if lang=='it':
                    pos_cats = [sentiment_dict[lang]['xwalk']['13']]
                    neg_cats = [sentiment_dict[lang]['xwalk']['16']]
                elif lang=='ur':
                    pos_cats = [sentiment_dict[lang]['xwalk']['31']]
                    neg_cats = [sentiment_dict[lang]['xwalk']['32']]
                elif sentiment_dict[lang]['year']==2007:
                    pos_cats = [sentiment_dict[lang]['xwalk']['126']]
                    neg_cats = [sentiment_dict[lang]['xwalk']['127']]
                elif sentiment_dict[lang]['year']==2001:
                    pos_cats = [sentiment_dict[lang]['xwalk']['13']]
                    neg_cats = [sentiment_dict[lang]['xwalk']['16']]
                else:
                    print("Put in year sentiment!")
                num, den = 0, 0
                for cat in pos_cats:
                    num += counts[cat]
                    den += counts[cat]
                for cat in neg_cats:
                    den += counts[cat]
                if den==0:
                    score=np.nan
                    hit_count = 0
                else:
                    score = round(num/den, args.score_digits)
                    hit_count = den
                del num, den, pos_cats, neg_cats, counts

            else:
                hit_count, score = 0, 0
                for word in words:
                    if word in sentiment_dict[lang]:
                        hit_count += 1
                        score += sentiment_dict[lang][word]
                if hit_count==0:
                    score = np.nan
                else:
                    score = round(score/hit_count, args.score_digits)

        else:
            score, hit_count = np.nan, np.nan

        del text, lang

        return pd.Series({'message_id':message_id, 'score':score, 'hit_count':hit_count})

    else:
        print("Not valid imputation method")

def by_chunk(df_split, imputation_method, sentiment_dict, args):
    df_sentiment = df_split.apply(lambda row: imputer(row, imputation_method=imputation_method, sentiment_dict=sentiment_dict, args=args), axis=1)
    return df_sentiment

def parallel_imputation(args, imputation_method):

    sentiment_dict = file_to_dict(imputation_method)

    results_dict = {}
    data_obs = sum(1 for line in open(os.path.join(args.data_path, 'text_{}.tsv.gz'.format(args.date)), encoding="utf8", errors='ignore'))-1
    nb_iters = int(np.ceil(data_obs/args.max_rows))

    start = time.time()
    for i in range(nb_iters):

        print("Reading in data from {} (iteration {} of {})...".format(args.date, i+1, nb_iters))

        df_split = pd.read_csv(
            os.path.join(args.data_path, 'text_{}.tsv.gz'.format(args.date)), sep='\t', low_memory=False,
            nrows=args.max_rows, skiprows=range(1, i*args.max_rows+1), usecols=['message_id', 'lang', 'text_clean']
        )
        df_split = np.array_split(df_split, args.nb_cores)
        pool = Pool(args.nb_cores)
        results_dict[i] = pd.concat(pool.starmap(by_chunk, [[df_split_i, imputation_method, sentiment_dict, args] for df_split_i in df_split]))
        del df_split
        pool.close()
        pool.join()
    print ("Imputation took {} seconds to process".format(round(time.time()-start, 2)))

    df = pd.DataFrame()
    for i in range(nb_iters):
        df = pd.concat([df, results_dict[i]])
        del results_dict[i]
    del results_dict

    df = df[df['score'].notnull()]

    return df
