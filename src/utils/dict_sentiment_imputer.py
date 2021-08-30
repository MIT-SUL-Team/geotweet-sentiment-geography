import numpy as np
import pandas as pd
import os
from collections import Counter
from multiprocessing import Pool
import time
import sys
import random
import functools
import operator
import re
import emoji

def read_dic(filepath):
    '''
    Reads a LIWC lexicon from a file in the .dic format, returning a tuple of
    (lexicon, category_names), where:
    * `lexicon` is a dict mapping string patterns to lists of category names
    * `categories` is a list of category names (as strings)
    '''
    # category_mapping is a mapping from integer string to category name
    category_mapping = {}
    # category_names is equivalent to category_mapping.values() but retains original ordering
    category_names = []
    lexicon = {}
    # the mode is incremented by each '%' line in the file
    mode = 0
    for line in open(filepath, encoding='utf-8-sig'):
        tsv = line.strip()
        if tsv:
            parts = tsv.split()
            if parts[0] == '%':
                mode += 1
            elif mode == 1:
                # definining categories
                category_names.append(parts[1])
                category_mapping[parts[0]] = parts[1]
            elif mode == 2:
                lexicon[parts[0]] = [category_mapping[category_id] for category_id in parts[1:]]
    return lexicon, category_names, category_mapping

def _build_trie(lexicon):
    '''
    Build a character-trie from the plain pattern_string -> categories_list
    mapping provided by `lexicon`.
    Some LIWC patterns end with a `*` to indicate a wildcard match.
    '''
    trie = {}
    for pattern, category_names in lexicon.items():
        cursor = trie
        for char in pattern:
            if char == '*':
                cursor['*'] = category_names
                break
            if char not in cursor:
                cursor[char] = {}
            cursor = cursor[char]
        cursor['$'] = category_names
    return trie


def _search_trie(trie, token, token_i=0):
    '''
    Search the given character-trie for paths that match the `token` string.
    '''
    if '*' in trie:
        return trie['*']
    elif '$' in trie and token_i == len(token):
        return trie['$']
    elif token_i < len(token):
        char = token[token_i]
        if char in trie:
            return _search_trie(trie[char], token, token_i + 1)
    return []

def load_token_parser(filepath):
    '''
    Reads a LIWC lexicon from a file in the .dic format, returning a tuple of
    (parse, category_names), where:
    * `parse` is a function from a token to a list of strings (potentially
      empty) of matching categories
    * `category_names` is a list of strings representing all LIWC categories in
      the lexicon
    '''
    lexicon, category_names, category_mapping = read_dic(filepath)
    trie = _build_trie(lexicon)
    return trie, category_mapping

def build_hedono_dict(lang):
    df = pd.read_csv('dicts/sentiment_dicts/hedonometer/hedonometer_{}.csv'.format(lang))
    df['score'] = df['Happiness Score']/10
    return dict(zip(df['Word'], df['score']))

def file_to_dict(sentiment_dict):
    ''' Converts a file with a word per line to a Python dictionary '''

    if sentiment_dict=="hedono":
        out_dict = {}
        for lang in ['ar', 'de', 'en', 'es', 'fr', 'ko', 'pt', 'ru', 'zh']:
            out_dict[lang] = build_hedono_dict(lang)

    elif sentiment_dict=="emoji":
        df = pd.read_csv('dicts/sentiment_dicts/emoji/Emoji_Sentiment_Data_v1.0.csv')
        df['score'] = (df['Neutral']*0.5+df['Positive'])/(df['Negative']+df['Neutral']+df['Positive'])
        out_dict = {}
        out_dict['emoji'] = dict(zip(df['Emoji'], df['score']))

    elif sentiment_dict=="liwc":

        dict_path = 'dicts/sentiment_dicts/liwc/'

        out_dict = {}
        for lang in ['ca', 'de', 'en', 'es', 'fr', 'it', 'nl', 'no', 'pt', 'ru', 'sr', 'ur']:
            out_dict[lang] = {}

        [out_dict['ca']['trie'], out_dict['ca']['xwalk']], out_dict['ca']['year'] = load_token_parser(dict_path+'Traditional_Chinese_LIWC2007_Dictionary.dic'), 2007
        [out_dict['de']['trie'], out_dict['de']['xwalk']], out_dict['de']['year'] = load_token_parser(dict_path+'German_LIWC2001_Dictionary.dic'), 2001
        [out_dict['en']['trie'], out_dict['en']['xwalk']], out_dict['en']['year'] = load_token_parser(dict_path+'LIWC2007_English100131.dic'), 2007
        [out_dict['es']['trie'], out_dict['es']['xwalk']], out_dict['es']['year'] = load_token_parser(dict_path+'Spanish_LIWC2007_Dictionary.dic'), 2007
        [out_dict['fr']['trie'], out_dict['fr']['xwalk']], out_dict['fr']['year'] = load_token_parser(dict_path+'French_LIWC2007_Dictionary.dic'), 2007
        [out_dict['it']['trie'], out_dict['it']['xwalk']], out_dict['it']['year'] = load_token_parser(dict_path+'Italian_LIWC2007_Dictionary.dic'), 2007
        [out_dict['nl']['trie'], out_dict['nl']['xwalk']], out_dict['nl']['year'] = load_token_parser(dict_path+'Dutch_LIWC2007_Dictionary.dic'), 2007
        [out_dict['no']['trie'], out_dict['no']['xwalk']], out_dict['no']['year'] = load_token_parser(dict_path+'Norwegian_LIWC2007_Dictionary.dic'), 2007
        [out_dict['pt']['trie'], out_dict['pt']['xwalk']], out_dict['pt']['year'] = load_token_parser(dict_path+'Brazilian_Portuguese_LIWC2007_Dictionary.dic'), 2007
        [out_dict['ru']['trie'], out_dict['ru']['xwalk']], out_dict['ru']['year'] = load_token_parser(dict_path+'Russian_LIWC2007_Dictionary.dic'), 2007
        [out_dict['sr']['trie'], out_dict['sr']['xwalk']], out_dict['sr']['year'] = load_token_parser(dict_path+'Serbian_LIWC2007_Dictionary.dic'), 2007
        [out_dict['ur']['trie'], out_dict['ur']['xwalk']], out_dict['ur']['year'] = load_token_parser(dict_path+'Ukrainian_LIWC2015_Dictionary.dic'), 2007

    else:
        print("Choose one of the existing sentiment dictionaries.")
        out_dict = {np.nan}

    return out_dict

def get_words_simple(string):
    split_words = string.lower().split()
    return split_words

def get_words_advanced(string):
    string = re.sub(r'(\#|\/|\-|\–|\—)', ' ', string)
    split_words = []
    for elem in string.lower().split():
        if re.match(r'(.+)?(\@|\.).+', elem)==None:
            elem = re.sub('[^(a-z|\')]', '', elem)
            if elem!='' and elem!='\'':
                split_words.append(elem)
    return split_words

def get_emojis(string):
    split_emoji = []
    for elem in emoji.emoji_lis(str(string)):
        split_emoji.append(elem['emoji'])
    return split_emoji

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
