import sys
import random
import pandas as pd
import numpy as np
import functools
import operator
import re
import emoji

from utils.liwc import load_token_parser

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
