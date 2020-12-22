import pandas as pd
import numpy as np
import os
import nltk
import re
import time
import emoji
from nltk.corpus import stopwords
from nltk import SnowballStemmer
import sys

# nltk.download("stopwords")
stopwords_dict = {}

eng_stopwords = stopwords.words('english')
add_stopwords = [word.replace('\'', '') for word in eng_stopwords]
add_stopwords += ['im', 'th', 'fr', 'bc', 'em', 'da'] # additional stopwords
stopwords_dict['en'] = list(set(eng_stopwords+add_stopwords))

def clean_for_content(string, lang):

    string = re.sub(r'\bhttps?\:\/\/[^\s]+', ' ', string) #remove websites

    # Classic replacements:
    string = re.sub(r'\&gt;', ' > ', string)
    string = re.sub(r'\&lt;', ' < ', string)
    string = re.sub(r'<\s?3', ' ❤ ', string)
    string = re.sub(r'\@\s', ' at ', string)

    if lang=='en':
        string = re.sub(r'(\&(amp)?|amp;)', ' and ', string)
        string = re.sub(r'(\bw\/?\b)', ' with ', string)
        string = re.sub(r'\brn\b', ' right now ', string)
        # string = re.sub(r'\bn\b', ' and ', string) # UNSURE OF THIS ONE! How about n-word? North? etc.
        # fr: for real
        # bc: because
        # tf: the fuck
        # mf: mother fucker
        # em: them
        # da: the
        # pls: please
        # tl: timeline

    # replace some user names by real names?

    string = re.sub(r'\s+', ' ', string).strip()

    return string
