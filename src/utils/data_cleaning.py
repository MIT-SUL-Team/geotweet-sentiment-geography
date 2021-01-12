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

    emoji_pattern = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642"
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
        "]+",
        flags=re.UNICODE
    )
    string = re.sub(emoji_pattern, ' ', string) # remove emojis

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
