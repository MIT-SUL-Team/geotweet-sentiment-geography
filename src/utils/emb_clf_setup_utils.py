import pandas as pd
import numpy as np
import os
import nltk
import re
import time
import emoji
import html
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

    string = html.unescape(string)

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

    # replace user names by @user
    string = re.sub(r'\@[A-z0-9\_]+', ' @user ', string)

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
    # Replace user names by @user

    string = re.sub(r'\s+', ' ', string).strip()

    return string

def test(i=None):
    if i is None:
        i = randrange(df.shape[0])
    print(i)
    print(df['text'][i])
    print(df['text_clean'][i])

def split_train_test(df, args):

    df_train, df_test = train_test_split(df, test_size=1-args.train_size, random_state=args.random_seed)
    train_ids = list(df_train.index)
    test_ids = list(df_test.index)

    print("TRAIN size:", len(train_ids))
    print("TEST size:", len(test_ids))

    with open('data/labeled_data/train_ids.txt', 'w') as fp:
        json.dump(train_ids, fp)
    with open('data/labeled_data/test_ids.txt', 'w') as fp:
        json.dump(test_ids, fp)

def train_model(train_df, train_embeddings, args):

    X = train_embeddings
    y = train_df['label'].values

    pca = PCA(n_components = args.pca_dims, random_state=args.random_seed)

    if args.reg_norm == 'l1':
        logreg = LogisticRegression(random_state=args.random_seed, solver='saga', max_iter=args.max_iter, C=args.reg, penalty='l1')
    else:
        logreg = LogisticRegression(random_state=args.random_seed, solver='lbfgs', max_iter=args.max_iter, C=args.reg, penalty=args.reg_norm)

    pipe = Pipeline([('pca', pca), ('logreg', logreg)])
    clf = pipe.fit(X, y)

    print('Training set accuracy: {}'.format(clf.score(X, y)))

    return clf

def test_model(clf, test_df, test_embeddings):

    print("Testing model...")

    test_pred = clf.predict(test_embeddings)

    correct = test_pred==test_df['label']
    wrong = test_pred!=test_df['label']
    tp = sum(correct & test_df['label'])
    tn = sum(correct & ~test_df['label'])
    fn = sum(wrong & test_df['label'])
    fp = sum(wrong & ~test_df['label'])
    t = sum(correct)

    print('Got {} out of {} correct.'.format(t, test_df.shape[0]))
    print('Accuracy rate is {}'.format(t/test_df.shape[0]))
    print('Precision is {}, Recall is {}'.format(tp / (tp+fp), tp / (tp + fn)))
