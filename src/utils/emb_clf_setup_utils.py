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
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
import json

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
