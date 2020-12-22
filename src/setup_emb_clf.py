# python3 src/setup_emb_clf.py --overwrite_embeddings=True

import pandas as pd
import numpy as np
import sklearn
import argparse
from sentence_transformers import SentenceTransformer
import joblib
from tqdm.auto import tqdm
import json
import sys
import re
import argparse
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

from utils.data_cleaning import clean_for_content

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

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--emb_model', type=str, default='xlm-r-bert-base-nli-stsb-mean-tokens', help='embedding model')
    parser.add_argument('--max_seq_length', type=int, default=32, help='maximum sequence length')
    parser.add_argument('--overwrite_embeddings', default=False, type=bool, help='Rerun the embeddings?')
    parser.add_argument('--train_size', default=0.8, type=float, help='What is the size of the training set?')
    parser.add_argument('--max_iter', type=int, default=100, help='number of max iterations for model fitting')
    parser.add_argument('--reg', type=float, default=1., help='inverse regularization stregnth, smaller values specify stronger regularization.')
    parser.add_argument('--reg_norm', type=str, default='l2', help='regularization norm')
    parser.add_argument('--random_seed', default=123, type=int, help='random seed')
    parser.add_argument('--batch_size', default = 100, type = int, help='batch size')
    parser.add_argument('--pca_dims', default = 100, type = int, help='number of embedding dimensions for classifier')
    args = parser.parse_args()

    print("Reading in training data")
    df = pd.read_csv('data/labeled_data/training_1600000_processed_noemoticon.csv', encoding='latin', header=None, usecols=[0,5])
    df.columns = ['label', 'text']

    print("Cleaning training data")
    df['label'] = [0 if x==0 else 1 for x in df['label']]
    df['lang'] = 'en'
    df['text'] = [clean_for_content(text, lang) for text, lang in tqdm(zip(df['text'], df['lang']), total=df.shape[0])]
    df = df[df['text']!=''].reset_index(drop=True)

    if args.overwrite_embeddings==True:
        print("Creating Embeddings")
        emb_model = SentenceTransformer(args.emb_model)
        emb_model.max_seq_length = args.max_seq_length
        embeddings = emb_model.encode(df['text'].values, show_progress_bar=True, batch_size=args.batch_size)
        np.save('data/labeled_data/embeddings.npy', np.array(embeddings))
        joblib.dump(emb_model, 'models/emb.pkl')

    embeddings = np.load('data/labeled_data/embeddings.npy')

    # Generate Training set
    print("Preparing training and test sets")
    split_train_test(df, args)
    with open('data/labeled_data/train_ids.txt', 'r') as fp:
        train_ids = json.load(fp)
    with open('data/labeled_data/test_ids.txt', 'r') as fp:
        test_ids = json.load(fp)

    # Create and Train Model
    train_df = df.loc[train_ids,:]
    train_embeddings = embeddings[train_ids,:]

    clf_model = train_model(train_df, train_embeddings, args)
    joblib.dump(clf_model, 'models/clf.pkl')

    print("\nPerformance on train set:")
    test_model(clf_model, train_df, train_embeddings)

    # Test Model:
    test_df = df.loc[test_ids,:]
    test_embeddings = embeddings[test_ids,:]

    print("\nPerformance on test set:")
    test_model(clf_model, test_df, test_embeddings)
