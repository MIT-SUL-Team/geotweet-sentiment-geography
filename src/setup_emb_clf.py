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

from utils.emb_clf_setup_utils import clean_for_content, split_train_test, train_model, test_model

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
        torch.save(emb_model, 'models/emb.pkl')
    else:
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
    torch.save(clf_model, 'models/clf.pkl')

    print("\nPerformance on train set:")
    test_model(clf_model, train_df, train_embeddings)

    # Test Model:
    test_df = df.loc[test_ids,:]
    test_embeddings = embeddings[test_ids,:]

    print("\nPerformance on test set:")
    test_model(clf_model, test_df, test_embeddings)
