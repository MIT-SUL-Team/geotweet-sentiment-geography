import pandas as pd
import numpy as np
import argparse
import torch
from tqdm.auto import tqdm
import torch

def create_embeddings(emb_model, df, args):
    emb = emb_model.encode(df['tweet_text_clean'].values, show_progress_bar=True, batch_size=args.batch_size)
    torch.cuda.empty_cache()
    return emb

def embedding_imputation(args):

    if args.overwrite_embeddings:

        df = pd.read_csv(
            args.data_path+'{}.tsv.gz'.format(args.date), sep='\t', low_memory=False,
            usecols=['tweet_id', 'lang', 'tweet_text_clean']
        )
        print("Read in data for {}: {} observations".format(args.date, df.shape[0]))
        df = df[df['tweet_text_clean'].notnull()].reset_index(drop=True)

        emb_model = torch.load('models/emb.pkl')
        clf_model = torch.load('models/clf.pkl')

        nb_iters = int(np.ceil(df.shape[0]/args.max_rows))

        predictions, scores = [], []

        for i in range(nb_iters):

            print("Iteration {} of {}...".format(i+1, nb_iters))
            df_split = df.iloc[i*args.max_rows:(i+1)*args.max_rows, :]

            embeddings = create_embeddings(emb_model, df_split, args)

            predictions += list(clf_model.predict(embeddings))
            scores += list(clf_model.predict_proba(embeddings)[:,1])
            del embeddings

        # print("Exporting")
        # np.save('data/embeddings/embeddings_{}.npy'.format(args.date), np.array(embeddings))
        # np.savetxt('data/embeddings/ids_{}.txt'.format(args.date), df['tweet_id'].values, fmt='%s', delimiter='\n', encoding="utf-8")

    else:
        print("Loading embeddings for {}".format(args.date))
        embeddings = np.load('data/embeddings/embeddings_{}.npy'.format(args.date))
        ids_file = open('data/embeddings/ids_{}.txt'.format(args.date), "r")
        ids = ids_file.read().strip().split('\n')

    df['score'] = np.round(scores, args.score_digits)

    df = df[['tweet_id', 'score']]

    return df
