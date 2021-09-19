import pandas as pd
import numpy as np
import torch
from tqdm.auto import tqdm
import os

from utils.data_read_in import read_in, clean_for_content

def create_embeddings(emb_model, df, args):
    emb = emb_model.encode(df['text'].values, show_progress_bar=True, batch_size=args.batch_size)
    torch.cuda.empty_cache()
    return emb

def embedding_imputation(file, args):

    df = read_in(file, args.data_path, cols=['message_id', 'tweet_lang', 'text'])
    df = df[df['text'].notnull()].reset_index(drop=True)

    print("Cleaning data:")
    df['text'] = [clean_for_content(text, lang) for text, lang in tqdm(zip(df['text'], df['lang']), total=df.shape[0])]
    df = df[df['text']!=""].reset_index(drop=True)

    # emb_model = torch.load('models/emb.pkl')
    # clf_model = torch.load('models/clf.pkl')

    nb_iters = int(np.ceil(df.shape[0]/args.max_rows))

    predictions, scores = [], []

    print("Imputing Sentiment:")
    for i in range(nb_iters):

        print("Chunk {} of {}...".format(i+1, nb_iters))
        df_split = df.iloc[i*args.max_rows:(i+1)*args.max_rows, :]

        embeddings = create_embeddings(args.emb_model, df_split, args)

        # predictions += list(args.clf_model.predict(embeddings))
        scores += list(args.clf_model.predict_proba(embeddings)[:,1])
        del embeddings

    df['score'] = np.round(scores, args.score_digits)

    df = df[['message_id', 'score']]

    return df
