import sys
import os
import numpy as np
import pandas as pd
import gzip


if __name__ == '__main__':

    geo_path = "/srv/data/twitter_geography/2012/"

    with gzip.open(''.join([geo_path, "geography_2012_2_23_21", ".csv.gz"])) as f:
        df = pd.read_csv(f, sep="\t")

    print("Geography key columns: ", df.columns)

    sent_path = "/srv/data/twitter_sentiment/2012/"

    with gzip.open(''.join([sent_path, "bert_sentiment_2012_2_23_21", ".csv.gz"])) as f:
        df = pd.read_csv(f, sep="\t")

    print("Sentiment key columns: ", df.columns)