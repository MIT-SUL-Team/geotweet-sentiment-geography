#!/bin/sh

# Prepare LIWC dictionaries:
python3 src/liwc_cleaning.py

# Train Sentiment classifier
python3 src/setup_emb_clf.py --overwrite_embeddings=True

# Run sentiment imputation
python3 src/main_sentiment_imputer.py 20200101 --dict_methods liwc emoji --emb_methods bert
