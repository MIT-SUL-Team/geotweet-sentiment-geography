# GeoTweets Sentiment and Geography Imputer

## Introduction

This repository imputes

## Requirements:

### Compute:
GPU is recommended, but the code will run on CPU as well.

### Python version:
Python 3

### Packages

- `argparse`
- `collections`
- `datetime`
- `emoji`
- `functools`
- `glob`
- `html`
- `json`
- `multiprocessing`
- `nltk`
- `numpy`
- `operator`
- `os`
- `pandas`
- `pyarrow`
- `pymapd`
- `random`
- `re`
- `sentence_transformers`
- `sklearn`
- `sys`
- `time`
- `torch`
- `tqdm`


## Example usage of scripts

### Prepare LIWC dictionaries:
```
python src/liwc_cleaning.py
```

### Train Sentiment classifier
```
python -u src/setup_emb_clf.py
```

### Run sentiment imputation
```
python src/main_sentiment_imputer.py --data_path /n/holyscratch01/cga/nicogj/main/ --output_path /n/holyscratch01/cga/nicogj/output/ --dict_methods --emb_methods bert
```

### Run aggregation:
```
python src/main_sentiment_aggregator.py twitter --geo_level global --text_path /n/cga/data/geo-tweets/cga-sbg/ --geo_path /n/cga/data/geo-tweets/cga-sbg-geography/ --sent_path /n/cga/data/geo-tweets/cga-sbg-sentiment/ --start_date 2016-01-01 --end_date 2019-12-31 --name_ext global_index
```
