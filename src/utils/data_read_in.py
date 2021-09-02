import pandas as pd
import os
import re
import html

def read_in(file, args, cols):

    df = pd.read_csv(
        os.path.join(args.data_path, file), sep='\t', low_memory=False, lineterminator='\n',
        usecols=cols
    )
    df.rename(columns={'tweet_lang':'lang'}, inplace=True)
    df = df[df['text'].notnull()].reset_index(drop=True)
    print("Read in data for {}: {} observations".format(file, df.shape[0]))
    return df

def deEmojify(text):
    regrex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags = re.UNICODE)
    return regrex_pattern.sub(r'',text)

def standardize_username(string):
    string = re.sub(r'\@[A-z0-9\_]+', ' @user ', string) # replace user names by @user
    return string

def clean_for_content(string, lang):

    string = re.sub(r'\bhttps?\:\/\/[^\s]+', ' ', string) #remove websites

    string = html.unescape(string)

    string = deEmojify(string) # remove emojis

    # Classic replacements:
    string = re.sub(r'\&gt;', ' > ', string)
    string = re.sub(r'\&lt;', ' < ', string)
    string = re.sub(r'<\s?3', ' ❤ ', string)
    string = re.sub(r'\@\s', ' at ', string)

    string = standardize_username(string) # replace user names by @user

    if lang=='en':
        string = re.sub(r'(\&(amp)?|amp;)', ' and ', string)
        string = re.sub(r'(\bw\/?\b)', ' with ', string)
        string = re.sub(r'\brn\b', ' right now ', string)

    string = re.sub(r'\s+', ' ', string).strip()

    return string

def clean_for_keywords(string, lang, stemming):

    string = clean_for_content(string, lang)

    string = string.lower()

    string = re.sub(r'\b\d\d?\d?\b', ' ', string) # remove 1, 2, 3 digit numbers
    string = re.sub(r'\b\d{4}\d+\b', ' ', string) # remove 5+ digit numbers

    string = ' '.join(re.findall('\w+', string))

    string = re.sub(r'\b[a-z]\b', ' ', string) # remove 1-letter words

    string = re.sub(r'\s+', ' ', string).strip()

    if stemming:

        import nltk
        from nltk.corpus import stopwords
        from nltk import SnowballStemmer

        if 'stopwords' not in os.listdir('~/nltk_data/corpora/'):
            nltk.download("stopwords")
        stopwords_dict = {}

        eng_stopwords = stopwords.words('english')
        add_stopwords = [word.replace('\'', '') for word in eng_stopwords]
        add_stopwords += ['im', 'th', 'fr', 'bc', 'em', 'da'] # additional stopwords
        stopwords_dict['en'] = list(set(eng_stopwords+add_stopwords))

        if lang=='en':

            stopwords = stopwords_dict[lang]
            stemmer=SnowballStemmer("english")

            string = string.split()
            string = [elem for elem in string if elem not in stopwords]

            string = [stemmer.stem(elem) if elem[0] not in ['#', '@'] else elem for elem in string] # Do not stem usernames and hashtags

            string = ' '.join(string)

    return string

def clean_for_emojis(string):
    import emoji
    string = ''.join(c for c in string if c in emoji.UNICODE_EMOJI)
    return string
