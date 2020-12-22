import re

## China
filename = 'Traditional_Chinese_LIWC2007_Dictionary.dic'
with open('dicts/sentiment_dicts/liwc/raw/{}'.format(filename), 'r') as f:
    new_file = []
    for line in f:
        line_clean = line
        new_file.append(line_clean)
with open('dicts/sentiment_dicts/liwc/{}'.format(filename), 'w') as f:
    for item in new_file:
        f.write(item)

## German
filename = 'German_LIWC2001_Dictionary.dic'
with open('dicts/sentiment_dicts/liwc/raw/{}'.format(filename), 'r', encoding='latin') as f:
    new_file = []
    for line in f:
        line_clean = line
        new_file.append(line_clean)
with open('dicts/sentiment_dicts/liwc/{}'.format(filename), 'w') as f:
    for item in new_file:
        f.write(item)

## English
filename = 'LIWC2007_English100131.dic'
with open('dicts/sentiment_dicts/liwc/raw/{}'.format(filename), 'r') as f:
    new_file = []
    for line in f:
        line_clean = line
        line_clean = re.sub(r'\t<of>131/125\b', '\t131\t125', line_clean)
        line_clean = re.sub(r'\t<of>135/126\b', '\t135\t126', line_clean)
        line_clean = re.sub(r'\t\(02 134\)125/464\b', '\t125\t464', line_clean)
        line_clean = re.sub(r'\t\(02 134\)126\b', '\t126', line_clean)
        new_file.append(line_clean)
with open('dicts/sentiment_dicts/liwc/{}'.format(filename), 'w') as f:
    for item in new_file:
        f.write(item)

## Spain
filename = 'Spanish_LIWC2007_Dictionary.dic'
with open('dicts/sentiment_dicts/liwc/raw/{}'.format(filename), 'r', encoding='latin') as f:
    new_file = []
    for line in f:
        line_clean = line
        new_file.append(line_clean)
with open('dicts/sentiment_dicts/liwc/{}'.format(filename), 'w') as f:
    for item in new_file:
        f.write(item)

## French
filename = 'French_LIWC2007_Dictionary.dic'
with open('dicts/sentiment_dicts/liwc/raw/{}'.format(filename), 'r', encoding='latin') as f:
    new_file = []
    for line in f:
        line_clean = line
        new_file.append(line_clean)
with open('dicts/sentiment_dicts/liwc/{}'.format(filename), 'w') as f:
    for item in new_file:
        f.write(item)

## Italy
filename = 'Italian_LIWC2007_Dictionary.dic'
with open('dicts/sentiment_dicts/liwc/raw/{}'.format(filename), 'r', encoding='latin') as f:
    new_file = []
    for line in f:
        line_clean = line
        new_file.append(line_clean)
with open('dicts/sentiment_dicts/liwc/{}'.format(filename), 'w') as f:
    for item in new_file:
        f.write(item)

## Netherlands
filename = 'Dutch_LIWC2007_Dictionary.dic'
with open('dicts/sentiment_dicts/liwc/raw/{}'.format(filename), 'r', encoding='latin') as f:
    new_file = []
    for line in f:
        line_clean = line
        line_clean = re.sub('^ï»¿%$', '%', line_clean)
        new_file.append(line_clean)
with open('dicts/sentiment_dicts/liwc/{}'.format(filename), 'w') as f:
    for item in new_file:
        f.write(item)

## Norwegian
filename = 'Norwegian_LIWC2007_Dictionary.dic'
with open('dicts/sentiment_dicts/liwc/raw/{}'.format(filename), 'r') as f:
    new_file = []
    for line in f:
        line_clean = line
        line_clean = re.sub(
            r'\b(29|37|46|48|153|238|245|254|350|352|353|365|366|367|368|369|456|565|568|577|1267|1278|1412)\b',
            '\t', line_clean
        )
        new_file.append(line_clean)
with open('dicts/sentiment_dicts/liwc/{}'.format(filename), 'w') as f:
    for item in new_file:
        f.write(item)

## Portuguese
filename = 'Brazilian_Portuguese_LIWC2007_Dictionary.dic'
with open('dicts/sentiment_dicts/liwc/raw/{}'.format(filename), 'r', encoding='latin') as f:
    new_file = []
    for line in f:
        line_clean = line
        new_file.append(line_clean)
with open('dicts/sentiment_dicts/liwc/{}'.format(filename), 'w') as f:
    for item in new_file:
        f.write(item)

## Russian
filename = 'Russian_LIWC2007_Dictionary.dic'
with open('dicts/sentiment_dicts/liwc/raw/{}'.format(filename), 'r') as f:
    new_file = []
    for line in f:
        line_clean = line
        line_clean = re.sub(r'\t<of>131/125\b', '\t131 125', line_clean)
        line_clean = re.sub(r'\t<of>135/126\b', '\t135 126', line_clean)
        line_clean = re.sub(r'^сорт\*\t131\t135$', 'сорт*\t131\t125\t135\t126', line_clean)
        new_file.append(line_clean)
with open('dicts/sentiment_dicts/liwc/{}'.format(filename), 'w') as f:
    for item in new_file:
        f.write(item)

## Serbia
filename = 'Serbian_LIWC2007_Dictionary.dic'
with open('dicts/sentiment_dicts/liwc/raw/{}'.format(filename), 'r') as f:
    new_file = []
    for line in f:
        line_clean = line
        line_clean = re.sub(
            r'\b(230|156|l250|365|249|145)\b',
            '\t', line_clean
        )
        new_file.append(line_clean)
with open('dicts/sentiment_dicts/liwc/{}'.format(filename), 'w') as f:
    for item in new_file:
        f.write(item)

## Ukrain:
filename = 'Ukrainian_LIWC2015_Dictionary.dic'
with open('dicts/sentiment_dicts/liwc/raw/{}'.format(filename), 'r') as f:
    new_file = []
    for line in f:
        if ' ' in line:
            continue
        else:
            line_clean = line
            new_file.append(line_clean)
with open('dicts/sentiment_dicts/liwc/{}'.format(filename), 'w') as f:
    for item in new_file:
        f.write(item)
