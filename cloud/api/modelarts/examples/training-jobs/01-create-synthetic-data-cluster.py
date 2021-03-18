import json
import re
import random
import string


# list of letters and numbers 
alphanumeric = string.ascii_lowercase + string.digits

# common OCR mistakes
errors = [  ['a', 'e'],
            ['e', 'a'],
            ['b', '6'],
            ['6', 'b'],
            ['a', '4'],
            ['4', 'a'],
            ['f', 's'],
            ['s', 'f'],
            ['h', '6'],
            ['6', 'h'],
            ['i', 'j'],
            ['j', 'i'],
            ['l', '1'],
            ['1', 'l'],
            ['m', 'n'],
            ['n', 'm'],
            ['o', '0'],
            ['0', 'o'],
            ['p', 'q'],
            ['q', 'p'],
            ['p', 'b'],
            ['b', 'p'],
            ['s', '5'],
            ['5', 's'],
            ['v', 'w'],
            ['w', 'v'],
            ['v', 'y'],
            ['y', 'v'],
            ['q', '9'],
            ['9', 'q'],
            ['h', 'b'],
            ['b', 'h'] ]


def hot_encode_word(word, size=50):
    he = ['-1'] * size
    ls = list(word)
    for i, c in enumerate(ls):
        he[i] = str(alphanumeric.index(c))
    return he


# read json file
with open('data/quotes.json', 'r') as json_file:
    data = json.load(json_file)

# create a list of quotes
quotes = []
for d in data:
    quotes.append(d['Quote'])

# removing duplicated quotes
quotes = list(set(quotes))

# create a csv files
hea_file = open('data/samples/hot-encoded-annotations.csv', 'w')

for quote in quotes:
    # clean text
    words = quote.split()
    for i, w in enumerate(words):
        words[i] = re.sub('[^a-zA-Z0-9]', '', w) 
    words = [word.lower() for word in words]
    
    if len(set(words)) <= 1:
        continue

    # select word
    word = random.choice(words)

    # discard small words
    if len(word) < 4:
        continue

    # create an "OCR" text it has 90% chance to be correct
    luck = random.random()

    # add the letter position in the alphabet, just to create a chart
    ocr = word
    if luck < .10:
        error = random.choice(errors)
        ocr = ocr.replace(*error)
    hea_file.write(f"{word},{ocr},{','.join(hot_encode_word(ocr))}" + "\n")
    
    # create several "user annotated" words
    for i in range(15):
        annotated = word 
        if luck < 0.4:
            error = random.choice(errors)
            annotated = annotated.replace(*error)
        hea_file.write(f"{word},{annotated},{','.join(hot_encode_word(annotated))}" + "\n")
        com_file.write(f'{word},{annotated}' + '\n')

hea_file.close() 