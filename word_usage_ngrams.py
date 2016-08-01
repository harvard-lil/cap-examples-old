import os
import unicodedata
from pyspark.sql.types import *
from pyspark.sql.utils import *
from pyspark.sql import SQLContext
import sys


reload(sys)
sys.setdefaultencoding("utf-8")
import json
# sqlContext = SQLContext(sc)
import re
import logging
import datetime
global date
global state
global word_dict

log_file = "word_usage_2.log"
error_file = "error_file.txt"
logging.basicConfig(format='%(levelname)s:%(message)s', filename=log_file,  level=logging.DEBUG)
exclude_states = ['Massachusetts']
word_dict = {}

def touch_open(filename, *args):
    fd = os.open(filename, os.O_RDWR | os.O_CREAT)
    return os.fdopen(fd, *args)

touch_open(error_file)
touch_open(log_file)

stopwords = [
    'get','what','who', "for",
    'and','that','have',
    'not','with','you','which',
    'are', 'then', 'them',
    'also','that', 'its', 'was', 'this',
    'that', 'and', 'the', 'has', 'can',
    ]
alphabet_words = re.compile('[^a-zA-Z]')

def split_string(s):
    s = s.lower().split(' ')
    result = map(filter_word, s)
    logging.info('WORD_USAGE_LOG split_string result %s', result)
    return result

def filter_word(word):
    logging.info('filter_word: %s', word)
    if word:
        try:
            word = word.decode('unicode_escape').encode('ascii', 'ignore')
        except UnicodeDecodeError as e:
            logging.warning('WORD_USAGE_LOG filter_word UnicodeDecodeError: %s', word)
            pass

        word = alphabet_words.sub('', word)
        if (word not in stopwords) and (len(word) > 2):
            logging.info('filter_word returning %s', word)
            return word

def get_decisiondate(x):
    global date
    date_string = x.select('decisiondate').collect()[0][0]['#VALUE']
    possible_dates = re.findall(r'\d+',date_string)
    now = datetime.datetime.now()
    for d in possible_dates:
        try:
            d = int(d)
            if d > 1500 and d < now.year + 2:
                date = d
                return date
        except:
            logger.warning('possible date error %s', single_file)
            pass
    raise Exception('Date not found')

def parse_elements(x):
    word_list = []
    if type(x) is int:
        return
    for element in x:
        logging.info('WORD_USAGE_LOG parse_elements: element %s type %s', element, type(element))
        if not element:
            continue
        if (type(element) is list):
            for n in element:
                word_list.append(parse_elements(n))
        elif (type(element) is str):
            word_list.append(split_string(element))
        elif type(element) is unicode:
            element = unicodedata.normalize('NFKD', element).encode('ascii','ignore')
            word_list.append(split_string(element))
        elif type(element) is int:
            continue
        else:
            val = extract_row_value(element)
            word_list = word_list + split_string(val)
    return word_list

def extract_row_value(row):
    return row['#VALUE']

def write_words_to_json_files():
    global date
    global state
    global word_dict
    date = str(date)

    for word in word_dict.iterkeys():
        word_filename = "./word_output/%s.txt" % word
        local_word_count = word_dict[word]
        with touch_open(word_filename, 'r+') as f:
            try:
                data = json.load(f)
            except Exception as e:
                data = f.read()
                if not data:
                    f.seek(0)
                    f.truncate()
                    data = {}
                pass
            if not data.get(state):
                data[state] = { date: local_word_count }
            else:
                data[state][date] = local_word_count if not data[state].get(date) else data[state][date] + local_word_count
            f.seek(0)
            f.truncate()
            json.dump(data, f)
    return

def get_local_word_counts(word):
    global word_dict
    word_dict[word] = word_dict[word] + 1 if word_dict.get(word) else 1

def get_columns(cols):
    legit_cols = []
    not_legit_cols = ['court','@firstpage','@lastpage','@xmlns','attorneys', 'docketnumber', 'otherdate','parties', 'decisiondate', 'headnotes']
    for col in cols:
        if col not in not_legit_cols:
            legit_cols.append(col)
    return legit_cols

def prepare_word_dictionary():
    global word_dict
    word_dict = {}

def is_xml(file):

    return file[-4:] == '.xml'

def is_file(file):
    # TODO: how does this happen?!
    return os.path.isfile(file)

def walk_through_words(words):
    for word in words:
        if type(word) is list:
            walk_through_words(word)
        else:
            get_local_word_counts(word)

def get_word_usage(sqlContext, sc, files_array=[]):
    prepare_word_dictionary()

    global state
    output_file = "%s_output.txt" % state

    for single_file in files_array:
        if not is_xml(single_file) or not is_file(single_file):
            continue
        logging.info('parsing file: %s', single_file)

        df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='casebody').load(single_file)
        cols = get_columns(df.columns)

        try:
            date = get_decisiondate(df)
            words = df.select(*(i for i in cols)).map(lambda x: parse_elements(x[0])).reduce(lambda l: [x for x in l if x is not None])
            walk_through_words(words)

        except Exception as e:
            logging.error('Error: %s, file: %s', e, single_file)
            with open(error_file, 'w') as ef:
                ef.write('%s\n' % single_file)
            continue

        write_words_to_json_files()

def get_words_for_all_in_dir(sqlContext, sc, current_dir='.'):
    for root, dirs, files in os.walk(current_dir):
        if len(files):
            global state
            # TODO: find better way to get state
            state = root.split('/')[3]
            if state in exclude_states:
                continue
            files = [os.path.join(root, f) for f in files]
            logging.info("WORD_USAGE_LOG %s", state)
            get_word_usage(sqlContext, sc, files_array=files)


