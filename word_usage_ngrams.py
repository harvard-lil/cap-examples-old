#!/usr/bin/env python
import io
import os
import sys
import unicodedata
from pyspark.sql.types import *
from pyspark.sql.utils import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
sc = SparkContext("local", "Simple App")
sqlContext = SQLContext(sc)

reload(sys)
sys.setdefaultencoding("utf-8")
import json
import re
import logging
import datetime
global date
global state
global word_dict
global region_path

log_file = "%s/word_usage_2.log" % os.path.curdir
error_file = "%s/error_file.txt" % os.path.curdir
word_ngrams_dir = "%s/word_ngrams_output/" % os.path.curdir
logging.basicConfig(format='%(levelname)s:%(message)s', filename=log_file,  level=logging.DEBUG)
exclude_states = []
word_dict = {}

if not os.path.exists(word_ngrams_dir):
    os.makedirs(word_ngrams_dir)

def touch_open(filename, *args):
    try:
        fd = os.open(filename, os.O_RDWR | os.O_CREAT)
    except:
        io.FileIO(filename, 'a')
        pass
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
            word = word.encode('utf8')
        except Exception as e:
            logging.warning('WORD_USAGE_LOG filter_word UnicodeDecodeError: %s %s', word, e)
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
    try:
        if type(x) is int:
            return
        for element in x:
            logging.info('WORD_USAGE_LOG parse_elements: element %s type %s', element, type(element))
            if not element: continue
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
    except Exception as e:
        pass
    return word_list

def extract_row_value(row):
    return row['#VALUE']



class NestedReadCreate(object):
     def __init__(self, root):
         self.root = root

     def __call__(self, lookups=[], count=None):
        obj = self.root
        try:
            for idx,key in enumerate(lookups):
                if (idx is len(lookups) - 1) and count:
                    if not obj.get(key):
                        obj[key] = count
                    else:
                        obj[key] = obj[key] + count
                else:
                    obj = obj[key]
            return obj

        except KeyError as e:
            obj[key] = {}
            if idx < len(lookups) - 1:
                self.__call__(lookups=lookups, count=count)

            return obj

def write_words_to_json_files(file_path):
    global date
    global state
    global word_dict
    global region_path

    for word in word_dict.iterkeys():
        word_filename = "%s%s.txt" % (word_ngrams_dir, word)
        local_word_count = word_dict[word]
        date = str(date)
        full_region_path = ('.').join([x for x in region_path])
        with touch_open(word_filename, 'r+') as f:
            try:
                data = json.load(f)
            except Exception as e:
                data = f.read()
                if not data:
                    clear_file(f)
                    data = {}
                pass
            nrc = NestedReadCreate(data)
            # checking if properties exist, creating them if they don't
            nrc(lookups=[full_region_path, date], count=local_word_count)
            nrc(lookups=[state, 'total_state', date], count=local_word_count)
            nrc(lookups=['total_country', date], count=local_word_count)
            clear_file(f)
            json.dump(data, f)
    return

def get_local_word_counts(word):
    global word_dict
    word_dict[word] = word_dict[word] + 1 if word_dict.get(word) else 1

def clear_file(f):
    f.seek(0)
    f.truncate()

def get_columns(cols):
    legit_cols = []
    not_legit_cols = ['court','@firstpage','@lastpage','@xmlns','attorneys', 'docketnumber', 'otherdate','parties', 'decisiondate', 'headnotes']
    for col in cols:
        if col not in not_legit_cols:
            legit_cols.append(col)
    return legit_cols

def parse_path(path):
    state_list = ['Alabama','Alaska','American Samoa','Arizona','Arkansas','California','Colorado','Connecticut','Delaware','District of Columbia','Federated States of Micronesia','Florida','Georgia','Guam','Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentucky','Louisiana','Maine','Marshall Islands','Maryland','Massachusetts','Michigan','Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada','New Hampshire','New Jersey','New Mexico','New York','North Carolina','North Dakota','Northern Mariana Islands','Ohio','Oklahoma','Oregon','Palau','Pennsylvania','Puerto Rico','Rhode Island','South Carolina','South Dakota','Tennessee','Texas','Utah','Vermont','Virgin Island','Virginia','Washington','West Virginia','Wisconsin','Wyoming']
    path_list = path.split('/')
    for (idx,s) in enumerate(path_list):
        if s in state_list:
            state = s
            # keep everything from path except last thing which is the .xml
            path = path_list[idx:]
            path.pop()
            return [state, path]
    # if state not here, take a guess
    return [path_list[3], path_list[4:]]

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

def get_word_usage(files_array=[]):
    prepare_word_dictionary()

    global state

    for single_file in files_array:
        if not is_xml(single_file) or not is_file(single_file):
            continue

        logging.info('parsing file: %s', single_file)

        df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='casebody').load(single_file)
        cols = get_columns(df.columns)

        try:
            date = get_decisiondate(df)
            words = df.select(*(i for i in cols)).map(lambda x: x[0] if not type(x[0]) is int else []).map(lambda x: parse_elements(x)).reduce(lambda l: [x for x in l if x is not None])
            walk_through_words(words)

        except Exception as e:
            logging.error('Error: %s, file: %s', e, single_file)
            with open(error_file, 'w') as ef:
                ef.write('%s\n' % single_file)
            continue

        write_words_to_json_files(file_path=single_file)

def get_words_for_all_in_dir():
    current_dir = sys.argv[1]
    for root, dirs, files in os.walk(current_dir):
        if len(files):
            global state
            global region_path

            files = [os.path.join(root, f) for f in files if f != '.DS_Store']
            try:
                state, region_path = parse_path(files[0])
            except IndexError as e:
                continue

            if state in exclude_states: continue

            logging.info("WORD_USAGE_LOG %s", state)
            get_word_usage(files_array=files)


if __name__ == "__main__":
    get_words_for_all_in_dir()
