#!/usr/bin/env python
import io
import os
import re
import sys
import json
import logging
import unicodedata
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.utils import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
from collections import Counter
from pyspark.sql.types import Row

sc = SparkContext("local", "Simple App")
sqlContext = SQLContext(sc)
reload(sys)
sys.setdefaultencoding("utf-8")
timestamp = re.sub(r'[\s\\:.+]', '_', str(datetime.today()))

log_file = "%s/word_usage_%s.log" % (os.path.curdir, timestamp)
logging.basicConfig(format='%(levelname)s:%(message)s', filename=log_file, level=logging.ERROR)
error_file = "%s/error_file_%s.txt" % (os.path.curdir, timestamp)
word_ngrams_dir = "%s/words_%s/" % (os.path.curdir, timestamp)

exclude_states = []

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
    'that', 'and', 'the', 'has', 'can']

alphabet_words = re.compile('[^a-zA-Z]')

def break_up_words(s):
    s = s.lower().split(' ')
    mapped = map(fix_word, s)
    return filter(filter_word, mapped)

def fix_word(word):
    try:
        word = word.encode('utf8')
    except Exception as e:
        pass

    return alphabet_words.sub('', word)

def filter_word(word):
    return (word not in stopwords) and (len(word) > 2)

def get_decisiondate(x):
    now = datetime.now().year
    if type(x) is int:
        return x
    else:
        try:
            possible_dates = x.split('-')
            d = [int(x) for x in possible_dates if x.isdigit() and len(x) == 4 and int(x) > 1500 and int(x) < now + 4 ]
            return d[0]
        except:
            raise Exception('Date not found')

def clean_string(val):
    if type(val) is unicode:
        return unicodedata.normalize('NFKD', val).encode('ascii','ignore')
    else:
        return val

def parse_elements(x):
    the_words = []
    try:
        if type(x) is Row:
            return parse_elements(x.asDict(recursive=True))
        elif type(x) is list:
            for n in x:
                the_words += parse_elements(n)
            return the_words
        elif type(x) is dict:
            for key,element in x.items():
                the_words += parse_elements(clean_string(element))
            return the_words
        elif type(x) is str:
            return break_up_words(x)
        elif type(x) is unicode:
            return break_up_words(clean_string(x))
        else:
            return []
    except Exception as e:
        return []

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

def write_words_to_json_files(word_dict, date):
    global state, region_path

    for word, count in word_dict.items():
        word_filename = "%s%s.txt" % (word_ngrams_dir, word)
        count = word_dict[word]
        date = str(date)
        full_region_path = ('.').join([x for x in region_path])
        with touch_open(word_filename, 'r+') as f:
            try:
                data = json.load(f)
            except Exception as e:
                data = f.read()
                logging.warning('write_words_to_json_files: %s', e)
                if not data:
                    clear_file(f)
                    data = {}
                pass

            nrc = NestedReadCreate(data)
            # checking if properties exist, creating them if they don't
            nrc(lookups=[full_region_path, date], count=count)
            nrc(lookups=[state, 'total_state', date], count=count)
            nrc(lookups=['total_country', date], count=count)

            clear_file(f)
            json.dump(data, f)
    return

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
            # keep everything from path except last thing which is the .xml
            path = path_list[idx:]
            path.pop()
            return [s, path]
    # if state not here, take a guess
    return [path_list[3], path_list[4:]]

def is_xml(file):
    return file[-4:] == '.xml'

def is_file(file):
    # TODO: how does this happen?!
    return os.path.isfile(file)

def get_word_usage(files_array=[]):
    global state
    word_list = []
    for single_file in files_array:
        if not is_xml(single_file) or not is_file(single_file): continue
        try:
            date = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='case').load(single_file).select('decisiondate').map(lambda x: get_decisiondate(x.decisiondate)).collect()[0]

            df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='xmlData').load(single_file)
            cols = get_columns(df.columns)

            columns = df.select(*(i for i in cols))
            parsed_dicts = columns.map(lambda x: x.asDict(recursive=True))
            parsed_words = parsed_dicts.flatMap(lambda x: [parse_elements(x[m]) for m in x])
            words = parsed_words.flatMap(lambda x: x).collect()
        except Exception as e:
            logging.error('get_word_usage: %s %s', single_file, e)
            print "exception:",e
            continue

        word_dict = Counter(words)
        write_words_to_json_files(word_dict, date)

def get_words_for_all_in_dir():
    current_dir = sys.argv[1]
    for root, dirs, files in os.walk(current_dir):
        if len(files):
            global state, region_path
            files = [os.path.join(root, f) for f in files if f != '.DS_Store']
            try:
                state, region_path = parse_path(files[0])
            except IndexError as e:
                continue

            if state in exclude_states: continue

            word_list = get_word_usage(files_array=files)

if __name__ == "__main__":
    get_words_for_all_in_dir()
