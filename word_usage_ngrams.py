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
logging.basicConfig(format='%(levelname)s:%(message)s', filename="word_usage.log",  level=logging.DEBUG)
date = 0
state = ''
error_file = "error_file.txt"
stopwords = [
    'get','what','who',
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
    print '==========================getting result==============================='
    print result
    return result

def get_words_for_all_in_dir(sqlContext, sc, current_dir='.'):
    for root, dirs, files in os.walk(current_dir):
        if len(files):
            files = [os.path.join(root, f) for f in files]
            state = root.split('/')[3]
            logging.info("WORD_USAGE_LOG %s", state)
            get_word_usage(sqlContext, sc, files_array=files, state=state)

def get_path(x, path):
    res = x
    ps = path.split('.')
    logging.info('WORD_USAGE_LOG get_path %s %s', ps, res)
    for p in ps:
        try:
            p = int(p)
        except:
            pass
        try:
            res = res[p]
        except Exception as e:
            logging.error('WORD_USAGE_LOG get_path catching error: %s', e)
            return None
    return res

def get_decisiondate(x):
    print "get_decisiondate", x
    print x.select('decisiondate')
    date_string = x.select('decisiondate').collect()[0][0]['#VALUE']
    possible_dates = re.findall(r'\d+',date_string)
    now = datetime.datetime.now()
    for date in possible_dates:
        date = int(date)
        if date > 1699 and date < now.year:
            return date


def get_casebody(x):
    return get_path(x, 'FContent.xmlData.casebody')

def get_text(x, area_of_text):
    arr = []
    try:
        text = x.select(area_of_text).collect()[0]
        print "get_text:", text, area_of_text
        for line in text:
            print "get_text line:", line
            for l in line:
                print "get_text l:", l
                arr.append(l)
        return arr
    except Exception as e:
        print "get_text Exception",e
        return

def full_casebody(x):
    res = get_casebody(x)
    try:
        return res.opinion
    except AttributeError as e:
        logging.error('WORD_USAGE_LOG full_casebody: %s', e)
        return None

def parse_elements(x, date, state):
    print "gettign x in parse_elements", x
    arr = []
    for element in x:
        logging.info('WORD_USAGE_LOG parse_elements: element %s type %s', element, type(element))
        print "parse_elements", element
        if not element:
            continue
        if (type(element) == list):
            for n in element:
                arr.append(split_string(n[0]))
        elif (type(element) == str):
            arr.append(split_string(element))
        elif type(element) == unicode:
            element = unicodedata.normalize('NFKD', element).encode('ascii','ignore')
            arr.append(split_string(element))
        elif type(element) == int:
            continue
        else:
            arr.append(split_string(element['#VALUE']))
    return arr

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
            print "preparing word",word
            return word

def get_casetext_step_two(x):
    arr = []
    if not x:
        return
    print "get_casetext_step_two", x
    arr.append(full_casebody(x).asDict())
    return arr

def write_word_to_json_file(word, date, state=''):
    word_filename = "./word_output/%s.txt" % word
    date = str(date)

    def touch_open(filename, *args):
        fd = os.open(filename, os.O_RDWR | os.O_CREAT)
        return os.fdopen(fd, *args)

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
            data[state] = { date: 1 }
        else:
            data[state][date] = 1 if not data[state].get(date) else data[state][date] + 1
        f.seek(0)
        f.truncate()
        json.dump(data, f)
    return

def get_word_usage(sqlContext, sc, files_array=[], state=''):
    output_file = "%s_output.txt" % state

    for single_file in files_array:
        if single_file[-4:] != '.xml':
            continue
        logging.info('parsing file: %s', single_file)

        df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='casebody').load(single_file)
        date = get_decisiondate(df)
        text = get_text(df, 'p')
        blockquote_text = get_text(df, 'blockquote')
        text.append(blockquote_text)
        arr = parse_elements(text, date=date, state=state)
        print arr
