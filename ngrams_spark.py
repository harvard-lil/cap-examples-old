import os
import io
import re
import sys
import json
import logging
from tqdm import tqdm
from datetime import datetime
from pyquery import PyQuery
from pyspark.sql import SparkSession, SQLContext
from pyspark.serializers import BatchedSerializer, PickleSerializer
from pyspark import SparkContext
from pyspark.sql.types import Row
from pyspark.ml.feature import NGram


if len(sys.argv) > 1:
    spark = SparkSession.builder.master("local").appName("Word Count").enableHiveSupport().getOrCreate()
    # sc = SparkContext("cap", "ngrams", serializer=PickleSerializer(), batchSize = -1)
    debug      = sys.argv[1]
    input_dir  = sys.argv[2]
    output_dir = sys.argv[3]
    log_file   = sys.argv[4]

else:
    debug = True
    input_dir  = '../research_set/output'
    output_dir = 'ngrams_test'
    log_file   = 'error.log'
    output_dir = '.'

if (debug): logging.basicConfig(format='%(levelname)s:%(message)s', filename=log_file, level=logging.INFO)

word_dirs = [
    '%s/1' % output_dir,
    '%s/2' % output_dir,
    '%s/3' % output_dir
]

alphanumeric = re.compile('[^\w+\s+]')

for w in word_dirs:
    if not os.path.exists(w):
        os.makedirs(w)

class NestedCountSet(object):
     def __init__(self, root):
         self.root = root

     def __call__(self, lookups=[], count=None):
        obj = self.root
        try:
            for idx,key in enumerate(lookups):
                if (idx is len(lookups) - 1) and count:
                    obj[key] = count if not obj.get(key) else obj[key] + count
                else:
                    obj = obj[key]
            return obj

        except KeyError as e:
            obj[key] = {}
            if idx < len(lookups) - 1:
                self.__call__(lookups=lookups, count=count)

            return obj

def write_to_file(word_tuple, date, path, state, region_path, n):
    word, count = word_tuple
    if len(str(word)) < 3:
        return

    n_word_dir = word_dirs[n-1]
    with touch_open("%s/%s.json" % (n_word_dir, word), 'r+') as f:
        try:
            data = json.load(f)
        except Exception as e:
            clear_file(f)
            data = {}
            pass

        ncs = NestedCountSet(data)
        ncs(lookups=[region_path, str(date)], count=count)
        ncs(lookups=[state, 'total_state', str(date)], count=count)
        ncs(lookups=['total_country', str(date)], count=count)
        clear_file(f)
        json.dump(data, f)
    return


def get_ngrams(cases, region_path):
    if (debug): logging.info(region_path)
    for case_path in tqdm(cases):
        parsed = parse_file(case_path)
        text = get_case_text(parsed)
        date = get_decision_date(parsed).year
        state = parsed("case|court").attr('jurisdiction').strip()
        text = text.encode("ascii", "ignore")
        clean_word_list = alphanumeric.sub('', text).lower().split()
        text_df = spark.createDataFrame([Row(inputTokens=clean_word_list)])
        for n in range(1,4):
            if n==1:
                ngrams = clean_word_list
            else:
                ngram_prepared = NGram(n=n, inputCol="inputTokens", outputCol="nGrams")
                ngrams = ngram_prepared.transform(text_df).head().nGrams
            sc.parallelize(ngrams).map(lambda word: (word,1)).reduceByKey(lambda v1,v2: v1 +  v2).map(lambda word_tuple: write_to_file(word_tuple, date, case_path, state, region_path, n=n)).collect()

def generate_ngrams_for_dir():
    for root, dirs, files in os.walk(input_dir):
        if len(files):
            files = [os.path.join(root, f) for f in files if f != '.DS_Store']
            if not len(files):
                continue
            region_path = root.lstrip(input_dir)
            word_list = get_ngrams(cases=files, region_path=region_path, )

if __name__ == "__main__":
    generate_ngrams_for_dir()
