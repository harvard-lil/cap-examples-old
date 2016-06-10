import json
import os
from collections import defaultdict
from glob import glob

import nltk
from tqdm import tqdm

from helpers import parse_file, makedirs, get_case_text, get_decision_date

source_dir = "/ftldata/research_set"
dest_dir = "/ftldata/ngrams"

ignore_words = {
    ',', '.', '&', ';', ':', '?', '!', '``', "''",
    u'\xa7',  # section symbol
    u'\u2014'  # m-dash
}
unicode_translate_table = dict((ord(a), ord(b)) for a, b in zip(u'\u201c\u201d\u2018\u2019', u'""\'\''))

def tokenize_text(text):
    return (w for w in nltk.word_tokenize(
                                          text.translate(unicode_translate_table) \
                                              .replace(u"\u2014", u" \u2014 ")  # add spaces around m-dashes
    ) if w not in ignore_words)

def ngram_volume(volume_path):
    counts = defaultdict(  # year
        lambda: defaultdict(  # ngram_len
            lambda: defaultdict(  # ngram
                int  # count
            )))

    for case_xml_path in glob(os.path.join(volume_path, '*.xml')):
        pq = parse_file(case_xml_path)
        tokens = tokenize_text(get_case_text(pq))
        history = []
        case_year = get_decision_date(pq).year
        for i, item in enumerate(tokens):
            history.append(item)
            for ngram_len in [1,2,3]:
                if len(history) >= ngram_len:
                    counts[case_year][ngram_len]["\t".join(history[-ngram_len:])] += 1
            if i >= 2:
                del history[0]

    for year, ngram_lens in counts.iteritems():
        out_dir = os.path.join(volume_path.replace(source_dir, dest_dir, 1), str(year))
        makedirs(out_dir)
        for ngram_len, data in ngram_lens.items():
            with open(os.path.join(out_dir, "%s.json" % ngram_len), 'w') as out:
                json.dump(data, out)

def ngram_all_volumes():
    for volume_path in tqdm(sorted(glob(os.path.join(source_dir, "*/*/*")))):
        ngram_volume(volume_path)

if __name__ == "__main__":
    ngram_all_volumes()
