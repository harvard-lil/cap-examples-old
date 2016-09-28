import csv
import re
import os
import sys
from collections import defaultdict
from glob import glob
from reporters_db import REPORTERS

from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import parse_file, get_case_text, makedirs

# paths
source_dir = "/ftldata/research_set"
dest_dir = "/ftldata/reporters"

# regex setup
word_chars = r'[A-Za-z0-9\(\)\'\.\&\-\,]*'
first_word = r'[A-Z]{word_chars}'.format(word_chars=word_chars)
one_word = r'(?:[A-Z0-9\(]{word_chars}|\&)'.format(word_chars=word_chars)
reporter_match = r'{first_word}(?:\s+{one_word}){{,5}}?'.format(first_word=first_word, one_word=one_word)
cite_match = re.compile(r'\b(\d{{1,4}})\s+({reporter_match})\s+(\d{{1,4}})\b'.format(reporter_match=reporter_match))


def test_reporter_match(reporter_match):
    """
        Print any reporter name from the CourtListener database that isn't matched.
        This makes sure that our regular expression is flexible enough to find unknown citations.
    """
    def all_series():
        for reporter_list in REPORTERS.itervalues():
            for reporter in reporter_list:
                for k in reporter["editions"].keys() + reporter["variations"].keys():
                    yield k

    for series in all_series():
        if not re.match(reporter_match, series):
            print series

def search_volumes():
    makedirs(dest_dir)
    for series_path in tqdm(sorted(glob(os.path.join(source_dir, "*/*")))):
        series_name = os.path.basename(series_path)
        known_series = defaultdict(
            lambda: {
                'count': 0, 'examples': []
            }
        )
        for volume_path in sorted(glob(os.path.join(series_path, "*"))):
            for case_xml_path in glob(os.path.join(volume_path, '*.xml')):
                pq = parse_file(case_xml_path)
                text = get_case_text(pq)
                cites = cite_match.findall(text)
                for series in cites:
                    ks = known_series[series[1]]
                    ks['count'] += 1
                    if len(ks['examples']) < 3:
                        ks['examples'].append(" ".join(series))

        # write to CSV
        out = [[k, v['count']]+v['examples'] for k, v in known_series.iteritems()]
        out.sort(key=lambda x: x[1], reverse=True)
        with open(os.path.join(dest_dir, '%s.csv' % series_name), 'wb') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['Series', 'Count', 'Example 1', 'Example 2', 'Example 3'])
            for row in out:
                csvwriter.writerow(row)

if __name__ == "__main__":
    search_volumes()