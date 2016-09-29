import csv
import json
import os
import re
from glob import glob
from reporters_db import REPORTERS

from tqdm import tqdm

from helpers import makedirs

# paths
source_dir = "/ftldata/reporters"
dest_dir = "/ftldata/reporters_aggregated"

strip_chars = re.compile(r'[^A-Za-z0-9]')

def cite_to_key(cite):
    return strip_chars.sub('', cite)

def aggregate_reporters():
    makedirs(dest_dir)
    aggregate = {}

    # get map of reporter key to canonical name in FLP db
    flp_keys = {}
    for reporter_list in REPORTERS.itervalues():
        for reporter in reporter_list:
            fields = [reporter['cite_type'], reporter['name']]
            for k in reporter["editions"].keys():
                flp_keys[cite_to_key(k)] = fields+[k]
            for k, v in reporter["variations"].items():
                flp_keys[cite_to_key(k)] = fields+[v]

    # get map of reporter key to name in Juris-M db
    juris_keys = {}
    for json_file, label in [['../lib/jurism-abbreviations/primary-us.json', 'primary'], ['../lib/jurism-abbreviations/secondary-us-bluebook.json', 'secondary']]:
        data = json.load(open(os.path.join(os.path.dirname(__file__), json_file)))
        for juris in data["xdata"].itervalues():
            for full_name, short_name in juris["container-title"].iteritems():
                key = cite_to_key(short_name)
                if key not in juris_keys:
                    juris_keys[key] = [label, short_name, full_name]

    # get map of reporter key to CAP reporter
    cap_keys = {}
    for reporter in json.load(open(os.path.join(os.path.dirname(__file__), '../lib/reporter-list/reporters.json'))):
        key = cite_to_key(reporter['short'])
        if key not in cap_keys:
            cap_keys[key] = [reporter['reporter'], reporter['short']]

    # aggregate rows in our collected citations
    for csv_path in tqdm(sorted(glob(os.path.join(source_dir, "*.csv")))):
        csvreader = csv.DictReader(open(csv_path))
        for row in csvreader:
            key = cite_to_key(row['Series'])
            if key in aggregate:
                aggregate[key]['Count'] += int(row['Count'])
            else:
                row['Examples'] = ['','','']
                row['Count'] = int(row['Count'])
                row['Series'] = key
                row['FLP'] = flp_keys.get(key, ['','',''])
                row['juris'] = juris_keys.get(key, ['','',''])
                row['CAP'] = cap_keys.get(key, ['',''])

                aggregate[key] = row

            aggregate[key]['Examples'] = [row['Example %s' %i] for i in [1,2,3] if row.get('Example %s' %i)] + aggregate[key]['Examples']

    # write to CSV
    out = [[k, v['Count']]+v['Examples'][:3]+v['CAP']+v['FLP']+v['juris'] for k, v in aggregate.iteritems() if v['Count'] >= 100]
    out.sort(key=lambda x: x[1], reverse=True)
    with open(os.path.join(dest_dir, 'aggregate.csv'), 'wb') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['Series', 'Count', 'Example 1', 'Example 2', 'Example 3', 'CAP Cite', 'CAP Full', 'FLP Type', 'FLP Name', 'FLP Cite', 'Juris-M Type', 'Juris-M Cite', 'Juris-M Full',])
        for row in out:
            csvwriter.writerow([unicode(s).encode("utf-8") for s in row])

if __name__ == "__main__":
    aggregate_reporters()