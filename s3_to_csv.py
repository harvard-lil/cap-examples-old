import os
import csv
from helpers import *
from tqdm import tqdm
from glob import glob
csv_path = "/ftldata/metadata/metadata_dump.csv"
errors_file = "/ftldata/metadata/errors.txt"

fieldnames = [
    'caseid', 'firstpage', 'lastpage', 'jurisdiction',
    'citation', 'docketnumber', 'decisiondate',
    'decisiondate_original', 'court', 'name',
    'court_abbreviation', 'name_abbreviation', 'volume',
    'reporter', 'timestamp']

def write_metadata_to_csv(writer, case_xml_path):
    if not "_CASEMETS_" in case_xml_path:
        return

    pq = parse_file(case_xml_path)
    lastpage = get_last_page_number(pq)
    citation = get_citation(pq)
    cite_parts = citation.split(" ")
    volume, reporter, firstpage = cite_parts[0], " ".join(cite_parts[1:-1]), cite_parts[-1]
    firstpage = int(firstpage)
    decisiondate = get_decision_date(pq)
    fieldnames = [ 'caseid', 'firstpage', 'lastpage', 'jurisdiction', 'citation', 'docketnumber', 'decisiondate', 'decisiondate_original', 'court', 'name', 'court_abbreviation', 'name_abbreviation', 'volume', 'reporter']
    court = get_court(pq)
    name = get_name(pq)
    court_abbreviation = get_court(pq, True)
    name_abbreviation = get_name(pq, True)
    jurisdiction = get_jurisdiction(pq)
    try:
        timestamp = get_timestamp_if_exists(case_xml_path)
    except:
        print 'ERROR: timestamp error:', case_xml_path
        timestamp = ''
        pass

    try:
        writer.writerow({
            'caseid':get_caseid(pq),
            'firstpage': firstpage,
            'lastpage': lastpage,
            'jurisdiction': normalize_unicode(jurisdiction),
            'citation': normalize_unicode(citation),
            'docketnumber': get_docketnumber(pq),
            'decisiondate': decisiondate.toordinal(),
            'decisiondate_original': get_original_decision_date(pq),
            'court': normalize_unicode(court),
            'name': normalize_unicode(name),
            'court_abbreviation': normalize_unicode(court_abbreviation),
            'name_abbreviation': normalize_unicode(name_abbreviation),
            'volume': normalize_unicode(volume),
            'reporter': normalize_unicode(reporter),
            'timestamp': timestamp
        })
    except Exception as e:
        print "ERROR:", e, case_xml_path
        pass

def write_all_existing_files(input_dir):
    if not os.path.isfile(csv_path):
        create_csv_file(csv_path)
    with open(csv_path,'a') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # for root, dirs, files in tqdm(os.walk(input_dir)):\
        [traverse_dir(writer, d) for d in glob('/ftldata/harvard-ftl-shared/from_vendor/*')]

def traverse_dir(writer, dir_name):
    [write_metadata_to_csv(writer, f) for f in tqdm(glob("%s/casemets/*.xml" % dir_name))]

def create_csv_file(cpath):
    with open(cpath,'a') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

def get_timestamp_if_exists(case_xml_path):
    path_parts = case_xml_path.split('/')
    reporter_num, ts = path_parts[-3].split('_redacted')
    return ts

if __name__ == '__main__':
    write_all_existing_files('/ftldata/harvard-ftl-shared/from_vendor')
