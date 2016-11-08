import os
import csv
from helpers import *
from tqdm import tqdm
from glob import glob
import time

errors_file = "/ftldata/metadata/errors.txt"
input_root_dir = "/ftldata/harvard-ftl-shared/from_vendor"
metadata_doc_root = "/ftldata/metadata"
metadata_dump_format = ".csv"

fieldnames = [
    'caseid', 'firstpage', 'lastpage', 'jurisdiction',
    'citation', 'docketnumber', 'decisiondate',
    'decisiondate_original', 'court', 'name',
    'court_abbreviation', 'name_abbreviation', 'volume',
    'reporter', 'timestamp']

def parse_for_metadata(writer, case_xml_path):
    if not "_CASEMETS_" in case_xml_path:
        return

    pq = parse_file(case_xml_path)

    citation = normalize_unicode(get_citation(pq))
    cite_parts = citation.split(" ")
    volume, reporter, firstpage = cite_parts[0], " ".join(cite_parts[1:-1]), cite_parts[-1]
    firstpage = int(firstpage)
    lastpage = normalize_unicode(get_last_page_number(pq))
    decisiondate = get_decision_date(pq)
    court = get_court(pq)
    name = get_name(pq)
    court_abbreviation = get_court(pq, True)
    name_abbreviation = get_name(pq, True)
    jurisdiction = get_jurisdiction(pq)

    try:
        timestamp = get_timestamp_if_exists(case_xml_path)
    except Exception as e:
        print 'ERROR (timestamp):', e, case_xml_path
        timestamp = ''
        pass

    return {
        'caseid': get_caseid(pq),
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
    }

def write_metadata(input_dir=input_root_dir):
    csv_path = generate_metadata_filepath()
    create_metadata_file(csv_path)
    with open(csv_path,'a') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # globbing in two steps to avoid extra memory usage
        [traverse_dir(writer, d) for d in glob("%s/*" % input_dir)]

def write_metadata_for_file(metadata_filepath, filename):
    with open(metadata_filepath,'a') as metadata_file:
        writer = csv.DictWriter(metadata_file, fieldnames=fieldnames)
        metadata = parse_for_metadata(writer, filename)
        write_row(writer, filename, metadata)

def write_row(writer, filename, metadata):
    try:
        writer.writerow(metadata)
        # this exception can happen when we get ascii characters I haven't accounted for
    except Exception as e:
        print "Uncaught ERROR:",e, filename
        pass

def traverse_dir(writer, dir_name):
    for f in tqdm(glob("%s/casemets/*.xml" % dir_name)):
        metadata = parse_for_metadata(writer, f)
        write_row(writer, f, metadata)

def create_metadata_file(cpath):
    if not os.path.isfile(cpath):
        with open(cpath,'a') as new_file:
            writer = csv.DictWriter(new_file, fieldnames=fieldnames)
            writer.writeheader()

def get_timestamp_if_exists(case_xml_path):
    path_parts = case_xml_path.split('/')
    reporter_num, ts = path_parts[-3].split('_redacted')
    return ts

def generate_metadata_filepath(metadata_doc_root=metadata_doc_root, metadata_dump_format=metadata_dump_format):
    timestamp = int(time.time())
    return "%s/metadata_dump_%s%s" % (metadata_doc_root, timestamp, metadata_dump_format)

if __name__ == '__main__':
    write_metadata()
