import os
import csv
from helpers import *
from tqdm import tqdm
from glob import glob
import time

errors_file = "/ftldata/metadata/errors.txt"
input_root_dir = "/ftldata/harvard-ftl-shared/from_vendor"
metadata_doc_root = "/ftldata/metadata"
metadata_inprogress_dir = "/tmp"
metadata_dump_format = ".csv"

fieldnames = [
    'type', 'caseid', 'firstpage', 'lastpage', 'jurisdiction',
    'citation', 'docketnumber', 'decisiondate',
    'decisiondate_original', 'court', 'name',
    'court_abbreviation', 'name_abbreviation', 'volume',
    'reporter', 'timestamp', 'voldate',
    'publisher','publisher_place', 'reporter_abbreviation',
    'reporter_name', 'volnumber', 'nominativereporter_name',
    'nominativereporter_abbreviation','nominativereporter_volnumber',
    'barcode',]

def safe_pq(filename, pq, parse_method, *args):
    try:
        return normalize_unicode(parse_method(pq, *args))
    except Exception as e:
        print "Error:",e, filename
        pass

def parse_for_metadata(xml_path):
    if "_CASEMETS_" in xml_path:
        return parse_case_for_metadata(xml_path)
    elif "_redacted_METS.xml" in xml_path:
        return parse_vol_for_metadata(xml_path)
    else:
        return

def parse_case_for_metadata(case_xml_path):
    pq = parse_file(case_xml_path)
    if not pq:
        return
    citation = safe_pq(case_xml_path, pq, get_citation)
    cite_parts = citation.split(" ")
    volume, reporter, firstpage = cite_parts[0], " ".join(cite_parts[1:-1]), cite_parts[-1]
    reporter = normalize_unicode(reporter)
    lastpage = safe_pq(case_xml_path, pq, get_last_page_number)
    decisiondate = get_decision_date(pq)
    court = safe_pq(case_xml_path, pq, get_court)
    name = safe_pq(case_xml_path, pq, get_name)
    court_abbreviation = safe_pq(case_xml_path, pq, get_court, True)
    name_abbreviation = safe_pq(case_xml_path, pq, get_name,  True)
    jurisdiction = safe_pq(case_xml_path, pq, get_jurisdiction)
    docketnumber = safe_pq(case_xml_path, pq, get_docketnumber)
    original_decision_date = safe_pq(case_xml_path, pq, get_original_decision_date)
    try:
        timestamp = get_timestamp_if_exists(case_xml_path)
    except Exception as e:
        print 'ERROR (timestamp):', e, case_xml_path
        timestamp = ''
        pass

    return {
        'type': 'case',
        'caseid': get_caseid(pq),
        'firstpage': firstpage,
        'lastpage': lastpage,
        'jurisdiction': jurisdiction,
        'citation': citation,
        'docketnumber': docketnumber,
        'decisiondate': decisiondate.toordinal(),
        'decisiondate_original': original_decision_date,
        'court': court,
        'name': name,
        'court_abbreviation': court_abbreviation,
        'name_abbreviation': name_abbreviation,
        'volume': volume,
        'reporter': reporter,
        'timestamp': timestamp
    }

def parse_vol_for_metadata(vol_xml_path):
    vpq = parse_volmets_file(vol_xml_path)
    if not vpq:
        return
    voldate = get_volume_voldate(vpq) or ''
    publisher = get_volume_publisher(vpq) or ''
    place = get_volume_publisher_place(vpq) or ''
    reporter_abbreviation = get_reporter_abbreviation(vpq) or ''
    reporter_name = get_reporter_name(vpq) or ''
    volnumber = get_volnumber(vpq) or ''
    barcode = get_vol_barcode(vpq) or vol_xml_path.split('_redacted_METS.xml')[0].split('/')[-1]
    nominativereporter_name = get_nominativereporter_name(vpq) or ''
    nominativereporter_volnumber = get_nominativereporter_volnumber(vpq) or ''
    nominativereporter_abbreviation = get_nominativereporter_abbreviation(vpq) or ''
    timestamp = get_file_timestamp(vol_xml_path)

    return {
        'type': 'volume',
        'voldate': voldate,
        'publisher': publisher,
        'publisher_place': place,
        'reporter_abbreviation': reporter_abbreviation,
        'reporter_name': reporter_name,
        'volnumber': volnumber,
        'nominativereporter_name': nominativereporter_name,
        'nominativereporter_abbreviation': nominativereporter_abbreviation,
        'nominativereporter_volnumber': nominativereporter_volnumber,
        'barcode': barcode,
        'timestamp': timestamp,
    }


def write_metadata(input_dir=input_root_dir):
    csv_path = generate_metadata_filepath()
    create_metadata_file(csv_path)
    try:
        with open(csv_path,'a') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            # globbing in two steps to avoid extra memory usage
            [traverse_dir(writer, d) for d in glob("%s/*" % input_dir)]
    finally:
        move_to_root_dir(csv_path)

def open_metadata_writer(metadata_filepath):
    csv_fp = open(metadata_filepath,'a')
    return csv_fp, csv.DictWriter(csv_fp, fieldnames=fieldnames)

def write_metadata_for_file(filename, writer):
    metadata = parse_for_metadata(filename)
    if metadata:
        write_row(writer, filename, metadata)

def write_row(writer, filename, metadata):
    try:
        writer.writerow(metadata)
        # this exception can happen when we get ascii characters I haven't accounted for
    except Exception as e:
        print "Uncaught ERROR:",e, filename
        pass

def traverse_dir(writer, dirname):
    for f in tqdm(glob("%s/*" % dirname)):
        if "_redacted_METS.xml" in f:
            # get volmets
            metadata = parse_for_metadata(f)
            write_row(writer, f, metadata)
        elif "/casemets" in f:
            # get casemets
            for fi in tqdm(glob("%s/*.xml" % f)):
                metadata = parse_for_metadata(fi)
                write_row(writer, fi, metadata)

def traverse_dir_for_volmets(writer, dirname):
    for f in tqdm(glob("%s/*" % dirname)):
        if "_redacted_METS.xml" in f:
            # get volmets
            metadata = parse_for_metadata(f)
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
    return "%s%s/metadata_dump_%s%s" % (metadata_doc_root, metadata_inprogress_dir, timestamp, metadata_dump_format)

def move_to_root_dir(csv_path):
    path_parts = csv_path.split('/')
    new_path = "%s/%s" % (metadata_doc_root, path_parts[-1])
    os.rename(csv_path, new_path)

if __name__ == '__main__':
    write_metadata()
