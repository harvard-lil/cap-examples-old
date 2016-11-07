import os
import csv
from helpers import *

csv_path = "/ftldata/metadata/metadata_dump.csv"


def write_metadata_to_csv(case_xml_path):
    if not "_CASEMETS_" in case_xml_path:
        return

    pq = parse_file(case_xml_path)
    firstpage, lastpage = get_page_nums(pq)
    citation = get_citation(pq)
    cite_parts = citation.split(" ")
    # TODO: figure out why pages don't always write ^
    volume, reporter, firstpage = cite_parts[0], " ".join(cite_parts[1:-1]), cite_parts[-1]
    firstpage = int(firstpage)
    decisiondate = get_decision_date(pq)
    fieldnames = [ 'caseid', 'firstpage', 'lastpage', 'jurisdiction', 'citation', 'docketnumber', 'decisiondate', 'decisiondate_original', 'court', 'name', 'court_abbreviation', 'name_abbreviation', 'volume', 'reporter']
    court = get_court(pq)
    name = get_name(pq)
    court_abbreviation = get_court(pq, True)
    name_abbreviation = get_name(pq, True)
    jurisdiction = get_jurisdiction(pq)

    # create file + write headers if file does not exist
    if not os.path.isfile(csv_path):
        with open(csv_path,'a') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
    try:
        with open(csv_path,'a') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
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
            })
    except UnicodeEncodeError as e:
        errors_file = 'errors.txt'
        with open(errors_file,'a') as ef:
            ef.write("%s\n" % case_xml_path)
        pass

def write_all_existing_files(input_dir):
    for root, dirs, files in os.walk(input_dir):
        if len(files):
            map(lambda f: write_metadata_to_csv(os.path.join(root, f)), files)
