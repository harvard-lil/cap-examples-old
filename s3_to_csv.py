import csv
import boto3
from helpers import *
csv_file = "temp_case_metadata.csv"

def write_metadata_to_csv(case_xml_path):
    pq = parse_file(case_xml_path)
    firstpage, lastpage = get_page_nums(pq)
    citation = get_citation(pq)
    cite_parts = citation.split(" ")
    volume, reporter = cite_parts[0], " ".join(cite_parts[1:-1])

    fieldnames = [ 'caseid', 'firstpage', 'lastpage', 'jurisdiction', 'citation', 'docketnumber', 'decisiondate', 'court', 'name', 'court_abbreviation', 'name_abbreviation', 'volume', 'reporter']
    with open(csv_file,'a') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow({
            'caseid':get_caseid(pq),
            'firstpage':firstpage,
            'lastpage':lastpage,
            'jurisdiction': get_jurisdiction(pq),
            'citation': citation,
            'docketnumber': get_docketnumber(pq),
            'decisiondate':get_decision_date(pq),
            'court': get_court(pq),
            'name': get_name(pq),
            'court_abbreviation': get_court(pq, True),
            'name_abbreviation': get_name(pq, True),
            'volume': volume,
            'reporter':reporter,
        })
