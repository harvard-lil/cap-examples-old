import os
import re
from glob import glob

from pyquery import PyQuery

from helpers import parse_file, get_decision_date, qn, namespaces, makedirs

source_dir = "/ftldata/research_set"
raw_source_dir = "/ftldata/harvard-ftl-shared/from_vendor/"
dest_dir = "/ftldata/reporters_notes"

def search_front_matter():
    for jurisdiction_path in sorted(glob(os.path.join(source_dir, "*"))):
        makedirs(jurisdiction_path.replace(source_dir, dest_dir))
        for series_path in glob(os.path.join(jurisdiction_path, "*")):
            print series_path
            try:
                out = u""
                for volume_path in sorted(glob(os.path.join(series_path, "*")), key=lambda x: int(x.rsplit('/',1)[1])):

                    # load first case in volume
                    case_paths = sorted(glob(os.path.join(volume_path, "*.xml")))
                    if not case_paths:
                        continue
                    first_case_path = case_paths[0]
                    pq = parse_file(first_case_path)

                    # stop processing volume after 1923
                    year = get_decision_date(pq).year
                    if year > 1923:
                        break

                    # get first alto file for first case
                    first_case_alto_file = pq('METS|fileGrp[USE="alto"] METS|FLocat')[0].attrib[qn("xlink|href")][3:]
                    first_case_alto_name = os.path.basename(first_case_alto_file)

                    # get directory for alto files for volume
                    case_id = pq("case|case").attr('caseid')
                    alto_dir = os.path.dirname(os.path.join(raw_source_dir, case_id, first_case_alto_file)).replace('_0001', '_redacted')

                    # process alto files until we hit the one for the first case in the volume
                    for alto_path in sorted(glob(os.path.join(alto_dir, "*"))):
                        if alto_path.endswith(first_case_alto_name):
                            break

                        # only bother parsing XML if we find 'reporter' in the text of the alto file somewhere
                        alto_data = open(alto_path).read()
                        if 'reporter' not in alto_data.lower():
                            continue
                        alto_pq = PyQuery(alto_data, parser='xml', namespaces=namespaces)

                        # extract OCR'd text from alto XML
                        alto_text = " ".join(x.attrib["CONTENT"] for x in alto_pq('alto|String'))

                        # if page has more than fifty lowercase words, less than 15 uppercase words (usually a list of judges),
                        # and less than 30 periods (usually a table of contents), print citation and page text
                        if len(re.findall(r'\b[a-z]+\b', alto_text))>50 and len(re.findall(r'\b[A-Z][A-Z]+\b', alto_text))<15 and len(re.findall(r'\.', alto_text))<30:
                            volume_cite = pq('case|citation[category="official"]').text().rsplit(" ",1)[0]
                            out += "%s\n%s\n%s\n\n" % (alto_path, volume_cite, alto_text)

                # write out all matched pages for series
                if out:
                    open((series_path.replace(source_dir, dest_dir) + ".txt").replace('..', '.'), "w").write(out.encode('utf8'))

            except Exception as e:
                print "Skipping -- %s" % e

if __name__ == "__main__":
    search_front_matter()
