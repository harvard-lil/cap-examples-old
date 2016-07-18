import os
import json
from pyquery import PyQuery
from datetime import datetime
from collections import defaultdict

"""
dmdSec: descriptive metadata section
amdSec: administrative metadata section
"""

# This XML is a sub-section of the larger "{http://www.loc.gov/METS}mets" format, but
# useful for seeing how the case metadata is organized
SAMPLE_CASE_XML = """
<case xmlns="http://nrs.harvard.edu/urn-3:HLS.Libr.US_Case_Law.Schema.Case:v1" caseid="32044132287095_0084" publicationstatus="published">
  <court abbreviation="Cal. Ct. App." jurisdiction="California">Court of Appeal of the State of California</court>
  <name abbreviation="People v. G.H.">THE PEOPLE, Plaintiff and Respondent, v. G.H., Defendant and Appellant</name>
  <docketnumber>No. E059718</docketnumber>
  <citation category="official" type="bluebook">230 Cal. App. 4th 1548</citation>
  <decisiondate>2014-10-30</decisiondate>
</case>
"""


### stuff for loading and parsing case xml ###

namespaces = {
    'METS': 'http://www.loc.gov/METS/',
    'case': 'http://nrs.harvard.edu/urn-3:HLS.Libr.US_Case_Law.Schema.Case:v1',
    'casebody': 'http://nrs.harvard.edu/urn-3:HLS.Libr.US_Case_Law.Schema.Case_Body:v1',
    'volume': 'http://nrs.harvard.edu/urn-3:HLS.Libr.US_Case_Law.Schema.Volume:v1',
}

def parse_file(path):
    return PyQuery(filename=path, parser='xml', namespaces=namespaces)

def get_case_files(case_root):
    for dir_name, subdir_list, file_list in os.walk(case_root):
        for file_name in file_list:
            if os.path.splitext(file_name)[1] == '.xml':
                yield os.path.join(dir_name, file_name)

def make_single_pq(paths):
    pq = PyQuery('<files/>', namespaces=namespaces)
    for path in paths:
        pq.extend(parse_file(path))
    return pq

def parse_files(paths):
    for path in paths:
        yield parse_file(path)

def get_case_text(case):
    # strip labels from footnotes:
    if type(case) == str and case[-4:] == '.xml':
        case = parse_file(case)
    for footnote in case('casebody|footnote'):
        label = footnote.attrib.get('label')
        if label and footnote[0].text.startswith(label):
            footnote[0].text = footnote[0].text[len(label):]

    text = case('casebody|p').text()

    # strip soft hyphens from line endings
    text = text.replace(u'\xad', '')

    return text

def get_decision_date(pq):
    decision_date_text = pq('case|decisiondate').text()
    try:
        return datetime.strptime(decision_date_text, '%Y-%m-%d')
    except ValueError:
        try:
            return datetime.strptime(decision_date_text, '%Y-%m')
        except ValueError:
            return datetime.strptime(decision_date_text, '%Y')

def makedirs(path):
    try:
        os.makedirs(path)
    except OSError:
        pass
