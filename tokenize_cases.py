import os
from glob import glob

import nltk
from tqdm import tqdm

from helpers import parse_file, makedirs, get_case_text

source_dir = "/ftldata/research_set"
dest_dir = "/ftldata/tokenized"

def tokenize_case(case_xml_path):
    out_path = case_xml_path.replace(source_dir, dest_dir, 1).replace('.xml', '.txt')
    if os.path.exists(out_path):
        return
    pq = parse_file(case_xml_path)
    case_text = get_case_text(pq)
    tokens = nltk.word_tokenize(case_text)
    makedirs(os.path.dirname(out_path))
    with open(out_path, 'w') as out:
        out.write(u"\n".join(tokens).encode("utf8"))

def tokenize_all_cases():
    for case_xml_path in tqdm(sorted(glob(os.path.join(source_dir, "*/*/*/*.xml")))):
        tokenize_case(case_xml_path)

if __name__ == "__main__":
    tokenize_all_cases()
