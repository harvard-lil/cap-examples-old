import json
import os
import re
from cityhash import CityHash64
from collections import defaultdict
from glob import glob

import nltk
from tqdm import tqdm

from helpers import get_case_text, parse_file, makedirs
from volume_ngrams import tokenize_text

pronunciations = nltk.corpus.cmudict.dict()
long_line_re = re.compile(r'^(.{0,2}?)1(.{0,2}?)1(.{0,2}?)1(.{0,2}?)$')
short_line_re = re.compile(r'^(.{0,2}?)1(.{0,2}?)1$')

source_dir = "/ftldata/research_set"
dest_dir = "/ftldata/limerick_lines"

def limerick_jurisdiction(jurisdiction_path):
    sentence_count = 0
    lines = defaultdict(  # line_type
        lambda: defaultdict(  # emphasis_pattern
            lambda: defaultdict(  # last_syllable
                lambda: defaultdict(  # last_token
                    list  # sentence
            ))))

    sentence_lookup = set()

    def write_lines():
        out_path = jurisdiction_path.replace(source_dir, dest_dir, 1).rstrip('/')+'.json'
        makedirs(os.path.dirname(out_path))
        with open(out_path, 'w') as out:
            json.dump(lines, out)

    case_xml_paths = glob(os.path.join(jurisdiction_path, '*/*/*.xml'))
    for file_count, case_xml_path in tqdm(enumerate(case_xml_paths)):
        pq = parse_file(case_xml_path)
        sentences = nltk.sent_tokenize(get_case_text(pq))
        for sentence in sentences:
            sentence_count += 1

            # make sure we only check each sentence once
            sentence_hash = CityHash64(sentence)
            if sentence_hash in sentence_lookup:
                continue
            sentence_lookup.add(sentence_hash)

            tokens = list(tokenize_text(sentence))
            syllables = []
            for token in tokens:
                token = token.lower()
                if token not in pronunciations:
                    break
                syllables += pronunciations[token][0]

            else:
                # no unknown words found -- we can continue
                emphasis = u''.join(s for s in u"".join(syllables) if s.isdigit())
                line_type = None
                m = long_line_re.match(emphasis)
                if m:
                    line_type = 'long'
                else:
                    m = short_line_re.match(emphasis)
                    if m:
                        line_type = 'short'

                if line_type:
                    emphasis_pattern = u"1".join("*" * len(g) for g in m.groups())
                    if line_type == 'short':
                        emphasis_pattern += u'1'
                    last_token = tokens[-1].lower()
                    last_syllable = None
                    for i in reversed(range(len(syllables))):
                        if syllables[i][-1].isdigit():
                            last_syllable = u"".join(syllables[i:])
                            break

                    if last_syllable:
                        lines[line_type][emphasis_pattern][last_syllable][last_token].append(sentence.encode('utf8'))

        if not (file_count % 1000):
            print "Writing results so far."
            write_lines()

    write_lines()

def limerick_all_jurisdictions():
    for jurisdiction_path in sorted(glob(os.path.join(source_dir, "*"))):
        print jurisdiction_path
        limerick_jurisdiction(jurisdiction_path)

if __name__ == "__main__":
    limerick_all_jurisdictions()
