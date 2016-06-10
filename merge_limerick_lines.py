import json
import os
from collections import defaultdict
from glob import glob

from tqdm import tqdm

from helpers import makedirs

source_dir = "/ftldata/limerick_lines"
dest_dir = "/ftldata/limerick_lines_merged"


def merge_limerick_lines():
    merged = defaultdict(  # line_type
        lambda: defaultdict(  # emphasis_pattern
            lambda: defaultdict(  # last_syllable
                lambda: defaultdict(  # last_token
                    list  # sentence
                ))))

    for path in tqdm(glob(os.path.join(source_dir, "*"))):
        line_types = json.load(open(path))
        for line_type, emphasis_patterns in line_types.iteritems():
            for emphasis_pattern, last_syllables in emphasis_patterns.iteritems():
                for last_syllable, last_tokens in last_syllables.iteritems():
                    for last_token, lines in last_tokens.iteritems():
                        merged[line_type][emphasis_pattern][last_syllable][last_token.lower()].extend(lines)

    filtered = defaultdict(  # line_type
        lambda: defaultdict(  # emphasis_pattern
            lambda: defaultdict(  # last_syllable
                lambda: dict)))

    for line_type, emphasis_patterns in merged.iteritems():
        for emphasis_pattern, last_syllables in emphasis_patterns.iteritems():

            # skip long lines that don't include '1**1**1'
            if line_type=='long' and '1**1**1' not in emphasis_pattern:  # len(emphasis_pattern)<5:
                continue

            # skip short lines that are too short
            if line_type=='short' and len(emphasis_pattern)<4:
                continue

            for last_syllable, last_tokens in last_syllables.iteritems():

                # skip groups with insufficient options
                if (line_type=='long' and len(last_tokens)<3) or (line_type=='short' and len(last_tokens)<2):
                    continue

                filtered[line_type][emphasis_pattern][last_syllable] = last_tokens

    makedirs(dest_dir)
    json.dump(filtered, open(os.path.join(dest_dir, 'limerick_lines.json'), 'w'))

if __name__ == "__main__":
    merge_limerick_lines()
