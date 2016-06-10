import json
import os
import random
import re
from itertools import permutations, combinations

from math import factorial
from tqdm import tqdm

source_dir = "/ftldata/limerick_lines_merged"


def dict_random_sample(population, k):
    keys = random.sample(population.keys(), k)
    return [population[key] for key in keys]

def dict_random_choice(population):
    key = random.choice(population.keys())
    return population[key]

def multiply_list(l):
    p = l[0]
    for i in l[1:]:
        p *= i
    return p

def format_line(line):
    quote_pairs = [[u'\u201c',u'\u201d'], [u'\u2018',u'\u2019'], [u' "', u'" ']]
    for start_quote, end_quote in quote_pairs:
        # strip quotes pointing in wrong way at end
        line = re.sub(u'^\s*%s\s*' % end_quote, '', line)
        line = re.sub(u'\s*%s\s*$' % start_quote, '', line)

        # add matching quotes to quotes in middle
        line = re.sub(u'^([^%s]+%s)' % (start_quote, end_quote), u'%s\\1' % start_quote, line)
        line = re.sub(u'(%s[^%s]+$)' % (start_quote, end_quote), u'\\1%s' % end_quote, line)

        # strip spaces on wrong side of quotes
        line = re.sub(u'\s+(%s)' % end_quote, r'\1', line)
        line = re.sub(u'(%s)\s+' % start_quote, r'\1', line)

        # clean up start and end
        line = re.sub(r'\s+(\W+)$', r'\1', line)
        line = line.capitalize()

    return line

def get_lines(emphasis_patterns, count):
    last_syllables = dict_random_choice(emphasis_patterns)
    last_tokens = dict_random_choice(last_syllables)
    line_sets = dict_random_sample(last_tokens, count)
    lines = [format_line(random.choice(line_set)) for line_set in line_sets]
    return lines

def generate_limerick():
    line_types = json.load(open(os.path.join(source_dir, "limerick_lines.json")))

    long_lines = get_lines(line_types['long'], 3)
    short_lines = get_lines(line_types['short'], 2)

    print "\n".join(long_lines[:2]+short_lines+long_lines[2:])

def count_possible_limericks():
    line_types = json.load(open(os.path.join(source_dir, "limerick_lines.json")))

    line_options = {'long': 0, 'short': 0}
    for line_type, emphasis_patterns in line_types.iteritems():
        line_count = 3 if line_type == 'long' else 2
        line_count_factorial = factorial(line_count)
        for emphasis_pattern in tqdm(emphasis_patterns.itervalues()):
            for last_syllable in emphasis_pattern.itervalues():
                for len_combination in combinations([len(i) for i in last_syllable.values()], line_count):
                    line_options[line_type] += multiply_list(len_combination)*line_count_factorial

    print line_options


if __name__ == "__main__":
    generate_limerick()
