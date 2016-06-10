import json
import os
from collections import defaultdict
from glob import glob

from tqdm import tqdm

from helpers import makedirs

source_dir = "/ftldata/ngrams"
dest_dir = "/ftldata/ngrams_summarized"

def save_counts(counts, jurisdiction_path, subdir):
    out_dir = os.path.join(jurisdiction_path.replace(source_dir, dest_dir, 1), str(subdir))
    makedirs(out_dir)
    for ngram_length, data in counts.iteritems():
        with open(os.path.join(out_dir, "%s.json" % ngram_length), 'w') as out:
            json.dump(data, out)

def ngram_all_volumes():
    for jurisdiction_path in glob(os.path.join(source_dir, "*")):
        print "Combining", jurisdiction_path
        year_dirs = list(glob(os.path.join(jurisdiction_path, "*/*/*")))
        years = set([int(os.path.basename(year_dir)) for year_dir in year_dirs])

        total_counts = defaultdict(  # ngram_length
            lambda: defaultdict(  # ngram
                int  # count
            ))

        for year in tqdm(years):

            counts = defaultdict(  # ngram_length
                lambda: defaultdict(  # ngram
                    int  # count
                ))

            for ngram_path in sorted(glob(os.path.join(jurisdiction_path, "*/*/%s/*.json" % year))):
                ngram_length = int(ngram_path.rsplit('/', 1)[1].split('.', 1)[0])

                # TEMP: Only count 1-grams because we run out of RAM otherwise
                if ngram_length > 1:
                    continue

                ngrams = json.load(open(ngram_path))
                for ngram, count in ngrams.iteritems():
                    counts[ngram_length][ngram] += count
                    total_counts[ngram_length][ngram] += count

            save_counts(counts, jurisdiction_path, year)

        save_counts(total_counts, jurisdiction_path, 'totals')

if __name__ == "__main__":
    ngram_all_volumes()
