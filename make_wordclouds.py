import csv
import json
import os
import re
from collections import defaultdict
from glob import glob

from tqdm import tqdm
from wordcloud import WordCloud

from helpers import makedirs

source_dir = "/ftldata/ngrams_summarized"
dest_dir = "/ftldata/wordclouds"
top_words_dir = "/ftldata/top_words"

lowercase_re = re.compile(r'^[a-z][a-z\-\']*$')
stop_words = set("""
    subd affd finds pursuant subdivision supra available noted your supreme thing code concurred chap post ante brief
    merit alia allegedly alleging asserted hereinafter complainant complainants claimant claimants paragraph insofar
    herein aforesaid
""".strip().split())

def process_word_dict(word_dict):
    out = []
    for word, freq in word_dict.iteritems():
        if len(word)>3 and lowercase_re.match(word) and word not in stop_words:
            out.append((word, freq))
    out.sort(key=lambda x: x[1], reverse=True)
    return out[:1000]

def save_wordcloud(freqs, path):
    wordcloud = WordCloud(width=800, height=800, background_color="white", random_state=0)
    wordcloud.generate_from_frequencies(freqs)
    image = wordcloud.to_image()
    image.save(path)

def save_top_list(headers, data, path):
    with open(path, 'wb') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headers)
        for row in data:
            csvwriter.writerow(row)

def wordcloud_all_jurisdictions():

    for jurisdiction_path in glob(os.path.join(source_dir, "Illinois")):
        print "Processing", jurisdiction_path
        out_dir = jurisdiction_path.replace(source_dir, dest_dir, 1)
        top_words_out_dir = jurisdiction_path.replace(source_dir, top_words_dir, 1)
        makedirs(out_dir)
        makedirs(top_words_out_dir)

        # write global
        global_freqs = process_word_dict(json.load(open(os.path.join(jurisdiction_path, 'totals/1.json'))))
        save_wordcloud(global_freqs, os.path.join(out_dir, 'totals.png'))

        # load year data
        bare_top_words_by_year = {}
        for year_path in tqdm(glob(os.path.join(jurisdiction_path, "*"))):
            year = os.path.basename(year_path)
            if year == 'totals':
                continue

            year_freqs = process_word_dict(json.load(open(os.path.join(year_path, '1.json'))))

            # skip years with few cases (probably typos)
            if sum(w[1] for w in year_freqs)<10000:
                continue

            bare_top_words_by_year[year] = [w[0] for w in year_freqs]

        # calculate global rankings
        word_to_ranking = defaultdict(lambda: 0)
        for year, words in tqdm(bare_top_words_by_year.iteritems()):
            for pos, word in enumerate(words):
                word_to_ranking[word] += (1000 - pos)
        average_rank = dict([(word, ranking/len(bare_top_words_by_year)) for word, ranking in word_to_ranking.iteritems()])

        # write average ranks CSV
        save_top_list(['word','average_rank'],
                      [[word, 1000-rank] for word, rank in average_rank.iteritems()],
                      os.path.join(top_words_out_dir, 'average_ranks.csv'))

        # calculate scores by year
        year_to_volatile_words = {}
        for year, words in tqdm(bare_top_words_by_year.iteritems()):
            word_to_ranking_delta = {}
            for pos, word in enumerate(words):
                rank_for_year = 1000 - pos
                word_to_ranking_delta[word] = (rank_for_year, average_rank[word], rank_for_year - average_rank[word])
                year_to_volatile_words[year] = sorted(word_to_ranking_delta.items(), key=lambda x: x[1][2], reverse=True)

        # wordclouds
        for year, words in tqdm(year_to_volatile_words.iteritems()):
            freqs = [(w[0], w[1][2]) for w in words[:200]]
            save_wordcloud(freqs, os.path.join(out_dir, '%s.png' % year))

            # write year ranks CSV
            save_top_list(['word', 'absolute_rank', 'relative_score'],
                          [(w[0], 1000-w[1][0], w[1][2]) for w in words],
                          os.path.join(top_words_out_dir, '%s_ranks.csv' % year))

if __name__ == "__main__":
    wordcloud_all_jurisdictions()
