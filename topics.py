
import os
import sys
reload(sys)    # to re-enable sys.setdefaultencoding()
sys.setdefaultencoding('utf-8')
import numpy as np
import bottleneck as bn
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from helpers import get_case_text, get_tm_data, format_tm_file_list
from sklearn.decomposition import NMF
import csv
import time

root_dir = "/ftldata/research_set"
train_filename = "/ftldata/topic_modeling/result_docs/case_sample_list.txt"
topic_modeling_pipeline = "/ftldata/topic_modeling/result_docs/topic_modeling_pipline.txt"
best_words_filename = "/ftldata/topic_modeling/result_docs/best_words.txt"
best_match_cases = "/ftldata/topic_modeling/result_docs/best_match_cases.txt"
cluster_output_filename = "/ftldata/topic_modeling/result_docs/topic_modeling_results.csv"

def get_topics_for_all_in_dir(current_dir='.'):
    for root, dirs, files in os.walk(current_dir):
        if len(files):
            files = [os.path.join(root, f) for f in files]
            get_topics(files_array=files)

def get_topics_for_new_files():
    topic_pipeline = format_tm_file_list(topic_modeling_pipline)
    get_topics(files=topic_pipeline)
    os.remove(topic_modeling_pipline)

# taken from https://github.com/madhuvijay95/ftl-sprint-case-clustering
def get_topics(files_array=[], n_clusters=100):
    if not len(files_array):
        return

    start_time = time.time()
    print 'Time at start: %.3f' % (time.time() - start_time)
    sys.stdout.flush()
    formatted_case_list = format_tm_file_list(train_filename)

    train_texts = map(get_case_text, formatted_case_list)
    print 'Time after getting train text from XML: %.3f' % (time.time() - start_time)
    sys.stdout.flush()

    vectorizer = CountVectorizer(max_df = 0.5)
    train_mat_init = vectorizer.fit_transform(train_texts)
    del train_texts
    vocab = vectorizer.vocabulary_
    vocab_rev = {v:k for k,v in vocab.items()}
    print 'Time after CountVectorizer on train data: %.3f' % (time.time() - start_time)
    sys.stdout.flush()

    tfidf_fit = TfidfTransformer().fit(train_mat_init)
    train_mat = tfidf_fit.transform(train_mat_init)
    del train_mat_init
    print 'Time after TFIDF on train data: %.3f' % (time.time() - start_time)
    sys.stdout.flush()

    NMF_fit = NMF(n_components=n_clusters, init='nndsvda').fit(train_mat)
    del train_mat
    H = NMF_fit.components_
    #W = NMF_fit.transform(train_mat)
    num_best = 50
    best_indices = map(lambda v : list(bn.argpartsort(-v,num_best)[0:num_best]), H)
    for i in range(len(best_indices)):
        best_indices[i].sort(key = lambda j : -H[i,j])
    best_words = [[vocab_rev[i] for i in lst] for lst in best_indices]

    print 'Time after NMF fit: %.3f\n' % (time.time() - start_time)

    if best_words_filename is not None:
        with open(best_words_filename, 'wb') as best_words_file:
            for c, lst in enumerate(best_words):
                best_words_file.write(str(c) + ' [' + ', '.join(map(lambda s : '\'' + s + '\'', lst)) + ']\n')
    else:
        print 'BEST WORDS FOR EACH CLUSTER:'
        for c, lst in enumerate(best_words):
           print '%d' % c, lst
        sys.stdout.flush()

    print '\nTime after NMF output: %.3f' % (time.time() - start_time)
    sys.stdout.flush()

    test_data = map(get_tm_data, files_array)
    test_texts = [t for f,y,j,t in test_data]

    test_mat_init = vectorizer.transform(test_texts)
    del test_texts
    test_mat = tfidf_fit.transform(test_mat_init)
    del test_mat_init
    test_W = NMF_fit.transform(test_mat)
    del test_mat
    test_clusters = map(np.argmax, test_W)

    print 'Time after NMF test transform: %.3f\n' % (time.time() - start_time)

    print 'NUMBER OF CASES PER CLUSTER:'
    cluster_sizes = [np.sum(np.array(test_clusters) == c) for c in range(n_clusters)]
    for c, sz in enumerate(cluster_sizes):
       print '%d: %d' % (c, sz)
    print
    sys.stdout.flush()

    results = zip(test_clusters, test_data)

    results.sort(key = lambda (c, (f,y,j,t)) : 2000 * c + y.year) # sort by cluster, then by year
    with open(cluster_output_filename, 'ab') as output_file:
        writer = csv.writer(output_file)
        for c, (f,y,j,t) in results:
            writer.writerow((f, y, j, c))

    print '\nTime after all remaining output: %.3f\n' % (time.time() - start_time)

    cluster_weights = zip(files_array, test_W)
    n_cases = 20
    if best_match_cases is not None:
        with open(best_match_cases, 'ab') as best_matches_file:
            for cluster_id in range(n_clusters):
                best_matches_file.write('FOR CLUSTER %d:\n' % cluster_id)
                cluster_weights.sort(key = lambda (case, weights) : -weights[cluster_id])
                clusters_ranked = map(lambda (c,w) : c, cluster_weights[0:n_cases])
                for case in clusters_ranked:
                    best_matches_file.write(case + '\n')
                best_matches_file.write('\n')

if __name__ == '__main__':
    get_topics_for_all_in_dir()
