import os
import time
from helpers import parse_file, get_decision_date, get_case_text

def get_data(filename):
    pq = parse_file(filename)
    jurisdiction = pq("case|court").attr('jurisdiction').strip()
    return filename, get_decision_date(pq), jurisdiction, get_case_text(pq)

def format_file_list(filename):
    with open(filename, 'rb') as input_file:
        train_case_list = list(input_file)
        train_case_list = map(lambda s : s.replace('\n','').replace('\r',''), train_case_list)
    return train_case_list

def get_training_text():
    training_text_filename = "/ftldata/topic_modeling/result_docs/training_text.txt"
    start_time = time.time()
    if os.path.isfile(training_text_filename):
        contents = [line.rstrip('\n') for line in open(training_text_filename)]
        print 'Time after reading file: %.3f' % (time.time() - start_time)
        return contents
    else:
        formatted_case_list = format_file_list(train_filename)
        contents = map(get_case_text, formatted_case_list)
        with open(training_text_filename, 'wb+') as f:
            for item in contents:
                f.write("%s\n" % item)
        print 'Time after writing file: %.3f' % (time.time() - start_time)
        return contents
