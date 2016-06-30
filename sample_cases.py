# remixed from https://github.com/madhuvijay95/ftl-sprint-case-clustering
import sys
import numpy as np
import os

source_dir = "/ftldata/harvard-ftl-shared/from_vendor"
threshold = float(sys.argv[1])

dirs = os.listdir(source_dir)
sample_dir_list = np.random.choice(dirs, size=int(threshold*len(dirs)))

for current_dir in sample_dir_list:
    dir_path = '%s/%s/casemets' % (source_dir,current_dir)
    if os.path.exists(dir_path):
        cases = os.listdir(dir_path)
        if len(cases):
            sample_case_list = np.random.choice(cases, size=int(threshold*len(cases)))
            if sys.argv[1]:
                with open(sys.argv[2], 'ab') as output_file:
                    for case in sample_case_list:
                        output_file.write(case + '\n')
            else:
                for case in sample_case_list:
                    print case
    else:
        pass
