import os
from glob import glob

import shutil
from tqdm import tqdm

from helpers import parse_file, makedirs

source_dir = "/ftldata/harvard-ftl-shared"
dest_dir = "/ftldata/research_set"
tmp_dest_dir = "/ftldata/.research_set_in_progress"

def sort_case(case_xml_path):
    pq = parse_file(case_xml_path)
    jurisdiction = pq("case|court").attr('jurisdiction').strip()
    citation = pq('case|citation[category="official"]').text().strip()
    cite_parts = citation.split(" ")
    volume, reporter, page_number = cite_parts[0], " ".join(cite_parts[1:-1]), cite_parts[-1]
    volume_dir = os.path.join(jurisdiction, reporter, volume)
    makedirs(volume_dir)
    dest_path = os.path.join(volume_dir, os.path.basename(case_xml_path))
    if os.path.exists(dest_path):
        os.remove(dest_path)
    os.link(case_xml_path, dest_path)

def sort_volume(volume_path):
    for case_xml_path in glob(os.path.join(volume_path, "casemets/*.xml")):
        sort_case(case_xml_path)

def sort_all_volumes():
    # make everything in a temp dir
    makedirs(tmp_dest_dir)
    os.chdir(tmp_dest_dir)

    dirs = sorted(glob(os.path.join(source_dir, "from_vendor/*")))
    for i, volume_path in enumerate(tqdm(dirs)):

        # skip dirs that are superceded by the following version
        base_name = volume_path.split('_redacted_',1)[0]
        if i < len(dirs)-1 and dirs[i+1].startswith(base_name):
            #print "Skipping", volume_path
            continue

        sort_volume(volume_path)

    # swap temp dir in place of existing dir
    shutil.rmtree(dest_dir)
    os.rename(tmp_dest_dir, dest_dir)

if __name__ == "__main__":
    sort_all_volumes()
