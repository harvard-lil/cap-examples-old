import json
import os

import sys
from itertools import islice
from multiprocessing.pool import ThreadPool

import time

from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from s3_to_ebs import save_file, make_output_dir
from s3_to_csv import write_metadata_for_file, generate_metadata_filepath,create_metadata_file, open_metadata_writer

# set up database connection
db_url = URL(drivername='mysql.mysqlconnector', host='ftl.cv97tsyby7rk.us-west-2.rds.amazonaws.com')
engine = create_engine(db_url, connect_args={'option_files':'/ftldata/misc/mysql.ftl.cnf'})
Session = sessionmaker(bind=engine)
session = Session()

# set up models
Base = automap_base()
Base.prepare(engine, reflect=True)
InnodataSharedCases = Base.classes.innodata_shared_cases
InnodataSharedVolumes = Base.classes.innodata_shared_volumes
InnodataSharedImages = Base.classes.innodata_shared_images

# helpers
WINDOW_SIZE = 1000
def qgen(query):
    start = 0
    while True:
        stop = start + WINDOW_SIZE
        things = query.slice(start, stop).all()
        if not things:
            break
        for thing in things:
            yield(thing)
        start += WINDOW_SIZE

threadpool = ThreadPool(20)
try:
    shelf = json.load(open(__file__+".json"))
except IOError:
    shelf = {}

def dump_table(model, symlink=False):
    print "Dumping", model

    min_id = shelf.get(model.__name__, 0)

    metadata_filepath = generate_metadata_filepath()
    create_metadata_file(metadata_filepath)
    metadata_writer = open_metadata_writer(metadata_filepath)

    items = qgen(session.query(model).filter(model.id>=min_id).order_by(model.id))

    def process_item(item):
        make_output_dir(os.path.dirname(item.s3key))
        output_file = save_file(item.s3key, symlink=symlink)
        if output_file:
            write_metadata_for_file(output_file, metadata_writer)

    while True:
        item_set = list(islice(items, 1000))
        if not item_set:
            break
        shelf[model.__name__] = item_set[0].id
        json.dump(shelf, open(__file__+".json", "w"))
        print datetime.now(), model.__name__, item_set[0].id, time.time()
        sys.stdout.flush()
        threadpool.map(process_item, item_set)
        #map(process_item, item_set)

dump_table(InnodataSharedCases)
dump_table(InnodataSharedImages, symlink=True)
dump_table(InnodataSharedVolumes)