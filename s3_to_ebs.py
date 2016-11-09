import json
import os
from multiprocessing.pool import ThreadPool
import boto3
from tqdm import tqdm
from helpers import makedirs

dest_dir = "/ftldata/harvard-ftl-shared"
s3_mount_dir = "/mnt/harvard-ftl-shared"

s3 = boto3.client('s3')
bucket_name = "harvard-ftl-shared"

def rename_file(from_key, to_key):
    copy_source = '%s/%s' % (bucket_name, from_key)
    print "copyring from ", copy_source
    s3.Object(bucket_name, to_key).copy_from(CopySource=copy_source)
    s3.Object(bucket_name, from_key).delete()

def list_dir(dir):
    client = boto3.client('s3')
    dirs = []
    files = []
    paginator = client.get_paginator('list_objects')
    dir = dir.rstrip('/')
    prefix = dir+'/' if dir else ''
    for result in paginator.paginate(Bucket=bucket_name, Delimiter='/', Prefix=prefix):
        dirs += [x['Prefix'] for x in result.get('CommonPrefixes', [])]
        files += [x['Key'] for x in result.get('Contents', []) if x['Key'] != prefix]
    return dirs, files

def save_file(key, symlink=False):
    output_file = os.path.join(dest_dir, key)
    if not os.path.exists(output_file):
        if symlink:
            os.symlink(os.path.join(s3_mount_dir, key), output_file)
        else:
            boto3.resource('s3').Bucket(bucket_name).download_file(key, output_file)
        return output_file

def make_output_dir(key):
    output_dir = os.path.join(dest_dir, key)
    makedirs(output_dir)
    return output_dir

def copy_files(key, symlink=False):
    make_output_dir(key)
    _, files = list_dir(key)
    for f in files:
        save_file(f, symlink=symlink)

def grab_dir(dir):
    print "Grabbing %s" % dir

    copy_files(dir)
    copy_files(dir + 'casemets')
    copy_files(dir + 'alto', symlink=True)
    copy_files(dir + 'images', symlink=True)

def download_all():
    dirs, _ = list_dir('from_vendor')
    # skip:
    # dirs = [dir for dir in dirs if dir > 'from_vendor/32044078676475_redacted/']
    ThreadPool(10).map(grab_dir, dirs)

def dump_all_keys():
    client = boto3.client('s3')
    out = open('keys.txt','w')
    paginator = client.get_paginator('list_objects')
    count = 0
    for result in paginator.paginate(Bucket=bucket_name):
        count += len(result['Contents'])
        print count
        for obj in result['Contents']:
            out.write(obj['Key']+"\n")
    out.close()

if __name__ == "__main__":
    download_all()
