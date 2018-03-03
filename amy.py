#!/usr/bin/env python3

"rename some files for amy p"

# import pprint
import sys

import boto3

import s3util

S3 = boto3.client("s3")

def get_new_name(old_url):
    """
    rename e.g.
    s3://bktname/long/path/to/file/D02-1B-EB.R1.fastq.gz

    to e.g.
    s3://bktname/long/path/to/file/D02-1B-EB.R1_000.fastq.gz

    """
    new_url = old_url.replace(".R1.fastq.gz", "_R1.fastq.gz")
    new_url = new_url.replace(".R2.fastq.gz", "_R2.fastq.gz")
    return new_url.replace(".fastq.gz", "_000.fastq.gz")

def test():
    "test"
    result = get_new_name("s3://bktname/long/path/to/file/D02-1B-EB.R1.fastq.gz")
    expected = "s3://bktname/long/path/to/file/D02-1B-EB_R1_000.fastq.gz"
    assert len(result) == len(expected)
    assert result == expected
    result = get_new_name("s3://bktname/long/path/to/file/D02-1B-EB.R2.fastq.gz")
    expected = "s3://bktname/long/path/to/file/D02-1B-EB_R2_000.fastq.gz"
    assert len(result) == len(expected)
    assert result == expected

def nmok(name):
    "util func"
    return name.endswith(".R1.fastq.gz") or name.endswith(".R2.fastq.gz")

def get_files_to_rename():
    "get files to rename!!"
    subdirs = ['161012_AHA5WRADXX-BHA5K9ADXX/',
               '161012_AHB686ADXX-AHGLYGADXX/',
               '161012_AHB686ADXX-BHGMFKADXX-AHBBN2ADXX/',
               '161012_BHGMFKADXX-AHBBN2ADXX/',
               '171220_AHB686ADXX-AHGLYGADXX/']
    pre = "SR/ngs/illumina/apaguiri"
    files_to_rename = []
    for subdir in subdirs:
        resp = S3.list_objects(Bucket="fh-pi-paguirigan-a",# Delimiter='/',
                               Prefix="{}/{}".format(pre, subdir, MaxKeys=999))
        if resp['IsTruncated']:
            print("Stop being lazy and support paginated results.")
            sys.exit(1)
        if 'Contents' in resp:
            files_to_rename.extend([x['Key'] for x in resp['Contents'] if nmok(x['Key'])])
    return ["s3://fh-pi-paguirigan-a/{}".format(x) for x in files_to_rename]


def doit():
    "do it all"
    files_to_rename = get_files_to_rename()
    new_urls = [get_new_name(x)  for x in files_to_rename]
    for i, file_to_rename in enumerate(files_to_rename):
        print("{}\n{}\n\n".format(file_to_rename, new_urls[i]))
        s3util.rename_s3_object(file_to_rename, new_urls[i])

if __name__ == '__main__':
    test()
    #..
    doit()
