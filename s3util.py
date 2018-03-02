#!/usr/bin/env python3

"s3 utilities"


from urllib.parse import urlparse
import sys

import boto3


s3 = boto3.client("s3") # pylint: disable=invalid-name

def object_exists(s3_url, delimiter='/'):
    "checks if the key represented by an s3 url exists"
    url = urlparse(s3_url)
    bucket = url.netloc
    path = url.path.lstrip("/")
    response = s3.list_objects(Bucket=bucket, Delimiter=delimiter, Prefix=path)
    return 'Contents' in response and len(response['Contents']) > 0

def rename_s3_object(old_name, new_name):
    "rename an object, keeping its tags"

    if object_exists(new_name):
        print("Whoa, destination file {} already exists!")
        return False

    old_url = urlparse(old_name)
    new_url = urlparse(new_name)
    old_bucket = old_url.netloc
    new_bucket = new_url.netloc
    old_path = old_url.path.lstrip("/")
    new_path = new_url.path.lstrip("/")
    copy_source = dict(Bucket=old_bucket, Key=old_path)
    print(copy_source)
    # TODO try/except these lines:
    s3.copy_object(CopySource=copy_source, Bucket=new_bucket, Key=new_path)
    s3.delete_object(Bucket=old_bucket, Key=old_path)
    return True

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("supply two s3 urls (old and new)")
        sys.exit(1)
    print(sys.argv)
    rename_s3_object(sys.argv[1], sys.argv[2])
