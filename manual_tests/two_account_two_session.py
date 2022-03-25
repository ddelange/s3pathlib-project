# -*- coding: utf-8 -*-

import boto3
from pathlib_mate import Path
from s3pathlib import S3Path, context

profile1 = ""
profile2 = ""
uri1 = ""
uri2 = ""

boto_ses1 = boto3.session.Session(profile_name=profile1)
boto_ses2 = boto3.session.Session(profile_name=profile2)

s3path1 = S3Path.from_s3_uri(uri1)
s3path2 = S3Path.from_s3_uri(uri2)

# make sure both file not exists
context.attach_boto_session(boto_ses1)
s3path1.delete_if_exists()
context.attach_boto_session(boto_ses2)
s3path2.delete_if_exists()

# upload file to s3path1
context.attach_boto_session(boto_ses1)
p_log_file = Path(Path(__file__).parent, "log.txt")
s3path1.upload_file(p_log_file.abspath)

# make sure s3path2 not exists
context.attach_boto_session(boto_ses2)
assert s3path2.exists() is False

# copy from s3path1 to s3path2 across account
context.attach_boto_session(boto_ses1)
with s3path1.open("r") as f1:
    context.attach_boto_session(boto_ses2)
    with s3path2.open("w") as f2:
        for line in f1:
            f2.write(line)

# make sure s3path2 exists
assert s3path2.exists() is True
