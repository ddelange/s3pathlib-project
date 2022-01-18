# -*- coding: utf-8 -*-

import boto3

boto_ses = boto3.session.Session()
s3_client = boto_ses.client("s3")
bucket = "aws-data-lab-sanhe-for-everything"
prefix = "unittest/s3pathlib"
