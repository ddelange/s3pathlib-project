# -*- coding: utf-8 -*-

import pytest
from s3pathlib.aws import context
from s3pathlib.core import S3Path
from s3pathlib.tests import boto_ses, bucket, prefix
from s3pathlib.utils import md5_binary

context.boto_ses = boto_ses
s3_client = context.s3_client


class TestS3Path:
    p_root = S3Path(bucket, prefix, "change-state")

    @classmethod
    def setup_class(cls):
        cls.p_root.delete_if_exists()

    # def test_etag(self):
    #     assert self.p.etag == md5_binary("Hello World!".encode("utf-8"))
    #
    # def test_metadata(self):
    #     assert self.p.metadata == {"creator": "Alice"}



if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
