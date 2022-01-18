# -*- coding: utf-8 -*-

import pytest
from s3pathlib.aws import context
from s3pathlib.core import S3Path
from s3pathlib.tests import boto_ses, bucket, prefix
from s3pathlib.utils import md5_binary

context.boto_ses = boto_ses
s3_client = context.s3_client


class TestS3Path:
    p = S3Path(bucket, prefix, "file.txt")

    @classmethod
    def setup_class(cls):
        s3_client.put_object(
            Bucket=cls.p.bucket,
            Key=cls.p.key,
            Body="Hello World!",
            Metadata={
                "creator": "Alice"
            }
        )

    def test_attributes(self):
        p = self.p
        assert p.etag == md5_binary("Hello World!".encode("utf-8"))
        _ = p.last_modified_at
        assert p.size == 12
        assert p.version_id is None
        assert p.expire_at is None
        assert p.metadata == {"creator": "Alice"}


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
