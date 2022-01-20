# -*- coding: utf-8 -*-

import pytest
from s3pathlib.aws import context
from s3pathlib.core import S3Path
from s3pathlib.tests import boto_ses, bucket, prefix
from s3pathlib.utils import md5_binary

context.attach_boto_session(boto_ses)
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
        assert p.size_for_human == "12 B"
        assert p.version_id is None
        assert p.expire_at is None
        assert p.metadata == {"creator": "Alice"}

    def test_clear_cache(self):
        p = self.p
        p.clear_cache()
        assert p._meta is None
        assert len(p.etag) == 32
        assert isinstance(p._meta, dict)

    def test_exists(self):
        # s3 bucket
        assert S3Path(bucket).exists() is True
        # have access but not exists
        assert S3Path(f"{bucket}-not-exists").exists() is False
        # doesn't have access
        with pytest.raises(Exception):
            assert S3Path("asdf").exists() is False

        # s3 object
        p = self.p
        assert p.exists() is True

        p = S3Path(bucket, "this-never-gonna-exists.exe")
        assert p.exists() is False

        # s3 directory
        p = self.p.parent
        assert p.exists() is True

        p = S3Path(bucket, "this-never-gonna-exists/")
        assert p.exists() is False

        # void path
        p = S3Path()
        with pytest.raises(TypeError):
            p.exists()

        # relative path
        p = S3Path.make_relpath("folder", "file.txt")
        with pytest.raises(TypeError):
            p.exists()


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
