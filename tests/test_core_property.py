# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path


class TestS3Path:
    def test_uri(self):
        p = S3Path("bucket", "folder", "file.txt")
        assert p.uri == "s3://bucket/folder/file.txt"

        p = S3Path("bucket", "folder", "subfolder/")
        assert p.uri == "s3://bucket/folder/subfolder/"

        p = S3Path("bucket")
        assert p.uri == "s3://bucket/"

        p = S3Path()
        assert p.uri is None

    def test_parent(self):
        p = S3Path("bucket", "folder", "file.txt").parent
        assert p._bucket == "bucket"
        assert p._parts == ["folder"]
        assert p._is_dir is True

        p = S3Path("bucket", "folder", "subfolder/").parent
        assert p._bucket == "bucket"
        assert p._parts == ["folder"]
        assert p._is_dir is True

        p = S3Path("bucket", "folder").parent
        assert p._bucket == "bucket"
        assert p._parts == []
        assert p._is_dir is True

        p = S3Path("bucket", "file.txt").parent
        assert p._bucket == "bucket"
        assert p._parts == []
        assert p._is_dir is True

        p = S3Path("bucket").parent
        assert p is None

        p = S3Path().parent
        assert p is None

    def test_basename(self):
        assert S3Path("bucket", "folder", "file.txt").basename == "file.txt"
        assert S3Path("bucket", "folder/").basename == "folder"
        assert S3Path("bucket").basename is None


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
