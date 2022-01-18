# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path


class TestS3Path:
    def test_uri_bucket_key(self):
        p = S3Path("bucket", "folder", "file.txt")
        assert p.uri == "s3://bucket/folder/file.txt"
        assert p.bucket == "bucket"
        assert p.key == "folder/file.txt"
        assert p.key_parts == ["folder", "file.txt"]
        assert p.console_url is not None
        assert str(p) == "S3Path('s3://bucket/folder/file.txt')"

        p = S3Path("bucket", "folder", "subfolder/")
        assert p.uri == "s3://bucket/folder/subfolder/"
        assert p.bucket == "bucket"
        assert p.key == "folder/subfolder/"
        assert p.key_parts == ["folder", "subfolder"]
        assert p.console_url is not None
        assert str(p) == "S3Path('s3://bucket/folder/subfolder/')"

        p = S3Path("bucket")
        assert p.uri == "s3://bucket/"
        assert p.bucket == "bucket"
        assert p.key == ""
        assert p.key_parts == []
        assert p.console_url is not None

        p = S3Path()
        assert p.uri is None
        assert p.bucket is None
        assert p.key == ""
        assert p.key_parts == []
        assert p.console_url is None
        
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

    def test_dirname(self):
        assert S3Path("bucket", "folder", "file.txt").dirname == "folder"
        assert S3Path("bucket", "folder/").dirname is None
        assert S3Path("bucket", "file.txt").dirname is None

    def test_fname(self):
        with pytest.raises(TypeError):
            _ = S3Path("bucket").fname

        with pytest.raises(TypeError):
            _ = S3Path("bucket", "folder/").fname

        assert S3Path("bucket", "file.txt").fname == "file"
        assert S3Path("bucket", "file").fname == "file"

        with pytest.raises(ValueError):
            _ = S3Path._from_parsed_parts(
                bucket=None,
                parts=[],
                is_dir=False,
            ).fname

    def test_ext(self):
        with pytest.raises(TypeError):
            _ = S3Path("bucket").ext

        with pytest.raises(TypeError):
            _ = S3Path("bucket", "folder/").ext

        assert S3Path("bucket", "file.txt").ext == ".txt"
        assert S3Path("bucket", "file").ext == ""

        with pytest.raises(ValueError):
            _ = S3Path._from_parsed_parts(
                bucket=None,
                parts=[],
                is_dir=False,
            ).ext


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
