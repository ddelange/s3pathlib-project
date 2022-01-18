# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path


class TestS3Path:
    def test_simple_string_parts(self):
        p = S3Path("bucket", "a", "b", "c")
        assert p._bucket == "bucket"
        assert p._parts == ["a", "b", "c"]
        assert p._is_dir is False

        p = S3Path("bucket", "a", "b", "c/")
        assert p._bucket == "bucket"
        assert p._parts == ["a", "b", "c"]
        assert p._is_dir is True

        p = S3Path("bucket/a/b/c")
        assert p._bucket == "bucket"
        assert p._parts == ["a", "b", "c"]
        assert p._is_dir is False

        p = S3Path("//bucket//a//b//c//")
        assert p._bucket == "bucket"
        assert p._parts == ["a", "b", "c"]
        assert p._is_dir is True

    def test_mixed_s3path_parts(self):
        p = S3Path(
            S3Path("bucket", "folder"),
            S3Path._from_parsed_parts(
                bucket=None,
                parts=["relpath", "file.txt"],
                is_dir=False,
            )
        )
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "relpath", "file.txt"]
        assert p._is_dir is False

        p = S3Path(
            S3Path("bucket", "folder"),
            S3Path._from_parsed_parts(
                bucket=None,
                parts=["relpath", ],
                is_dir=True,
            )
        )
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "relpath"]
        assert p._is_dir is True

    def test_empty_path(self):
        p = S3Path()
        assert p._bucket is None
        assert p._parts == []
        assert p._is_dir is None

    def test_many_empty_path(self):
        p = S3Path(S3Path(), S3Path(), S3Path())
        assert p._bucket is None
        assert p._parts == []
        assert p._is_dir is None

    def test_bucket_root(self):
        p = S3Path("bucket")
        assert p._is_dir is True

    def test_exceptions(self):
        with pytest.raises(TypeError):
            S3Path(1)

        with pytest.raises(TypeError):
            S3Path("bucket", 1)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
