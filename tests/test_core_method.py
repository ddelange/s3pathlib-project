# -*- coding: utf-8 -*-

"""
Test those stateless, non API call method
"""

import pytest
from s3pathlib.core import S3Path


class TestS3Path:
    def test_to_dict(self):
        assert S3Path("bucket", "folder", "file.txt").to_dict() == {
            "bucket": "bucket",
            "parts": ["folder", "file.txt"],
            "is_dir": False,
        }

    def test_type_test(self):
        """
        Test if the instance is a ...

        - is_dir()
        - is_file()
        - is_relpath()
        - is_bucket()
        - is_void()
        """
        # s3 object
        p = S3Path("bucket", "file.txt")
        assert p.is_void() is False
        assert p.is_dir() is False
        assert p.is_file() is True
        assert p.is_bucket() is False
        assert p.is_relpath() is False

        # s3 directory
        p = S3Path("bucket", "folder/")
        assert p.is_void() is False
        assert p.is_dir() is True
        assert p.is_file() is False
        assert p.is_bucket() is False
        assert p.is_relpath() is False

        # s3 bucket
        p = S3Path("bucket")
        assert p.is_void() is False
        assert p.is_dir() is True
        assert p.is_file() is False
        assert p.is_bucket() is True
        assert p.is_relpath() is False

        # void path
        p = S3Path()
        assert p.is_void() is True
        assert p.is_dir() is False
        assert p.is_file() is False
        assert p.is_bucket() is False
        assert p.is_relpath() is True

        # relative path
        p = S3Path("bucket", "file.txt").relative_to(S3Path("bucket"))
        assert p.is_void() is False
        assert p.is_dir() is False
        assert p.is_file() is True
        assert p.is_bucket() is False
        assert p.is_relpath() is True

        p = S3Path("bucket", "folder/").relative_to(S3Path("bucket"))
        assert p.is_void() is False
        assert p.is_dir() is True
        assert p.is_file() is False
        assert p.is_bucket() is False
        assert p.is_relpath() is True

        p = S3Path("bucket").relative_to(S3Path("bucket"))
        assert p.is_void() is True
        assert p.is_dir() is False
        assert p.is_file() is False
        assert p.is_bucket() is False
        assert p.is_relpath() is True

    def test_relative_to(self):
        p = S3Path("bucket", "a", "b", "c").relative_to(S3Path("bucket", "a"))
        assert p.is_relpath()
        assert p._parts == ["b", "c"]
        assert p._is_dir is False

        p = S3Path("bucket", "a", "b", "c").relative_to(S3Path("bucket", "a/"))
        assert p.is_relpath()
        assert p._parts == ["b", "c"]
        assert p._is_dir is False

        # if self is dir, then relpath also a dir
        p = S3Path("bucket", "a", "b", "c/").relative_to(S3Path("bucket", "a"))
        assert p.is_relpath()
        assert p._parts == ["b", "c"]
        assert p._is_dir is True

        p = S3Path("bucket", "a", "b", "c/").relative_to(S3Path("bucket", "a/"))
        assert p.is_relpath()
        assert p._parts == ["b", "c"]
        assert p._is_dir is True

        # relpath to bucket root
        p = S3Path("bucket", "a", "b").relative_to(S3Path("bucket"))
        assert p.is_relpath()
        assert p._parts == ["a", "b"]
        assert p._is_dir is False

        p = S3Path("bucket", "a", "b/").relative_to(S3Path("bucket"))
        assert p.is_relpath()
        assert p._parts == ["a", "b"]
        assert p._is_dir is True

        # relative to self
        p = S3Path("bucket", "a").relative_to(S3Path("bucket", "a"))
        assert p._parts == []
        assert p._is_dir is None

        p = S3Path("bucket", "a/").relative_to(S3Path("bucket", "a/"))
        assert p._parts == []
        assert p._is_dir is None

        p = S3Path("bucket").relative_to(S3Path("bucket"))
        assert p._parts == []
        assert p._is_dir is None

        # exceptions
        with pytest.raises(ValueError):
            S3Path("bucket1").relative_to(S3Path("bucket2"))

        with pytest.raises(ValueError):
            S3Path("bucket", "a").relative_to(S3Path("bucket", "b"))

        with pytest.raises(ValueError):
            S3Path("bucket", "a").relative_to(S3Path("bucket", "a", "b"))

    def test_copy(self):
        p1 = S3Path()
        p2 = p1.copy()
        assert p1 is not p2

    def test_ensure_object(self):
        with pytest.raises(TypeError):
            S3Path("bucket", "folder/").ensure_object()
        S3Path("bucket", "file.txt").ensure_object()

    def test_ensure_dir(self):
        with pytest.raises(TypeError):
            S3Path("bucket", "file.txt").ensure_dir()
        S3Path("bucket", "folder/").ensure_dir()

    def test_ensure_not_relpath(self):
        # not relpath
        S3Path("bucket", "file.txt").ensure_not_relpath()

        # not relpath
        S3Path("bucket", "folder/").ensure_not_relpath()

        # is relpath
        with pytest.raises(TypeError):
            p = S3Path._from_parsed_parts(
                bucket=None, parts=["a", "b"], is_dir=True
            ).ensure_not_relpath()

        # is relpath
        with pytest.raises(TypeError):
            S3Path._from_parsed_parts(
                bucket=None, parts=["a", "b"], is_dir=False
            ).ensure_not_relpath()

        # is relpath
        with pytest.raises(TypeError):
            S3Path._from_parsed_parts(
                bucket=None, parts=["a", "b"], is_dir=None
            ).ensure_not_relpath()


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
