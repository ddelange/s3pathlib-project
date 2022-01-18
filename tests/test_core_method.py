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

    def test_is_empty(self):
        assert S3Path()._is_empty() is True
        assert S3Path("bucket", "folder")._is_empty() is False
        assert S3Path("bucket", "file.txt")._is_empty() is False

    def test_is_dir_is_file(self):
        # empty path are neither dir / file / relpath
        p = S3Path()

        with pytest.raises(ValueError):
            p.is_dir()

        with pytest.raises(ValueError):
            p.is_file()

        assert p.is_relpath() is False

        assert S3Path("bucket").is_file() is False
        assert S3Path("bucket", "folder/").is_file() is False
        assert S3Path("bucket", "file.txt").is_file() is True

    def test_is_relpath(self):
        assert S3Path().is_relpath() is False

        p = S3Path._from_parsed_parts(
            bucket=None,
            parts=["folder"],
            is_dir=True,
        )
        assert p.is_relpath() is True
        assert p.is_dir() is True
        assert p.is_file() is False
        assert str(p) == "S3Path('folder/')"

        p = S3Path._from_parsed_parts(
            bucket=None,
            parts=["file.txt"],
            is_dir=False,
        )
        assert p.is_relpath() is True
        assert p.is_dir() is False
        assert p.is_file() is True
        assert str(p) == "S3Path('file.txt')"

        p = S3Path._from_parsed_parts(
            bucket=None,
            parts=["folder", "file.txt"],
            is_dir=None,
        )
        assert p.is_relpath() is False

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
        assert p._is_dir is False

        p = S3Path("bucket", "a/").relative_to(S3Path("bucket", "a/"))
        assert p._parts == []
        assert p._is_dir is True

        p = S3Path("bucket").relative_to(S3Path("bucket"))
        assert p._parts == []
        assert p._is_dir is True

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
        S3Path("bucket", "file.txt").ensure_not_relpath()
        S3Path("bucket", "folder/").ensure_not_relpath()

        with pytest.raises(TypeError):
            S3Path._from_parsed_parts(
                bucket=None, parts=["a", "b"], is_dir=True
            ).ensure_not_relpath()

        with pytest.raises(TypeError):
            S3Path._from_parsed_parts(
                bucket=None, parts=["a", "b"], is_dir=False
            ).ensure_not_relpath()

        S3Path._from_parsed_parts(
            bucket=None, parts=["a", "b"], is_dir=None
        ).ensure_not_relpath()


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
