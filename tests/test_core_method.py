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
        p = S3Path()
        with pytest.raises(ValueError):
            p.is_dir()

        assert S3Path("bucket").is_file() is False
        assert S3Path("bucket", "folder/").is_file() is False
        assert S3Path("bucket", "file.txt").is_file() is True

    def test_is_relpath(self):
        assert S3Path().is_relpath() is False

        assert S3Path._from_parsed_parts(
            bucket=None,
            parts=["folder"],
            is_dir=True,
        ).is_relpath() is True

        assert S3Path._from_parsed_parts(
            bucket=None,
            parts=["file.txt"],
            is_dir=False,
        ).is_relpath() is True

        assert S3Path._from_parsed_parts(
            bucket=None,
            parts=["folder", "file.txt"],
            is_dir=None,
        ).is_relpath() is False


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
