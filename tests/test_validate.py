# -*- coding: utf-8 -*-

import pytest
from s3pathlib import validate as val


def test_validate_s3_bucket():
    test_cases = [
        # bad case
        ("", False),
        ("a", False),
        ("a" * 100, False),

        ("bucket@example.com", False),
        ("my_bucket", False),

        ("-my-bucket", False),
        ("my-bucket-", False),

        # good case
        ("my-bucket", True),
    ]
    for bucket, flag in test_cases:
        if flag:
            val.validate_s3_bucket(bucket)
        else:
            with pytest.raises(Exception):
                val.validate_s3_bucket(bucket)


def test_validate_s3_key():
    test_cases = [
        # bad cases
        ("a" * 2000, False),
        ("%20", False),

        # good cases
        ("", True,),
        ("abcd", True,),
    ]
    for key, flag in test_cases:
        if flag:
            val.validate_s3_key(key)
        else:
            with pytest.raises(Exception):
                val.validate_s3_key(key)


def test_validate_s3_uri():
    test_cases = [
        # bad cases
        ("bucket/key", False),
        ("s3://bucket", False),

        # good cases
        ("s3://bucket/key", True),
        ("s3://bucket/folder/file.txt", True),
        ("s3://bucket/", True),
    ]
    for uri, flag in test_cases:
        if flag:
            val.validate_s3_uri(uri)
        else:
            with pytest.raises(Exception):
                val.validate_s3_uri(uri)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
