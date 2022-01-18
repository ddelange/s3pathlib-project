# -*- coding: utf-8 -*-

import os
import pytest

from s3pathlib import utils
from s3pathlib.tests import s3_client, bucket, prefix

dir_here = os.path.dirname(os.path.abspath(__file__))
dir_tests = dir_here


class TestS3ClientEnhancement:

    @classmethod
    def setup_class(cls):
        s3_client.put_object(
            Bucket=bucket,
            Key=f"{prefix}/s3_client_enhancement/hello.txt",
            Body="Hello World!"
        )

    def test_exists(self):
        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=f"{prefix}/s3_client_enhancement/hello.txt",
        ) is True

        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=f"{prefix}/s3_client_enhancement/",
        ) is False

        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=f"{prefix}/s3_client_enhancement",
        ) is False

        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=f"{prefix}/s3_client_enhancement/never-exists",
        ) is False

    # def test_upload_dir(self):
    #     local_dir = os.path.join(dir_tests, "test_upload_dir")
    #
    #     utils.upload_dir(
    #         s3_client=s3_client,
    #         bucket=bucket,
    #         prefix=f"{prefix}/s3_client_enhancement/test_upload_dir/",
    #         local_dir=local_dir,
    #         overwrite=True,
    #     )
    #
    #     with pytest.raises(FileNotFoundError):
    #         utils.upload_dir(
    #             s3_client=s3_client,
    #             bucket=bucket,
    #             prefix=f"{prefix}/s3_client_enhancement/test_upload_dir",
    #             local_dir=local_dir,
    #             overwrite=False,
    #         )

    def test_iter_objects(self):
        for dct in utils.iter_objects(
            s3_client=s3_client,
            bucket=bucket,
            # prefix=f"{prefix}/s3_client_enhancement/test_delete_dir/"
            prefix="poc",
            limit=2300,
        ):
            print(dct)
    # def test_delete_dir(self):
    #     local_dir = os.path.join(dir_tests, "test_delete_dir")
    #
    #     utils.upload_dir(
    #         s3_client=s3_client,
    #         bucket=bucket,
    #         prefix=f"{prefix}/s3_client_enhancement/test_delete_dir/",
    #         local_dir=local_dir,
    #         overwrite=True,
    #     )
    #
    #     utils.delete_dir(
    #         s3_client=s3_client,
    #         bucket=bucket,
    #         # prefix=f"{prefix}/s3_client_enhancement/test_delete_dir/"
    #         prefix="poc",
    #         limit=2300,
    #     )

if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
