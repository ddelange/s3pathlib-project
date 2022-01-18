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

    def test_upload_dir(self):
        local_dir = os.path.join(dir_tests, "test_upload_dir")

        # regular upload
        utils.upload_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=f"{prefix}/s3_client_enhancement/test_upload_dir/",
            local_dir=local_dir,
            overwrite=True,
        )

        # upload to bucket root directory also works
        utils.upload_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix="",
            local_dir=local_dir,
            overwrite=True,
        )

        with pytest.raises(FileExistsError):
            utils.upload_dir(
                s3_client=s3_client,
                bucket=bucket,
                prefix=f"{prefix}/s3_client_enhancement/test_upload_dir",
                local_dir=local_dir,
                overwrite=False,
            )




    def test_iter_objects(self):
        # setup s3 directory
        utils.upload_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=f"{prefix}/s3_client_enhancement/test_iter_objects",
            local_dir=os.path.join(dir_tests, "test_iter_objects"),
            overwrite=True,
        )

        # invalid batch_size
        with pytest.raises(ValueError):
            list(utils.iter_objects(
                s3_client=None, bucket=None, prefix=None,
                batch_size=-1,
            ))

        # invalid batch_size
        with pytest.raises(ValueError):
            list(utils.iter_objects(
                s3_client=None, bucket=None, prefix=None,
                batch_size=9999,
            ))

        # batch_size < limit
        result = list(
            utils.iter_objects(
                s3_client=s3_client,
                bucket=bucket,
                prefix=f"{prefix}/s3_client_enhancement/test_iter_objects",
                batch_size=3,
                limit=5,
            )
        )
        assert len(result) == 5

        # batch_size > limit
        result = list(
            utils.iter_objects(
                s3_client=s3_client,
                bucket=bucket,
                prefix=f"{prefix}/s3_client_enhancement/test_iter_objects",
                batch_size=10,
                limit=3,
            )
        )
        assert len(result) == 3

        # limit >> total number objects,
        result = list(
            utils.iter_objects(
                s3_client=s3_client,
                bucket=bucket,
                prefix=f"{prefix}/s3_client_enhancement/test_iter_objects",
                batch_size=10,
            )
        )
        assert len(result) == 10

    def test_calculate_total_size(self):
        # prefix not exists
        _, total_size = utils.calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=f"{prefix}/s3_client_enhancement/test_count_objects",
        )
        assert total_size == 0

    def test_count_objects(self):
        # prefix not exists
        assert utils.count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=f"{prefix}/s3_client_enhancement/test_count_objects",
        ) == 0

    def test_delete_dir(self):
        # setup s3 directory
        utils.upload_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=f"{prefix}/s3_client_enhancement/test_delete_dir",
            local_dir=os.path.join(dir_tests, "test_iter_objects"),
            overwrite=True,
        )

        # before state
        assert utils.count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=f"{prefix}/s3_client_enhancement/test_delete_dir",
        ) == 10

        assert utils.calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=f"{prefix}/s3_client_enhancement/test_delete_dir",
        )[1] > 0

        # call api
        utils.delete_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=f"{prefix}/s3_client_enhancement/test_delete_dir",
        )

        # after state
        assert utils.count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=f"{prefix}/s3_client_enhancement/test_delete_dir",
        ) == 0

        assert utils.calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=f"{prefix}/s3_client_enhancement/test_delete_dir",
        )[1] == 0


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
