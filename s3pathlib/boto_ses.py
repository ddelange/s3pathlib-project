# -*- coding: utf-8 -*-

from typing import Optional, TYPE_CHECKING

try:
    import boto3
except ImportError:  # pragma: no cover
    pass
except:  # pragma: no cover
    raise


class BotoSesManager:
    def __init__(
        self,
        boto_ses: Optional['boto3.session.Session'] = None,
    ):
        if boto_ses is None:  # pragma: no cover
            boto_ses = boto3.session.Session()
        self.boto_ses = boto_ses
        self._client_cache = dict()
        self._aws_account_id = None
        self._aws_region = None

    @property
    def aws_account_id(self) -> str:
        if self._aws_account_id is None:
            self._aws_account_id = self.sts_client.get_caller_identity()["Account"]
        return self._aws_account_id

    @property
    def aws_region(self) -> str:
        if self._aws_region is None:
            self._aws_region = self.boto_ses.region_name
        return self._aws_region

    def _get_client(self, service_name: str):
        try:
            return self._client_cache[service_name]
        except KeyError:
            client = self.boto_ses.client(service_name)
            self._client_cache[service_name] = client
            return client

    @property
    def s3_client(self):
        return self._get_client("s3")

    @property
    def sts_client(self):
        return self._get_client("sts")
