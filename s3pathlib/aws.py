# -*- coding: utf-8 -*-

from typing import Optional

try:
    import boto3
except ImportError:  # pragma: no cover
    pass
except:  # pragma: no cover
    raise


class Context:
    def __init__(self):
        self.boto_ses: Optional[boto3.session.Session] = None
        self._s3_client = None

    @property
    def s3_client(self):
        if self._s3_client is None:
            self._s3_client = self.boto_ses.client("s3")
        return self._s3_client


context = Context()
