# -*- coding: utf-8 -*-

from typing import Tuple, List, Dict, Union, Optional
# from pathlib_mate import Path
from pathlib import PurePath
from . import utils


class S3Path:
    __slots__ = (
        "_bucket",
        "_parts",
        "_is_dir",
    )

    def __new__(
        cls,
        *args: Union[str, 'S3Path'],
    ) -> 'S3Path':
        return cls._from_parts(args)

    @classmethod
    def _from_parts(
        cls,
        args: List[Union[str, 'S3Path']],
        init: bool = True,
    ) -> 'S3Path':
        self = object.__new__(cls)
        self._bucket = None
        self._parts = list()
        self._is_dir = None

        if len(args) == 0:
            return self

        # resolve self._bucket
        arg = args[0]
        if isinstance(arg, str):
            utils.validate_s3_bucket(arg)
            parts = utils.split_parts(arg)
            self._bucket = parts[0]
            self._parts.extend(parts[1:])
        elif isinstance(arg, S3Path):
            self._bucket = arg._bucket
            self._parts.extend(arg._parts)
        else:
            raise TypeError

        # resolve self._parts
        for arg in args[1:]:
            if isinstance(arg, str):
                utils.validate_s3_key(arg)
                self._parts.extend(utils.split_parts(arg))
            elif isinstance(arg, S3Path):
                self._parts.extend(arg._parts)
            else:
                raise TypeError

        # resolve self._is_dir
        # inspect the last argument
        if isinstance(arg, str):
            self._is_dir = arg.endswith("/")
        elif isinstance(arg, S3Path):
            self._is_dir = arg._is_dir
        else:  # pragma: no cover
            raise TypeError

        if (self._bucket is not None) and len(self._parts) == 0:  # bucket root
            self._is_dir = True

        # init is needed
        if init:  # pragma: no cover
            self._init()
        return self

    @classmethod
    def _from_parsed_parts(
        cls,
        bucket: Optional[str],
        parts: List[str],
        is_dir: Optional[bool],
        init: bool = True,
    ) -> 'S3Path':
        self = object.__new__(cls)
        self._bucket = bucket
        self._parts = parts
        self._is_dir = is_dir
        if init:
            self._init()
        return self

    def _init(self):
        pass

    def to_dict(self) -> dict:
        return {
            "bucket": self._bucket,
            "parts": self._parts,
            "is_dir": self._is_dir,
        }

    def _is_empty(self) -> bool:
        return (self._bucket is None) and (len(self._parts) == 0)

    def is_dir(self) -> bool:
        if self._is_dir is None:
            raise ValueError
        else:
            return self._is_dir

    def is_file(self) -> bool:
        return not self.is_dir()

    def is_relpath(self) -> bool:
        return (self._bucket is None) and \
               (len(self._parts) != 0) and \
               (self._is_dir is not None)

    @property
    def uri(self) -> Optional[str]:
        """
        Return AWS S3 Uri.

        - if it is a directory, it always ends with ``"/"``.
        - if it is bucket only (no key), it returns ``"s3://{bucket}/"``
        - otherwise it returns ``"s3://{bucket}/{key}"``
        - if it is not an concrete S3Path, it returns ``None``
        """
        if self._bucket is None:
            return None
        if len(self._parts):
            return "s3://{}/{}{}".format(
                self._bucket,
                "/".join(self._parts),
                "/" if self._is_dir else ""
            )
        else:
            return "s3://{}/".format(self._bucket)

    @property
    def parent(self) -> Optional['S3Path']:
        """
        Return the parent s3 directory.

        - if current object is on s3 bucket root directory, it returns bucket
            root directory
        - it is always a directory (``s3path.is_dir() is True``)
        - if it is already s3 bucket root directory, it returns ``None``

        Examples::

            >>> S3Path("my-bucket", "my-folder", "my-file.json").parent.uri
            s3://my-bucket/my-folder/

            >>> S3Path("my-bucket", "my-folder", "my-subfolder/").parent.uri
            s3://my-bucket/my-folder/

            >>> S3Path("my-bucket", "my-folder").parent.uri
            s3://my-bucket/

            >>> S3Path("my-bucket", "my-file.json").parent.uri
            s3://my-bucket/
        """
        if len(self._parts) == 0:
            return None
        else:
            return self._from_parsed_parts(
                bucket=self._bucket,
                parts=self._parts[:-1],
                is_dir=True,
            )

    @property
    def basename(self) -> Optional[str]:
        if len(self._parts):
            return self._parts[-1]
        else:
            return None
