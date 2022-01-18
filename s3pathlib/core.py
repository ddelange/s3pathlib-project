# -*- coding: utf-8 -*-

from typing import Tuple, List, Dict, Union, Optional, Any
from datetime import datetime

try:
    import botocore.exceptions
except ImportError:  # pragma: no cover
    pass
except:  # pragma: no cover
    raise

try:
    import smart_open
except ImportError:  # pragma: no cover
    pass
except:  # pragma: no cover
    raise

from . import utils
from .aws import context


class S3Path:
    __slots__ = (
        "_bucket",
        "_parts",
        "_is_dir",
        "_meta",  # s3 object metadata cache object
    )

    # --------------------------------------------------------------------------
    #               Objective Oriented Programming Implementation
    # --------------------------------------------------------------------------
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
        self._meta = None

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
        self._meta = None
        if init:
            self._init()
        return self

    def _init(self):
        pass

    def copy(self) -> 'S3Path':
        """
        Create an duplicate copy of S3Path object that logically equals to
        this one, but is actually different identity in memory. Also the
        cache data are cleared.
        """
        return self._from_parsed_parts(
            bucket=self._bucket,
            parts=list(self._parts),
            is_dir=self._is_dir,
        )

    def clear_cache(self) -> None:
        """
        Clear all cache that stores S3 state information.
        """
        self._meta = None

    # --------------------------------------------------------------------------
    #                Method that DOESN't need boto3 API call
    # --------------------------------------------------------------------------
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

        - if it is a directory, the s3 uri always ends with ``"/"``.
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
    def bucket(self) -> Optional[str]:
        """
        Return bucket name as string, if available.
        """
        s3_uri = self.uri
        if s3_uri is None:
            return None
        return utils.split_s3_uri(s3_uri)[0]

    @property
    def key(self) -> Optional[str]:
        """
        Return object or directory key as string, if available.
        """
        s3_uri = self.uri
        if s3_uri is None:
            return None
        return utils.split_s3_uri(s3_uri)[1]

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

    @property
    def dirname(self) -> Optional[str]:
        return self.parent.basename

    @property
    def fname(self) -> str:
        """
        The final path component, minus its last suffix (file extension).
        Only if it is not a directory.
        """
        if self.is_dir():
            raise TypeError
        basename = self.basename
        if basename is None:
            raise ValueError
        i = basename.rfind(".")
        if 0 < i < len(basename) - 1:
            return basename[:i]
        else:
            return basename

    @property
    def ext(self) -> str:
        """
        The final component's last suffix, if any. Usually it is the file
        extension. Only if it is not a directory.
        """
        if self.is_dir():
            raise TypeError
        basename = self.basename
        if basename is None:
            raise ValueError
        i = basename.rfind(".")
        if 0 < i < len(basename) - 1:
            return basename[i:]
        else:
            return ""

    def ensure_object(self) -> None:
        """
        A validator method that ensure it represents a S3 object.
        """
        if self.is_file() is False:
            raise TypeError(f"S3 URI: {self.uri} is not a valid s3 object!")

    # --------------------------------------------------------------------------
    #                   Method that need boto3 API call
    # --------------------------------------------------------------------------
    def _head_object(self) -> dict:
        return context.s3_client.head_object(
            Bucket=self.bucket,
            Key=self.key
        )

    def _get_meta_value(
        self,
        key: str,
        default: Any = None,
    ) -> Any:
        if self._meta is None:
            dct = self._head_object()
            if "ResponseMetadata" in dct:
                del dct["ResponseMetadata"]
            self._meta = dct
        return self._meta.get(key, default)

    @property
    def etag(self) -> str:
        """
        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
        """
        return self._get_meta_value(key="ETag")[1:-1]

    @property
    def last_modified_at(self) -> datetime:
        """
        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
        """
        return self._get_meta_value(key="LastModified")

    @property
    def size(self) -> int:
        """
        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
        """
        return self._get_meta_value(key="ContentLength")

    @property
    def version_id(self) -> int:
        """
        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
        """
        return self._get_meta_value(key="VersionId")

    @property
    def expire_at(self) -> int:
        """
        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
        """
        return self._get_meta_value(key="Expires")

    @property
    def metadata(self) -> dict:
        """
        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
        """
        return self._get_meta_value(key="Metadata", default=dict())

    # --------------------------------------------------------------------------
    #            Method that change the state of S3 bucket / objects
    # --------------------------------------------------------------------------
    def exists(self) -> bool:
        """
        """
        try:
            self._get_meta_value(key="")
            return True
        except botocore.exceptions.ClientError as e:
            if "Not Found" in str(e):
                return False
            else:
                raise e
        except:
            raise

    def delete_if_exists(
        self,
        mfa: str = None,
        version_id: str = None,
        request_payer: str = None,
        bypass_governance_retention: bool = None,
        expected_bucket_owner: str = None,
    ):
        """
        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object
        """
        if self.is_file():
            if self.exists():
                return 1
            else:
                return 0
        elif self.is_dir():
            pass
        else:
            raise ValueError

    # def select_file(self):