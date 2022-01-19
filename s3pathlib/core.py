# -*- coding: utf-8 -*-

"""
This module implements the core OOP interface :class:`S3Path`.

Import::

    >>> from s3pathlib.core import S3Path
    >>> from s3pathlib import S3Path
"""

from typing import Tuple, List, Iterable, Union, Optional, Any
from datetime import datetime
from pathlib_mate import Path

try:
    import botocore.exceptions
except ImportError:  # pragma: no cover
    pass
except:  # pragma: no cover
    raise

from . import utils
from .aws import context


class S3Path:
    """
    Similar to ``pathlib.Path``. An objective oriented programming interface
    for AWS S3 object or logical directory.

    You can use this class

    1. pure s3 object / directory path manipulation without actually
        talking to AWS API.
    2. method that

    **Constructor**

    The :class:`S3Path` itself is a constructor. It takes ``str`` and other
    relative :class:`S3Path` as arguments.

    1. The first argument defines the S3 bucket
    2. The rest of arguments defines the path parts of the S3 key
    3. The final argument defines the type whether is a file (object) or
        a directory

    First let's create a S3 object path from string::

        # first argument becomes the bucket
        >>> s3path = S3Path("bucket", "folder", "file.txt")
        # print S3Path object gives you info in S3 URI format
        >>> s3path
        S3Path('s3://bucket/folder/file.txt')

        # last argument defines that it is a file
        >>> s3path.is_file()
        True
        >>> s3path.is_dir()
        True

        # "/" separator will be automatically handled
        >>> S3Path("bucket", "folder/file.txt")
        S3Path('s3://bucket/folder/file.txt')

        >>> S3Path("bucket/folder/file.txt")
        S3Path('s3://bucket/folder/file.txt')

    Then let's create a S3 directory path::

        >>> s3path= S3Path("bucket/folder/")
        >>> s3path
        S3Path('s3://bucket/folder/')

        # last argument defines that it is a directory
        >>> s3path.is_dir()
        True
        >>> s3path.is_file()
        True
    """
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

    def _init(self) -> None:
        """
        Additional instance initialization
        """
        pass

    @classmethod
    def make_relpath(
        cls,
        *parts: str,
    ):
        """
        A construct method that create a relative S3 Path.

        Definition of relative path:

        - no bucket (self.bucket is None)
        - has some parts or no part. when no part, it is a special relative path.
            Any path add this relative path resulting to itself. We call this
            special relative path **Void relative path**. A
            **Void relative path** is logically equivalent to **Void s3 path**.
        - relative path can be a file (object) or a directory. The
            **Void relative path** is neither a file or a directory.

        :param parts:
        :return:

        **中文文档**

        相对路径的概念是 p1 + p2 = p3, 其中 p1 和 p3 都是实际存在的路径, 而 p2 则是
        相对路径.

        相对路径的功能是如果 p3 - p1 = p2, 那么 p1 + p2 必须还能等于 p3. 有一个特殊情况是
        如果 p1 - p1 = p0, 两个相同的绝对路径之间的相对路径是 p0, 我们还是需要满足
        p1 + p0 = p1 以保证逻辑上的一致.
        """
        _parts = list()
        last_non_empty_part = None
        for part in parts:
            chunks = utils.split_parts(part)
            if len(chunks):
                last_non_empty_part = part
            for _part in chunks:
                _parts.append(_part)

        if len(_parts):
            _is_dir = last_non_empty_part.endswith("/")
        else:
            _is_dir = None

        return cls._from_parsed_parts(
            bucket=None,
            parts=_parts,
            is_dir=_is_dir,
        )

    # --------------------------------------------------------------------------
    #                Method that DOESN't need boto3 API call
    # --------------------------------------------------------------------------
    def to_dict(self) -> dict:
        return {
            "bucket": self._bucket,
            "parts": self._parts,
            "is_dir": self._is_dir,
        }

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

    def is_void(self) -> bool:
        return (self._bucket is None) and (len(self._parts) == 0)

    def is_dir(self) -> bool:
        if self._is_dir is None:
            return False
        else:
            return self._is_dir

    def is_file(self) -> bool:
        if self._is_dir is None:
            return False
        else:
            return not self._is_dir

    def is_bucket(self) -> bool:
        return (self._bucket is not None) and \
               (len(self._parts) == 0) and \
               (self._is_dir is True)

    def is_relpath(self) -> bool:
        """
        Relative path is a special path supposed to join with other concrete
        path.

        :return:
        """
        if self._bucket is None:
            if len(self._parts) == 0:
                if self._is_dir is None:
                    return True
                else:
                    return False
            else:
                return True
        else:
            return False

    @property
    def bucket(self) -> Optional[str]:
        """
        Return bucket name as string, if available.

        Example::

            >>> S3Path("bucket/folder/file.txt").bucket
            'bucket'
        """
        return self._bucket

    @property
    def key(self) -> Optional[str]:
        """
        Return object or directory key as string, if available.

        Examples::

            # a s3 object
            >>> S3Path("bucket/folder/file.txt").key
            'folder/file.txt'

            # a s3 object
            >>> S3Path("bucket/folder/").key
            'folder/file.txt'

            # a relative path
            >>> S3Path("bucket/folder/file.txt").relative_to(S3Path("bucket")).key
            'folder/file.txt

            >>> S3Path("bucket/folder/").relative_to(S3Path("bucket")).key
            'folder/'

            # an empty S3Path
            >>> S3Path().key
            ''
        """
        if len(self._parts):
            return "{}{}".format(
                "/".join(self._parts),
                "/" if self._is_dir else ""
            )
        else:
            return ""

    @property
    def uri(self) -> Optional[str]:
        """
        Return AWS S3 URI.

        - for regular s3 object, it returns ``"s3://{bucket}/{key}"``
        - if it is a directory, the s3 uri always ends with ``"/"``.
        - if it is bucket only (no key), it returns ``"s3://{bucket}/"``
        - if it is not an concrete S3Path, it returns ``None``
        - it has to have bucket, if not (usually because it is an relative path)
            it returns ``None``

        Examples::

            >>> S3Path("bucket", "folder", "file.txt").uri
            's3://bucket/folder/file.txt'

            >>> S3Path("bucket", "folder/").uri
            's3://bucket/folder/'

            >>> S3Path("bucket").uri
            's3://bucket/'

            >>> S3Path().uri
            None

            >>> S3Path("bucket/folder/file.txt").relative_to(S3Path("bucket")).uri
            None
        """
        if self._bucket is None:
            return None
        if len(self._parts):
            return "s3://{}/{}".format(
                self.bucket,
                self.key,
            )
        else:
            return "s3://{}/".format(self._bucket)

    @property
    def console_url(self) -> Optional[str]:
        """
        Return an AWS S3 Console url that can inspect the details.
        """
        uri = self.uri
        if uri is None:
            return None
        else:
            console_url = utils.make_s3_console_url(s3_uri=uri)
            return console_url

    @property
    def arn(self) -> Optional[str]:
        """
        Return an AWS S3 Resource ARN.
        """
        if self._bucket is None:
            return None
        if len(self._parts):
            return "arn:aws:s3:::{}/{}".format(
                self.bucket,
                self.key,
            )
        else:
            return "arn:aws:s3:::{}".format(self._bucket)

    @property
    def parts(self) -> List[str]:
        """
        Provides sequence-like access to the components in the filesystem path.
        """
        return self._parts

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
            return self
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

    @property
    def abspath(self) -> str:
        """
        The Unix styled absolute path if it is a S3 object / S3 directory /
        S3 bucket.

        Example::

            >>>
        """
        if self._bucket is None:
            raise TypeError("relative path doesn't have absolute path!")
        if self.is_dir():
            return "/" + "/".join(self._parts) + "/"
        elif self.is_file():
            return "/" + "/".join(self._parts)
        else:  # pragma: no cover
            raise TypeError

    def __repr__(self):
        classname = self.__class__.__name__
        if self.is_relpath():
            key = self.key
            if len(key):
                return "{}('{}')".format(classname, key)
            else:
                return "{}()".format(classname)
        else:
            uri = self.uri
            return "{}('{}')".format(classname, uri)

    def __str__(self):
        return self.__repr__()

    def relative_to(self, other: 'S3Path') -> 'S3Path':
        """
        Return the relative path to another path. If the operation
        is not possible (because this is not a sub path of the other path),
        raise ValueError.

        Examples::

            >>> S3Path("bucket", "a/b/c").relative_to(S3Path("bucket", "a")).parts
            ['b', 'c']

            >>> S3Path("bucket", "a").relative_to(S3Path("bucket", "a")).parts
            []

            >>> S3Path("bucket", "a").relative_to(S3Path("bucket", "a/b/c")).parts
            ValueError ...

        :param other:

        :return: an relative path object, which is a special version of S3Path
        """
        if (self._bucket != other._bucket) or \
            (self._bucket is None) or \
            (other._bucket is None):
            raise ValueError

        n = len(other._parts)
        if self._parts[:n] != other._parts:
            msg = "{} does not start with {}".format(
                self.uri,
                other.uri,
            )
            raise ValueError(msg)
        rel_parts = self._parts[n:]
        if len(rel_parts):
            is_dir = self._is_dir
        else:
            is_dir = None
        return self._from_parsed_parts(
            bucket=None,
            parts=rel_parts,
            is_dir=is_dir,
        )

    def ensure_object(self) -> None:
        """
        A validator method that ensure it represents a S3 object.
        """
        if self.is_file() is not True:
            raise TypeError(f"S3 URI: {self.uri} is not a valid s3 object!")

    def ensure_dir(self) -> None:
        """
        A validator method that ensure it represents a S3 object.
        """
        if self.is_dir() is not True:
            raise TypeError(f"S3 URI: {self.uri} is not a valid s3 directory!")

    def ensure_not_relpath(self) -> None:
        """
        A validator method that ensure it represents a S3 relative path.

        Can be used if you want to raise error if it is not an relative path.
        """
        if self.is_relpath() is True:
            raise TypeError(f"S3 URI: {self.uri} is not a valid s3 relative path!")

    # --------------------------------------------------------------------------
    #                   Method that need boto3 API call
    # --------------------------------------------------------------------------
    def clear_cache(self) -> None:
        """
        Clear all cache that stores S3 state information.
        """
        self._meta = None

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
        if self.is_file() is not True:  # pragma: no cover
            raise TypeError("it has to be an file (not dir) for ``exists()`` check")
        try:
            # refresh cache if possible
            dct = self._head_object()
            if "ResponseMetadata" in dct:
                del dct["ResponseMetadata"]
            self._meta = dct
            return True
        except botocore.exceptions.ClientError as e:
            if "Not Found" in str(e):
                return False
            else:  # pragma: no cover
                raise e
        except:  # pragma: no cover
            raise

    def ensure_not_exists(self) -> None:
        if self.exists():
            utils.raise_file_exists_error(self.uri)

    def upload_file(
        self,
        path: str,
        overwrite: bool = False,
    ):
        p = Path(path)
        if overwrite is False:
            self.ensure_not_exists()
        return context.s3_client.upload_file(
            p.abspath,
            Bucket=self.bucket,
            Key=self.key
        )

    def upload_dir(
        self,
        local_dir: str,
        pattern: str = "**/*",
        overwrite: bool = False,
    ):
        return utils.upload_dir(
            s3_client=context.s3_client,
            bucket=self.bucket,
            prefix=self.key,
            local_dir=local_dir,
            pattern=pattern,
            overwrite=overwrite,
        )

    def iter_objects(
        self,
        batch_size: int = 1000,
        limit: int = None,
    ) -> Iterable['S3Path']:
        for dct in utils.iter_objects(
            s3_client=context.s3_client,
            bucket=self.bucket,
            prefix=self.key,
            batch_size=batch_size,
            limit=limit,
        ):
            p = S3Path(self.bucket, dct["Key"])
            p._meta = {
                "Key": dct["Key"],
                "LastModified": dct["LastModified"],
                "ETag": dct["ETag"],
                "Size": dct["Size"],
                "StorageClass": dct["StorageClass"],
                "Owner": dct.get("Owner", {}),
            }
            yield p

    def calculate_total_size(self) -> Tuple[int, int]:
        self.ensure_dir()
        return utils.calculate_total_size(
            s3_client=context.s3_client,
            bucket=self.bucket,
            prefix=self.key
        )

    def count_objects(self) -> int:
        """
        Count how many objects are under this s3 directory.
        """
        self.ensure_dir()
        return utils.count_objects(
            s3_client=context.s3_client,
            bucket=self.bucket,
            prefix=self.key
        )

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
                kwargs = dict(
                    Bucket=self.bucket,
                    Key=self.key,
                )
                additional_kwargs = utils.collect_not_null_kwargs(
                    MFA=mfa,
                    VersionId=version_id,
                    RequestPayer=request_payer,
                    BypassGovernanceRetention=bypass_governance_retention,
                    ExpectedBucketOwner=expected_bucket_owner,
                )
                kwargs.update(additional_kwargs)
                res = context.s3_client.delete_object(**kwargs)
                return 1
            else:
                return 0
        elif self.is_dir():
            return utils.delete_dir(
                s3_client=context.s3_client,
                bucket=self.bucket,
                prefix=self.key,
                mfa=mfa,
                request_payer=request_payer,
                bypass_governance_retention=bypass_governance_retention,
                expected_bucket_owner=expected_bucket_owner,
            )
        else:  # pragma: no cover
            raise ValueError

    def copy_file(
        self,
        dst: 'S3Path',
        overwrite: bool = False,
    ) -> dict:
        """
        Copy an S3 directory to a different S3 directory, including all
        sub directory and files.

        :param dst: copy to s3 object, it has to be an object
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.

        :return: number of object are copied, 0 or 1.
        """
        self.ensure_object()
        dst.ensure_object()
        self.ensure_not_relpath()
        dst.ensure_not_relpath()
        if overwrite is False:
            dst.ensure_not_exists()
        return context.s3_client.copy_object(
            Bucket=dst.bucket,
            Key=dst.key,
            CopySource={
                "Bucket": self.bucket,
                "Key": self.key
            }
        )

    def copy_dir(
        self,
        dst: 'S3Path',
        overwrite: bool = False,
    ):
        """
        Copy an S3 directory to a different S3 directory, including all
        sub directory and files.

        :param dst: copy to s3 directory, it has to be a directory
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.

        :return: number of objects are copied
        """
        self.ensure_dir()
        dst.ensure_dir()
        self.ensure_not_relpath()
        dst.ensure_not_relpath()
        todo: List[Tuple[S3Path, S3Path]] = list()
        for p_src in self.iter_objects():
            p_relpath = p_src.relative_to(self)
            p_dst = S3Path(dst, p_relpath)
            todo.append((p_src, p_dst))

        if overwrite is False:
            for p_src, p_dst in todo:
                p_dst.ensure_not_exists()

        for p_src, p_dst in todo:
            p_src.copy_file(p_dst, overwrite=True)

        return len(todo)

    def copy_to(
        self,
        dst: 'S3Path',
        overwrite: bool = False,
    ) -> int:
        """
        Copy s3 object or s3 directory from one place to another place.

        :param dst: copy to s3 path
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.
        """
        if self.is_dir():
            return self.copy_dir(
                dst=dst,
                overwrite=overwrite,
            )
        elif self.is_file():
            self.copy_file(
                dst=dst,
                overwrite=overwrite,
            )
            return 1
        else:  # pragma: no cover
            raise TypeError

    def move_to(
        self,
        dst: 'S3Path',
        overwrite: bool = False,
    ) -> int:
        """
        Move s3 object or s3 directory from one place to another place. It is
        firstly :meth:`S3Path.copy_to` then :meth:`S3Path.delete_if_exists`

        :param dst: copy to s3 path
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.
        """
        count = self.copy_to(
            dst=dst,
            overwrite=overwrite,
        )
        self.delete_if_exists()
        return count
