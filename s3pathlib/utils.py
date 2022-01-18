# -*- coding: utf-8 -*-

import hashlib
from typing import Tuple, List, Dict, Iterable, Optional, Any
from pathlib_mate import Path

try:
    import botocore.exceptions
except ImportError:  # pragma: no cover
    pass
except:  # pragma: no cover
    raise


def split_s3_uri(
    s3_uri: str,
) -> Tuple[str, str]:
    """
    Split AWS S3 URI, returns bucket and key.

    :param s3_uri: example, ``"s3://my-bucket/my-folder/data.json"``
    """
    parts = s3_uri.split("/")
    bucket = parts[2]
    key = "/".join(parts[3:])
    return bucket, key


def join_s3_uri(
    bucket: str,
    key: str,
) -> str:
    """
    Join AWS S3 URI from bucket and key.

    :param bucket: example, ``"my-bucket"``
    :param key: example, ``"my-folder/data.json"`` or ``"my-folder/"``
    """
    return "s3://{}/{}".format(bucket, key)


def split_parts(key) -> List[str]:
    """
    Split s3 key parts using "/" delimiter.

    Example::

        >>> split_parts("a/b/c")
        ["a", "b", "c"]
        >>> split_parts("//a//b//c//")
        ["a", "b", "c"]
    """
    return [part for part in key.split("/") if part]


def smart_join_s3_key(
    parts: List[str],
    is_dir: bool,
) -> str:
    """
    Note, it assume that there's no such double slack in your path. It ensure
    that there's only one consecutive "/" in the s3 key.

    :param parts: list of s3 key path parts, could have "/"
    :param is_dir: if True, the s3 key ends with "/". otherwise enforce no
        tailing "/".

    Example::

        >>> smart_join_s3_key(parts=["/a/", "b/", "/c"], is_dir=True)
        a/b/c/
        >>> smart_join_s3_key(parts=["/a/", "b/", "/c"], is_dir=False)
        a/b/c
    """
    new_parts = list()
    for part in parts:
        new_parts.extend(split_parts(part))
    key = "/".join(new_parts)
    if is_dir:
        return key + "/"
    else:
        return key


def make_s3_console_url(
    bucket: str = None,
    prefix: str = None,
    s3_uri: str = None,
) -> str:
    """
    Return an AWS Console url that you can use to open it in your browser.

    :param bucket: example, ``"my-bucket"``
    :param prefix: example, ``"my-folder/"``
    :param s3_uri: example, ``"s3://my-bucket/my-folder/data.json"``

    Example::

        >>> make_s3_console_url(s3_uri="s3://my-bucket/my-folder/data.json")
        https://s3.console.aws.amazon.com/s3/object/my-bucket?prefix=my-folder/data.json
    """
    if s3_uri is None:
        if not ((bucket is not None) and (prefix is not None)):
            raise ValueError
    else:
        if not ((bucket is None) and (prefix is None)):
            raise ValueError
        bucket, prefix = split_s3_uri(s3_uri)

    if prefix.endswith("/"):
        s3_type = "buckets"
    else:
        s3_type = "object"
    return "https://s3.console.aws.amazon.com/s3/{s3_type}/{bucket}?prefix={prefix}".format(
        s3_type=s3_type,
        bucket=bucket,
        prefix=prefix
    )


def ensure_s3_object(
    s3_key_or_uri: str,
) -> None:
    """
    Raise exception if the string is not in valid format for a AWS S3 object
    """
    if s3_key_or_uri.endswith("/"):
        raise ValueError("'{}' doesn't represent s3 object!".format(s3_key_or_uri))


def ensure_s3_dir(
    s3_key_or_uri: str
) -> None:
    """
    Raise exception if the string is not in valid format for a AWS S3 directory
    """
    if not s3_key_or_uri.endswith("/"):
        raise ValueError("'{}' doesn't represent s3 dir!".format(s3_key_or_uri))


def validate_s3_bucket(bucket):
    """
    Ref:
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    """
    pass


def validate_s3_key(key):
    """
    Ref:
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-guidelines
    """
    pass


MAGNITUDE_OF_DATA = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]


def repr_data_size(
    size_in_bytes: int,
    precision: int = 2,
) -> str:  # pragma: no cover
    """
    Return human readable string represent of a file size. Doesn't support
    size greater than 1EB.

    For example:

    - 100 bytes => 100 B
    - 100,000 bytes => 97.66 KB
    - 100,000,000 bytes => 95.37 MB
    - 100,000,000,000 bytes => 93.13 GB
    - 100,000,000,000,000 bytes => 90.95 TB
    - 100,000,000,000,000,000 bytes => 88.82 PB
    ...

    Magnitude of data::

        1000         kB    kilobyte
        1000 ** 2    MB    megabyte
        1000 ** 3    GB    gigabyte
        1000 ** 4    TB    terabyte
        1000 ** 5    PB    petabyte
        1000 ** 6    EB    exabyte
        1000 ** 7    ZB    zettabyte
        1000 ** 8    YB    yottabyte
    """
    if size_in_bytes < 1024:
        return "%s B" % size_in_bytes

    index = 0
    while 1:
        index += 1
        size_in_bytes, mod = divmod(size_in_bytes, 1024)
        if size_in_bytes < 1024:
            break
    template = "{0:.%sf} {1}" % precision
    s = template.format(size_in_bytes + mod / 1024.0, MAGNITUDE_OF_DATA[index])
    return s


def hash_binary(
    b: bytes,
    hash_meth: callable,
) -> str:  # pragma: no cover
    """
    Get the hash of a binary object.

    :param b: binary object
    :param hash_meth: callable hash method, example: hashlib.md5

    :return: hash value in hex digits.
    """
    m = hash_meth()
    m.update(b)
    return m.hexdigest()


def md5_binary(
    b: bytes,
) -> str:  # pragma: no cover
    """
    Get the md5 hash of a binary object.

    :param b: binary object

    :return: hash value in hex digits.
    """
    return hash_binary(b, hashlib.md5)


def sha256_binary(
    b: bytes,
) -> str:  # pragma: no cover
    """
    Get the md5 hash of a binary object.

    :param b: binary object

    :return: hash value in hex digits.
    """
    return hash_binary(b, hashlib.sha256)


DEFAULT_CHUNK_SIZE = 1 << 6


def hash_file(
    abspath: str,
    hash_meth: callable,
    nbytes: int = 0,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> str:  # pragma: no cover
    """
    Get the hash of a file on local drive.

    :param abspath: absolute path of the file
    :param hash_meth: callable hash method, example: hashlib.md5
    :param nbytes: only hash first nbytes of the file
    :param chunk_size: internal option, stream chunk_size of the data for hash
        each time, avoid high memory usage.

    :return: hash value in hex digits.
    """
    if nbytes < 0:
        raise ValueError("nbytes cannot smaller than 0")
    if chunk_size < 1:
        raise ValueError("nbytes cannot smaller than 1")
    if (nbytes > 0) and (nbytes < chunk_size):
        chunk_size = nbytes

    m = hash_meth()
    with open(abspath, "rb") as f:
        if nbytes:  # use first n bytes
            have_reads = 0
            while True:
                have_reads += chunk_size
                if have_reads > nbytes:
                    n = nbytes - (have_reads - chunk_size)
                    if n:
                        data = f.read(n)
                        m.update(data)
                    break
                else:
                    data = f.read(chunk_size)
                    m.update(data)
        else:  # use entire content
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                m.update(data)

    return m.hexdigest()


# --------------------------------------------------------------------------
#                      boto3 s3 client enhancement
# --------------------------------------------------------------------------

def exists(
    s3_client,
    bucket: str,
    key: str,
) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if "Not Found" in str(e):
            return False
        else:
            raise e
    except:
        raise


def upload_dir(
    s3_client,
    bucket: str,
    prefix: str,
    local_dir: str,
    pattern: str = "**/*",
    overwrite: bool = False,
):
    """
    Recursively upload a local directory and files in its sub directory.

    :param local_dir: absolute path of the
    :param bucket: s3 bucket name
    :param prefix: the s3 prefix (logic directory) you want to upload to
    :param overwrite: if False, non of the file will be upload / overwritten
        if any of target s3 location already taken.

    Ref:

    - pattern: https://docs.python.org/3/library/pathlib.html#pathlib.Path.glob
    """
    if prefix.endswith("/"):
        prefix = prefix[:-1]

    p_local_dir = Path(local_dir)

    if p_local_dir.is_file():  # pragma: no cover
        raise TypeError

    if p_local_dir.exists() is False:  # pragma: no cover
        raise FileNotFoundError

    # list of (local file path, target s3 key)
    todo: List[Tuple[str, str]] = list()
    for p in p_local_dir.glob(pattern):
        if p.is_file():
            relative_path = p.relative_to(p_local_dir)
            key = "{}/{}".format(prefix, "/".join(relative_path.parts))
            todo.append((p.abspath, key))

    # make sure all target s3 location not exists
    if overwrite is False:
        error_msg = (
            "cannot copy {} to s3://{}/{}, "
            "s3 object ALREADY EXISTS! "
            "open console for more details {}."
        )
        for abspath, key in todo:
            if exists(s3_client, bucket, key) is True:
                console_url = make_s3_console_url(bucket=bucket, prefix=key)
                raise FileExistsError(
                    error_msg.format(
                        abspath, bucket, key, console_url
                    )
                )

    # execute upload
    for abspath, key in todo:
        s3_client.upload_file(abspath, bucket, key)


def iter_objects(
    s3_client,
    bucket: str,
    prefix: str,
    # pattern: str,
    limit: int = None,
) -> Iterable[dict]:
    if limit is None:
        limit = (1 << 31) - 1
    batch_size = 1000
    next_token: Optional[str] = None
    count: int = 0
    while 1:
        kwargs = dict(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=batch_size,
        )
        if next_token is not None:
            kwargs["ContinuationToken"] = next_token
        res = s3_client.list_objects_v2(**kwargs)
        contents = res.get("Contents", [])
        n_objects = len(contents)
        count += n_objects
        if count <= limit:
            for dct in contents:
                yield dct
        else:
            first_n_only = batch_size - (count - limit)
            for dct in contents[:first_n_only]:
                yield dct
            count = limit
            break
        next_token = res.get("NextContinuationToken")
        if next_token is None:
            break
    print(count)

def delete_dir(
    s3_client,
    bucket: str,
    prefix: str,
    # pattern,
    limit: int = None,
):
    if limit is None:
        limit = (1 << 31) - 1
    batch_size = 1000
    next_token: Optional[str] = None
    count: int = 0
    to_delete_keys: List[str] = list()
    while 1:
        kwargs = dict(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=batch_size,
        )
        if next_token is not None:
            kwargs["ContinuationToken"] = next_token
        res = s3_client.list_objects_v2(**kwargs)
        contents = res.get("Contents", [])
        n_objects = len(contents)
        count += n_objects
        if count <= limit:
            for dct in contents:
                to_delete_keys.append(dct)
        else:
            first_n_only = batch_size - (count - limit)
            for dct in contents[:first_n_only]:
                to_delete_keys.append(dct)
            count = limit
            break
        next_token = res.get("NextContinuationToken")
        if next_token is None:
            break
    print(count)
    print(len(to_delete_keys))
    # print(len(res.get("Contents", [])))
    # print(res.get("NextContinuationToken"))
