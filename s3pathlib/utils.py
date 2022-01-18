# -*- coding: utf-8 -*-

import typing


def split_s3_uri(
    s3_uri: str,
) -> typing.Tuple[str, str]:
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


def split_parts(key) -> typing.List[str]:
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
    parts: typing.List[str],
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
