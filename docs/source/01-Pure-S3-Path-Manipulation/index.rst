Pure S3 Path Manipulation
==============================================================================

.. contents::
    :class: this-will-duplicate-information-and-it-is-still-useful-here
    :depth: 1
    :local:


What is Pure S3 Path
------------------------------------------------------------------------------
Pure S3 Path is just a python object representing an AWS S3 bucket, object, folder, etc. It doesn't call ANY AWS API and also doesn't mean that the corresponding S3 object exists.


Different Type of S3 Path
------------------------------------------------------------------------------
S3 Path is a logical concept. It can map to different AWS S3 concepts. Here is the list of common S3 Path type.

1. **Classic AWS S3 object**: representing ``s3://bucket/folder/file.txt``
2. **Logical AWS S3 directory**: representing ``s3://bucket/folder/``
3. **AWS S3 bucket**: representing ``s3://bucket/``
4. **Relative Path**: for example, the relative path from ``s3://bucket/folder/file.txt`` to ``s3://bucket/`` **IS** ``folder/file.txt``
5. **Void S3 Path**: no bucket, no key, no nothing.


Construct a S3 Path
------------------------------------------------------------------------------
**The most intuitive way would be from string**, here's a Classic AWS S3 object example:

.. code-block:: python

    # import
    >>> from s3pathlib import S3Path

    # construct from string, auto join parts
    >>> p = S3Path("bucket", "folder", "file.txt")

    # print
    >>> p
    S3Path('s3://bucket/folder/file.txt')

    # construct from full path also works
    >>> S3Path("bucket/folder/file.txt")
    S3Path('s3://bucket/folder/file.txt')

**Directory is only logical concept in AWS S3**. AWS uses ``"/"`` as the path delimiter in S3 key. A folder CAN NOT exist alone without any S3 object in it. For example ``s3://bucket/folder/`` is invalid unless ``s3://bucket/folder/some-file.txt`` exists.

**Here's a Logical AWS S3 directory example, use** ``"/"`` **at the end to indicate that it is a directory**:

.. code-block:: python

    # construct from string, auto join parts
    >>> S3Path("bucket", "folder", "subfolder/")
    S3Path('s3://bucket/folder/subfolder/')

In the above examples, you can see that **the first argument in the constructor is always interpreted as the bucket in** ``s3pathlib``.


S3 Path Attributes
------------------------------------------------------------------------------
:class:`~s3pathlib.core.S3Path` is immutable and hashable. These attributes doesn't need AWS boto3 API call and generally available. For attributes like :attr:`~s3pathlib.core.S3Path.etag`, :attr:`~s3pathlib.core.S3Path.size` that need API call, see :ref:`SOMESECTION`

.. code-block:: python

    # create an instance
    >>> p = S3Path("bucket", "folder", "file.txt")


- :attr:`~s3pathlib.core.S3Path.bucket`

.. code-block:: python

    >>> p.bucket
    'bucket'

- :attr:`~s3pathlib.core.S3Path.key`

.. code-block:: python

    >>> p.key
    'folder/file.txt'

- :attr:`~s3pathlib.core.S3Path.uri`, `unique resource identifier <https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html>`_

.. code-block:: python

    >>> p.uri
    's3://bucket/folder/file.txt'

- :attr:`~s3pathlib.core.S3Path.console_url`, open console to preview

.. code-block:: python

    >>> p.console_url
    'https://s3.console.aws.amazon.com/s3/object/bucket?prefix=folder/file.txt'

- :attr:`~s3pathlib.core.S3Path.console_url`, `aws resource namespace <https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>`_

.. code-block:: python

    >>> p.console_url
    'arn:aws:s3:::bucket/folder/file.txt'

Logically a :class:`~s3pathlib.core.S3Path` is also a file system like object. So it should have those **file system concepts** too:

.. code-block:: python

    # create an instance
    >>> p = S3Path("bucket", "folder", "file.txt")

- :attr:`~s3pathlib.core.S3Path.basename`, the file name with extension.

.. code-block:: python

    >>> p.basename
    'file.txt'

- :attr:`~s3pathlib.core.S3Path.fname`, file name without file extension.

.. code-block:: python

    >>> p.fname
    'file'

- :attr:`~s3pathlib.core.S3Path.ext`, file extension, if available

.. code-block:: python

    >>> p.ext
    '.txt'

- :attr:`~s3pathlib.core.S3Path.dirname`, the basename of the parent directory

.. code-block:: python

    >>> p.dirname
    'folder'

- :attr:`