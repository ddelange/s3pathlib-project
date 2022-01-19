Stateless S3 API
==============================================================================

.. contents::
    :class: this-will-duplicate-information-and-it-is-still-useful-here
    :depth: 1
    :local:


.. _what-is-stateless-s3-api:

What is Stateless S3 API
------------------------------------------------------------------------------
There are lots of `AWS S3 API <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`_ available. But some of them only retrieve information from the server, but never change the state of the S3 bucket. No bucket, files are moved, changed, deleted. So called **Stateless S3 API**.

For example, you can use stateless S3 api to get the metadata of the object.


S3 Object Metadata
------------------------------------------------------------------------------
See definition of server side object metadata here: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html.

- :attr:`~s3pathlib.core.S3Path.etag`
- :attr:`~s3pathlib.core.S3Path.last_modified_at`
- :attr:`~s3pathlib.core.S3Path.size`
- :attr:`~s3pathlib.core.S3Path.size_for_human`
- :attr:`~s3pathlib.core.S3Path.version_id`
- :attr:`~s3pathlib.core.S3Path.expire_at`

.. note::

    S3 object metadata are cached and only one API call is used. If you want to get the latest server side value, you can call :meth:`~s3pathlib.core.S3Path.clear_cache()` method and then moving forward.

    .. code-block:: python
    
        >>> p = S3Path("bucket", "file.txt")
        >>> p.etag
        'aaa...'

        >>> # you did something like put_object
        >>> p.clear_cache()
        >>> p.etag
        'bbb...'


Exists
------------------------------------------------------------------------------
You can test if

- For **S3 bucket**: check if the bucket exists. If you don't have the access, then it raise exception.
- For **S3 object**: check if the object exists
- For **S3 directory**: since S3 directory is a logical concept and never physically exists. It returns True only if there is at least one object under this directory (prefix)
- You cannot check existence for Void path and Relative path.

Example:

.. code-block:: python

    # check if bucket exists
    >>> S3Path("bucket").exists()

    # check if object exists
    >>> S3Path("bucket", "folder/file.txt").exists()

    # check if directory has at least one file
    >>> S3Path("bucket", "folder/").exists()
