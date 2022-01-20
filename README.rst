.. image:: https://readthedocs.org/projects/s3pathlib/badge/?version=latest
    :target: https://s3pathlib.readthedocs.io/en/latest/
    :alt: Documentation Status

.. image:: https://github.com/MacHu-GWU/s3pathlib-project/workflows/CI/badge.svg
    :target: https://github.com/MacHu-GWU/s3pathlib-project/actions?query=workflow:CI

.. image:: https://codecov.io/gh/MacHu-GWU/s3pathlib-project/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/MacHu-GWU/s3pathlib-project

.. image:: https://img.shields.io/pypi/v/s3pathlib.svg
    :target: https://pypi.python.org/pypi/s3pathlib

.. image:: https://img.shields.io/pypi/l/s3pathlib.svg
    :target: https://pypi.python.org/pypi/s3pathlib

.. image:: https://img.shields.io/pypi/pyversions/s3pathlib.svg
    :target: https://pypi.python.org/pypi/s3pathlib

.. image:: https://img.shields.io/badge/STAR_Me_on_GitHub!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/s3pathlib-project

------


.. image:: https://img.shields.io/badge/Link-Document-blue.svg
    :target: https://s3pathlib.readthedocs.io/en/latest/

.. image:: https://img.shields.io/badge/Link-API-blue.svg
    :target: https://s3pathlib.readthedocs.io/en/latest/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Source_Code-blue.svg
    :target: https://s3pathlib.readthedocs.io/en/latest/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Install-blue.svg
    :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
    :target: https://github.com/MacHu-GWU/s3pathlib-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
    :target: https://github.com/MacHu-GWU/s3pathlib-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
    :target: https://github.com/MacHu-GWU/s3pathlib-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
    :target: https://pypi.org/pypi/s3pathlib#files


Welcome to ``s3pathlib`` Documentation
==============================================================================

``s3pathlib`` is the python package provides the objective oriented programming (OOP) interface to manipulate AWS S3 object / directory. The api is similar to ``pathlib`` and very intuitive for human.

.. note::

    You may not viewing the full document

    The full document is here: https://s3pathlib.readthedocs.io/en/latest/

.. contents::
    :class: this-will-duplicate-information-and-it-is-still-useful-here
    :depth: 1
    :local:


Examples
------------------------------------------------------------------------------
**Create a S3Path**

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

**Attributes**

.. code-block:: python

    # create an instance
    >>> p = S3Path("bucket", "folder", "file.txt")

    >>> p.bucket
    'bucket'

    >>> p.key
    'folder/file.txt'

    >>> p.parts
    ['folder', 'file.txt']

    >>> p.uri
    's3://bucket/folder/file.txt'

    # click to preview it in AWS console
    >>> p.console_url
    'https://s3.console.aws.amazon.com/s3/object/bucket?prefix=folder/file.txt'

    >>> p.arn
    'arn:aws:s3:::bucket/folder/file.txt'

**File System Liked API**

.. code-block:: python

    # create an instance
    >>> p = S3Path("bucket", "folder", "file.txt")

    >>> p.basename
    'file.txt'

    >>> p.fname
    'file'

    >>> p.ext
    '.txt'

    >>> p.dirname
    'folder'

    >>> p.abspath
    '/folder/file.txt'

    >>> p.parent
    S3Path('s3://bucket/folder/')


**Get Metadata / Statistics Information from S3**

From now, we need really invoke some AWS S3 API under the hood. We need to **configure the authentication first**. For a comprehensive guide to configure a valid AWS API session, you can find it `here <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/session.html>`_.

``s3pathlib`` maintains a globally available ``Context`` object. Once you configured a boto session and attach to the ``Context`` object, the underlying AWS S3 API call will automatically use it.

.. code-block:: python

    >>> import boto3
    >>> from s3pathlib import context
    >>> context.attach_boto_session(
    ...     boto3.session.Session(
    ...         region_name="us-east-1",
    ...         profile_name="my_aws_profile",
    ...     )
    ... )

Now, let's get some information about a S3 object:

.. code-block:: python

    # create an instance
    >>> p = S3Path("bucket", "folder", "file.txt")

    >>> p.etag
    '3e20b77868d1a39a587e280b99cec4a8'

    >>> p.size
    56789000

    >>> p.size_for_human
    '51.16 MB'

    # create an folder
    >>> p = S3Path("bucket", "datalake/")

    >>> p.count_objects()
    7164 # number of files under this prefix

    >>> p.calculate_total_size()
    (7164, 236483701963) # 7164 objects, 220.24 GB

    >>> p.calculate_total_size(for_human=True)
    (7164, '220.24 GB') # 7164 objects, 220.24 GB


**Upload, Copy, Move, Delete**

Native S3 Write API (those operation that change the state of S3) only operate on object level. And the `list_objects <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2>`_ API returns 1000 objects at a time. You need additional effort to manipulate objects recursively.

``s3pathlib`` **CAN SAVE YOUR LIFE**

.. code-block:: python

    # create a S3 folder
    >>> p = S3Path("bucket", "github", "repos", "my-repo/")

    # upload all python file from /my-github-repo to s3://bucket/github/repos/my-repo/
    >>> p.upload_dir("/my-repo", pattern="**/*.py", overwrite=False)

    # copy entire s3 folder to another s3 folder
    >>> p2 = S3Path("bucket", "github", "repos", "another-repo/")
    >>> p1.copy_to(p2, overwrite=True)

    # delete all objects in the folder, recursively, to clean up your test bucket
    >>> p.delete_if_exists()
    >>> p2.delete_if_exists()


Getting Help
------------------------------------------------------------------------------
Please use the ``python-s3pathlib`` tag on Stack Overflow to get help.

Submit a ``I want help`` issue tickets on `GitHub Issues <https://github.com/MacHu-GWU/s3pathlib-project/issues/new/choose>`_


.. _install:

Install
------------------------------------------------------------------------------

``s3pathlib`` is released on PyPI, so all you need is:

.. code-block:: console

    $ pip install s3pathlib

To upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade s3pathlib