Stateful S3 API
==============================================================================

.. contents::
    :class: this-will-duplicate-information-and-it-is-still-useful-here
    :depth: 1
    :local:


What is Stateful S3 API
------------------------------------------------------------------------------
Comparing to :ref:`Stateless S3 API <what-is-stateless-s3-api>`, **Stateful S3 API will change the state of the S3**. For example, upload a file, put object, copy object, delete object.

``s3pathlib`` provides a Pythonic way to work with


:class:`~s3pathlib.core.S3Path` is a OS path-liked object. So you should be able to, copy, move, delete, overwrite.

- :meth:`~s3pathlib.core.S3Path.copy_to`

- :meth:`~s3pathlib.core.S3Path.move_to`

- :meth:`~s3pathlib.core.S3Path.delete_if_exists`

- :meth:`~s3pathlib.core.S3Path.upload_file`

- :meth:`~s3pathlib.core.S3Path.upload_dir`