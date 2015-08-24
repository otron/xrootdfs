# -*- coding: utf-8 -*-
#
# This file is part of xrootdfs
# Copyright (C) 2015 CERN.
#
# xrootdfs is free software; you can redistribute it and/or modify it under the
# terms of the Revised BSD License; see LICENSE file for more details.

"""."""

from __future__ import absolute_import, print_function, unicode_literals

import fs
import fs.filelike
from fs.path import basename, dirname
from XRootD.client import File as XFile
from XRootD.client.flags import OpenFlags

from .utils import translate_file_mode_to_flags


class XRootDFile(fs.filelike.FileLikeBase):
    """
    This class understands and will accept the following mode strings,
    with any additional characters being ignored:

        * r    - open the file for reading only.
        * r+   - open the file for reading and writing.
        * r-   - open the file for streamed reading; do not allow seek/tell.
        * w    - open the file for writing only; create the file if
                 it doesn't exist; truncate it to zero length.
        * w+   - open the file for reading and writing; create the file
                 if it doesn't exist; truncate it to zero length.
        * w-   - open the file for streamed writing; do not allow seek/tell.
        * a    - open the file for writing only; create the file if it
                 doesn't exist; place pointer at end of file.
        * a+   - open the file for reading and writing; create the file
                 if it doesn't exist; place pointer at end of file.

    These are mostly standard except for the "-" indicator, which has
    been added for efficiency purposes in cases where seeking can be
    expensive to simulate (e.g. compressed files).  Note that any file
    opened for both reading and writing must also support seeking.
    """

    def __init__(self, path, mode='r', buffering=-1, encoding=None,
                 errors=None, newline=None, line_buffering=False, **kwargs):
        """."""
        super(XRootDFile, self).__init__()
        # set ._file to empty xrootd.client.File-object.
        self._file = XFile()
        self._ifp = 0
        self._path = path
        # self._flags = flags
        self._mode = mode
        self._timeout = 0
        self._callback = None

        # flag translation
        flags = 0
        if 'r' in self._mode:
            flags += OpenFlags.READ

        status, response = self._file.open(path, flags)
        # todo: raise appropriate errors

    def seek(self, offset, whence=0):
        """."""
        self._ifp = offset

    def read(self, size=-1):
        """."""
        if size == -1:
            size = 0
        if not self._file.is_open():
            status, res = self._file.open(self._path, self._flags, self._mode,
                                          self._timeout, self._callback)
            print(status)

        status, res = self._file.read(offset=self._ifp, size=size,
                                      timeout=self._timeout,
                                      callback=self._callback)
        return res

    def tell():
        """."""
        pass

    def truncate(size=None):
        """."""
        pass

    def write(string):
        """."""
        pass
