# -*- coding: utf-8 -*-
#
# This file is part of xrootdfs
# Copyright (C) 2015 CERN.
#
# xrootdfs is free software; you can redistribute it and/or modify it under the
# terms of the Revised BSD License; see LICENSE file for more details.

"""Test of XRootDFS."""

from __future__ import absolute_import, print_function, unicode_literals

import os
from os.path import exists, join

import pytest
from fixture import mkurl, tmppath
from fs.errors import BackReferenceError, DestinationExistsError, \
    DirectoryNotEmptyError, InvalidPathError, ResourceInvalidError
from xrootdfs import XRootDFile, XRootDFS
from xrootdfs.utils import spliturl


def test_init(tmppath):
    """Test initialization."""

    fname = 'testa.txt'
    fpath = 'data/'
    fcontents = 'testa.txt'
    full_fpath = join(tmppath, fpath, fname)
    xfile = XRootDFile(full_fpath)
    assert xfile
    assert type(xfile == XRootDFile)
