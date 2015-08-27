# -*- coding: utf-8 -*-
#
# This file is part of xrootdfs
# Copyright (C) 2015 CERN.
#
# xrootdfs is free software; you can redistribute it and/or modify it under the
# terms of the Revised BSD License; see LICENSE file for more details.

"""Test of XRootDOpener."""

from __future__ import absolute_import, print_function, unicode_literals

from fixture import mkurl, tmppath
from fs.opener import opener
from xrootdfs import XRootDFile, XRootDFS, XRootDOpener


def test_parse(tmppath):
    """Test parse."""
    rooturl = mkurl(tmppath)
    fs, path = opener.parse(rooturl + "/data")
    assert fs
    assert path == "data"


def test_opendir(tmppath):
    """Test opendir."""
    rooturl = mkurl(tmppath)
    fs = opener.opendir(rooturl + "/data")
    assert fs.listdir()


def test_open():
    """."""
    # fsfile = opener.open('root://localhost/foo/bar/README')
