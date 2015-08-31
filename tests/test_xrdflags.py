# -*- coding: utf-8 -*-
#
# This file is part of xrootdfs
# Copyright (C) 2015 CERN.
#
# xrootdfs is free software; you can redistribute it and/or modify it under the
# terms of the Revised BSD License; see LICENSE file for more details.

"""Test of XRootD File Flags."""

from __future__ import absolute_import, print_function, unicode_literals

from os.path import join

import pytest
from fixture import mkurl, tmppath
from fs.errors import FSError, PathError
from XRootD import client as xclient
from XRootD.client.flags import OpenFlags
from xrootdfs.utils import is_valid_path, spliturl, \
    translate_file_mode_to_flags


# If "test" is in its name then pytest picks it up.
def tstfile_a(p):
    fname = 'data/testa.txt'
    with open(join(p, fname)) as f:
        fconts = f.read()

    return fname, fconts


def test_READ(tmppath):
    fname, fconts = tstfile_a(tmppath)
    ffpath = join(tmppath, fname)
    xf = xclient.File()
    xf.open(mkurl(ffpath), OpenFlags.READ)
    assert xf

    # Can we read?
    statmsg, content = xf.read()
    assert content == fconts
    assert statmsg.ok

    # Can we write?
    statmsg, res = xf.write('chhhh-eck it')
    print((statmsg, ))  # Print returned status in case the test fails.
    assert xf.read()[1] == content
    assert not statmsg.ok
    assert statmsg.error

    # Can we truncate?
    statmsg, res = xf.truncate(0)
    print((statmsg, ))
    assert not statmsg.ok
    assert statmsg.error


def test_APPEND(tmppath):
    fname, fconts = tstfile_a(tmppath)
    ffpath = join(tmppath, fname)
    xf = xclient.File()
    xf.open(mkurl(ffpath), OpenFlags.APPEND)
    assert xf

    # Can we read?
    statmsg, content = xf.read()
    assert content == fconts
    assert statmsg.ok

    # Can we write?
    statmsg, res = xf.write('chhhh-eck it')
    assert xf.read()[1] == content
    print((statmsg, ))
    assert not statmsg.ok
    assert statmsg.error


def test_UPDATE(tmppath):
    fname, fconts = tstfile_a(tmppath)
    ffpath = join(tmppath, fname)
    xf = xclient.File()
    xf.open(mkurl(ffpath), OpenFlags.UPDATE)
    assert xf

    # Can we read?
    statmsg, content = xf.read()
    assert content == fconts
    assert statmsg.ok

    # Can we write?
    statmsg, res = xf.write('chhhh-eck it')
    # assert xf.read()[1] == content
    # doesn't truncate file
    print((statmsg, res))
    assert statmsg.ok
    assert not statmsg.error

    # Can we truncate?
    statmsg, res = xf.truncate(0)
    print((statmsg, res))
    assert statmsg.ok
    assert not statmsg.error
    assert xf.read()[1] == ""


def test_DELETE(tmppath):
    fname, fconts = tstfile_a(tmppath)
    ffpath = join(tmppath, fname)
    xf = xclient.File()
    xf.open(mkurl(ffpath), OpenFlags.DELETE)
    assert xf
    statmsg, res = xf.read()
    print((statmsg, res))
    assert statmsg.ok
    assert not statmsg.error
    assert res == ""

    # Can we write now?
    newc = "whaat"
    statmsg, res = xf.write(newc)
    print((statmsg, res))
    assert statmsg.ok
    assert not statmsg.error
    assert xf.read()[1] == newc
    print(xf.read())
