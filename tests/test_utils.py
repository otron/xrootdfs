# -*- coding: utf-8 -*-
#
# This file is part of xrootdfs
# Copyright (C) 2015 CERN.
#
# xrootdfs is free software; you can redistribute it and/or modify it under the
# terms of the Revised BSD License; see LICENSE file for more details.

"""Test of XRootDFS utils."""

from __future__ import absolute_import, print_function, unicode_literals

import pytest
from fs.errors import FSError, PathError
from XRootD.client.flags import OpenFlags
from xrootdfs.utils import is_valid_path, spliturl, \
    translate_file_mode_to_flags


def test_spliturl():
    """Test spliturl."""
    root, path, args = spliturl("root://eosuser.cern.ch//eos/user/")
    assert root == "root://eosuser.cern.ch"
    assert path == "//eos/user/"

    root, path, args = spliturl("root://eosuser.cern.ch//")
    assert root == "root://eosuser.cern.ch"
    assert path == "//"

    root, path, arg = spliturl("root://eosuser.cern.ch//eos?xrd.wantprot=krb5")
    assert root == "root://eosuser.cern.ch"
    assert path == "//eos"
    assert arg == "xrd.wantprot=krb5"

    root, path, arg = spliturl("root://localhost//")
    assert root == "root://localhost"
    assert path == "//"
    assert arg == ""


def test_is_valid_path():
    assert is_valid_path("//")
    assert is_valid_path("//something/wicked/this/tub/comes/")
    assert is_valid_path("//every/time")
    assert not is_valid_path("")
    assert not is_valid_path("/")
    assert not is_valid_path("///")
    assert not is_valid_path("//missing//what")


def test_translate_file_mode_to_flags():
    assert translate_file_mode_to_flags('r') == OpenFlags.READ
    assert translate_file_mode_to_flags('r-') == OpenFlags.READ
    assert bool(translate_file_mode_to_flags('r+') & OpenFlags.UPDATE)

    assert translate_file_mode_to_flags('a') == OpenFlags.APPEND

    assert bool(translate_file_mode_to_flags('w') &
                OpenFlags.DELETE + OpenFlags.UPDATE)
