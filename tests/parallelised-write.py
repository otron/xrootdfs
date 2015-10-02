# -*- coding: utf-8 -*-
#
# This file is part of xrootdfs
# Copyright (C) 2015 CERN.
#
# xrootdfs is free software; you can redistribute it and/or modify it under the
# terms of the Revised BSD License; see LICENSE file for more details.

"""Can we write to the same file on EOS from n threads from the same client
simultaneously?"""

from __future__ import absolute_import, print_function

import cProfile
import sys
import os
import pstats
import shutil
import StringIO
import tempfile
import threading
from os.path import join
from zlib import crc32

from xrootdfs import XRootDFile, XRootDFS


class XRDWriteThread(threading.Thread):
    def __init__(self, src, startpt, size, dest):
        super(XRDWriteThread, self).__init__()
        self.startpt = startpt
        self.size = size
        self.src = src
        self.contents = None
        self.dest = dest

    def _readfile(self):
        f = open(self.src, 'rb')
        f.seek(self.startpt)
        self.contents = f.read(self.size)
        f.close()
        # print("Have read {0} bytes -- [{1}, {2}]".format(self.size, self.startpt, self.startpt+self.size))

    def _openfile(self):
        xrdfs = XRootDFS(os.path.dirname(self.dest))
        mode = 'r+' if xrdfs.exists(os.path.basename(self.dest)) else 'w+'
        xrdfs.close()
        return XRootDFile(self.dest, mode)

    def _writefile(self):
        # xrdf = self._openfile()
        xrdf = XRootDFile(self.dest, 'r+')
        # print("Opening file and seeking to ", self.startpt)
        xrdf.seek(self.startpt)
        xrdf.write(self.contents)
        xrdf.close()
        # print("File closed!")

    def run(self):
        if self.contents is None:
            self._readfile()
        self._writefile()


def teardown(tmppath):
    """Tear down performance test."""
    shutil.rmtree(tmppath)


def setup(size=10):
    """Setup test path for tests."""
    tmppath = tempfile.mkdtemp()
    filepath = join(tmppath, "testfile")

    # Create test file with random data
    os.system("openssl rand -out {0} -hex {1}".format(
       filepath, size))

    return tmppath, filepath


def profile_start():
    """Start profiling code."""
    pr = cProfile.Profile()
    pr.enable()
    return pr


def profile_end(pr):
    """Write profile output."""
    pr.disable()
    s = StringIO.StringIO()
    sortby = 'tottime'
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    print(s.getvalue())


def create_threads(count, src, target):
    """Create `count` XRDWriteThread instances."""
    # Get size of src
    file_size = os.path.getsize(src)
    sizes = [file_size//count for x in range(0, count)]
    # Is it cleanly divisible by `count`?
    sizes[-1] += file_size % count
    sps = [(file_size//count)*i for i in range(0, count)]

    # print("Startpt, size arr:\n", [(sps[i], sizes[i]) for i in range(0, count)])

    return [XRDWriteThread(src, sps[i], sizes[i], target)
            for i in range(0, count)]


def multithread_write(src, target, count):
    """Attempt to write to the same file from two threads."""
    threads = create_threads(count, src, target)
    xrdf = XRootDFile(target, 'w')
    xrdf.close()
    # print("Created ", len(threads), " threads.")
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    with open(src) as f:
        lc = f.read()
        # print(lc)
        csum_local = crc32(lc)

    xf = XRootDFile(target)
    rc = xf.read()
    # print(rc)
    csum_remote = crc32(rc)
    xf.close()

    print(csum_local == csum_remote)


def single_write(fsize, filepath, url):
    remote_path = join(url, 'testfile2')

    print("Doing write test w/ file size = {0} kB".format(fsize))
    target = XRootDFile(remote_path, 'w+')
    local_file = open(filepath, 'rb')
    # ps = profile_start()
    content = local_file.read()
    target.write(content)
    # profile_end(ps)

    # Read file and verify crc32
    local_crc32 = crc32(content)
    target.seek(0)
    remote_crc32 = crc32(target.read())
    print(local_crc32 == remote_crc32)
    target.close()


def main():
    """Entry point."""
    fsize = 10000
    tmppath, testfilepath = setup(fsize)
    print("fsize reported as {0}".format(os.path.getsize(testfilepath)))

    # url = 'root://eosuser.cern.ch//eos/user/o/otrondru'
    url = 'root://localhost/' + tmppath
    try:
        multithread_write(testfilepath, join(url, 'testfile2'), 3)
        # single_write(fsize, testfilepath, url)
    finally:
        teardown(tmppath)

def trace(frame, event, arg):
    print("{0}, {1}:{2}".format(event, frame.f_code.co_filename, frame.f_lineno))
    return trace

if __name__ == "__main__":
    # sys.settrace(trace)
    main()
