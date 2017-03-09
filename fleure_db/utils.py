#
# -*- coding: utf-8 -*-
#
# Copyright (C) 2017 Satoru SATOH <ssato@redhat.com>
# Copyright (C) 2017 Red Hat, Inc.
# License: MIT
#
"""fleure DB - utilities
"""
from __future__ import absolute_import

import datetime
import itertools
import os.path
import subprocess


def timestamp(dtobj=False):
    """Generate timestamp, formatted date and time.

    :param dtobj: object :func:`~datetime.datetime` returns
    :return: str represents the formatted date and time

    >>> timestamp(datetime.datetime(2017, 3, 9, 11, 45, 9))
    '2017-03-09:11:45:09'
    """
    return (dtobj if dtobj else datetime.datetime.now()).strftime("%F:%T")


def is_dnf_available():
    """Is dnf available instead of yum?
    """
    return os.path.exists("/etc/dnf")


def make_cache(repos, options, root=os.path.sep):
    """
    :param repos: List of repo IDs
    :param options: List of options passed to dnf/yum command
    :param root: Root dir in which cachdir, e.g. /var/cache/dnf/, exists

    :raises: :class:`~subprocess.CalledProcessError` may be raised on failure
    """
    yum_cmd = "/usr/bin/dnf" if is_dnf_available() else "/usr/bin/yum"
    ropts = itertools.chain.from_iterable(("--enablerepo", r) for r in repos)
    cmd = [yum_cmd, "makecache", "--installroot", os.path.abspath(root),
           "--disablerepo", "*"] + list(ropts) + options
    subprocess.check_call(cmd)

# vim:sw=4:ts=4:et:
