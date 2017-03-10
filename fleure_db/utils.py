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
import hashlib
import itertools
import os.path
import subprocess

try:
    CHAIN_FROM_ITR = itertools.chain.from_iterable
except AttributeError:
    # Borrowed from library doc, 9.7.1 Itertools functions:
    def _from_iterable(iterables):
        for itr in iterables:
            for element in itr:
                yield element

    CHAIN_FROM_ITR = _from_iterable


def timestamp(dtobj=False):
    """Generate timestamp, formatted date and time.

    :param dtobj: object :func:`~datetime.datetime` returns
    :return: str represents the formatted date and time

    >>> timestamp(datetime.datetime(2017, 3, 9, 11, 45, 9))
    '2017-03-09:11:45:09'
    """
    return (dtobj if dtobj else datetime.datetime.now()).strftime("%F:%T")


def sgroupby(items, kfn, kfn2=None):
    """
    itertools.groupby + sorted

    :param items: Iterable object, e.g. a list, a tuple, etc.
    :param kfn: Key function to sort `items` and group it
    :param kfn2: Key function to sort each group in result

    :return: A generator to yield items in `items` grouped by `kf`

    >>> from operator import itemgetter
    >>> items = [(1, 2, 10), (3, 4, 2), (3, 2, 1), (1, 10, 5)]
    >>> list(sgroupby(items, itemgetter(0)))
    [[(1, 2, 10), (1, 10, 5)], [(3, 4, 2), (3, 2, 1)]]
    >>> list(sgroupby(items, itemgetter(0), itemgetter(2)))
    [[(1, 10, 5), (1, 2, 10)], [(3, 2, 1), (3, 4, 2)]]
    """
    return (list(g) if kfn2 is None else sorted(g, key=kfn2) for _k, g
            in itertools.groupby(sorted(items, key=kfn), kfn))


_C_DIGITS = "0 1 2 3 4 5 6 7 8 9".split()
_H2I_BASE = ord('a') - len(_C_DIGITS) - 1  # needed to skip digits.


def _c2i(char):
    """
    :param char: a character of [:alnum:]
    :return: force-converted int from given `char`

    >>> (_c2i('0'), _c2i('1'), _c2i('9'))
    (0, 1, 9)
    >>> _c2i('a')
    11
    >>> _c2i('z')
    36
    """
    return int(char) if char in _C_DIGITS else ord(char) - _H2I_BASE


def _hex2int(hstr):
    """Force to convert a hexdigest string `hstr` to int.

    :param hstr: Hexdigest string only consists of chars of [:alnum:]
    """
    return int(''.join("{:0>2}".format(_c2i(c)) for c in hstr))


def gen_id_for_values(values):
    """
    :param values: List of objct can become str
    :return: Integer represents 'ID' value to identify `values` somehow
    """
    # How long it should be?
    vals_s = ' '.join(str(v).encode('utf-8') for v in values)
    digest = hashlib.sha256(vals_s.encode('utf8')).hexdigest()
    return _hex2int(digest[:8])


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
    ropts = CHAIN_FROM_ITR(("--enablerepo", r) for r in repos)
    cmd = [yum_cmd, "makecache", "--installroot", os.path.abspath(root),
           "--disablerepo", "*"] + list(ropts) + options
    subprocess.check_call(cmd)

# vim:sw=4:ts=4:et:
