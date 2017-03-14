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
import operator
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

try:
    from yum import compareEVR as cmp_evrs
except ImportError:
    def cmp_evrs(evr0, evr1):
        """Naive alternative implementation of yum.compareEVR by comparisons of
        epochs, versions and releases in this order.

        :param evr0, evr1: Tuples of (epoch, version, release)
        """
        (epoch0, ver0, rel0) = evr0
        (epoch1, ver1, rel1) = evr1

        if epoch0 == epoch1:
            return cmp(rel0, rel1) if ver0 == ver1 else cmp(ver0, ver1)
        else:
            return cmp(epoch0, epoch1)


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
_H2I_BASE = ord('a') - len(_C_DIGITS)  # needed to skip digits.


def _c2i(char):
    """
    :param char: a character of [:alnum:]
    :return: force-converted int from given `char` as str with 0 padding

    >>> (_c2i('0'), _c2i('1'), _c2i('9'))
    ('00', '01', '09')
    >>> _c2i('a')
    '10'
    >>> _c2i('z')
    '35'
    """
    if char in _C_DIGITS:
        return "0{}".format(char)
    else:
        return str(ord(char) - _H2I_BASE)


def _hex2int(hstr):
    """Force to convert a hexdigest string `hstr` to int.

    :param hstr: Hexdigest string only consists of chars of [:alnum:]
    """
    return int(''.join(_c2i(c) for c in hstr))


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


def _evr_from_package(pkg):
    """
    :return: Tuple of (epoch, version, release) of given package `pkg`
    """
    return operator.itemgetter("epoch", "version", "release")(pkg)


def cmp_packages(pkg0, pkg1):
    """
    Compare versions of given packages `pkg0` and `pkg1`.

    :param pkg0:
        Mapping object represents package info has keys, name, epoch, version, release,
        arch at least.
    :param pkg1: Likewise
    :return:
        0 (verisons are same) or 1 (`pkg0` is newer than `pkg1`) or -1 (`pkg0`
        is older than `pkg2`), or None with ValueError raised.
    """
    try:
        if pkg0["name"] != pkg1["name"]:
            raise ValueError("Compare versions of different packages! "
                             "pkg0=%r, pkg1=%r" % (pkg0, pkg1))

        if pkg0["arch"] != pkg1["arch"]:
            raise ValueError("Compare versions of packages w/ different archs! "
                             "pkg0=%r, pkg1=%r" % (pkg0, pkg1))

        return cmp_evrs(_evr_from_package(pkg0), _evr_from_package(pkg1))

    except (AttributeError, KeyError) as exc:
        raise ValueError("Wrong pkg object were/was given! exc=%r,\n"
                         "pkg0=%r, pkg1=%r" % (exc, pkg0, pkg1))


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
