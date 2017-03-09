#
# -*- coding: utf-8 -*-
#
# Copyright (C) 2017 Satoru SATOH <ssato@redhat.com>
# Copyright (C) 2017 Red Hat, Inc.
# License: GPLv3+
#
"""fleure DB CLI frontend.
"""
from __future__ import absolute_import

import argparse
import logging
import os.path
import os
import sys

import fleure_db.utils
import fleure_db.create


LOG = logging.getLogger(__name__)
LOG.addHandler(logging.StreamHandler())
LOG.setLevel(logging.INFO)

_LOG_LEVELS = (logging.WARNING, logging.INFO, logging.DEBUG)


def make_parser():
    """Parse arguments.
    """
    tstamp = fleure_db.utils.timestamp()
    defaults = dict(repos=[], outdir="out-{}".format(tstamp), root=os.path.sep,
                    makecache=False, yum="dnf", verbosity=0)
    psr = argparse.ArgumentParser()
    psr.set_defaults(**defaults)

    add_arg = psr.add_argument
    add_arg("-M", "--makecache", action="store_true",
            help="Specify this if to make cache in advance")
    add_arg("-O", "--outdir",
            help="Dir to save outputs. Created if not exist. "
                 "[{outdir}]".format(**defaults))
    add_arg("-R", "--root",
            help="Root dir to search for updateinfo.xml.gz cached by yum "
                 "or dnf in advance or on demand with -M/--makecache "
                 "option. [{root}]".format(**defaults))
    add_arg("-r", "--repo", dest="repos", action="append",
            help="Yum repo to fetch errata info, e.g. 'rhel-x86_64-server-6'. "
                 "It can be given multiple times to specify multiple yum "
                 "repos. If any repos are not given by this option, repos are "
                 "guess from data in RPM DBs automatically, and please not "
                 "that any other repos are disabled if this option was set.")
    add_arg("-Y", "--yum", help="Specify yum command to run [%default]")
    add_arg("-v", "--verbose", action="count", dest="verbosity",
            help="Verbose mode")
    add_arg("-D", "--debug", action="store_const", dest="verbosity",
            const=2, help="Debug mode (same as -vv)")
    add_arg("subcmd", choices=("makecache", "create"))

    return psr


def main(argv=None):
    """Cli main.
    """
    if argv is None:
        argv = sys.argv[1:]

    psr = make_parser()
    args = psr.parse_args(argv)

    LOG.setLevel(_LOG_LEVELS[args.verbosity])

    if not args.repos:
        psr.print_help()
        sys.exit(0)

    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)

    if args.makecache or args.subcmd == "makecache":
        fleure_db.utils.make_cache(args.repos,
                                   ["--verbose" if args.verbose else
                                    "--quiet"],
                                   root=args.root, yum_cmd=args.yum)
    if args.subcmd == "create":
        fleure_db.create.convert_uixmlgzs(args.repos, args.outdir,
                                          root=args.root)


if __name__ == '__main__':
    main()

# vim:sw=4:ts=4:et:
