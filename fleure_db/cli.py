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
import datetime
import logging
import os.path
import os
import sys

import anyconfig
import fleure_db.globals
import fleure_db.create
import fleure_db.utils


LOG = logging.getLogger(fleure_db.globals.PACKAGE)
LOG.addHandler(logging.StreamHandler())
LOG.setLevel(logging.INFO)

_LOG_LEVELS = (logging.WARNING, logging.INFO, logging.DEBUG)


def load_configuration(conf_path=fleure_db.globals.FLEURE_DB_SYSCONF):
    """
    :param conf_path: Configuration dir or file or glob files pattern
    :return: Mapping object holding configurations
    """
    if os.path.isdir(conf_path):
        conf_path = os.path.join(conf_path, "*")

    return anyconfig.load(conf_path)


def make_parser():
    """Parse arguments.
    """
    tstamp = str(fleure_db.utils.timestamp()).replace(':', '_')
    defaults = dict(conf=None, repos=[], outdir="out-{}".format(tstamp),
                    root=os.path.sep, makecache=False, analyze=False,
                    verbosity=0)
    psr = argparse.ArgumentParser()
    psr.set_defaults(**defaults)

    add_arg = psr.add_argument
    add_arg("-C", "--conf",
            help="Specify configuration file[s] with a file or dir path or "
                 "glob pattern [{conf}]".format(**defaults))
    add_arg("-M", "--makecache", action="store_true",
            help="Specify this if to make cache in advance")
    add_arg("-A", "--analyze", action="store_true",
            help="Do some exntended analysis also")
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
    add_arg("-v", "--verbose", action="count", dest="verbosity",
            help="Verbose mode")
    add_arg("-D", "--debug", action="store_const", dest="verbosity",
            const=2, help="Debug mode (same as -vv)")
    add_arg("subcmd", help="Sub commands are m[akecache], c[reate]")

    return psr


def main(argv=None):
    """Cli main.
    """
    start = datetime.datetime.now()
    if argv is None:
        argv = sys.argv[1:]

    psr = make_parser()
    args = psr.parse_args(argv)

    LOG.setLevel(_LOG_LEVELS[args.verbosity])

    if args.conf:
        LOG.info("Load configuration from: %s", args.conf)
        cnf = load_configuration(args.conf)
        if cnf:
            psr.set_defaults(**cnf)
            args = psr.parse_args(argv)  # Re-parse args with new default.

    if not args.repos:
        psr.print_help()
        sys.exit(0)

    if args.makecache or args.subcmd.startswith("m"):  # makecache
        vopt = "--verbose" if args.verbosity else "--quiet"
        fleure_db.utils.make_cache(args.repos, [vopt], root=args.root)

    if args.subcmd.startswith("c"):  # create
        if not os.path.exists(args.outdir):
            os.makedirs(args.outdir)

        fleure_db.create.convert_uixmlgzs(args.repos, args.outdir,
                                          root=args.root,
                                          analyze=args.analyze)
    end = datetime.datetime.now()
    LOG.info("Ended: %s elapsed.", end - start)


if __name__ == '__main__':
    main()

# vim:sw=4:ts=4:et:
