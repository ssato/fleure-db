#
# Copyright (C) 2013 Satoru SATOH <ssato@redhat.com>
# Copyright (C) 2013 - 2017 Red Hat, Inc.
# License: GPLv3+
#
"""Functions to make datasets.
"""
from __future__ import absolute_import

import logging
import tablib

from fleure_db.globals import _


LOG = logging.getLogger(__name__)


def _fmt_bzs(bzs, summary=False):
    """
    :param cves: List of CVE dict {cve, score, url, metrics} or str "cve".
    :return: List of CVE strings
    """
    def fmt(bze):
        """bugzilla entry formatter"""
        return ("bz#%(id)s: "
                "%(summary)s " if summary and "summary" in bze else ""
                "(%(url)s)")
    try:
        bzs = [fmt(bz) % bz for bz in bzs]
    except KeyError:
        LOG.warn(_("BZ Key error: %s"), str(bzs))

    return bzs


def _make_cell_data(obj, key, default="N/A"):
    """Make up cell data.
    """
    if key == "cves":
        return ", ".join("%(title)s" % cve for cve in obj.get(key, []))
    elif key == "bzs":
        bzs = obj.get("bzs", [])
        return ", ".join(_fmt_bzs(bzs)) if bzs else default
    else:
        val = obj.get(key, default)
        return ", ".join(val) if isinstance(val, (list, tuple)) else val


def make_dataset(data, title, headers, lheaders=None):
    """
    :param data: List of data :: [dict]
    :param title: Dataset title to be used as worksheet's name
    :param headers: Dataset headers to be used as column headers, etc.
    :param lheaders: Localized version of `headers`

    TODO: Which is better?
        - tablib.Dataset(); [tablib.append(vals) for vals in data]
        - tablib.Dataset(*data, header=...)
    """
    # .. note::
    #    We need to check title as valid worksheet name, length <= 31, etc.
    #    See also xlwt.Utils.valid_sheet_name.
    tdata = [[_make_cell_data(val, h) for h in headers] for val in data]
    return tablib.Dataset(*tdata, title=title[:30], headers=lheaders)

# vim:sw=4:ts=4:et:
