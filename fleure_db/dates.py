#
# Copyright (C) 2013 Satoru SATOH <ssato@redhat.com>
# Copyright (C) 2013 - 2017 Red Hat, Inc.
# License: GPLv3+
#
"""Functions to process dates (periods).
"""
from __future__ import absolute_import

import datetime


def _to_date(date_s):
    """
    .. note::
       Errata issue_date and update_date format: month/day/year, e.g. 12/16/10.

    >>> _to_date("12/16/10")
    (2010, 12, 16)
    >>> _to_date("2014-10-14 00:00:00")
    (2014, 10, 14)
    """
    if '-' in date_s:
        return tuple(int(x) for x in date_s.split()[0].split('-'))
    else:
        (month, day, year) = date_s.split('/')
        return (int("20" + year), int(month), int(day))


def _to_datetime(date_s):
    """
    :return: object `datetime.datetime` returns
    """
    return datetime.datetime(*_to_date(date_s))


def days_ago(delta):
    """
    :param ago: Number of days to get 'days ago'
    :return: object `datetime.datetime` returns
    """
    return datetime.datetime.now() - datetime.timedelta(days=delta)


def weeks_ago(delta):
    """Likewise but `delta` is in unit 'week'.
    """
    return datetime.datetime.now() - datetime.timedelta(weeks=delta)


def in_last_x_weeks(date_s, ago=2, end=None):
    """
    :param date_s: date string such as "12/16/10", "2014-10-14 00:00:00"
    :param ago: number of weeks to search `date_s` within

    :return: True if given date (:: str) in last `ago` weeks

    >>> in_last_x_weeks("2017-03-10", 1, "2017-03-14")
    True
    >>> in_last_x_weeks("2017-03-15", 1, "2017-03-14")
    False
    """
    now = datetime.datetime.now() if end is None else _to_datetime(end)

    the_date = _to_datetime(date_s)
    start_date = weeks_ago(ago)

    return start_date < the_date and the_date <= now

# vim:sw=4:ts=4:et:
