#
# -*- coding: utf-8 -*-
#
# Copyright (C) 2017 Satoru SATOH <ssato@redhat.com>
# Copyright (C) 2017 Red Hat, Inc.
# License: AGPLv3+
#
"""fleure DB initializer.
"""
from __future__ import absolute_import

import glob
import gzip
import itertools
import logging
import operator
import os.path
import os
import re
import pwd
import sqlite3

import anyconfig
import fleure_db.analysis
import fleure_db.utils


LOG = logging.getLogger(__name__)
LOG.addHandler(logging.StreamHandler())
LOG.setLevel(logging.INFO)


def find_uixmlgz_path(repo, root=os.path.sep):
    """
    - Cached updatein.xml.gz is updated by 'yum makecache' or 'dnf makecache'.

    - Path patterns of updateinfo.xml.gz:

      - yum: /var/cache/yum/<arch>/<ver>/<repo>/<checksum>-updateinfo.xml.gz
      - dnf:
        - root: /var/cache/dnf/<repo>-*/repodata/<checksum>-updateinfo.xml.gz
        - user:
          /var/tmp/dnf-<user>-*/<repo>-*/repodata/<checksum>-updateinfo.xml.gz

      where repo is repo id, e.g. "rhel-7-server-rpms"
            checksum is checksum of xml.gz file, e.g. 531b74...
            arch is architecture, e.g. "x86_64"
            ver is OS version, e.g. 7Server

    .. todo:: How to change cache root wiht dnf's option?

    :param repo: Repo ID, e.g. rhel-7-server-rpms (RH CDN)
    :param root: Root dir in which cachdir, e.g. /var/cache/dnf/, exists
    :return: Path of the latest updateinfo.xml.gz or None if not found
    """
    uid = os.getuid()
    user = pwd.getpwuid(uid).pw_name

    if fleure_db.utils.is_dnf_available():
        rcdir = "/var/cache/dnf/" if uid == 0 else "/var/tmp/dnf-{user}-*/"
    else:
        rcdir = "/var/cache/yum/*/*/{repo}/"

    pathf = os.path.join(rcdir, "{repo}-*/repodata/*-updateinfo.xml.gz")
    paths = sorted(glob.glob(pathf.format(repo=repo, root=root, user=user)),
                   key=os.path.getctime, reverse=True)
    return paths[0] if paths else None  # Try the first one only.


def _save_data_as_json(data, filepath, top_key="data"):
    """
    :param filepath: JSON file path
    :param data: Data to save, maybe a list or mapping object
    :para top_key:
        Top level mapping key to be used to save list data (valid JSON data is
        a mapping object)
    """
    if not hasattr(data, "keys"):
        data = {top_key: data}  # Necessary to make `data` as valid JSON data.

    if not os.path.exists(os.path.dirname(filepath)):
        os.makedirs(os.path.dirname(filepath))

    anyconfig.dump(data, filepath)
    LOG.debug("saved: %s", filepath)


def load_uixmlgz(repo, outdir, root=os.path.sep):
    """
    :param repo: Repo ID, e.g. rhel-7-server-rpms (RH CDN)
    :param outdir: Dir to save outputs
    :param root: Root dir in which cachdir, e.g. /var/cache/dnf/, exists

    :return: [update loaded from updateinfo.xml]
    """
    LOG.debug("Loading updateinfo.xml.gz for %s [root=%s]", repo, root)
    uixmlgz = find_uixmlgz_path(repo, root=root)
    if uixmlgz is None:
        LOG.warn("Could not find updateinfo.xml.gz: repo=%s, root=%s",
                 repo, root)
        return False

    with gzip.open(uixmlgz) as inp:
        # FIXME: Not work as expected, 'ParseError: not well-formed ...'
        # uidata = anyconfig.load(inp, ac_parser="xml")
        uidata = anyconfig.loads(inp.read(), ac_parser="xml",
                                 ac_parse_value=True, merge_attrs=True)

    if not uidata or "updates" not in uidata:
        LOG.error("Failed to load or parse updateinfo.xml: repo=%s, root=%s",
                  repo, root)
        return []

    if not uidata["updates"] or "update" not in uidata["updates"][0]:
        LOG.error("Failed to parse or empty updateinfo.xml: repo=%s, root=%s",
                  repo, root)
        return []

    if not os.path.exists(outdir):
        LOG.info("Creating dir to save results: %s", outdir)
        os.makedirs(outdir)
    elif not os.path.isdir(outdir):
        raise RuntimeError("Output dir '{}' is not a dir!".format(outdir))

    # Save parsed but not modified data.
    _save_data_as_json(uidata, os.path.join(outdir, repo, "updateinfo.json"))

    return uidata["updates"]


# Mapping of update type vs. int.
_UTYPE_INT_MAP = dict(RHSA=0, RHBA=1, RHEA=2)
_UTYPE_TYPE_MAP = {RHSA: fleure_db.globals.RHSA, RHBA: fleure_db.globals.RHBA,
                   RHEA: fleure_db.globals.RHEA}


def _int_from_update(adv, typemap=None):
    """
    :param adv:
        Update ID (errata advisory), e.g. RHBA-2016:2423, RHSA-2016:2872
    :return: Int represents unique update

    >>> _int_from_update("RHBA-2016:2423")
    1010201624230
    >>> _int_from_update("RHSA-2016:2872")
    1000201628720
    """
    if typemap is None:
        typemap = _UTYPE_INT_MAP

    (utype, serial) = adv.split('-')
    (tid, (year, seq)) = (typemap.get(utype, len(typemap)), serial.split(':'))

    return int("10{}0{}{}0".format(tid, year, seq))


def _type_from_update(adv, typemap=None):
    """
    :param adv: Update ID (errata advisory), e.g. RHBA-2016:2423.
    :return: Type string represents update's type

    >>> _type_from_update("RHBA-2016:2423")
    'bug'
    >>> _type_from_update("RHSA-2016:2872")
    'security'
    """
    if typemap is None:
        typemap = _UTYPE_TYPE_MAP

    utype = adv.split('-')[0]
    return typemap[utype]


_NEVRA = operator.itemgetter(*"name epoch version release arch".split())


def _repo_and_pkgs_from_update(update, repo):
    """
    :param update: Update info dict, {id, titile, ..., pkglist, ...}
    :param repo: Repo IDs, e.g. 'rhel-7-server-rpms'

    :return: A tuple of (repo_id :: str, repo_name :: str, [package :: dict])
    """
    try:
        pkc = update["pkglist"]["collection"]
        rid = _get_value(pkc, "short") or repo
        if "@children" in pkc:
            rname = pkc["@children"][0]["name"]
            pkgs = [p["package"] for p in pkc["@children"] if "package" in p]
        else:
            rname = pkc["name"]
            pkgs = [pkc["package"]] if "package" in pkc else []

        pkgs_with_ids = []
        for pkg in pkgs:
            pkg["id"] = fleure_db.utils.gen_id_for_values(_NEVRA(pkg))
            pkgs_with_ids.append(pkg)

        return (rid, rname, pkgs_with_ids)

    except (KeyError, AttributeError):
        msg = "Corrupt update info: {}".format(update.get("id", "Unknown!"))
        raise ValueError(msg)


def _get_value(dic, key):
    """
    :param dic: nested dict holding a value for key
    :return: Value for given key
    """
    candidate = dic.get(key, None)
    if candidate is None:
        return None
    elif isinstance(candidate, dict):
        # Search value with the new key found at first recursively.
        return _get_value(candidate, candidate.keys()[0])
    elif isinstance(candidate, list):
        return _get_value(candidate[0], key)  # Workaround for invalid ones.
    else:
        return candidate


def _url_from_update(update):
    """
    :param update: Update info dict, {id, titile, ..., pkglist, ...}
    :return: A string represents URL of the update
    """
    refs = update["references"]
    if isinstance(refs, dict):
        return refs["reference"].get("href")
    elif isinstance(refs, list):
        return refs[0]["reference"].get(" href")
    else:
        raise ValueError("refs=%r" % refs)
        return None  # It should not reach here although...


_EURL_RE = re.compile(r"http://access.redhat.com/errata/(RH.A-\d{4}:\d+).html")


def _process_refs_itr(refs):
    """
    :param ref: A list of mapping object, {reference: {href, type, id, title}}
    :return: modified ref
    """
    if not refs or not isinstance(refs, list):
        return  # There is only self ref. and no errata/rhbz references.

    for ref in (r["reference"] for r in refs):
        if any(k not in ref for k in ("id", "type")):
            continue

        if ref["type"] == "self" or ref["id"] == "classification":
            continue  # Ignore and skip them.

        if ref["type"] == "other":
            m = _EURL_RE.match(ref["href"])
            if m:  # That is, it's a cross ref to other errata.
                adv = m.groups()[0]
                ref["type"] = "errata"
                ref["title"] = adv
                ref["id"] = _int_from_update(adv)

        yield ref


def process_uixmlgz_itr(repo, outdir, root=os.path.sep, **options):
    """
    :param repo: Repo ID, e.g. rhel-7-server-rpms (RH CDN)
    :param outdir: Dir to save outputs
    :param root: Root dir in which cachdir, e.g. /var/cache/dnf/, exists
    :param options: Extra keyword options

    :return: [dict], data will be modified to make post processing easier
    """
    uidata = load_uixmlgz(repo, outdir, root=root)
    if not uidata:
        return

    for upd in (u["update"] for u in uidata):
        # Swap uid["id"] with int.
        uid = _int_from_update(upd["id"])
        upd["advisory"] = upd["id"]
        upd["id"] = uid
        upd["type"] = _type_from_update(upd["id"])

        # Unify types of items and eliminate intermediate dicts to simplify
        # analysis and creating tables later.
        (rid, rname, upd["pkglist"]) = _repo_and_pkgs_from_update(upd, repo)
        upd["pkgnames"] = [p["name"] for p in upd["pkglist"]]

        upd["url"] = _url_from_update(upd)
        upd["repos"] = [dict(uid=uid, repo_id=rid, repo_name=rname)]

        # drop self and other references and just keep cve and bugzilla refs.
        upd["references"] = list(_process_refs_itr(upd.get("references", [])))
        upd["cves"] = [r for r in upd["references"] if r["type"] == "cve"]
        upd["bzs"] = [r for r in upd["references"] if r["type"] == "bugzilla"]

        # Do I need to convert them?
        # :seealso: https://www.sqlite.org/datatype3.html
        # :seealso: https://www.sqlite.org/lang_datefunc.html
        for key in ("issued", "updated"):
            upd[key] = upd[key]["date"]  # Eliminate intermediate dicts.

        yield upd


def _exec_sql_stmt(cur, stmt, values=None):
    """
    :param cur: :class:`sqlite3.Cursor` object
    :param stmt: SQL statement to execute
    """
    try:
        return (cur.execute(stmt)
                if values is None else cur.execute(stmt, values))
    except (sqlite3.OperationalError, sqlite3.IntegrityError,
            sqlite3.InterfaceError):
        LOG.error("Failed to exec: stmt=%s, values=%r", stmt, values)
        raise


def _create_tbl_stmt(name, keys, pkey="id", pktype="INTEGER"):
    """
    :param name: Table name
    :param keys: Keys
    :param pkey: Primary key name
    :param pktype: SQL type of primary key

    :return: SQL statement to create a table
    """
    kts = ["{} {}".format(k, pktype + " PRIMARY KEY" if k == pkey else "TEXT")
           for k in keys]
    return "CREATE TABLE IF NOT EXISTS '{}' ({})".format(name, ", ".join(kts))


_PKG_KEYS = ("id", "name", "version", "release", "epoch", "arch", "src")
_REF_KEYS = ("id", "title", "type", "href")
_UPD_KEYS = ("id", "type", "title", "summary", "description", "solution",
             "issued", "updated", "release", "severity", "url",
             "reboot_suggested")  # optional: release, severity, ...
_REPO_KEYS = ("id", "repo_id", "repo_name")


def _create_tables(conn, pkeys=_PKG_KEYS, rkeys=_REF_KEYS, ukeys=_UPD_KEYS):
    """
    :param conn: An initialized instance of :class:`~sqlite3.Connection`
    """
    cur = conn.cursor()

    _exec_sql_stmt(cur, _create_tbl_stmt("packages", pkeys))
    _exec_sql_stmt(cur, _create_tbl_stmt("refs", rkeys, pktype="TEXT"))
    _exec_sql_stmt(cur, _create_tbl_stmt("updates", ukeys))

    _exec_sql_stmt(cur, "PRAGMA foreign_keys = ON")
    conn.commit()

    _exec_sql_stmt(cur,
                   "CREATE TABLE IF NOT EXISTS update_packages "
                   "(uid TEXT, pid INTEGER, "
                   " FOREIGN KEY(uid) REFERENCES updates(id), "
                   " FOREIGN KEY(pid) REFERENCES packages(id))")
    _exec_sql_stmt(cur,
                   "CREATE TABLE IF NOT EXISTS update_refs "
                   "(uid TEXT, rid TEXT, "
                   " FOREIGN KEY(uid) REFERENCES updates(id), "
                   " FOREIGN KEY(rid) REFERENCES refs(id))")
    _exec_sql_stmt(cur,
                   "CREATE TABLE IF NOT EXISTS update_repos"
                   "(uid TEXT, repo_id TEXT, repo_name TEXT, "
                   " FOREIGN KEY(uid) REFERENCES updates(id))")
    conn.commit()


def _insert_values(cur, name, keys, values):
    """
    :param cur: :class:`sqlite3.Cursor` object
    :param name: Name of the table to insert data
    :param keys: Key names for values
    :param values:
        Values to insert. The order of items and the length are same as `key`.
    """
    if any(v is None for v in values):
        keys = [k for k, v in itertools.izip(keys, values) if v is not None]
        values = [v for v in values if v is not None]
        stmt = ("INSERT OR IGNORE INTO {}({}) VALUES ({})"
                "".format(name, ", ".join(keys),
                          ", ".join("?" for v in values)))
    else:
        stmt = ("INSERT OR IGNORE INTO {} VALUES ({})"
                "".format(name, ", ".join("?" for v in values)))

    _exec_sql_stmt(cur, stmt, values)


def _insert_list_values_with_rels(cur, data, items_key, tbl_info, rel_info):
    """
    :param cur: :class:`sqlite3.Cursor` object
    """
    (tbl, keys) = tbl_info
    (rel_tbl, rel_keys) = rel_info

    for item in data.get(items_key, []):
        vals = tuple(item[k] for k in keys)
        _insert_values(cur, tbl, keys, vals)
        _insert_values(cur, rel_tbl, rel_keys, (data["id"], item["id"]))


def save_uidata_to_sqlite(updates, outdir, pkeys=_PKG_KEYS,
                          rkeys=_REF_KEYS, ukeys=_UPD_KEYS):
    """
    :param updates: List of updateinfo data (nested dict)
    :param outdir: Dir to save outputs
    """
    dbpath = os.path.join(outdir, "updates.db")

    with sqlite3.connect(dbpath) as conn:
        _create_tables(conn)
        cur = conn.cursor()

        for upd in updates:
            vals = [_get_value(upd, k) for k in ukeys]
            _insert_values(cur, "updates", ukeys, vals)

            # see :fun:`process_uixmlgz_itr`
            _insert_list_values_with_rels(conn, upd, "pkglist",
                                          ("packages", pkeys),
                                          ("update_packages", ("uid", "pid")))
            conn.commit()

            _insert_list_values_with_rels(conn, upd, "references",
                                          ("refs", rkeys),
                                          ("update_refs", ("uid", "rid")))
            conn.commit()

            repokeys = ("id", "repo_id", "repo_name")
            for repo in upd.get("repos", []):
                vals = [upd["id"]] + [repo[k] for k in repokeys if k != "id"]
                _insert_values(cur, "update_repos", repokeys, vals)

        conn.commit()

    LOG.debug("saved: %s", dbpath)


def convert_uixmlgz(repo, outdir, root=os.path.sep, **options):
    """
    Convert updateinfo.xml.gz per repo.

    :param repo: Repo IDs, e.g. 'rhel-7-server-rpms'
    :param outdir: Dir to save outputs
    :param root: Root dir in which cachdir, e.g. /var/cache/dnf/, exists
    :param options: Extra keyword options

    :return: Mapping object holding updateinfo data
    """
    ups = sorted(process_uixmlgz_itr(repo, outdir, root=root, **options),
                 key=operator.itemgetter("id"))
    routdir = os.path.join(outdir, repo)

    # 1. Save modified updateinfo data as JSON file.
    _save_data_as_json(ups, os.path.join(routdir, "updates.json"))

    # 2. Convert and save SQLite database.
    try:
        save_uidata_to_sqlite(ups, routdir)
    except (AttributeError, KeyError):
        raise

    return ups


def _load_and_merge_repos_data(repos, outdir, root, **options):
    """
    :param repos: List of Repo IDs, e.g. ['rhel-7-server-rpms']
    :param outdir: Dir to save outputs
    :param root: Root dir in which cachdir, e.g. /var/cache/dnf/, exists
    :param options: Extra keyword options such as tokenize
    """
    if not repos:
        return []

    # Initialize ups :: {Update ID: Update} for the first repo.
    ups = dict((u["id"], u) for u in convert_uixmlgz(repos[0], outdir, root))
    LOG.info("Loaded first repo's data: repo=%s", repos[0])

    # Process other repos' data.
    for repo in repos[1:]:
        for upd in convert_uixmlgz(repo, outdir, root):
            uid = upd["id"]
            urepos = upd["repos"]  # {repo_id, repo_name}

            if uid in ups:
                for urepo in urepos:
                    if urepo["repo_id"] not in ups[uid]["repos"]:
                        ups[uid]["repos"].append(urepo)
            else:
                upd["repos"] = urepos
                if options.get("tokenize"):
                    desc = upd["description"]
                    upd["tokens"] = fleure_db.analysis.tokenize(desc)

                ups[uid] = upd

        LOG.info("Loaded the repo's data: repo=%s", repo)

    return sorted(ups.values(), key=operator.itemgetter("id"))


def convert_uixmlgzs(repos, outdir, root=os.path.sep, **options):
    """
    Convert updateinfo.xml.gz of given all repos.

    :param repos: List of Repo IDs, e.g. ['rhel-7-server-rpms']
    :param outdir: Dir to save outputs
    :param root: Root dir in which cachdir, e.g. /var/cache/dnf/, exists
    :param options: Extra keyword options

    :return: True if success and False if not
    """
    LOG.info("Start loading of repos' data")
    ups = _load_and_merge_repos_data(repos, outdir, root, **options)

    # 1. Save all repos' updates data as JSON file again.
    _save_data_as_json(ups, os.path.join(outdir, "updates.json"))

    if options.get("analyze", False):
        texts = [u["description"] for u in ups]  # TODO: Might exhaust RAM.
        fleure_db.analysis.make_word2vec_model(texts, outdir, **options)
        fleure_db.analysis.make_topic_models(texts, outdir, **options)

    # 2. Convert and save SQLite database.
    try:
        save_uidata_to_sqlite(ups, outdir)
        LOG.info("saved SQLite database in: %s", outdir)
    except (AttributeError, KeyError):
        raise

# vim:sw=4:ts=4:et:
