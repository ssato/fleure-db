#
# -*- coding: utf-8 -*-
# Copyright (C) 2013 Satoru SATOH <ssato@redhat.com>
# Copyright (C) 2013 - 2017 Red Hat, Inc.
# License: AGPLv3+
#
# pylint: disable=too-many-arguments,too-many-locals,no-member
"""Analysis module
"""
from __future__ import absolute_import

import cython  # pylint: disable=unused-argument
import gensim
import itertools
import logging
import os.path
import os
import nltk
import tablib

from operator import itemgetter

import fleure_db.globals
import fleure_db.utils

from fleure_db.globals import _, RHSA, RHBA, RHEA
from fleure_db.utils import CHAIN_FROM_ITR
from fleure_db.datasets import make_dataset


LOG = logging.getLogger(__name__)
STEMMER = nltk.PorterStemmer()

# .. note::
#    To run 'nltk.download()' is required to use nltk.corpus or built RPM w/
#    https://github.com/ssato/misc/blob/master/rpmspecs/python-nltk-data.spec
#    and install it in advance.
try:
    _DEFAULT_STOPWORDS = tuple(nltk.corpus.stopwords.words("english"))
except LookupError:
    LOG.warn("Corpus data was not found. Try to install python-nltk-data or "
             "run nltk.download() to install it.")
    _DEFAULT_STOPWORDS = ()


def tokenize(text, stemming=False, stopwords=_DEFAULT_STOPWORDS):
    """
    :param text: A string represents a bit long text
    :param stemming: Stemming words if True
    :param stopwords: Stop words to be ignored and skipped from results

    :return: List of tokens (words), [str]
    """
    if stemming:
        return [STEMMER.stem(w) for w in nltk.wordpunct_tokenize(text)
                if w not in stopwords]
    else:
        return [w for w in nltk.wordpunct_tokenize(text) if w not in stopwords]


def make_word2vec_model(texts, outdir, w2v_options=None, **options):
    """
    :param texts: Iterable yields text consists of sentences
    :param outdir: Output dir to save results
    :param w2v_options: Mapping object represents keyword options passed to gen
    :param options: Keyword options

    :return: An instance of :class:`~gensim.models.word2vec.Word2Vec`
    """
    # Requires nltk.download() or get and install the NLTK data anyhow.
    corpus = options.get("corpus", "tokenizers/punkt/english.pickle")
    tokenizer = nltk.data.load(corpus)
    sentences = CHAIN_FROM_ITR(tokenizer.tokenize(t) for t in texts)
    model = gensim.models.Word2Vec(sentences, **(w2v_options or {}))

    opath = os.path.join(outdir, "gensim.word2vec")
    model.save(opath)
    LOG.info("Saved word2vec data: %s", opath)

    return model


# :seealso: https://radimrehurek.com/gensim/tut1.html#corpus-formats
# :seealso: https://radimrehurek.com/gensim/tut2.html
def make_topic_models(texts, outdir, ntopics=300, **options):
    """
    :param texts: Iterable yields text consists of sentences :: [str]
    :param outdir: Output dir to save results and intermediate data
    :param ntopics: Number of topics to find out in topic models.
    :param options: Extra keyword options

    :return: {corpus, tfidf, lsi, lda}
    """
    if not os.path.exists(outdir):
        os.makedirs(outdir)
        LOG.info("Created dir to save results: %s", outdir)

    tokensets = [tokenize(t) for t in texts]  # [[str]]

    dic = gensim.corpora.Dictionary(tokensets)
    dicpath = os.path.join(outdir, "gensim.wordids")
    dic.save(dicpath)
    LOG.info("Saved corpora.Dictionary data: %s", dicpath)

    corpus = [dic.doc2bow(tokens) for tokens in tokensets]
    cpath = os.path.join(outdir, "gensim.mm")
    gensim.corpora.MmCorpus.serialize(cpath, corpus)
    LOG.info("Saved corpus data: %s", cpath)

    tfidf = gensim.models.TfidfModel(corpus)
    tpath = os.path.join(outdir, "gensim.tfidf")
    tfidf.save(tpath)
    LOG.info("Saved tfidf data: %s", tpath)

    lsimod = gensim.models.lsimodel.LsiModel(tfidf[corpus], id2word=dic,
                                             num_topics=ntopics)
    lsipath = os.path.join(outdir, "gensim.lsimodel")
    lsimod.save(lsipath)
    LOG.info("Saved LSI model: %s", lsipath)
    LOG.debug("LSI model: topics = %r", lsimod.show_topics())

    ldamod = gensim.models.ldamodel.LdaModel(corpus, id2word=dic,
                                             num_topics=ntopics)
    ldapath = os.path.join(outdir, "gensim.ldamodel")
    ldamod.save(ldapath)
    LOG.info("Saved LDA model: %s", ldapath)
    LOG.debug("LDA model: topics = %r", ldamod.show_topics())

    return dict(corpus=corpus, tfidf=tfidf, lsi=lsimod, lda=ldamod)


def list_latest_errata_by_updates(ers):
    """
    :param ers: A list of errata dict
    :return: A list of items in `ers` grouped by update names
    """
    ung = lambda e: sorted(set(u["name"] for u in e.get("updates", [])))
    return [xs[-1] for xs
            in fleure_db.utils.sgroupby(ers, ung, itemgetter("issued"))]


def list_updates_from_errata(ers):
    """
    :param ers: List of mapping object represents update (errata)
    :return: List of latest udpate packages from errata
    """
    ups = sorted((p for p in CHAIN_FROM_ITR(e.get("pkglist", []) for e in ers)),
                 key=itemgetter("name", "arch"))

    return [sorted(g, cmp=fleure_db.utils.cmp_packages, reverse=True)[0] for g
            in fleure_db.utils.sgroupby(ups, itemgetter("name", "arch"))]


def _errata_keywords(names, keywords, pkeywords):
    """
    Make a list of errata keywords of given list of packages `names`.

    :param names: Package names
    :param keywords: A tuple of keywords to filter 'important' RHBAs
    :param pkeywords: Similar to above but a dict gives the list per RPMs
    :return: A set of keywords

    >>> names = ["kernel", "glibc"]
    >>> keywords = ['crash', 'panic', 'hang']
    >>> pkeywords = dict(kernel=["xfs", "kvm"], glibc=["nss", "segv"])
    >>> kwds = _errata_keywords(names, keywords, pkeywords)
    >>> ref = keywords
    >>> for keys in pkeywords.values():
    ...     ref.extend(keys)
    >>> all(k in ref for k in kwds)
    True
    """
    return set(itertools.chain(keywords,
                               *[pkeywords.get(n, []) for n in names]))


def errata_of_keywords_g(ers, keywords=fleure_db.globals.ERRATA_KEYWORDS,
                         pkeywords=None, stemming=True):
    """
    :param ers: A list of errata
    :param keywords: A tuple of keywords to filter 'important' RHBAs
    :param pkeywords: Similar to above but a dict gives the list per RPMs
    :param stemming: Strict matching of keywords with using NLTK stemmer
    :return:
        A generator to yield errata of which description contains any of
        given keywords

    >>> ert0 = dict(advisory="RHSA-2015:XXX1",
    ...             description="system hangs, or crash...")
    >>> ert1 = dict(advisory="RHEA-2015:XXX2",
    ...             description="some enhancement and changes")
    >>> ers = list(errata_of_keywords_g([ert0], ("hang", ), stemming=True))
    >>> ert0 in ers
    True
    >>> ers[0]["keywords"]  # 'hangs' with stemming matches.
    ['hang']
    >>> ers = list(errata_of_keywords_g([ert0, ert1], ("hang", "crash"),
    ...                                 stemming=False))
    >>> ert0 in ers
    True
    >>> ers[0]["keywords"]  # 'hangs' w/o stemming does not match with 'hang'.
    ['crash']
    >>> ert1 in ers
    False
    """
    if pkeywords is None:
        pkeywords = fleure_db.globals.ERRATA_PKEYWORDS

    for ert in ers:
        tokens = set(tokenize(ert["description"], stemming))
        kwds = _errata_keywords(ert.get("package_names", []), keywords,
                                pkeywords)
        matched = kwds & tokens
        if matched:
            LOG.debug(_("%s matched: keywords=%s"), ert["advisory"],
                      ', '.join(matched))
            ert["keywords"] = list(matched)
            yield ert


def errata_of_rpms_g(ers, rpms=fleure_db.globals.CORE_RPMS):
    """
    :param ers: A list of errata
    :param rpms: A list of RPM names to select relevant errata
    :return: A generator to yield errata relevant to any of given RPM names

    >>> ert0 = dict(advisory="RHSA-2015:XXX1",
    ...             pkgnames=["kernel", "tzdata"])
    >>> ert1 = dict(advisory="RHSA-2015:XXX2",
    ...             pkgnames=["glibc", "tzdata"])
    >>> ers = errata_of_rpms_g([ert0, ert1], ("kernel", ))
    >>> ert0 in ers
    True
    >>> ert1 in ers
    False
    """
    for ert in ers:
        if any(n in ert["pkgnames"] for n in rpms):
            yield ert


def list_update_errata_pairs(ers):
    """
    :param ers: A list of errata dict
    :return: A list of (update_name, [errata_advisory])

    >>> ers = [dict(advisory="RHSA-2015:XXX1",
    ...             pkgnames=["kernel", "tzdata"]),
    ...        dict(advisory="RHSA-2014:XXX2",
    ...             pkgnames=["glibc", "tzdata"])
    ...        ]
    >>> list_update_errata_pairs(ers) == [
    ...     ('glibc', ['RHSA-2014:XXX2']),
    ...     ('kernel', ['RHSA-2015:XXX1']),
    ...     ('tzdata', ['RHSA-2015:XXX1', 'RHSA-2014:XXX2'])
    ... ]
    True
    """
    ues = CHAIN_FROM_ITR(((u["name"], e["advisory"]) for u in e["pkglist"])
                         for e in ers)
    return [(u, sorted((t[1] for t in g), reverse=True)) for u, g
            in itertools.groupby(ues, itemgetter(0))]


def list_updates_by_num_of_errata(uess):
    """
    List number of specific type of errata for each package names.

    :param uess: A list of (update, [errata_advisory]) pairs
    :return: [(package_name :: str, num_of_relevant_errata :: Int)]

    >>> ers = [{'advisory': u'RHSA-2015:1623',
    ...         'pkgnames': ['kernel-headers', 'kernel']},
    ...        {'advisory': 'RHSA-2015:1513',
    ...         'pkgnames': ['bind-utils']},
    ...        {'advisory': 'RHSA-2015:1081',
    ...         'pkgnames': ['kernel-headers', 'kernel']}
    ...        ]
    >>> list_updates_by_num_of_errata(list_update_errata_pairs(ers))
    [('kernel', 2), ('kernel-headers', 2), ('bind-utils', 1)]
    >>>
    """
    return sorted(((u, len(es)) for u, es in uess), key=itemgetter(1),
                  reverse=True)


def analyze_rhsa(rhsa):
    """
    Compute and return statistics of RHSAs from some view points.

    :param rhsa: A list of security errata (RHSA) dicts
    :return: RHSA analized data and metrics
    """
    cri_rhsa = [e for e in rhsa if e["severity"] == "Critical"]
    imp_rhsa = [e for e in rhsa if e["severity"] == "Important"]

    rhsa_rate_by_sev = [("Critical", len(cri_rhsa)),
                        ("Important", len(imp_rhsa)),
                        ("Moderate",
                         len([e for e in rhsa
                              if e["severity"] == "Moderate"])),
                        ("Low",
                         len([e for e in rhsa
                              if e["severity"] == "Low"]))]

    rhsa_ues = list_update_errata_pairs(rhsa)
    _ups_by_nes = lambda es: \
        list_updates_by_num_of_errata(list_update_errata_pairs(es))

    return {'list': rhsa,
            'list_critical': cri_rhsa,
            'list_important': imp_rhsa,
            'list_latest_critical': list_latest_errata_by_updates(cri_rhsa),
            'list_latest_important': list_latest_errata_by_updates(imp_rhsa),
            'list_critical_updates': list_updates_from_errata(cri_rhsa),
            'list_important_updates': list_updates_from_errata(imp_rhsa),
            'rate_by_sev': rhsa_rate_by_sev,
            'list_n_by_pnames': list_updates_by_num_of_errata(rhsa_ues),
            'list_n_cri_by_pnames': _ups_by_nes(cri_rhsa),
            'list_n_imp_by_pnames': _ups_by_nes(imp_rhsa),
            'list_by_packages': rhsa_ues}


def analyze_rhba(rhba, keywords=fleure_db.globals.ERRATA_KEYWORDS,
                 pkeywords=None, core_rpms=fleure_db.globals.CORE_RPMS):
    """
    Compute and return statistics of RHBAs from some view points.

    :param rhba: A list of bug errata (RHBA) dicts
    :param keywords: A tuple of keywords to filter 'important' RHBAs
    :param pkeywords: Similar to above but a dict gives the list per RPMs
    :param core_rpms: Core RPMs to filter errata by them
    :return: RHSA analized data and metrics
    """
    kfn = lambda e: (len(e.get("keywords", [])), e["issued"],
                     e["pkgnames"])
    rhba_by_kwds = sorted(errata_of_keywords_g(rhba, keywords, pkeywords),
                          key=kfn, reverse=True)
    rhba_of_core_rpms_by_kwds = \
        sorted(errata_of_rpms_g(rhba_by_kwds, core_rpms),
               key=kfn, reverse=True)
    rhba_of_rpms = sorted(errata_of_rpms_g(rhba, core_rpms),
                          key=itemgetter("pkgnames"), reverse=True)
    latest_rhba_of_rpms = list_latest_errata_by_updates(rhba_of_rpms)
    rhba_ues = list_update_errata_pairs(rhba)

    return {'list': rhba,
            'list_by_kwds': rhba_by_kwds,
            'list_of_core_rpms': rhba_of_rpms,
            'list_latests_of_core_rpms': latest_rhba_of_rpms,
            'list_by_kwds_of_core_rpms': rhba_of_core_rpms_by_kwds,
            'list_updates_by_kwds': list_updates_from_errata(rhba_by_kwds),
            'list_n_by_pnames': list_updates_by_num_of_errata(rhba_ues),
            'list_by_packages': rhba_ues}


def analyze_errata(ers, keywords=fleure_db.globals.ERRATA_KEYWORDS,
                   pkeywords=None, core_rpms=fleure_db.globals.CORE_RPMS):
    """
    :param ers: A list of applicable errata sorted by severity
        if it's RHSA and advisory in ascending sequence
    :param keywords: A tuple of keywords to filter 'important' RHBAs
    :param pkeywords: Similar to above but a dict gives the list per RPMs
    :param core_rpms: Core RPMs to filter errata by them
    """
    rhsa = [e for e in ers if e["type"] == RHSA]
    rhba = [e for e in ers if e["type"] == RHBA]
    rhea = [e for e in ers if e["type"] == RHEA]

    rhsa_data = analyze_rhsa(rhsa)
    rhba_data = analyze_rhba(rhba, keywords=keywords, pkeywords=pkeywords,
                             core_rpms=core_rpms)
    return dict(rhsa=rhsa_data,
                rhba=rhba_data,
                rhea=dict(list=rhea,
                          list_by_packages=list_update_errata_pairs(rhea)),
                rate_by_type=[("Security", len(rhsa)),
                              ("Bug", len(rhba)),
                              ("Enhancement", len(rhea))])


def padding_row(row, mcols):
    """
    :param rows: A list of row data :: [[]]

    >>> padding_row(['a', 1], 3)
    ['a', 1, '']
    >>> padding_row([], 2)
    ['', '']
    """
    return row + [''] * (mcols - len(row))


def mk_overview_dataset(data, keywords=fleure_db.globals.ERRATA_KEYWORDS,
                        core_rpms=None, **kwargs):
    """
    :param data: RPMs, Update RPMs and various errata data summarized
    :param keywords: A tuple of keywords to filter 'important' RHBAs
    :param core_rpms: Core RPMs to filter errata by them

    :return: An instance of tablib.Dataset becomes a worksheet represents the
        overview of analysys reuslts
    """
    rows = [[_("Critical or Important RHSAs (Security Errata)")],
            [_("# of Critical RHSAs"),
             len(data["errata"]["rhsa"]["list_critical"])],
            [_("# of Critical RHSAs (latests only)"),
             len(data["errata"]["rhsa"]["list_latest_critical"])],
            [_("# of Important RHSAs"),
             len(data["errata"]["rhsa"]["list_important"])],
            [_("# of Important RHSAs (latests only)"),
             len(data["errata"]["rhsa"]["list_latest_important"])],
            [_("Update RPMs by Critical or Important RHSAs at minimum")],
            [_("# of Update RPMs by Critical RHSAs at minimum"),
             len(data["errata"]["rhsa"]["list_critical_updates"])],
            [_("# of Update RPMs by Important RHSAs at minimum"),
             len(data["errata"]["rhsa"]["list_important_updates"])],
            [],
            [_("RHBAs (Bug Errata) by keywords: %s") % ", ".join(keywords)],
            [_("# of RHBAs by keywords"),
             len(data["errata"]["rhba"]["list_by_kwds"])],
            [_("# of Update RPMs by RHBAs by keywords at minimum"),
             len(data["errata"]["rhba"]["list_updates_by_kwds"])]]

    if core_rpms is not None:
        rows += [[],
                 [_("RHBAs of core rpms: %s") % ", ".join(core_rpms)],
                 [_("# of RHBAs of core rpms (latests only)"),
                  len(data["errata"]["rhba"]["list_latests_of_core_rpms"])]]

    rows += [[],
             [_("# of RHSAs"), len(data["errata"]["rhsa"]["list"])],
             [_("# of RHBAs"), len(data["errata"]["rhba"]["list"])],
             [_("# of RHEAs (Enhancement Errata)"),
              len(data["errata"]["rhea"]["list"])],
             [_("# of Update RPMs"), len(data["updates"]["list"])],
             [_("# of Installed RPMs"), len(data["installed"]["list"])],
             [],
             [_("Origin of Installed RPMs")],
             [_("# of Rebuilt RPMs"), len(data["installed"]["list_rebuilt"])],
             [_("# of Replaced RPMs"),
              len(data["installed"]["list_replaced"])],
             [_("# of RPMs from other vendors (non Red Hat)"),
              len(data["installed"]["list_from_others"])]]

    headers = (_("Item"), _("Value"), _("Notes"))
    dataset = tablib.Dataset(headers=headers)
    dataset.title = _("Overview of analysis results")

    mcols = len(headers)
    for row in rows:
        if row and len(row) == 1:  # Special case: separator
            dataset.append_separator(row[0])
        else:
            dataset.append(padding_row(row, mcols))

    return dataset


def dump_xls(dataset, filepath):
    """XLS dump function"""
    book = tablib.Databook(dataset)
    with open(filepath, 'wb') as out:
        out.write(book.xls)


def analyze_and_dump_results(errata, outdir, rpms=(), details=False,
                             **options):
    """
    Analyze and dump package level static analysis results.

    :param errata: List of mapping objects represents errata (update)
    :param rpms: List of RPMs to select from `errata`
    :param outdir: Dir to save results
    """
    installed = dict(list=rpms, list_rebuilt=[], list_replaced=[],
                     list_from_others=[])
    updates = []  # TBD.

    for pkg in rpms:
        for key in ("rebuilt", "replaced", "from_others"):
            if pkg.get(key, False):
                installed["list_" + key].append(pkg)

    ers = analyze_errata(errata, **options)
    data = dict(errata=ers,
                installed=installed,
                updates=dict(list=updates,
                             rate=[(_("packages need updates"), len(updates)),
                                   (_("packages not need updates"),
                                    len(rpms) - len(updates))]))

    # TODO: Keep DRY principle.
    rpmkeys = ("name", "epoch", "version", "release", "arch")
    lrpmkeys = [_("name"), _("epoch"), _("version"), _("release"), _("arch")]

    rpmdkeys = list(rpmkeys) # TODO: + ["summary", "vendor", "buildhost"]
    lrpmdkeys = lrpmkeys # TODO: + [_("summary"), _("vendor"), _("buildhost")]

    sekeys = ("advisory", "severity", "summary", "url", "pkgnames")
    lsekeys = (_("advisory"), _("severity"), _("summary"), _("url"),
               _("pkgnames"))
    bekeys = ("advisory", "keywords", "summary", "url", "pkgnames")
    lbekeys = (_("advisory"), _("keywords"), _("summary"), _("url"),
               _("pkgnames"))

    mds = [mk_overview_dataset(data, **dargs),
           make_dataset((data["errata"]["rhsa"]["list_latest_critical"] +
                         data["errata"]["rhsa"]["list_latest_important"]),
                        _("Cri-Important RHSAs (latests)"), sekeys, lsekeys),
           make_dataset(sorted(data["errata"]["rhsa"]["list_critical"],
                               key=itemgetter("pkgnames")) +
                        sorted(data["errata"]["rhsa"]["list_important"],
                               key=itemgetter("pkgnames")),
                        _("Critical or Important RHSAs"), sekeys, lsekeys),
           make_dataset(data["errata"]["rhba"]["list_by_kwds_of_core_rpms"],
                        _("RHBAs (core rpms, keywords)"), bekeys, lbekeys),
           make_dataset(data["errata"]["rhba"]["list_by_kwds"],
                        _("RHBAs (keyword)"), bekeys, lbekeys),
           make_dataset(data["errata"]["rhba"]["list_latests_of_core_rpms"],
                        _("RHBAs (core rpms, latests)"), bekeys, lbekeys),
           make_dataset(data["errata"]["rhsa"]["list_critical_updates"],
                        _("Update RPMs by RHSAs (Critical)"), rpmkeys,
                        lrpmkeys),
           make_dataset(data["errata"]["rhsa"]["list_important_updates"],
                        _("Updates by RHSAs (Important)"), rpmkeys, lrpmkeys),
           make_dataset(data["errata"]["rhba"]["list_updates_by_kwds"],
                        _("Updates by RHBAs (Keyword)"), rpmkeys, lrpmkeys)]

    for key, title in (("list_rebuilt", _("Rebuilt RPMs")),
                       ("list_replaced", _("Replaced RPMs")),
                       ("list_from_others", _("RPMs from other vendors"))):
        if data["installed"][key]:
            mds.append(make_dataset(data["installed"][key], title, rpmdkeys,
                                    lrpmdkeys))

    dump_xls(mds, os.path.join(dumpdir, "errata_summary.xls"))

    if details:
        dds = [make_dataset(errata, _("Errata Details"),
                            ("advisory", "type", "severity", "summary",
                             "description", "issued", "updated", "url",
                             "cves", "bzs", "pkgnames"),
                            (_("advisory"), _("type"), _("severity"),
                             _("summary"), _("description"), _("issued"),
                             _("updated"), _("url"), _("cves"),
                             _("bzs"), _("pkgnames"))),
               make_dataset(updates, _("Update RPMs"), rpmkeys, lrpmkeys),
               make_dataset(rpms, _("Installed RPMs"), rpmdkeys, lrpmdkeys)]

        dump_xls(dds, os.path.join(dumpdir, "errata_details.xls"))

# vim:sw=4:ts=4:et:
