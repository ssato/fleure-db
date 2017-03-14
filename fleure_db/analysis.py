#
# -*- coding: utf-8 -*-
# Copyright (C) 2013 Satoru SATOH <ssato@redhat.com>
# Copyright (C) 2013 - 2017 Red Hat, Inc.
# License: AGPLv3+
#
# pylint: disable=no-member
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

from fleure_db.globals import _
from fleure_db.utils import CHAIN_FROM_ITR


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
            in fleure_db.utils.sgroupby(ers, ung, itemgetter("issue_date"))]


def list_updates_from_errata(ers):
    """
    :param ers: List of mapping object represents update (errata)
    :return: List of latest udpate packages from errata
    """
    ups = sorted(CHAIN_FROM_ITR(e.get("pkglist", []) for e in ers),
                 itemgetter("name"))

    return [sorted(g, cmp=fleure_db.utils.cmp_packages, reverse=True)[0] for g
            in fleure_db.utils.sgroupby(ups, itemgetter("name"))]


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
    ...             update_names=["kernel", "tzdata"])
    >>> ert1 = dict(advisory="RHSA-2015:XXX2",
    ...             update_names=["glibc", "tzdata"])
    >>> ers = errata_of_rpms_g([ert0, ert1], ("kernel", ))
    >>> ert0 in ers
    True
    >>> ert1 in ers
    False
    """
    for ert in ers:
        if any(n in ert["update_names"] for n in rpms):
            yield ert


def list_update_errata_pairs(ers):
    """
    :param ers: A list of errata dict
    :return: A list of (update_name, [errata_advisory])

    >>> ers = [dict(advisory="RHSA-2015:XXX1",
    ...             update_names=["kernel", "tzdata"]),
    ...        dict(advisory="RHSA-2014:XXX2",
    ...             update_names=["glibc", "tzdata"])
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
    ...         'update_names': ['kernel-headers', 'kernel']},
    ...        {'advisory': 'RHSA-2015:1513',
    ...         'update_names': ['bind-utils']},
    ...        {'advisory': 'RHSA-2015:1081',
    ...         'update_names': ['kernel-headers', 'kernel']}
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
    kfn = lambda e: (len(e.get("keywords", [])), e["issue_date"],
                     e["update_names"])
    rhba_by_kwds = sorted(errata_of_keywords_g(rhba, keywords, pkeywords),
                          key=kfn, reverse=True)
    rhba_of_core_rpms_by_kwds = \
        sorted(errata_of_rpms_g(rhba_by_kwds, core_rpms),
               key=kfn, reverse=True)
    rhba_of_rpms = sorted(errata_of_rpms_g(rhba, core_rpms),
                          key=itemgetter("update_names"), reverse=True)
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


def _cve_socre_ge(cve, score=0, default=False):
    """
    :param cve: A dict contains CVE and CVSS info.
    :param score: Lowest score to select CVEs (float). It's Set to 4.0 (PCIDSS
        limit) by default:

        * NVD Vulnerability Severity Ratings: http://nvd.nist.gov/cvss.cfm
        * PCIDSS: https://www.pcisecuritystandards.org

    :param default: Default value if failed to get CVSS score to compare with
        given score

    :return: True if given CVE's socre is greater or equal to given score.
    """
    if "score" not in cve:
        LOG.warn(_("CVE %(cve)s lacks of CVSS base metrics and score"), cve)
        return default
    try:
        return float(cve["score"]) >= float(score)
    except (KeyError, ValueError):
        LOG.warn(_("Failed to compare CVE's score: %s, score=%.1f"),
                 str(cve), score)

    return default


def higher_score_cve_errata_g(ers, score=0):
    """
    :param ers: A list of errata
    :param score: CVSS base metrics score
    """
    for ert in ers:
        # NOTE: Skip older CVEs do not have CVSS base metrics and score.
        cves = [c for c in ert.get("cves", []) if "score" in c]
        if cves and any(_cve_socre_ge(cve, score) for cve in cves):
            cvsses_s = ", ".join("{cve} ({score}, {metrics})".format(**c)
                                 for c in cves)
            cves_s = ", ".join("{cve} ({url})".format(**c) for c in cves)
            ert["cvsses_s"] = cvsses_s
            ert["cves_s"] = cves_s

            yield ert


def analyze_errata(ers, score=fleure_db.globals.CVSS_MIN_SCORE,
                   keywords=fleure_db.globals.ERRATA_KEYWORDS,
                   pkeywords=None, core_rpms=fleure_db.globals.CORE_RPMS):
    """
    :param ers: A list of applicable errata sorted by severity
        if it's RHSA and advisory in ascending sequence
    :param score: CVSS base metrics score
    :param keywords: A tuple of keywords to filter 'important' RHBAs
    :param pkeywords: Similar to above but a dict gives the list per RPMs
    :param core_rpms: Core RPMs to filter errata by them
    """
    rhsa = [e for e in ers if e["advisory"][2] == 'S']
    rhba = [e for e in ers if e["advisory"][2] == 'B']
    rhea = [e for e in ers if e["advisory"][2] == 'E']

    rhsa_data = analyze_rhsa(rhsa)
    rhba_data = analyze_rhba(rhba, keywords=keywords, pkeywords=pkeywords,
                             core_rpms=core_rpms)
    if score > 0:
        rhba_by_score = list(higher_score_cve_errata_g(rhba, score))
        us_of_rhba_by_score = list_updates_from_errata(rhba_by_score)
    else:
        rhsa_by_score = []
        rhba_by_score = []
        us_of_rhsa_by_score = []
        us_of_rhba_by_score = []

    rhsa_data["list_higher_cvss_score"] = rhsa_by_score
    rhba_data["list_higher_cvss_score"] = rhba_by_score
    rhsa_data["list_higher_cvss_updates"] = us_of_rhsa_by_score
    rhba_data["list_higher_cvss_updates"] = us_of_rhba_by_score

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


def mk_overview_dataset(data, score=fleure_db.globals.CVSS_MIN_SCORE,
                        keywords=fleure_db.globals.ERRATA_KEYWORDS,
                        core_rpms=None, **kwargs):
    """
    :param data: RPMs, Update RPMs and various errata data summarized
    :param score: CVSS base metrics score limit
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

    if score > 0:
        rows += [[],
                 [_("RHSAs and RHBAs by CVSS score")],
                 [_("# of RHSAs of CVSS Score >= %.1f") % score,
                  len(data["errata"]["rhsa"]["list_higher_cvss_score"])],
                 [_("# of Update RPMs by the above RHSAs at minimum"),
                  len(data["errata"]["rhsa"]["list_higher_cvss_updates"])],
                 [_("# of RHBAs of CVSS Score >= %.1f") % score,
                  len(data["errata"]["rhba"]["list_higher_cvss_score"])],
                 [_("# of Update RPMs by the above RHBAs at minimum"),
                  len(data["errata"]["rhba"]["list_higher_cvss_updates"])]]

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

# vim:sw=4:ts=4:et:
