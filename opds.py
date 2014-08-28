from collections import defaultdict
from nose.tools import set_trace
import re
import os
import site
import sys
import datetime
import random
import urllib
from urlparse import urlparse, urljoin
import md5
from sqlalchemy.sql.expression import func
from sqlalchemy.orm.session import Session

from lxml import builder, etree

d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    WorkIdentifier,
    WorkRecord,
    Work,
    )
from flask import request, url_for

from lane import Lane, Unclassified

ATOM_NAMESPACE = atom_ns = 'http://www.w3.org/2005/Atom'
app_ns = 'http://www.w3.org/2007/app'
xhtml_ns = 'http://www.w3.org/1999/xhtml'
dcterms_ns = 'http://purl.org/dc/terms/'
opds_ns = 'http://opds-spec.org/2010/catalog'

nsmap = {
    None: atom_ns,
    'app': app_ns,
    'dcterms' : dcterms_ns,
    'opds' : opds_ns,
}

def _strftime(d):
    """
Format a date the way Atom likes it (RFC3339?)
"""
    return d.strftime('%Y-%m-%dT%H:%M:%SZ%z')

E = builder.ElementMaker(typemap={datetime: lambda e, v: _strftime(v)},
                         nsmap=nsmap)


class URLRewriter(object):

    epub_id = re.compile("/([0-9]+)")

    GUTENBERG_MIRROR_HOST = "https://s3.amazonaws.com/book-covers.nypl.org/Gutenberg-Illustrated"
    GENERATED_COVER_HOST = "https://s3.amazonaws.com/gutenberg-corpus.nypl.org/Generated+covers"
    CONTENT_CAFE_MIRROR_HOST = "https://s3.amazonaws.com/book-covers.nypl.org/CC"

    @classmethod
    def rewrite(cls, url):
        parsed = urlparse(url)
        if parsed.hostname in ('www.gutenberg.org', 'gutenberg.org'):
            return cls._rewrite_gutenberg(parsed)
        else:
            return url % dict(content_cafe_mirror=cls.CONTENT_CAFE_MIRROR_HOST,
                              gutenberg_illustrated_mirror=cls.GUTENBERG_MIRROR_HOST)

    @classmethod
    def _rewrite_gutenberg(cls, parsed):
        if parsed.path.startswith('/cache/epub/'):
            new_path = parsed.path.replace('/cache/epub/', '', 1)
        elif '.epub' in parsed.path:
            text_id = cls.epub_id.search(parsed.path).groups()[0]
            if 'noimages' in parsed.path:
                new_path = "%(pub_id)s/pg%(pub_id)s.epub" 
            else:
                new_path = "%(pub_id)s/pg%(pub_id)s-images.epub"
            new_path = new_path % dict(pub_id=text_id)
        else:
            new_path = parsed_path
        return cls.GUTENBERG_MIRROR_HOST + '/' + new_path


class AtomFeed(object):

    def __init__(self, title, url):
        self.feed = E.feed(
            E.id(url),
            E.title(title),
            E.updated(_strftime(datetime.datetime.utcnow())),
            E.link(href=url),
            E.link(href=url, rel="self"),
        )

    def add_link(self, **kwargs):
        self.feed.append(E.link(**kwargs))

    def __unicode__(self):
        return etree.tostring(self.feed, pretty_print=True)

class OPDSFeed(AtomFeed):

    ACQUISITION_FEED_TYPE = "application/atom+xml;profile=opds-catalog;kind=acquisition"
    NAVIGATION_FEED_TYPE = "application/atom+xml;profile=opds-catalog;kind=navigation"

    FEATURED_REL = "http://opds-spec.org/featured"
    RECOMMENDED_REL = "http://opds-spec.org/recommended"
    OPEN_ACCESS_REL = "http://opds-spec.org/acquisition/open-access"
    FULL_IMAGE_REL = "http://opds-spec.org/image" 
    EPUB_MEDIA_TYPE = "application/epub+zip"

    @classmethod
    def lane_url(cls, lane, order=None):
        if isinstance(lane, Lane):
            lane = lane.name

        return url_for('feed', lane=lane, order=order, _external=True)

class AcquisitionFeed(OPDSFeed):

    def __init__(self, _db, title, url, works, facet_url_generator=None):
        super(AcquisitionFeed, self).__init__(title, url=url)
        lane_link = dict(rel="collection", href=url)
        for work in works:
            self.add_entry(work, lane_link)

        if facet_url_generator:
            for title, order, facet_group, in [
                    ('Title', 'title', 'Sort by'),
                    ('Author', 'author', 'Sort by')]:
                link = dict(href=facet_url_generator(order),
                            title=title)
                link['rel'] = "http://opds-spec.org/facet"
                link['{%s}facetGroup' % opds_ns] = facet_group
                self.add_link(**link)

    @classmethod
    def featured(cls, _db, languages, lane):
        if isinstance(lane, Lane):
            lane = lane.name

        url = cls.lane_url(lane)
        links = []
        feed_size = 20
        works = Work.quality_sample(_db, languages, lane, 75, 1, feed_size)
        return AcquisitionFeed(
            _db, "%s: featured" % lane, url, works)

    @classmethod
    def active_loans_for(cls, patron):
        db = Session.object_session(patron)
        url = url_for('active_loans', _external=True)
        return AcquisitionFeed(db, "Active loans", url, patron.works_on_loan())

    def add_entry(self, work, lane_link, loan=None):
        entry = self.create_entry(work, lane_link, loan)
        if entry is not None:
            self.feed.append(entry)
        return entry

    def create_entry(self, work, lane_link, loan=None):
        """Turn a work into an entry for an acquisition feed."""
        # Find the .epub link
        epub_href = None
        p = None

        active_license_pool = None
        if loan:
            # The active license pool is the one associated with
            # the loan.
            active_license_pool = loan.license_pool
        else:
            # The active license pool is the one that *would* be associated
            # with a loan, were a loan to be issued right now.
            open_access_license_pool = None
            for p in work.license_pools:
                if p.open_access:
                    # Make sure there's a usable link--it might be
                    # audio-only or something.
                    if p.work_record().best_open_access_link:
                        open_access_license_pool = p
                else:
                    active_license_pool = p
                    break
            if not active_license_pool:
                active_license_pool = open_access_license_pool

        # There's no reason to present a book that has no active license pool.
        if not active_license_pool:
            return None

        # TODO: If there's an active loan, the links and the license
        # information should be much different. But we currently don't
        # include license information at all, because OPDS For
        # Libraries is still in flux. So for now we always put up an
        # open access link that leads to the checkout URL.
        identifier = active_license_pool.identifier
        checkout_url = url_for(
            "checkout", data_source=active_license_pool.data_source.name,
            identifier=identifier.identifier, _external=True)

        links=[E.link(rel=self.OPEN_ACCESS_REL, 
                      href=checkout_url)]

        cover_quality = 0
        qualities = [("Work quality", work.quality)]
        if work.cover:
            if work.cover.mirrored_path:
                url = URLRewriter.rewrite(work.cover.mirrored_path)
            else:
                url = URLRewriter.rewrite(work.cover.href)
            links.append(E.link(rel=self.FULL_IMAGE_REL, href=url))
            qualities.append(("Cover quality", work.cover.quality))
        elif identifier.type == WorkIdentifier.GUTENBERG_ID:
            host = URLRewriter.GENERATED_COVER_HOST
            url = host + urllib.quote(
                    "/Gutenberg ID/%s.png" % identifier.identifier)
            links.append(E.link(rel=self.FULL_IMAGE_REL, href=url))

        tag = "tag:work:%s" % work.id

        if work.summary:
            summary = work.summary.content
            qualities.append(("Summary quality", work.summary.quality))
        else:
            summary = ""
        summary += "<ul>"
        for name, value in qualities:
            summary += "<li>%s: %.1f</li>" % (name, value)
        summary += "</ul>"

        entry = E.entry(
            E.id(tag),
            E.title(work.title),
            E.author(E.name(work.authors or "")),
            E.summary(summary),
            E.link(href=checkout_url),
            E.updated(_strftime(datetime.datetime.utcnow())),
            *links
        )
        
        language = work.language_code
        if language:
            language_tag = E._makeelement("{%s}language" % dcterms_ns)
            language_tag.text = language
            entry.append(language_tag)
        return entry


class NavigationFeed(OPDSFeed):

    @classmethod
    def main_feed(self, parent_lane):
        feed = NavigationFeed(
            "Navigation feed",
            url=url_for('navigation_feed', _external=True))

        for lane in sorted(parent_lane.self_and_sublanes(), key=lambda x: x.name):
            if not lane.name:
                continue
            lane = lane.name
            links = []

            for title, order, rel in [
                    ('All books', 'author', 'subsection'),
                    ('Featured', None, self.FEATURED_REL)
            ]:
                link = E.link(
                    type=self.ACQUISITION_FEED_TYPE,
                    href=self.lane_url(lane, order),
                    rel=rel,
                    title=title,
                )
                links.append(link)

            feed.feed.append(
                E.entry(
                    E.id("tag:%s" % (lane)),
                    E.title(lane),
                    E.link(href=self.lane_url(lane)),
                    E.updated(_strftime(datetime.datetime.utcnow())),
                    *links
                )
            )
        return feed
