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
from pyatom import AtomFeed
import md5
from sqlalchemy.sql.expression import func

d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    WorkIdentifier,
    WorkRecord,
    Work,
    )
from flask import request, url_for

from lane import Lane, Unclassified

class URLRewriter(object):

    epub_id = re.compile("/([0-9]+)")

    GUTENBERG_MIRROR_HOST = "http://gutenberg.10.128.36.172.xip.io/"
    GENERATED_COVER_HOST = "http://covers.10.128.36.172.xip.io/"

    @classmethod
    def rewrite(cls, url):
        parsed = urlparse(url)
        if parsed.hostname in ('www.gutenberg.org', 'gutenberg.org'):
            return cls._rewrite_gutenberg(parsed)
        else:
            return url

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
        return urljoin(cls.GUTENBERG_MIRROR_HOST, new_path)


class OPDSFeed(AtomFeed):

    ACQUISITION_FEED_TYPE = "application/atom+xml;profile=opds-catalog;kind=acquisition"
    NAVIGATION_FEED_TYPE = "application/atom+xml;profile=opds-catalog;kind=navigation"

    RECOMMENDED_REL = "http://opds-spec.org/recommended"
    OPEN_ACCESS_REL = "http://opds-spec.org/acquisition/open-access"
    THUMBNAIL_IMAGE_REL = "http://opds-spec.org/image/thumbnail" 
    FULL_IMAGE_REL = "http://opds-spec.org/image" 
    EPUB_MEDIA_TYPE = "application/epub+zip"

    @classmethod
    def lane_url(cls, languages, lane, order=None):
        if isinstance(lane, Lane):
            lane = lane.name
        if isinstance(languages, list):
            languages = ",".join(languages)

        return url_for('feed', languages=languages, lane=lane, order=order,
                       _external=True)

class AcquisitionFeed(OPDSFeed):

    def __init__(self, _db, title, url, works):
        super(AcquisitionFeed, self).__init__(title, [], url=url)
        lane_link = dict(rel="collection", href=url)
        for work in works:
            self.add_entry(work, lane_link)

    @classmethod
    def by_title(cls, _db, languages, lane):
        if isinstance(lane, Lane):
            lane = lane.name
        if not isinstance(languages, list):
            languages = [languages]

        url = cls.lane_url(languages, lane, "title")
        return AcquisitionFeed(_db, "%s: by title" % lane, url, query)

    @classmethod
    def by_author(cls, _db, languages, lane):
        if isinstance(lane, Lane):
            lane = lane.name
        if not isinstance(languages, list):
            languages = [languages]

        url = cls.lane_url(languages, lane, "author")
        query = _db.query(Work).filter(
            Work.languages.in_(languages),
            Work.lane==lane).order_by(Work.authors).limit(50)
        return AcquisitionFeed(_db, "%s: by author" % lane, url, query)

    @classmethod
    def recommendations(cls, _db, languages, lane):
        if isinstance(lane, Lane):
            lane = lane.name
        if not isinstance(languages, list):
            languages = [languages]

        url = cls.lane_url(languages, lane)
        links = []
        feed_size = 20
        works = Work.quality_sample(_db, languages, lane, 75, 1, feed_size)
        return AcquisitionFeed(
            _db, "%s: recommendations" % lane, url, works)

    def create_entry(self, work, lane_link):
        """Turn a work into an entry for an acquisition feed."""
        # Find the .epub link
        epub_href = None
        p = None
        active_license_pool = None
        for p in work.license_pools:
            if p.open_access:
                active_license_pool = p
                break

        # There's no reason to present a book that has no active license pool.
        if not active_license_pool:
            return False

        work_record = active_license_pool.work_record()
        identifier = work_record.primary_identifier
        key = "/".join(map(urllib.quote, [work_record.data_source.name, identifier.identifier]))
        checkout_url = url_for(
            "checkout", data_source=work_record.data_source.name,
            identifier=identifier.identifier)

        links=[dict(rel=self.OPEN_ACCESS_REL, 
                    href=checkout_url)]

        if work.thumbnail_cover_link:
            url = URLRewriter.rewrite(work.thumbnail_cover_link)
            links.append(dict(rel=self.THUMBNAIL_IMAGE_REL, href=url))
        if work.full_cover_link:
            url = URLRewriter.rewrite(work.full_cover_link)
            links.append(dict(rel=self.FULL_IMAGE_REL, href=url))
        elif identifier.type == WorkIdentifier.GUTENBERG_ID:
            host = URLRewriter.GENERATED_COVER_HOST
            url = urljoin(
                host, urllib.quote(
                    "/Gutenberg ID/%s.png" % identifier.identifier))
            links.append(dict(rel=self.FULL_IMAGE_REL, href=url))


        tag = "tag:work:%s" % work.id
        entry = dict(title=work.title, url=checkout_url, id=tag,
                    author=work.authors or "", 
                    summary="Quality: %s" % work.quality,
                    links=links,
                    updated=datetime.datetime.utcnow())
        return entry

    def add_entry(self, work, lane_link):
        entry = self.create_entry(work, lane_link)
        if entry:
            self.add(**entry)


class NavigationFeed(OPDSFeed):

    @classmethod
    def main_feed(self, parent_lane, languages):
        if isinstance(languages, basestring):
            languages = [languages]
        language_code = ",".join(sorted(languages))
        feed = NavigationFeed(
            "Navigation feed", [],
            url=url_for('navigation_feed', languages=language_code,
                        _external=True))

        for lane in sorted(parent_lane.self_and_sublanes(), key=lambda x: x.name):
            if not lane.name:
                continue
            lane = lane.name
            links = []
            for title, order, rel in [
                    ('By title', 'title', 'subsection'),
                    ('By author', 'author', 'subsection'),
                    ('Recommended', None, self.RECOMMENDED_REL)]:
                link = dict(
                    type=self.ACQUISITION_FEED_TYPE,
                    href=self.lane_url(languages, lane, order),
                    rel=rel,
                    title=title,
                )
                links.append(link)

            feed.add(
                title=lane,
                id="tag:%s:%s" % (language_code, lane),
                url=self.lane_url(languages, lane),
                links=links,
                updated=datetime.datetime.utcnow(),
            )
        return feed
