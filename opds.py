# Some code for the checkout manager.
            # r = p.primary_work_record
            # if not self.OPEN_ACCESS_REL in r.links:
            #     continue
            # for l in r.links[open_access]:
            #     if l['type'].startswith(self.EPUB_MEDIA_TYPE):
            #         epub_href, epub_type = l['href'], l['type']

            #         # If we find a 'noimages' epub, we'll keep
            #         # looking in hopes of finding a better one.
            #         if not 'noimages' in epub_href:
            #             break


from nose.tools import set_trace
import os
import site
import sys
import datetime
import random
import urllib
from urlparse import urljoin
from pyatom import AtomFeed
import md5
from sqlalchemy.sql.expression import func

d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    SessionManager,
    WorkRecord,
    Work,
    )
from lane import Lane, Unclassified
from database_credentials import SERVER, CONFIG, MAIN_DB

db = SessionManager.session(SERVER, MAIN_DB)
from collections import defaultdict

class OPDSFeed(AtomFeed):

    ACQUISITION_FEED_TYPE = "application/atom+xml;profile=opds-catalog;kind=acquisition"
    NAVIGATION_FEED_TYPE = "application/atom+xml;profile=opds-catalog;kind=navigation"

    RECOMMENDED_REL = "http://opds-spec.org/recommended"
    OPEN_ACCESS_REL = "http://opds-spec.org/acquisition/open-access"
    THUMBNAIL_IMAGE_REL = "http://opds-spec.org/image/thumbnail" 
    FULL_IMAGE_REL = "http://opds-spec.org/image" 
    EPUB_MEDIA_TYPE = "application/epub+zip"

    @classmethod
    def _url(cls, base):
        return urljoin(CONFIG['site']['root'], base)

    @classmethod
    def lane_url(cls, languages, lane, order=None):
        if isinstance(lane, Lane):
            lane = lane.name
        d = dict(
            language=urllib.quote(",".join(languages)),
            lane=urllib.quote(lane),
        )

        base = "/lanes/%(language)s/%(lane)s" % d
        if order:
            base += "?order=%(order)s" % dict(order=urllib.quote(order))

        return cls._url(base)


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
        query = _db.query(Work).filter(
            Work.languages.in_(languages),
            Work.lane==lane).order_by(Work.title).limit(50)
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
        works = cls.quality_sample(_db, languages, lane, 75, 1, feed_size)
        return AcquisitionFeed(
            _db, "%s: recommendations" % lane, url, works)

    def create_entry(self, work, lane_link):
        """Turn a work into an entry for an acquisition feed."""
        # Find the .epub link
        epub_href = None
        p = None
        active_license_pool = False
        for p in work.license_pools:
            if p.open_access:
                active_license_pool = True
                break

        # There's no reason to present a book that has no active license pool.
        if not active_license_pool:
            return False

        links=[dict(rel=self.OPEN_ACCESS_REL, 
                    href=self._url("/works/%s/checkout" % work.id))]

        if work.thumbnail_cover_link:
            links.append(dict(rel=self.THUMBNAIL_IMAGE_REL,
                              href=work.thumbnail_cover_link))
        if work.full_cover_link:
            links.append(dict(rel=self.FULL_IMAGE_REL,
                              href=work.full_cover_link))

        url = "tag:work:%s" % work.id
        entry = dict(title=work.title, url=url, id=url,
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
        language_code = ",".join(sorted(languages))
        feed = NavigationFeed(
            "Navigation feed", [],
            url=urljoin(CONFIG['site']['root'], "/lanes/%s" % urllib.quote(language_code)))

        for lane in parent_lane.self_and_sublanes():
            if lane == Lane:
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
