from pdb import set_trace
import os
import site
import sys
import datetime
import random
import urllib
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

    @classmethod
    def url(cls, languages, lane, order):
        if isinstance(lane, Lane):
            lane = lane.name
        d = dict(
            base=CONFIG['base_url'],
            language=urllib.quote(",".join(languages)),
            lane=urllib.quote(lane),
            order=urllib.quote(order))
        return "%(base)s/lanes/%(language)s/%(lane)s?order=%(order)s" % (d)


class AcquisitionFeed(OPDSFeed):

    def __init__(self, _db, title, url, works)
        super(AcquisitionFeed, self).__init__(title, [], url=url)
        lane_link = dict(rel="collection", href=url)
        for work in works:
            work_o = cls.make_entry(work, lane_link)
            if work_o:
                self.add(**work_o)
        return feed        

    @classmethod
    def by_title(cls, _db, languages, lane):
        if isinstance(lane, Lane):
            lane = lane.name
        url = cls.url(languages, lane, "title")
        query = db.query(Work).filter(
            Work.languages.in_(languages),
            Work.lane==lane).order_by(Work.title).limit(50)
        return AcquisitionFeed(_db, "%s: by title" % lane, url, query)

    @classmethod
    def by_author(cls, _db, languages, lane):
        if isinstance(lane, Lane):
            lane = lane.name
        url = cls.url(languages, lane, "author")
        query = db.query(Work).filter(
            Work.languages.in_(languages),
            Work.lane==lane).order_by(Work.authors).limit(50)
        return AcquisitionFeed(_db, "%s: by author" % lane, url, query)


    @classmethod
    def quality_sample(
            cls, _db, languages, lane, quality_min_start,
            quality_min_rock_bottom, target_size):
        """Get randomly selected Works that meet minimum quality criteria.

        Bring the quality criteria as low as necessary to fill a feed
        of the given size, but not below `quality_min_rock_bottom`.
        """
        quality_min = start_at
        results = []
        while quality_min >= rock_bottom and len(results) < target_size:
            remaining = target_size - len(results)
            query = db.query(Work).filter(
                Work.languages.in_(language),
                Work.lane==lane,
                Work.quality > quality_min).order_by(
                    func.random()).limit(
                        remaining)
            results += query.all()
            quality_min *= 0.5
        return results

    @classmethod
    def recommendations(cls, _db, languages, lane):
        if isinstance(lane, Lane):
            lane = lane.name
        url = cls.url(languages, lane, "recommended")
        links = []
        feed_size = 20
        works = cls.quality_sample(_db, languages, lane, 75, 1, feed_size)
        return AcquisitionFeed(
            _db, "%s: recommendations" % lane, url, works)

    def add_entry(self, work, lane_link):
        """Turn a work into an entry in this acquisition feed."""
        # Find the .epub link
        epub_href = None
        p = None
        for p in work.license_pools:
            r = p.primary_work_record
            if not self.OPEN_ACCESS_REL in r.links:
                continue
            for l in r.links[open_access]:
                if l['type'].startswith("application/epub+zip"):
                    epub_href, epub_type = l['href'], l['type']

                    # If we find a 'noimages' epub, we'll keep
                    # looking in hopes of finding a better one.
                    if not 'noimages' in epub_href:
                        break

        if not epub_href:
            # This work has no available epub links. Most likely situation
            # is it's an audio book.
            return None

        links=[dict(rel=self.OPEN_ACCESS_REL,
                    href=epub_href, type=epub_type),
               lane_link,
        ]

        if work.thumbnail_cover_link:
            links.append(dict(rel=self.THUMBNAIL_IMAGE_REL,
                              href=work.thumbnail_cover_link))
        if work.full_cover_link:
            links.append(dict(rel=self.FULL_IMAGE_REL,
                              href=work.full_cover_link))

        url = "tag:licensepool:%s" % p.id
        entry = dict(title=work.title, url=url, id=url,
                    author=work.authors or "", 
                    summary="Quality: %s" % work.quality,
                    links=links,
                    updated=datetime.datetime.utcnow())
        self.add(**entry)


class NavigationFeed(OPDSFeed):

    pass

class MainNavigationFeed(NavigationFeed):

    def __init__(self, _db, languages):
        language_code = ",".join(sorted(languages))
        super(MainNavigationFeed, self).__init__(
            "Navigation feed", [],
            url=CONFIG['base_url'] + "/lanes/%s" % language_code)

        for lane in Lane.self_and_sublanes():
            if lane == Lane:
                continue
            lane = lane.name
            links = []
            for order, rel in [
                    ('title', 'subsection'),
                    ('author', 'subsection'),
                    ('recommended', self.RECOMMENDED_REL)]:
                link = dict(
                    type=self.ACQUISITION_FEED_TYPE,
                    href=cls.url(language_code, lane, order),
                    rel=rel,
                )
                links.append(link)

            self.add(
                title=lane,
                id="tag:%s:%s" % (language_code, lane),
                links=links,
                updated=datetime.datetime.utcnow(),
            )

