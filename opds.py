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

thumbnail_image = "http://opds-spec.org/image/thumbnail" 
full_image = "http://opds-spec.org/image" 

class OPDSFeed(object):

    @classmethod
    def make_entry(cls, work, lane_link):

        # Find the .epub link
        open_access = "http://opds-spec.org/acquisition/open-access"
        epub_href = None
        id = None
        for r in work.work_records:
            if not open_access in r.links:
                continue
            for l in r.links[open_access]:
                if l['type'].startswith("application/epub+zip"):
                    epub_href, epub_type = l['href'], l['type']
                    if not 'noimages' in epub_href:
                        break

        if not epub_href:
            # print "No epub link for %s, probably an audiobook." % work.title
            return None
        #work_id = md5.md5(epub_href).hexdigest()
        url = "http://localhost/works/%s" % r.id

        links=[dict(rel=open_access,
                    href=epub_href, type=epub_type),
               lane_link,
        ]

        if work.thumbnail_cover_link:
            links.append(dict(rel=thumbnail_image,
                              href=work.thumbnail_cover_link))
        if work.full_cover_link:
            links.append(dict(rel=full_image, href=work.full_cover_link))

        return dict(title=work.title, url=url, id=url,
                    author=work.authors or "", 
                    summary="Quality: %s" % work.quality,
                    links=links,
                    updated=datetime.datetime.utcnow())

    @classmethod
    def make_feed(cls, url, title, works): 
        lane_link = dict(rel="collection", href=url)
        title = title
        feed = AtomFeed(title, [], url=url)
        for work in works:
            work_o = cls.make_entry(work, lane_link)
            if work_o:
                feed.add(**work_o)
        return feed

    @classmethod
    def main_navigation_feed(cls, _db, language):
        navigation_feed = AtomFeed("Navigation feed (%s)" % language, [],
                                   url=CONFIG['base_url'] + "/lanes/%s" % language)

        for lane in Lane.self_and_sublanes():
            if lane == Lane:
                continue
            lane = lane.name
            links = []
            for order, rel in [
                    ('title', 'subsection'),
                    ('recommended', "http://opds-spec.org/recommended")]:
                link = dict(
                    type="application/atom+xml;profile=opds-catalog;kind=acquisition",
                    href=cls.url(language, lane, order),
                    rel=rel,
                )
                links.append(link)

            navigation_feed.add(
                title=lane,
                id="tag:%s:%s" % (language, lane),
                links=links,
                updated=datetime.datetime.utcnow(),
            )
        return navigation_feed

    @classmethod
    def url(cls, language, lane, order):
        d = dict(
            base=CONFIG['base_url'],
            language=urllib.quote(language),
            lane=urllib.quote(lane),
            order=urllib.quote(order))
        return "%(base)s/lanes/%(language)s/%(lane)s?order=%(order)s" % (d)

    @classmethod
    def recommended_feed(cls, db, language, lane):
        if isinstance(lane, Lane):
            lane = lane.name
        url = cls.url(language, lane, "recommended")
        links = []
        feed_size = 20
        query = db.query(Work).filter(
            Work.languages==language,
            Work.lane==lane,
            Work.quality > 5,
            Work.quality < 1000).order_by(Work.quality).limit(1000)
        c = query.count()
        results = query.all()
        if len(results) < feed_size:
            sample = results
            we_need = feed_size - len(results)
            query = db.query(Work).filter(
                Work.languages==language,
                Work.lane==lane,
                Work.quality > 1, Work.quality < 5).order_by(Work.quality).limit(we_need)
            sample += query.all()
        else:
            sample = random.sample(results, feed_size)

        return cls.make_feed(url, lane, sample)

    @classmethod
    def title_feed(cls, db, language, lane):
        if isinstance(lane, Lane):
            lane = lane.name
        url = cls.url(language, lane, "title")
        # Build a collection by title
        query = db.query(Work).filter(
            Work.languages==language,
            Work.lane==lane).order_by(Work.title).limit(50)

        return cls.make_feed(url, lane, query)
