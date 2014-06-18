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
from database_credentials import SERVER, MAIN_DB

db = SessionManager.session(SERVER, MAIN_DB)
from collections import defaultdict

thumbnail_image = "http://opds-spec.org/image/thumbnail" 
full_image = "http://opds-spec.org/image" 

class OPDSFeed(self):
   
    def make_entry(work, lane_link):

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

    def make_feed(url, title, works): 
        lane_link = dict(rel="collection", href=url)
        title = title
        feed = AtomFeed(title, [], url=url)
        for work in works:
            work_o = make_entry(work, lane_link)
            if work_o:
                feed.add(**work_o)
        return feed

    def main_navigation_feed(self, _db, language):

        navigation_feed.add(
            title=lane.name,
            id="tag:%s:%s" % (language, lane.name),
            links=links,
            updated=datetime.datetime.utcnow(),
        )

        for Lane in _db.query(Lane):
            
        pass

    def url(self, language, lane, order):
        d = dict(
            language=urllib.quote(language),
            name=urllib.quote(lane.name),
            order=urllib.quote(order))
        return "/lanes/%(language)s/%(lange)s?order=%(order)s" % d

    def recommended_feed(self, language, lane):
        url = self.url(language, lane, "recommended")
        links = []
        feed_size = 20
        query = db.query(Work).filter(
            Work.languages==language,
            Work.lane==lane.name,
            Work.quality > 5,
            Work.quality < 1000).order_by(Work.quality).limit(1000)
        c = query.count()
        results = query.all()
        if len(results) < feed_size:
            sample = results
            we_need = feed_size - len(results)
            query = db.query(Work).filter(
                Work.languages==language,
                Work.lane==lane.name,
                Work.quality > 1, Work.quality < 5).order_by(Work.quality).limit(we_need)
            sample += query.all()
        else:
            sample = random.sample(results, feed_size)

        return self.make_feed(url, lane.name, sample)

    def title_feed(self, language, lane):
        url = self.url(language, lane, "title")
        # Build a collection by title
        query = db.query(Work).filter(
            Work.languages==language,
            Work.lane==lane.name).order_by(Work.title).limit(50)

        return make_feed(title_url, lane.name, query)
