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
from lane import Lane
from database_credentials import SERVER, MAIN_DB

db = SessionManager.session(SERVER, MAIN_DB)
from collections import defaultdict

thumbnail_image = "http://opds-spec.org/image/thumbnail" 
full_image = "http://opds-spec.org/image" 

def hack_link(url):
    return url.replace("content=L", "content=M").replace(
        "Type=L", "Type=M")

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
        return None
    #work_id = md5.md5(epub_href).hexdigest()
    url = "http://localhost/works/%s" % r.id
    return dict(title=work.title, url=url, id=url,
                author=work.authors or "", 
                links=[dict(rel=open_access,
                            href=epub_href, type=epub_type),
                       dict(rel=thumbnail_image,
                            href=hack_link(work.thumbnail_cover_link)),
                       dict(rel=full_image,
                            href=hack_link(work.full_cover_link)),
                       lane_link,
                   ],
                updated=datetime.datetime.utcnow())

dest = "opds"
if not os.path.exists(dest):
    os.makedirs(dest)

for language in ["eng", "fre"]:
    navigation_feed = AtomFeed("Navigation feed (%s)" % language, [],
                               url="http://localhost/lanes/" + language)
    for lane in Lane.self_and_sublanes():
        if lane is Lane:
            continue

        rec_url = "file://%s.%s.recommended.html" % (urllib.quote(language),
                                                     urllib.quote(lane.name))

        title_url = "file://%s.%s.title.html" % (urllib.quote(language),
                                                    urllib.quote(lane.name))

        links=[]
        for url, rel in ((rec_url, "http://opds-spec.org/recommended"),
                         (title_url, "subsection")):

            links.append(
                dict(
                    type="application/atom+xml;profile=opds-catalog;kind=acquisition",
                    href=url,
                    rel=rel,
                ))

        navigation_feed.add(
            title=lane.name,
            id=url,
            links=links,
            updated=datetime.datetime.utcnow(),
        )


        # Build a recommended collection
        lane_link = dict(rel="collection", href=url)
        title = "%s (%s)" % (lane.name, language)
        feed = AtomFeed(title, [], url=url)
        for work in db.query(Work).filter(
                Work.languages==language,
                Work.lane==lane.name).order_by(func.random()).limit(20):
            work_o = make_entry(work, lane_link)
            if work_o:
                feed.add(**work_o)

        if feed.entries:
            print "Creating %s/%s" % (language, lane.name)
            path = os.path.join(dest, "%s.%s.recommended.xml" % (language, lane.name))
            out = open(path, "w")
            out.write(unicode(feed).encode("utf-8"))
            out.close()

        # Build a collection by title
        lane_link = dict(rel="collection", href=title_url)
        title = "%s (%s, by title)" % (lane.name, language)
        feed = AtomFeed(title, [], url=url)
        for work in db.query(Work).filter(
                Work.languages==language,
                Work.lane==lane.name).order_by(Work.title).limit(50):
            work_o = make_entry(work, lane_link)
            if work_o:
                feed.add(**work_o)

        if feed.entries:
            print "Creating %s/%s by title" % (language, lane.name)
            path = os.path.join(dest, "%s.%s.title.xml" % (language, lane.name))
            out = open(path, "w")
            out.write(unicode(feed).encode("utf-8"))
            out.close()
            
    path = os.path.join(dest, "Navigation.%s.xml" % language)
    out = open(path, "w")
    out.write(unicode(navigation_feed).encode("utf-8"))
    out.close()
        
