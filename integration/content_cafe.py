import os
from cStringIO import StringIO

from PIL import Image
import requests
from nose.tools import set_trace

from sqlalchemy import and_
from core.model import (
    DataSource,
    Resource,
    Identifier,
    )

class ContentCafeMirror(object):
    """Associates up to four resources with an ISBN."""

    image_url = "http://contentcafe2.btol.com/ContentCafe/Jacket.aspx?userID=%(userid)s&password=%(password)s&Type=L&Value=%(isbn)s"
    overview_url="http://contentcafe2.btol.com/ContentCafeClient/ContentCafe.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    review_url = "http://contentcafe2.btol.com/ContentCafeClient/ReviewsDetail.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    summary_url = "http://contentcafe2.btol.com/ContentCafeClient/Summary.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    author_notes_url = "http://contentcafe2.btol.com/ContentCafeClient/AuthorNotes.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"

    COVER_DIR = "cover"
    REVIEW_DIR = "review"
    SUMMARY_DIR = "summary"
    AUTHOR_NOTES_DIR = "author_notes"
    SCALED_SUBDIR = "scaled"

    ORIGINAL_PATH_VARIABLE = "content_cafe_mirror"
    SCALED_PATH_VARIABLE = "scaled_content_cafe_mirror"
    DATA_SOURCE = DataSource.CONTENT_CAFE

    @classmethod
    def data_directory(cls, base_directory):
        return os.path.join(base_directory, DataSource.CONTENT_CAFE)

    @classmethod
    def scaled_image_directory(self, base_data_directory):
        return os.path.join(base_data_directory, DataSource.CONTENT_CAFE,
                            self.SCALED_SUBDIR)

    def __init__(self, db, data_dir, userid, password):
        
        self.base_cache_dir = self.data_directory(data_dir)
        for i in (self.COVER_DIR, self.REVIEW_DIR,
                  self.SUMMARY_DIR, self.AUTHOR_NOTES_DIR):
            p = os.path.join(self.base_cache_dir, i)
            if not os.path.exists(p):
                os.makedirs(p)
        self._db = db
        self.userid = userid
        self.password = password
        self.data_source = DataSource.lookup(self._db, DataSource.CONTENT_CAFE)

    def run(self):
        # Find all ISBNs that don't have any associated Content Cafe
        # resources.
        c = and_(Identifier.id==Resource.work_identifier_id,
                 Resource.data_source==self.data_source)
        qu = self._db.query(Identifier).outerjoin(
            Resource, c).filter(
                Identifier.type==Identifier.ISBN).filter(
                    Resource.id==None)
        resultset = qu.limit(100).all()
        while resultset:
            for wi in resultset:
                self.mirror_isbn(wi)
            self._db.commit()
            resultset = qu.limit(100).all()
        self._db.commit()

    def _process_path(self, path):
        path = path.replace("%", "%%")
        path = path.replace(
            self.base_cache_dir, "%(" + self.PATH_VARIABLE + ")s", 1)
        return path

    def mirror_isbn(self, work_identifier):
        # First grab an image.
        isbn = work_identifier.identifier
        args = dict(userid=self.userid, password=self.password, isbn=isbn)
        image_url = self.image_url % args
        # TODO: assuming everything is a JPEG might not actually work.
        path = os.path.join(
            self.base_cache_dir, self.COVER_DIR, "%s.jpg" % isbn)
        if os.path.exists(path):
            cached = True
            content_type = "image/jpeg"
            c = open(path).read()
        else:
            cached = False
            response = requests.get(image_url)
            content_type = response.headers['Content-Type']

        resource, ignore = work_identifier.add_resource(
            Resource.IMAGE, image_url, self.data_source)
        if content_type.startswith('image'):
            if not cached:
                # Write it to disk and log it as mirrored.
                c = response.content
                # Content Cafe jams a short HTML page onto the end of
                # every image. Strip it.
                i = c.rindex("<!DOCTYPE")
                if i != -1:
                    c = c[:i]
                if len(c) == 2070:
                    # This is a placeholder image. This indicates that
                    # Content Cafe may have heard of this ISBN, but
                    # doesn't have a cover for it. We'll keep making
                    # requests.
                    resource.could_not_mirror()
                    return
                else:
                    print "%s %s %s" % (
                        work_identifier.identifier, Resource.IMAGE, image_url)
                    with open(path, "w") as f:
                        f.write(c)
            path = self._process_path(path)
            resource.mirrored_to(path, content_type, c)            
            # We got an image, now get the other stuff.
            if resource.mirrored and not cached:
                self.get_summary(work_identifier, args)
                self.get_reviews(work_identifier, args)
                self.get_author_notes(work_identifier, args)

        else:
            # Content Cafe served us an HTML page instead of an
            # image. This indicates that Content Cafe has no knowledge
            # of this ISBN. There is no need to make any more
            # requests.
            resource.could_not_mirror()

    def get_associated_web_resource(
            self, work_identifier, args, url, 
            phrase_indicating_missing_data,
            cache_directory,
            rel):
        url = url % args
        response = requests.get(url)
        content_type = response.headers['Content-Type']
        if not phrase_indicating_missing_data in response.content:
            print "%s %s %s" % (work_identifier.identifier, rel, url)
            path = os.path.join(
                self.base_cache_dir, cache_directory, "%s.html" % work_identifier.identifier)
            c = response.content
            with open(path, "w") as f:
                f.write(c)
            path = self._process_path(path)
            resource, ignore = work_identifier.add_resource(
                rel, url, self.data_source)
            resource.mirrored_to(path, content_type, c)

    def get_reviews(self, work_identifier, args):
        return self.get_associated_web_resource(
            work_identifier, args, self.review_url,
            'No review info exists for this item',
            self.REVIEW_DIR, Resource.REVIEW)

    def get_summary(self, work_identifier, args):
        return self.get_associated_web_resource(
            work_identifier, args, self.summary_url,
            'No annotation info exists for this item',
            self.SUMMARY_DIR, Resource.DESCRIPTION)

    def get_author_notes(self, work_identifier, args):
        return self.get_associated_web_resource(
            work_identifier, args, self.author_notes_url,
            'No author notes info exists for this item',
            self.AUTHOR_NOTES_DIR, Resource.AUTHOR)
