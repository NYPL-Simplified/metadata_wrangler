from nose.tools import set_trace
import datetime
import gzip
import os
import random
import urlparse
import requests
import time

from sqlalchemy import or_
from sqlalchemy.orm import (
    aliased,
)

from core.model import (
    DataSource,
    Hyperlink,
    Resource,
    Representation,
)
from core.s3 import S3Uploader


class CoverImageMirror(object):
    """Downloads images via HTTP, saves them to the database,
    then uploads them to S3.
    """

    DATA_SOURCE = None

    def __init__(self, db):
        self._db = db
        self.data_source = DataSource.lookup(self._db, self.DATA_SOURCE)
        self.uploader = S3Uploader()

    def run(self):
        """Mirror all image resources associated with this data source."""
        q = self._db.query(Resource).filter(
                Resource.data_source==self.data_source)
        print "Mirroring %d images." % q.count()
        self.mirror_all_resources(q)

    def mirror_all_resources(self, q, force=False):
        """Mirror all resources that match a query."""
        # Only mirror images.
        q = q.filter(Hyperlink.rel==Hyperlink.IMAGE)
        now = datetime.datetime.utcnow()
        q = q.join(Hyperlink.resource).outerjoin(
            Resource.representation)
        if force:
            # Restrict to resources that are not already mirrored.
            q = q.filter(Representation.mirrored_at < now)
        else:
            q = q.filter(Representation.mirrored_at == None)

        resultset = q.limit(100).all()
        while resultset:
            print "Mirroring %d images." % len(resultset)
            to_upload = []
            for hyperlink in resultset:
                resource = hyperlink.resource
                if not resource.representation:
                    resource.representation, cached = Representation.get(
                        self._db, hyperlink.resource.url)
                representation = resource.representation
                extension = self.image_extensions_for_types.get(
                    representation.media_type, '')
                filename = "cover" + extension
                representation.mirror_url = self.uploader.cover_image_url(
                    hyperlink.data_source, hyperlink.identifier,
                    filename)

                to_upload.append(representation)

            self.uploader.mirror_batch(to_upload)
            for rep in to_upload:
                print "%s => %s %s" % (rep.url, rep.mirror_url, rep.mirrored_at)
            self._db.commit()
            resultset = q.limit(100).all()
        self._db.commit()

    types_for_image_extensions = { ".jpg" : "image/jpeg",
                                   ".gif" : "image/gif",
                                   ".png" : "image/png"}

    image_extensions_for_types = {}
    for k, v in types_for_image_extensions.items():
        image_extensions_for_types[v] = k

    def mirror_edition(self, edition):
        """Make sure that one specific edition has its cover(s) mirrored."""
        # Find all resources for this edition's primary identifier.
        q = self._db.query(Hyperlink).filter(
            Hyperlink.identifier==edition.primary_identifier).filter(
                Hyperlink.rel==Hyperlink.IMAGE)
        self.mirror_all_resources(q)


class ImageScaler(object):

    DEFAULT_WIDTH = 200
    DEFAULT_HEIGHT = 300

    def __init__(self, db, mirrors):
        self._db = db
        self.data_source_ids = []
        self.uploader = S3Uploader()

        for mirror in mirrors:
            data_source_name = mirror.DATA_SOURCE
            data_source = DataSource.lookup(self._db, data_source_name)
            self.data_source_ids.append(data_source.id)


    def run(self, destination_height=None, destination_width=None,
            batch_size=100, upload=True):
        q = self._db.query(Resource).filter(
            Resource.data_source_id.in_(self.data_source_ids))
        self.scale_all_resources(q, destination_height, destination_width,
                                 batch_size, upload)

    def scale_edition(self, edition, destination_height=None,
                      destination_width=None, upload=True):
        """Make sure that one specific edition has its cover(s) scaled."""
        # Find all resources for this edition's primary identifier.
        q = self._db.query(Hyperlink).filter(
            Hyperlink.identifier==edition.primary_identifier)
        self.scale_all_resources(
            q, destination_height, destination_width,
            upload=upload)

    def scale_all_resources(
            self, q, destination_height=None, destination_width=None,
            batch_size=100, upload=True, force=False):

        destination_width = destination_width or self.DEFAULT_WIDTH
        destination_height = destination_height or self.DEFAULT_HEIGHT

        # Find all resources that either don't have a thumbnail, or
        # whose thumbnail was not mirrored.
        q = q.filter(Hyperlink.rel==Hyperlink.IMAGE)
        q = q.join(Hyperlink.resource).join(Resource.representation).filter(
            Representation.fetched_at != None).filter(
            Representation.fetch_exception == None)
        thumbnail = aliased(Representation)
        q = q.outerjoin(thumbnail, Representation.thumbnails)

        if force:
            now = datetime.datetime.utcnow()
            q = q.filter(or_(
                    thumbnail.id==None, thumbnail.mirrored_at==None,
                    thumbnail.mirrored_at<now))
        else:
            q = q.filter(or_(
                    thumbnail.id==None, 
                    thumbnail.mirrored_at==None))
        resultset = q.limit(batch_size).all()
        while resultset:
            total = 0
            a = time.time()
            to_upload = []
            for hyperlink in resultset:
                destination_url = self.uploader.cover_image_url(
                    hyperlink.data_source, hyperlink.identifier,
                    "cover.jpg", destination_height)
                thumbnail, is_new = hyperlink.resource.representation.scale(
                    destination_height, destination_width,
                    destination_url, "image/jpeg", force=True)
                if thumbnail.scaled_exception:
                    print "Could not scale %s: %s" % (
                        r.url, thumbnail.scaled_exception)
                elif not is_new:
                    pass
                else:
                    to_upload.append(thumbnail)
                    total += 1
            print "%.2f sec to scale %d" % ((time.time()-a), total)
            a = time.time()
            if upload:
                self.uploader.mirror_batch(to_upload)
            self._db.commit()
            print "%.2f sec to upload %d" % ((time.time()-a), total)
            a = time.time()
            resultset = q.limit(batch_size).all()
        self._db.commit()
