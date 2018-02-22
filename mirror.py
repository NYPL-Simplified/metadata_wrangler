from nose.tools import set_trace
import datetime
import logging
import time

from sqlalchemy import or_
from sqlalchemy.orm import (
    aliased,
)

from core.model import (
    DataSource,
    Identifier,
    Hyperlink,
    Resource,
    Representation,
)
from core.mirror import MirrorUploader


class CoverImageMirror(object):
    """Downloads images via HTTP, saves them to the database,
    then uploads them to S3.
    """

    DATA_SOURCE = None
    ONE_YEAR = datetime.timedelta(days=365)

    def __init__(self, db, uploader=None):
        self._db = db
        self.data_source = DataSource.lookup(self._db, self.DATA_SOURCE)
        self.uploader = uploader or MirrorUploader.sitewide(self._db)
        self.log = logging.getLogger("Cover Image Mirror")

    def run(self):
        """Mirror all image resources associated with this data source."""
        q = self._db.query(Hyperlink).filter(
                Hyperlink.data_source==self.data_source)
        self.mirror_all_resources(q)

    def mirror_all_resources(self, q, force=False):
        """Mirror all resources that match a query."""
        # Only mirror images.
        batch_size=100
        now = datetime.datetime.utcnow()
        q = q.filter(or_(Hyperlink.rel==Hyperlink.IMAGE,
                         Hyperlink.rel==Hyperlink.THUMBNAIL_IMAGE))
        q = q.join(Hyperlink.resource).outerjoin(Resource.representation)
        # Restrict to resources that are not already mirrored.
        if force:
            q = q.filter(Representation.mirrored_at < now)
            q = q.filter(Representation.fetch_exception==None)
        else:
            q = q.filter(Representation.mirrored_at == None)
            q = q.filter(Representation.fetch_exception==None)
            q = q.filter(Representation.mirror_exception==None)

        resultset = q.limit(batch_size).all()
        blacklist = set()
        #print "About to mirror %d images." % q.count()
        while resultset:
            #print "Mirroring %d images." % len(resultset)
            to_upload = []
            for hyperlink in resultset:
                blacklist.add(hyperlink.id)
                representation = self.mirror_hyperlink(hyperlink)
                if not representation.fetch_exception:
                    to_upload.append(representation)
            self.uploader.mirror_batch(to_upload)
            for rep in to_upload:
                self.log.info("%s => %s %s" % (rep.url, rep.mirror_url, rep.mirrored_at))
            resultset = q.filter(~Hyperlink.id.in_(blacklist)).limit(batch_size).all()
            #print "Blacklist size now %d" % len(blacklist)
        self._db.commit()

    def mirror_hyperlink(self, hyperlink):
        resource = hyperlink.resource
        if not resource.representation:
            resource.representation, cached = Representation.get(
                self._db, resource.url, max_age=self.ONE_YEAR)
            representation = resource.representation
            if not representation.media_type or not representation.media_type.startswith('image/'):
                representation.fetch_exception = (
                    'Representation is not an image as expected.')
                return representation

            extension = self.image_extensions_for_types.get(
                representation.media_type, '')
            filename = "cover" + extension
            representation.mirror_url = self.uploader.cover_image_url(
                hyperlink.data_source, hyperlink.identifier,
                filename)
        self._db.commit()
        return resource.representation

    types_for_image_extensions = { ".jpg" : "image/jpeg",
                                   ".gif" : "image/gif",
                                   ".png" : "image/png"}

    image_extensions_for_types = {}
    for k, v in types_for_image_extensions.items():
        image_extensions_for_types[v] = k

    def mirror_edition(self, edition):
        """Make sure that one specific edition has its cover(s) mirrored."""
        if isinstance(edition, Identifier):
            identifier = edition
        else:
            identifier = edition.primary_identifier
        # Find all resources for this edition's primary identifier.
        q = self._db.query(Hyperlink).filter(
            Hyperlink.identifier==identifier).filter(
                or_(Hyperlink.rel==Hyperlink.IMAGE,
                    Hyperlink.rel==Hyperlink.THUMBNAIL_IMAGE))
        self.mirror_all_resources(q)


class ImageScaler(object):

    DEFAULT_WIDTH = 200
    DEFAULT_HEIGHT = 300

    def __init__(self, db, mirrors, uploader=None):
        self._db = db
        self.data_source_ids = []
        self.uploader = uploader or MirrorUploader.sitewide(self._db)
        self.log = logging.getLogger("Cover Image Scaler")

        for mirror in mirrors:
            data_source_name = mirror.DATA_SOURCE
            data_source = DataSource.lookup(self._db, data_source_name)
            self.data_source_ids.append(data_source.id)


    def run(self, destination_height=None, destination_width=None,
            batch_size=100, upload=True, force=False):
        q = self._db.query(Hyperlink).filter(
            Hyperlink.data_source_id.in_(self.data_source_ids))
        self.scale_all_resources(q, destination_height, destination_width,
                                 batch_size, upload, force=force)

    def scale_edition(self, edition, destination_height=None,
                      destination_width=None, upload=True):
        """Make sure that one specific edition has its cover(s) scaled."""
        # Find all resources for this edition's primary identifier.
        if isinstance(edition, Identifier):
            identifier = edition
        else:
            identifier = edition.primary_identifier
        q = self._db.query(Hyperlink).filter(
            Hyperlink.identifier==identifier).filter(
                Hyperlink.rel==Hyperlink.IMAGE)
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
        blacklist = set()
        resultset = q.limit(batch_size).all()
        while len(resultset):
            self.log.debug("About to scale %d", len(resultset))
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
                if thumbnail.scale_exception:
                    self.log.error("Could not scale %s: %s" % (
                        hyperlink.resource.url, thumbnail.scale_exception))
                else:
                    # Most likely this resource doesn't need to be
                    # thumbnailed. Add it to the blacklist so we don't
                    # pick it up again.
                    blacklist.add(hyperlink.resource.id)
                    if thumbnail.resource:
                        blacklist.add(thumbnail.resource.id)
                    to_upload.append(thumbnail)
                    total += 1
            self.log.debug("%.2f sec to scale %d", (time.time()-a), total)
            a = time.time()
            if upload:
                self.uploader.mirror_batch(to_upload)
            self._db.commit()
            self.log.debug("%.2f sec to upload %d", (time.time()-a), total)
            a = time.time()
            resultset = q.filter(~Resource.id.in_(blacklist)).limit(batch_size).all()

        self._db.commit()
