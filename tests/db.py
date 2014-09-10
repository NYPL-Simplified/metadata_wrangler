import os
from datetime import datetime
from nose.tools import set_trace
from sqlalchemy.orm.session import Session
from model import (
    CoverageRecord,
    DataSource,
    Genre,
    SessionManager,
    LicensePool,
    Patron,
    Resource,
    WorkIdentifier,
    WorkRecord,
    Work,
    get_one_or_create
)
from classifier import Classifier
from tests import DBInfo


class DatabaseTest(object):
    def setup(self):
        self.__transaction = DBInfo.connection.begin_nested()
        self._db = Session(DBInfo.connection)
        self.counter = 0

    def teardown(self):
        self._db.close()
        self.__transaction.rollback()

    @property
    def _id(self):
        self.counter += 1
        return self.counter

    @property
    def _str(self):
        return str(self._id)

    @property
    def default_patron(self):
        """The patron automatically created for the test dataset and 
        used by default when authenticating.
        """
        return self._db.query(Patron).filter(
            Patron.authorization_identifier=="200").one()

    def _patron(self, external_identifier=None):
        external_identifier = external_identifier or self._str
        return get_one_or_create(
            self._db, Patron, external_identifier=external_identifier)[0]

    def _workidentifier(self, identifier_type=WorkIdentifier.GUTENBERG_ID):
        id = self._str
        return WorkIdentifier.for_foreign_id(self._db, identifier_type, id)[0]

    def _workrecord(self, data_source_name=DataSource.GUTENBERG,
                    identifier_type=WorkIdentifier.GUTENBERG_ID,
                    with_license_pool=False, with_open_access_download=False):
        id = self._str
        source = DataSource.lookup(self._db, data_source_name)
        wr = WorkRecord.for_foreign_id(
            self._db, source, identifier_type, id)[0]
        if with_license_pool or with_open_access_download:
            pool = self._licensepool(wr, data_source_name=data_source_name,
                                     with_open_access_download=with_open_access_download)                
            return wr, pool
        return wr

    def _work(self, title=None, authors=None, genre=None, language=None,
              audience=None, fiction=True, with_license_pool=False, 
              with_open_access_download=False, quality=100):
        if with_open_access_download:
            with_license_pool = True
        language = language or "eng"
        title = title or self._str
        genre = genre or self._str
        audience = audience or Classifier.AUDIENCE_ADULT
        if fiction is None:
            fiction = True
        wr = self._workrecord(with_license_pool=with_license_pool,
                              with_open_access_download=with_open_access_download)
        if with_license_pool:
            wr, pool = wr
        work, ignore = get_one_or_create(
            self._db, Work, create_method_kwargs=dict(
                title=title, language=language,
                audience=audience,
                fiction=fiction,
                authors=authors, quality=quality), id=self._id)
        if not isinstance(genre, Genre):
            genre, ignore = Genre.lookup(self._db, genre, autocreate=True)
        work.genres = [genre]
        if with_license_pool:
            work.license_pools.append(pool)
        work.primary_work_record = wr
        return work

    def _coverage_record(self, workrecord, coverage_source):
        record, ignore = get_one_or_create(
            self._db, CoverageRecord,
            work_record=workrecord,
            data_source=coverage_source,
            create_method_kwargs = dict(date=datetime.utcnow()))
        return record

    def _licensepool(self, workrecord, open_access=True, 
                     data_source_name=DataSource.GUTENBERG,
                     with_open_access_download=False):
        source = DataSource.lookup(self._db, data_source_name)
        if not workrecord:
            workrecord = self._workrecord(data_source_name)

        pool, ignore = get_one_or_create(
            self._db, LicensePool,
            create_method_kwargs=dict(
                open_access=open_access),
            identifier=workrecord.primary_identifier, data_source=source)

        if with_open_access_download:
            pool.open_access = True
            pool.identifier.add_resource(
                Resource.OPEN_ACCESS_DOWNLOAD, "http://foo.com/" + self._str,
                source, pool, "application/epub+zip")

        return pool
