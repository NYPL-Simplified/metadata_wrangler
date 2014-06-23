from nose.tools import set_trace
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session
from model import (
    Base,
    SessionManager,
    DataSource,
    LicensePool,
    WorkIdentifier,
    WorkRecord,
    Work,
    get_one_or_create
)
from config import SERVER, TEST_DB

def setup_module():
    global transaction, connection, engine

    # Connect to the database and create the schema within a transaction
    engine, connection = SessionManager.initialize(SERVER, TEST_DB)
    Base.metadata.drop_all(connection)
    Base.metadata.create_all(connection)
    transaction = connection.begin()

def teardown_module():
    # Roll back the top level transaction and disconnect from the database
    transaction.rollback()
    connection.close()
    engine.dispose()


class DatabaseTest(object):
    def setup(self):
        self.__transaction = connection.begin_nested()
        self._db = Session(connection)
        SessionManager.initialize_data(self._db)
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

    def _workidentifier(self, identifier_type=WorkIdentifier.GUTENBERG_ID):
        id = self._str
        return WorkIdentifier.for_foreign_id(self._db, identifier_type, id)[0]

    def _workrecord(self, data_source_name=DataSource.GUTENBERG,
                    identifier_type=WorkIdentifier.GUTENBERG_ID,
                    with_license_pool=False):
        id = self._str
        source = DataSource.lookup(self._db, data_source_name)
        wr = WorkRecord.for_foreign_id(
            self._db, source, identifier_type, id)[0]
        if with_license_pool:
            pool = self._licensepool(wr)
            return wr, pool
        return wr

    def _work(self, title=None, authors=None, lane=None, languages=None,
              with_license_pool=False):
        languages = languages or "eng"
        if isinstance(languages, list):
            languages = ",".join(languages)
        title = title or self._str
        lane = lane or self._str
        wr = self._workrecord(with_license_pool=with_license_pool)
        if with_license_pool:
            wr, pool = wr
        work, ignore = get_one_or_create(
            self._db, Work, create_method_kwargs=dict(
                title=title, languages=languages, lane=lane,
                authors=authors), id=self._str)
        if with_license_pool:
            work.license_pools.append(pool)
        work.primary_work_record = wr
        return work

    def _licensepool(self, workrecord, open_access=True, 
                     data_source_name=DataSource.GUTENBERG):
        source = DataSource.lookup(self._db, data_source_name)
        if not workrecord:
            workrecord = self._workrecord(data_source_name)

        pool, ignore = get_one_or_create(
            self._db, LicensePool,
            create_method_kwargs=dict(
                open_access=open_access),
            identifier=workrecord.primary_identifier, data_source=source)
        return pool
