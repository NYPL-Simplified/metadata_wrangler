import datetime

from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    backref,
    relationship,
)
from sqlalchemy.orm.exc import (
    NoResultFound
)
from sqlalchemy.exc import (
    IntegrityError
)
from sqlalchemy import (
    create_engine, 
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Index,
    String,
    Table,
    Unicode,
    UniqueConstraint,
)

from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import sessionmaker

from database_credentials import SERVER, MAIN_DB

DEBUG = False

class SessionManager(object):

    @classmethod
    def engine(cls, server, database):
        url = URL(server['engine'], database['username'], database['password'],
                  server['host'], server['port'], database['database'])
        return create_engine(url, echo=DEBUG)

    @classmethod
    def initialize(cls, server, database):
        engine = cls.engine(server, database)
        Base.metadata.create_all(engine)
        return engine.connect()

def get_one_or_create(db, model, create_method='',
                      create_method_kwargs=None,
                      **kwargs):
    try:
        return db.query(model).filter_by(**kwargs).one(), True
    except NoResultFound:
        kwargs.update(create_method_kwargs or {})
        created = getattr(model, create_method, model)(**kwargs)
        try:
            db.add(created)
            db.flush()
            return created, False
        except IntegrityError:
            db.rollback()
            return self.query(model).filter_by(**kwargs).one(), True

Base = declarative_base()

class DataSource(Base):
    """A source for information about books, and possibly the books themselves."""

    GUTENBERG = "Gutenberg"
    OVERDRIVE = "Overdrive"
    THREEM = "3M"
    OCLC = "OCLC Classify"
    AXIS_360 = "Axis 360"
    WEB = "Web"

    __tablename__ = 'datasources'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    # One DataSource can generate many WorkRecords.
    work_records = relationship("WorkRecord", backref="data_source") 

    @classmethod
    def well_known_sources(cls, _db):
        """Make sure all the well-known sources exist."""
        for name in (cls.GUTENBERG, cls.OVERDRIVE, cls.THREEM, cls.AXIS_360,
                     cls.OCLC, cls.WEB):
            yield get_one_or_create(_db, DataSource, name=name)

# A join table for the many-to-many relationship between WorkRecord
# and WorkIdentifier.
workrecord_workidentifier = Table(
    'workrecord_workidentifier',
    Base.metadata,
    Column('workrecord_id', Integer, ForeignKey('workidentifiers.id')),
    Column('workidentifier_id', Integer, ForeignKey('workrecords.id'))
)

class WorkIdentifier(Base):
    """A way of uniquely referring to a particular text.
    Whatever "text" means.
    """
    
    # Common types of identifiers.
    OVERDRIVE_ID = "Overdrive ID"
    THREEM_ID = "3M ID"
    GUTENBERG_ID = "Gutenberg ID"
    QUERY_STRING = "Query string"
    ISBN = "ISBN"
    OCLC_WORK = "OCLC Work"
    OCLC_SWID = "OCLC SWID" # Let's try to do without this one, OK?
    URI = "URI"
    DOI = "DOI"
    UPC = "UPC"

    __tablename__ = 'workidentifiers'
    id = Column(Integer, primary_key=True)
    type = Column(String(64))
    identifier = Column(String)

    # One WorkIdentifier may serve as the primary identifier for
    # several WorkRecords.
    primarily_identifies = relationship(
        "WorkRecord", backref="primary_identifier"
    )

    # Type + identifier is unique.
    __table_args__ = (
        UniqueConstraint('type', 'identifier'),
    )


class WorkRecord(Base):

    """A lightly schematized collection of metadata for a work, or an
    edition of a work, or a book, or whatever. If someone thinks of it
    as a "book" with a "title" it can go in here.
    """

    __tablename__ = 'workrecords'
    id = Column(Integer, primary_key=True)

    data_source_id = Column(Integer, ForeignKey('datasources.id'))
    primary_identifier_id = Column(Integer, ForeignKey('workidentifiers.id'))

    # Many WorkRecords may be equivalent to the same WorkIdentifier,
    # and a single WorkRecord may be equivalent to many
    # WorkIdentifiers.
    equivalent_identifiers = relationship(
        "WorkIdentifier",
        secondary=workrecord_workidentifier,
        backref="equivalent_works")

    title = Column(Unicode)
    subtitle = Column(Unicode)
    series = Column(Unicode)
    authors = Column(JSON, default=[])
    subjects = Column(JSON, default=[])
    summary = Column(JSON, default=None)

    languages = Column(JSON, default=[])
    publisher = Column(Unicode)

    # `published is the original publication date of the text. `issued`
    # is when made available in an edition. A Project Gutenberg text
    # was likely `published` long before being `issued`.
    issued = Column(Date)
    published = Column(Date)

    links = Column(JSON)

    extra = Column(JSON, default={})
    
    # Common link relation URIs for the links.
    OPEN_ACCESS_DOWNLOAD = "http://opds-spec.org/acquisition/open-access"
    IMAGE = "http://opds-spec.org/image"
    THUMBNAIL_IMAGE = "http://opds-spec.org/image/thumbnail"
    SAMPLE = "http://opds-spec.org/acquisition/sample"

    def add_language(self, language, type="ISO-639-1"):
        # TODO: Convert ISO-639-2 to ISO-639-1
        if not self.languages:
            self.languages = []
        if language:
            self.languages.append(language)

    @classmethod
    def _content(cls, content, is_html=False):
        """Represent content that might be plain-text or HTML.

        e.g. a book's summary.
        """
        if not content:
            return None
        if is_html:
            type = "html"
        else:
            type = "text"
        return dict(type=type, value=content)

    @classmethod
    def _link(cls, rel, href, type=None, description=None):
        """Represent a hypermedia link.
        
        `type`: Media type of the link.
        `description`: Human-readable description of the link.
        """
        d = dict(rel=rel, href=href)
        if type:
            d['type'] = type
        if description:
            d['description'] = type
        return d

    @classmethod
    def _subject(cls, type, id, value=None, weight=None):
        """Represent a subject a book might be classified under.

        ``type``: Classification scheme; one of the constants from SubjectType.
        ``id``: Internal ID of the subject according to the classification
                scheme.
        ``value``: Human-readable description of the subject, if different
                   from the ID.

        ``weight``: How confident this source of work
                    information is in classifying a book under this
                    subject. The meaning of this number depends entirely
                    on the source of the information.
        """
        d = dict(type=type, id=id)
        if value:
            d['value'] = value
        if weight:
            d['weight'] = weight
        return d

    @classmethod
    def _author(self, name, role=None, aliases=None):
        """Represent an entity who had some role in creating a book."""
        if not role:
            role = "author"
        a = dict(name=name, role=role)
        if aliases:
            a['aliases'] = aliases
        return a

class LicensePool(Base):

    """A pool of undifferentiated licenses for a work from a given source.

    The source is identified in terms of the WorkRecord we created for it.
    """

    __tablename__ = 'licensepools'
    id = Column(Integer, primary_key=True)

    # Each LicensePool is associated with one WorkRecord.
    work_record_id = Column(Integer, ForeignKey('workrecords.id'))
    work_record = relationship(
        "WorkRecord", uselist=False, backref="license_pool")

    # One LicensePool can have many CirculationEvents
    circulation_events = relationship(
        "CirculationEvent", backref="license_pool")

    open_access = Column(Boolean)
    last_checked = Column(DateTime)
    licenses_owned = Column(Integer)
    licenses_available = Column(Integer)
    licenses_reserved = Column(Integer)
    patrons_in_hold_queue = Column(Integer)

    # A WorkRecord should have at most one LicensePool.
    __table_args__ = (UniqueConstraint('work_record_id'),)

    @classmethod
    def open_access_license_for(self, _db, work_record):
        """Find or create an open-access license for the given
        WorkRecord."""
        return get_one_or_create(
            _db,
            LicensePool,
            work_record=work_record,
            create_method_kwargs=dict(
                open_access=True,
                last_checked=datetime.datetime.utcnow()
            )
        )

    def needs_update(self, maximum_stale_time):
        """Is it time to update the circulation info for this pool?"""
        now = datetime.datetime.now()
        if not self.last_checked:
            # This pool has never had its circulation info checked.
            return True
        age = now - self.last_checked
        return age > maximum_stale_time

    def update_from_event(self, event):
        """Update circulation info based on an Event object."""

    @classmethod
    def compare_circulation_estimate_to_reality(self, estimate):
        """Bring a circulation estimate up to date with reality.

        Yields one event for every kind of change that happened.
        """
        for field, actual_value, more, fewer in (
            [self.patrons_in_hold_queue, Event.HOLD_PLACE, Event.HOLD_RELEASE], 
            [self.licenses_available, Event.CHECKIN, Event.CHECKOUT], 
            [self.licenses_reserved, Event.AVAILABILITY_NOTIFY, None], 
            [self.licenses_owned, Event.LICENSE_ADD, Event.LICENSE_REMOVE]):
            estimated_value = estimate.get(key,0)
            if estimated_value == actual_value:
                # Our estimate was accurate. No need to generate 
                # any events.
                continue

            if actual_value < estimated_value:
                # There are fewer of (whatever) than we estimated.
                name = fewer
            else:
                # There are more of (whatever) than we estimated.
                name = more
            if name is None:
                # We have no event for this.
                continue

                        
            d = dict()
            d[Event.OLD_VALUE] = estimated_value
            d[Event.NEW_VALUE] = actual_value
            d[Event.DELTA] = actual_value-estimated_value
            d[Event.SOURCE] = self.work_record.source
            d[Event.SOURCE_BOOK_ID] = self.work_record.source_id
            d[Event.START_TIME] = datetime.datetime.strptime(
                reality[LicensedWork.LAST_CHECKED], Event.TIME_FORMAT)
            d[Event.EVENT_TYPE] = name
            yield d

    def update_from_event(cls, event):
        """Update the license pool based on an event."""

        # TODO: Update to owned needs to increase availability.
        # This needs to wait until we have a proper lifecycle
        # state machine, though.
        name = event[Event.EVENT_TYPE]
        if Event.DELTA not in event:
            delta = 1
        else:
            delta = abs(event[Event.DELTA])

        if name == Event.LICENSE_ADD:
            self.licenses_owned += delta
        elif name == Event.LICENSE_REMOVE:
            self.licenses_owned -= delta
        elif name == Event.CHECKOUT:
            licenses_available -= delta
        elif name == Event.CHECKIN:
            licenses_available += delta
        elif name == Event.AVAILABILITY_NOTIFY:
            # People move from the hold queue to the reserves.
            licenses_reserved += delta
            patrons_in_hold_queue -= delta
        elif name == Event.HOLD_RELEASE:
            patrons_in_hold_queue -= delta
        elif name == Event.HOLD_PLACE:
            patrons_in_hold_queue += delta

class CirculationEvent(Base):

    """Changes to a license pool's circulation status.

    We log these so we can measure things like the velocity of
    individual books.
    """
    __tablename__ = 'circulationevent'

    id = Column(Integer, primary_key=True)
    type = Column(String(32))
    start = Column(DateTime)
    end = Column(DateTime)
    old_value = Column(Integer)
    new_value = Column(Integer)
    foreign_patron_id = Column(Integer)

    # One LicensePool can have many circulation events.
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'))

    # The names of the circulation events we recognize.
    CHECKOUT = "check_out"
    CHECKIN = "check_in"
    HOLD_PLACE = "hold_place"
    HOLD_RELEASE = "hold_release"
    LICENSE_ADD = "license_add"
    LICENSE_REMOVE = "license_remove"
    AVAILABILITY_NOTIFY = "availability_notify"
    CIRCULATION_CHECK = "circulation_check"
    SERVER_NOTIFICATION = "server_notification"
    TITLE_ADD = "title_add"
    TITLE_REMOVE = "title_remove"
    UNKNOWN = "unknown"

    # The time format used when exporting to JSON.
    TIME_FORMAT = "%Y-%m-%dT%H:%M:%S+00:00"


class Timestamp(Base):
    """A general-purpose timestamp for external services."""

    __tablename__ = 'timestamps'
    service = Column(String(255), primary_key=True)
    type = Column(String(255), primary_key=True)
    timestamp = Column(DateTime)


class SubjectType(object):
    """Constants for common types of subject classification"""
    LCC = "LCC"   # Library of Congress Classification
    LCSH = "LCSH" # Library of Congress Subject Headings
    DDC = "DDC"   # Dewey Decimal Classification
    FAST = "FAST"
    TAG = "tag"   # Folksonomic tags.

    by_uri = {
        "http://purl.org/dc/terms/LCC" : LCC,
        "http://purl.org/dc/terms/LCSH" : LCSH,
    }
