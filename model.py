import datetime
from nose.tools import set_trace

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
        return engine, engine.connect()

def get_one_or_create(db, model, create_method='',
                      create_method_kwargs=None,
                      **kwargs):
    try:
        return db.query(model).filter_by(**kwargs).one(), False
    except NoResultFound:
        kwargs.update(create_method_kwargs or {})
        created = getattr(model, create_method, model)(**kwargs)
        try:
            db.add(created)
            db.flush()
            return created, True
        except IntegrityError:
            db.rollback()
            return db.query(model).filter_by(**kwargs).one(), False

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
    name = Column(String, unique=True)
    offers_licenses = Column(Boolean, default=False)
    primary_identifier_type = Column(String)
    extra = Column(JSON, default={})

    # One DataSource can generate many WorkRecords.
    work_records = relationship("WorkRecord", backref="data_source")

    # One DataSource can grant access to many LicensePools.
    license_pools = relationship("LicensePool", backref="data_source")

    @classmethod
    def lookup(cls, _db, name):
        return _db.query(cls).filter_by(name=name).one()

    @classmethod
    def well_known_sources(cls, _db):
        """Make sure all the well-known sources exist."""

        for (name, offers_licenses, primary_identifier_type,
             refresh_rate) in (
                 (cls.GUTENBERG, True, WorkIdentifier.GUTENBERG_ID, None),
                 (cls.OVERDRIVE, True, WorkIdentifier.OVERDRIVE_ID, 0),
                 (cls.THREEM, True, WorkIdentifier.THREEM_ID, 60*60*6),
                 (cls.AXIS_360, True, WorkIdentifier.AXIS_360_ID, 0),
                 (cls.OCLC, False, WorkIdentifier.OCLC_WORK, None),
                 (cls.WEB, True, WorkIdentifier.URI, None)
        ):

            extra = dict()
            if refresh_rate:
                extra['circulation_refresh_rate_seconds'] = refresh_rate

            obj, new = get_one_or_create(
                _db, DataSource,
                name=name,
                create_method_kwargs=dict(
                    offers_licenses=offers_licenses,
                    primary_identifier_type=primary_identifier_type,
                    extra=extra,
                )
            )
            yield obj

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
    AXIS_360_ID = "Axis 360 ID"
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

    # One WorkIdentifier may serve as the identifier for
    # a single LicensePool.
    licensed_through = relationship(
        "LicensePool", backref="identifier", uselist=False
    )

    # Type + identifier is unique.
    __table_args__ = (
        UniqueConstraint('type', 'identifier'),
    )

    @classmethod
    def for_foreign_id(cls, _db, foreign_identifier_type, foreign_id):
        work_identifier, was_new = get_one_or_create(
            _db, cls, type=foreign_identifier_type,
            identifier=foreign_id)
        return work_identifier, was_new

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
    imprint = Column(Unicode)

    # `published is the original publication date of the
    # text. `issued` is when made available in this ebook edition. A
    # Project Gutenberg text was likely `published` long before being
    # `issued`.
    issued = Column(Date)
    published = Column(Date)

    links = Column(JSON, default={})

    extra = Column(JSON, default={})
    
    # Common link relation URIs for the links.
    OPEN_ACCESS_DOWNLOAD = "http://opds-spec.org/acquisition/open-access"
    IMAGE = "http://opds-spec.org/image"
    THUMBNAIL_IMAGE = "http://opds-spec.org/image/thumbnail"
    SAMPLE = "http://opds-spec.org/acquisition/sample"

    @classmethod
    def for_foreign_id(cls, _db, data_source,
                       foreign_id_type, foreign_id):
        """Find the WorkRecord representing the given data source's view of
        the work that it primarily identifies by foreign ID.

        e.g. for_foreign_id(_db, DataSource.OVERDRIVE,
                            WorkIdentifier.OVERDRIVE_ID, uuid)

        finds the WorkRecord for Overdrive's view of a book identified
        by Overdrive UUID.

        This:

        for_foreign_id(_db, DataSource.OVERDRIVE, WorkIdentifier.ISBN, isbn)

        will probably return nothing, because although Overdrive knows
        that books have ISBNs, it doesn't use ISBN as a primary
        identifier.
        """
        # Look up the data source if necessary.
        if isinstance(data_source, basestring):
            data_source = DataSource.lookup(_db, data_source)

        # Then look up the identifier.
        work_identifier, ignore = WorkIdentifier.for_foreign_id(
            _db, foreign_id_type, foreign_id)

        # Combine the two to get/create a WorkRecord.
        return get_one_or_create(
            _db, WorkRecord, data_source=data_source,
            primary_identifier=work_identifier)

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
    def _add_link(cls, links, rel, href, type=None, description=None):
        """Add a hypermedia link to a dictionary of links.

        `links`: A dictionary of links like the one stored in WorkRecord.links.
        `rel`: The relationship between a WorkRecord and the resource
               on the other end of the link.
        `type`: Media type of the representation available at the
                other end of the link.
        `description`: Human-readable description of the link.
        """
        if rel not in links:
            links[rel] = []
        d = dict(href=href)
        if type:
            d['type'] = type
        if description:
            d['description'] = type
        links[rel].append(d)

    @classmethod
    def _add_subject(cls, subjects, type, id, value=None, weight=None):
        """Add a new entry to a dictionary of bibliographic subjects.

        ``type``: Classification scheme; one of the constants from SubjectType.
        ``id``: Internal ID of the subject according to that classification
                scheme.
        ``value``: Human-readable description of the subject, if different
                   from the ID.

        ``weight``: How confident this source of work
                    information is in classifying a book under this
                    subject. The meaning of this number depends entirely
                    on the source of the information.
        """
        if type not in subjects:
            subjects[type] = []
        d = dict(id=id)
        if value:
            d['value'] = value
        if weight:
            d['weight'] = weight
        subjects[type].append(d)

    @classmethod
    def _add_author(self, authors, name, role=None, aliases=None):
        """Represent an entity who had some role in creating a book."""
        a = { Author.NAME : name }
        if role:
            a[Author.ROLE] = role
        if aliases:
            a[Author.ALTERNATE_NAME] = aliases
        authors.append(a)

class LicensePool(Base):

    """A pool of undifferentiated licenses for a work from a given source.
    """

    __tablename__ = 'licensepools'
    id = Column(Integer, primary_key=True)

    # Each LicensePool is associated with one DataSource and one
    # WorkIdentifier.
    data_source_id = Column(Integer, ForeignKey('datasources.id'))
    identifier_id = Column(Integer, ForeignKey('workidentifiers.id'))

    # One LicensePool can have many CirculationEvents
    circulation_events = relationship(
        "CirculationEvent", backref="license_pool")


    open_access = Column(Boolean)
    last_checked = Column(DateTime)
    licenses_owned = Column(Integer,default=0)
    licenses_available = Column(Integer,default=0)
    licenses_reserved = Column(Integer,default=0)
    patrons_in_hold_queue = Column(Integer,default=0)

    # A WorkIdentifier should have at most one LicensePool.
    __table_args__ = (UniqueConstraint('identifier_id'),)

    @classmethod
    def for_foreign_id(self, _db, data_source, foreign_id_type, foreign_id):

        # Get the DataSource.
        if isinstance(data_source, basestring):
            data_source = DataSource.lookup(_db, data_source)

        # The data source must be one that offers licenses.
        if not data_source.offers_licenses:
            raise ValueError(
                'Data source "%s" does not offer licenses.' % data_source.name)

        # The type of the foreign ID must be the primary identifier
        # type for the data source.
        if foreign_id_type != data_source.primary_identifier_type:
            raise ValueError(
                "License pools for data source '%s' are keyed to "
                "identifier type '%s' (not '%s', which was provided)" % (
                    data_source.name, data_source.primary_identifier_type,
                    foreign_id_type
                )
            )

        # Get the WorkIdentifier.
        identifier, ignore = WorkIdentifier.for_foreign_id(
            _db, foreign_id_type, foreign_id
            )

        # Get the LicensePool that corresponds to the DataSource and
        # the WorkIdentifier.
        license_pool, was_new = get_one_or_create(
            _db, LicensePool, data_source=data_source, identifier=identifier)
        return license_pool, was_new

    def needs_update(self):
        """Is it time to update the circulation info for this license pool?"""
        now = datetime.datetime.now()
        if not self.last_checked:
            # This pool has never had its circulation info checked.
            return True
        maximum_stale_time = self.data_source.extra.get(
            'circulation_refresh_rate_seconds')
        if maximum_stale_time is None:
            # This pool never needs to have its circulation info checked.
            return False
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

    def update_from_event(self, event):
        """Update the license pool based on an event."""
        name = event.type
        delta = event.delta
        if name in (
                CirculationEvent.LICENSE_ADD,
                CirculationEvent.LICENSE_REMOVE):
            self.licenses_owned = event.new_value
            self.licenses_available += delta
        elif name in (CirculationEvent.CHECKIN, CirculationEvent.CHECKOUT):
            self.licenses_available = event.new_value
        elif name == CirculationEvent.AVAILABILITY_NOTIFY:
            # People move from the hold queue to the reserves.
            self.licenses_available -= delta
            self.licenses_reserved += delta
            self.patrons_in_hold_queue -= delta
        elif name in (CirculationEvent.HOLD_RELEASE,
                      CirculationEvent.HOLD_PLACE):
            self.patrons_in_hold_queue = event.new_value

class CirculationEvent(Base):

    """Changes to a license pool's circulation status.

    We log these so we can measure things like the velocity of
    individual books.
    """
    __tablename__ = 'circulationevent'

    id = Column(Integer, primary_key=True)

    # One LicensePool can have many circulation events.
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'))

    type = Column(String(32))
    start = Column(DateTime, index=True)
    end = Column(DateTime)
    old_value = Column(Integer)
    delta = Column(Integer)
    new_value = Column(Integer)
    foreign_patron_id = Column(Integer)

    # A given license pool can only have one event of a given type for
    # a given patron at a given time.
    __table_args__ = (UniqueConstraint('license_pool_id', 'type', 'start',
                                       'foreign_patron_id'),)

    # Constants for use in logging circulation events to JSON
    SOURCE = "source"
    TYPE = "event"

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

    @classmethod
    def _get_datetime(cls, data, key):
        date = data.get(key, None)
        if not date:
            return None
        elif isinstance(date, datetime.date):
            return date
        else:
            return datetime.datetime.strptime(date, cls.TIME_FORMAT)

    @classmethod
    def _get_int(cls, data, key):
        value = data.get(key, None)
        if not value:
            return value
        else:
            return int(value)

    @classmethod
    def from_string(cls, _db, s):
        """Find or create a CirculationEvent based on an entry in a JSON
        stream.

        e.g.

        {"foreign_patron_id": "23333085908570", "start": "2013-05-04T00:17:39+00:00", "id": "d5o289", "source": "3M", "event": "hold_place"}
        """

        data = json.loads(s.strip())
        for k in 'start', 'end':
            if k in data:
                data[k] = self._parse_date(k)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, _db, data):

        # Identify the source of the event.
        source_name = data['source']
        source = DataSource.lookup(_db, source_name)

        # Identify which LicensePool the event is talking about.
        foreign_id = data['id']
        identifier_type = source.primary_identifier_type

        license_pool, was_new = LicensePool.for_foreign_id(
            _db, source, identifier_type, foreign_id)

        # Finally, gather some information about the event itself.
        type = data.get("type")
        start = cls._get_datetime(data, 'start')
        end = cls._get_datetime(data, 'end')
        old_value = cls._get_int(data, 'old_value')
        new_value = cls._get_int(data, 'new_value')
        delta = cls._get_int(data, 'delta')
        foreign_patron_id = data.get("foreign_patron_id")

        # Finally, get or create the event.
        event, was_new = get_one_or_create(
            _db, CirculationEvent, license_pool=license_pool,
            type=type, start=start, foreign_patron_id=foreign_patron_id,
            create_method_kwargs=dict(
                old_value=old_value,
                new_value=new_value,
                delta=delta,
                end=end)
            )

        if was_new:
            # Update the LicensePool to reflect the information in this event.
            print event.type
            if event.type == 'availablity_notify':
                set_trace()
            license_pool.update_from_event(event)
        return event, was_new

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


class Author(object):
    """Constants for common author fields."""
    NAME = 'name'
    ALTERNATE_NAME = 'alternateName'
    ROLE = 'role'
    AUTHOR_ROLE = 'author'

