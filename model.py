from collections import (
    Counter,
    defaultdict,
)
import datetime
from nose.tools import set_trace
import random

from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    backref,
    relationship,
)
from sqlalchemy.orm import (
    aliased
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
from util import MetadataSimilarity

#import logging
#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

from sqlalchemy.orm.session import Session

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

    @classmethod
    def session(cls, server, database):
        engine, connection = cls.initialize(server, database)
        session = Session(connection)
        cls.initialize_data(session)
        session.commit()
        return session

    @classmethod
    def initialize_data(cls, session):
        list(DataSource.well_known_sources(session))
        session.commit()

def get_one(db, model, **kwargs):
    try:
        return db.query(model).filter_by(**kwargs).one()
    except NoResultFound:
        return None


def get_one_or_create(db, model, create_method='',
                      create_method_kwargs=None,
                      **kwargs):
    one = get_one(db, model, **kwargs)
    if one:
        return one, False
    else:
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
    Column('workrecord_id', Integer, ForeignKey('workrecords.id')),
    Column('workidentifier_id', Integer, ForeignKey('workidentifiers.id'))
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
    OCLC_WORK = "OCLC Work ID"
    OCLC_NUMBER = "OCLC Number"
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

    # A WorkRecord may be associated with a Work
    work_id = Column(Integer, ForeignKey('works.id'))

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
                       foreign_id_type, foreign_id,
                       create_if_not_exists=True):
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
        if create_if_not_exists:
            f = get_one_or_create
        else:
            f = get_one
        return f(_db, WorkRecord, data_source=data_source,
                 primary_identifier=work_identifier)

    def equivalent_work_records(self, _db):
        """All WorkRecords whose primary ID is among this WorkRecord's
        equivalent IDs.
        """
        return _db.query(WorkRecord).filter(
            WorkRecord.primary_identifier_id.in_(
                [x.id for x in self.equivalent_identifiers])).all()

    @classmethod
    def missing_coverage_from(cls, _db, primary_id_type, *not_identified_by):
        """Find WorkRecords with primary identifier of the given type
        `primary_id_type` and with no alternative identifiers of the types
        `not_identified_by`.

        e.g.

        missing_coverage_from(_db, WorkIdentifier.GUTENBERG_ID,
                                   WorkIdentifier.OCLC_WORK, 
                                   WorkIdentifier.OCLC_NUMBER)

        will find WorkRecords primarily associated with a Project
        Gutenberg ID and not also identified with any OCLC Work ID or
        OCLC Number. These are Gutenberg books that need to have an
        OCLC lookup done.

        Equivalent SQL:

        select wr.* from workrecords wr join workidentifiers prim2 on wr.primary_identifier_id=prim2.id
         where prim2.type='Gutenberg ID'
         and wr.id not in (
          select workrecords.id from
            workrecords join workidentifiers prim on workrecords.primary_identifier_id=prim.id
            join workrecord_workidentifier wr_wi on workrecords.id=wr_wi.workrecord_id
            join workidentifiers secondary on wr_wi.workidentifier_id=secondary.id
            where prim.type='Gutenberg ID' and secondary.type in ('OCLC Work ID', 'OCLC Number')
        );
        """

        # First build the subquery. This will find all the WorkRecords whose primary identifiers are
        # of the correct type and who are *also* identified by one of the other types.
        primary_identifier = aliased(WorkIdentifier)
        secondary_identifier = aliased(WorkIdentifier)
        qu = _db.query(WorkRecord.id).join(primary_identifier, WorkRecord.primary_identifier).join(
            workrecord_workidentifier, WorkRecord.id==workrecord_workidentifier.columns['workrecord_id']).join(secondary_identifier).filter(
                primary_identifier.type==primary_id_type,
                secondary_identifier.type.in_(not_identified_by))

        # Now build the main query. This will find all the WorkRecords whose primary identifiers qualify them for
        # the first list, but who just aren't in the first list.
        primary_identifier = aliased(WorkIdentifier)
        main_query = _db.query(WorkRecord).join(primary_identifier, WorkRecord.primary_identifier).filter(
            primary_identifier.type==primary_id_type,
            ~WorkRecord.id.in_(qu.subquery()))
        return main_query.all()

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
    def _add_author(self, authors, name, roles=None, aliases=None, **kwargs):
        """Represent an entity who had some role in creating a book."""
        if roles and not isinstance(roles, list) and not isinstance(roles, tuple):
            roles = [roles]            
        a = { Author.NAME : name }
        a.update(kwargs)
        if roles:
            a.setdefault(Author.ROLES, []).extend(roles)
        if aliases:
            a[Author.ALTERNATE_NAME] = aliases
        authors.append(a)
   
    def similarity_to(self, other_record):
        """How likely is it that this record describes the same book as the
        given record?

        1 indicates very strong similarity, 0 indicates no similarity
        at all.

        For now we just compare the sets of words used in the titles
        and the authors' names. This should be good enough for most
        cases given that there is usually some preexisting reason to
        suppose that the two records are related (e.g. OCLC said
        they were).

        Confounding factors include:

        * Abbreviated names.
        * Titles that include subtitles.
        """
        title_quotient = MetadataSimilarity.title_similarity(
            self.title, other_record.title)

        author_quotient = MetadataSimilarity.author_similarity(
            self.authors, other_record.authors)

        return (title_quotient * 0.80) + (author_quotient * 0.20)

class Work(Base):

    __tablename__ = 'works'
    id = Column(Integer, primary_key=True)

    # One Work may have copies scattered across many LicensePools.
    license_pools = relationship("LicensePool", backref="work")

    # A single Work may claim many WorkRecords.
    work_records = relationship("WorkRecord", backref="work")
    
    title = Column(Unicode)
    authors = Column(Unicode)
    languages = Column(Unicode)
    thumbnail_cover_link = Column(Unicode)
    full_cover_link = Column(Unicode)
    lane = Column(Unicode)

    def __repr__(self):
        return "%s/%s/%s/%s (%s work records, %s license pools)" % (
            self.id, self.title, self.authors, self.languages,
            len(self.work_records), len(self.license_pools))

    def title_histogram(self):
        histogram = Counter()
        words = 0.0
        for record in self.work_records:
            for word in MetadataSimilarity.SEPARATOR.split(record.title):
                if word:
                    histogram[word.lower()] += 1
                    words += 1
        for k, v in histogram.items():
            histogram[k] = v/words
        return histogram

    def title_histogram_difference(self, other_work):
        my_histogram = self.title_histogram()
        other_histogram = other_work.title_histogram()
        differences = []
        # For every word that appears in this work's titles, compare
        # its frequency against the frequency of that word in the
        # other work's histogram.
        for k, v in my_histogram.items():
            difference = abs(v - other_histogram.get(k, 0))
            differences.append(difference)

        # Add the frequency of every word that appears in the other work's
        # titles but not in this work's titles.
        for k, v in other_histogram.items():
            if k not in my_histogram:
                differences.append(abs(v))
        return sum(differences)

    def similarity_to(self, other_work):
        """How likely is it that this work describes the same book as the
        given work?

        A high number indicates very strong similarity; a low or
        negative number indicates low similarity.
        """
        title_quotient = (1-self.title_histogram_difference(other_work))
        author_quotient = MetadataSimilarity.title_similarity(
            self.authors, other_work.authors)

        return (title_quotient * 0.80) + (author_quotient * 0.20)

    def merge_into(self, _db, target_work):
        """This Work ceases to exist and is replaced by target_work."""
        #print "Merging %r\n into %r" % (self, target_work)
        my_histogram = self.title_histogram()
        target_histogram = target_work.title_histogram()
        if 'abroad' in self.title:
            set_trace()

        target_work.license_pools.extend(self.license_pools)
        target_work.work_records.extend(self.work_records)
        target_work.calculate_presentation()
        # print "The resulting work: %r" % target_work
        _db.delete(self)

    def calculate_presentation(self):
        """Figure out the 'best' title/author/subjects for this Work.

        For the time being, 'best' means the most common among this
        Work's WorkRecords.
        """
        #isbn = random.randint(1,1000000)
        #self.thumbnail_cover_link = "http://covers.openlibrary.org/b/id/%s-S.jpg" % isbn
        #self.full_cover_link = "http://covers.openlibrary.org/b/id/%s-L.jpg" % isbn

        titles = Counter()
        lcc = Counter()
        authors = Counter()
        languages = Counter()

        shortest_title = None

        for r in self.work_records:
            titles[r.title] += 1
            if not shortest_title or len(r.title) < len(shortest_title):
                shortest_title = r.title

            if 'LCC' in r.subjects:
                for s in r.subjects['LCC']:
                    lcc[s['id']] += 1
            for a in r.authors:
                authors[a['name']] += 1
            if r.languages:
                languages[tuple(r.languages)] += 1

        # Do not consider titles that are more than 3x longer than the
        # shortest title.
        short_enough_titles = Counter()
        for t, i in titles.items():
            if len(t) < len(shortest_title) * 3:
                short_enough_titles[t] = i

        self.title = short_enough_titles.most_common(1)[0][0]

        if len(languages) > 1:
            print "%s includes work records from several different languages: %r" % (self.title, languages)
                
            set_trace()
        self.languages = languages.most_common(1)[0][0]

        if authors:
            self.authors = authors.most_common(1)[0][0]
        if lcc:
            lcc = lcc.most_common(1)[0][0]
            if lcc == 'PR':
                self.lane = "Fiction"
            elif lcc == 'PZ':
                self.lane = "Children's Fiction"
            else:
                self.lane = "Nonfiction"
        else:
            self.lane = "Unknown"

class LicensePool(Base):

    """A pool of undifferentiated licenses for a work from a given source.
    """

    __tablename__ = 'licensepools'
    id = Column(Integer, primary_key=True)

    # A LicensePool may be associated with a Work. (If it's not, no one
    # can check it out.)
    work_id = Column(Integer, ForeignKey('works.id'))

    # Each LicensePool is associated with one DataSource and one
    # WorkIdentifier, and therefore with one original WorkRecord.
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

    def work_record(self, _db):
        """The LicencePool's primary WorkRecord.

        This is (our view of) the book's entry on whatever website
        hosts the licenses.
        """
        return _db.query(WorkRecord).filter_by(
            data_source=self.data_source,
            primary_identifier=self.identifier).one()

    @classmethod
    def with_no_work(self, _db):
        """Find LicensePools that have no corresponding Work."""
        return _db.query(LicensePool).outerjoin(Work).filter(
            Work.id==None).all()

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
            d[Event.SOURCE] = self.work_record().source
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

    @classmethod
    def consolidate_works(cls, _db):
        """Assign a (possibly new) Work to every unassigned LicensePool."""
        for unassigned in cls.with_no_work(_db):
            etext, new = unassigned.calculate_work(_db)

    def calculate_work(self, _db):
        """Find or create a Work for this LicensePool."""
        primary_work_record = self.work_record(_db)
        self.languages = primary_work_record.languages
        if primary_work_record.work is not None:
            # That was a freebie.
            return primary_work_record.work, False

        # Find all work records connected to this LicensePool's
        # primary work record.
        equivalent_work_records = primary_work_record.equivalent_work_records(
            _db) + [primary_work_record]

        # Find all existing Works that have claimed one or more of
        # those work records.
        claimed_records_by_work = defaultdict(list)

        my_unclaimed_work_records = []

        most_likely_existing_work = None

        shortest_title = None
        for r in equivalent_work_records:
            if not r.work:
                if not shortest_title or r.title < shortest_title:
                    shortest_title = r.title

        for r in equivalent_work_records:
            if not r.work:
                # This work record has not been claimed by anyone. 
                #
                # TODO: apply much more lenient terms if the match is
                # based on ISBN or other unique identifier.
                similarity = primary_work_record.similarity_to(r)
                if similarity >= 0.5 and len(r.title) < (len(shortest_title) * 3):
                    # It's similar enough to this LicensePool's
                    # primary WorkRecord that we'll be claiming it for
                    # whichever Work this LicensePool ends up associated
                    # with.
                    my_unclaimed_work_records.append(r)
                else:
                    # It's not all that similar to this LicensePool's
                    # primary WorkRecord. Leave it alone.
                    pass
            else:
                # This work record has been claimed by a Work. This
                # strengthens the tie between this LicensePool and that
                # Work.
                records_for_this_work = claimed_records_by_work[r.work]
                records_for_this_work.append(r)

        # Find all existing Works that claimed more WorkRecords than
        # this Licensepool claimed on its own. These are all better
        # choices than creating a new Work. In fact, there's a good
        # chance they are all the same work.
        better_choices = [
            (work, len(records)) for work, records in claimed_records_by_work.items()
            if len(records) > len(my_unclaimed_work_records)
        ]

        if better_choices:
            # One or more Works are better choices than creating a new
            # Work for this LicensePool. Merge them all into the most
            # popular Work and associate the LicencePool with that
            # Work.
   
            by_popularity = sorted(better_choices, key=lambda x: x[1], reverse=True)

            work = by_popularity[0][0]
            for less_popular, popularity in by_popularity[1:]:
                similarity = less_popular.similarity_to(work)
                if similarity < 0.5:
                    # print "NOT MERGING %r into %r, the works are too different." % (less_popular, work)
                    pass
                else:
                    less_popular.merge_into(_db, work)
            work.license_pools.append(self)
            created = False
        else:
            # There is no better choice than creating a new Work for this
            # LicensePool.
            work = Work(license_pools=[self])
            _db.add(work)
            _db.flush()
            created = True

        # Associate the unclaimed WorkRecords with the Work we decided
        # on/created.
        work.work_records.extend(my_unclaimed_work_records)

        # Recalculate the display information for the Work, since the
        # associated WorkRecords have changed.
        work.calculate_presentation()
        # All done!
        return work, created


class CirculationEvent(Base):

    """Changes to a license pool's circulation status.

    We log these so we can measure things like the velocity of
    individual books.
    """
    __tablename__ = 'circulationevents'

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
    ROLES = 'roles'
    BIRTH_DATE = 'birthDate'
    DEATH_DATE = 'deathDate'

    # Specific common roles
    AUTHOR_ROLE = 'Author'
    ILLUSTRATOR_ROLE = 'Illustrator'
    EDITOR_ROLE = 'Editor'
    UNKNOWN_ROLE = 'Unknown'
