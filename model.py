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
from sqlalchemy.ext.mutable import (
    MutableDict,
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
    Float,
    ForeignKey,
    Integer,
    Index,
    String,
    Table,
    Unicode,
    UniqueConstraint,
)

from classification import Classification
from lane import Lane
from util import (
    random_isbns,
    MetadataSimilarity,
)

#import logging
#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

from sqlalchemy.orm.session import Session

from sqlalchemy.dialects.postgresql import (
    ARRAY,
    HSTORE,
    JSON,
)
from sqlalchemy.orm import sessionmaker

from database_credentials import SERVER, MAIN_DB, CONTENT_CAFE

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
    OPEN_LIBRARY = "Open Library"

    __tablename__ = 'datasources'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, index=True)
    offers_licenses = Column(Boolean, default=False)
    primary_identifier_type = Column(String, index=True)
    extra = Column(MutableDict.as_mutable(JSON), default={})

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
                 (cls.OPEN_LIBRARY, False, WorkIdentifier.OPEN_LIBRARY_ID, None),
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
    Column('workrecord_id', Integer, ForeignKey('workrecords.id'), index=True),
    Column('workidentifier_id', Integer,
           ForeignKey('workidentifiers.id'), index=True)
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
    OCLC_TITLE_AUTHOR_SEARCH = "OCLC title/author search"
    OCLC_WORK = "OCLC Work ID"
    OCLC_NUMBER = "OCLC Number"
    URI = "URI"
    DOI = "DOI"
    UPC = "UPC"

    OPEN_LIBRARY_ID = "OLID"

    __tablename__ = 'workidentifiers'
    id = Column(Integer, primary_key=True)
    type = Column(String(64), index=True)
    identifier = Column(String, index=True)

    def __repr__(self):
        return (u"%s: %s/%s" % (self.id, self.type, self.identifier))

    # One WorkIdentifier may serve as the primary identifier for
    # several WorkRecords.
    primarily_identifies = relationship(
        "WorkRecord", backref="primary_identifier",
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

    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)
    primary_identifier_id = Column(
        Integer, ForeignKey('workidentifiers.id'), index=True)

    # A WorkRecord may be associated with a Work
    work_id = Column(Integer, ForeignKey('works.id'), index=True)

    # Many WorkRecords may be equivalent to the same WorkIdentifier,
    # and a single WorkRecord may be equivalent to many
    # WorkIdentifiers.
    equivalent_identifiers = relationship(
        "WorkIdentifier",
        secondary=workrecord_workidentifier,
        backref="equivalent_workrecords")

    title = Column(Unicode)
    subtitle = Column(Unicode)
    series = Column(Unicode)
    authors = Column(JSON, default=[])
    subjects = Column(JSON, default=[])
    summary = Column(MutableDict.as_mutable(JSON), default={})

    languages = Column(JSON, default=[])
    publisher = Column(Unicode)
    imprint = Column(Unicode)

    # `published is the original publication date of the
    # text. `issued` is when made available in this ebook edition. A
    # Project Gutenberg text was likely `published` long before being
    # `issued`.
    issued = Column(Date)
    published = Column(Date)

    links = Column(MutableDict.as_mutable(JSON), default={})

    extra = Column(MutableDict.as_mutable(JSON), default={})
    
    # Common link relation URIs for the links.
    OPEN_ACCESS_DOWNLOAD = "http://opds-spec.org/acquisition/open-access"
    IMAGE = "http://opds-spec.org/image"
    THUMBNAIL_IMAGE = "http://opds-spec.org/image/thumbnail"
    SAMPLE = "http://opds-spec.org/acquisition/sample"

    def __repr__(self):
        return (u"WorkRecord %s (%s/%s/%s)" % (
            self.id, self.title, ", ".join([x['name'] for x in self.authors]),
            ", ".join(self.languages))).encode("utf8")

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
            kwargs = dict(create_method_kwargs=dict(
                equivalent_identifiers=[work_identifier]))
        else:
            f = get_one
            kwargs = dict()
        return f(_db, WorkRecord, data_source=data_source,
                 primary_identifier=work_identifier,
                 **kwargs)

    def equivalent_work_records(self, _db):
        """All WorkRecords whose primary ID is among this WorkRecord's
        equivalent IDs.
        """
        return _db.query(WorkRecord).filter(
            WorkRecord.primary_identifier_id.in_(
                [x.id for x in self.equivalent_identifiers])).all()

    @classmethod 
    def equivalent_to_equivalent_identifiers_query(self, _db):
        targets = aliased(WorkRecord)
        equivalent_identifiers = aliased(workrecord_workidentifier)
        equivalent_identifiers_2 = aliased(workrecord_workidentifier)
        me = aliased(WorkRecord)
        q = _db.query(targets).join(
            equivalent_identifiers,
            targets.id==equivalent_identifiers.columns['workrecord_id']
        ).join(
            equivalent_identifiers_2,
            equivalent_identifiers.columns['workidentifier_id']==equivalent_identifiers_2.columns['workidentifier_id']
        ).join(
            me, 
            (me.id==equivalent_identifiers_2.columns['workrecord_id'])
        )
        return q, me

    def equivalent_to_equivalent_identifiers(self, _db):
        """Find all WorkRecords that are equivalent to one of this
        WorkRecord's equivalent identifiers.
        """
        q, me = self.equivalent_to_equivalent_identifiers_query(_db)
        return q.filter(me.id==self.id).all()

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
        qu = qu.distinct()

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

        Most of the WorkRecords are from OCLC Classify, and we expect
        to get some of them wrong (e.g. when a single OCLC work is a
        compilation of several novels by the same author). That's okay
        because those WorkRecords aren't backed by
        LicensePools. They're purely informative. We will have some
        bad information in our database, but the clear-cut cases
        should outnumber the fuzzy cases, so we we should still group
        the WorkRecords that really matter--the ones backed by
        LicensePools--together correctly.

        TODO: apply much more lenient terms if the two WorkRecords are
        identified by the same ISBN or other unique identifier.
        """

        if set(other_record.languages) == set(self.languages):
            # The languages match perfectly.
            language_factor = 1
        elif self.languages and other_record.languages:
            # Each record specifies a different set of languages. This
            # is an immediate disqualification.
            #
            # TODO: edge case when one record's languages are a subset
            # of the other's.
            return 0
        else:
            # One record specifies a language and one does not. This
            # is a little tricky. We're going to apply a penalty, but
            # since the majority of records we're getting from OCLC are in
            # English, the penalty will be less if one of the
            # languages is English. It's more likely that an unlabeled
            # record is in English than that it's in some other language.
            if 'eng' in self.languages or 'eng' in other_record.languages:
                language_factor = 0.80
            else:
                language_factor = 0.50

        title_quotient = MetadataSimilarity.title_similarity(
            self.title, other_record.title)

        author_quotient = MetadataSimilarity.author_similarity(
            self.authors, other_record.authors)

        # We weight title more heavily because it's much more likely
        # that one author wrote two different books than that two
        # books with the same title have different authors.
        return language_factor * (
            (title_quotient * 0.80) + (author_quotient * 0.20))

    def apply_similarity_threshold(self, candidates, threshold=0.5):
        """Yield the WorkRecords from the given list that are similar 
        enough to this one.
        """
        for candidate in candidates:
            if self == candidate:
                yield candidate
            else:
                similarity = self.similarity_to(candidate)
                if similarity >= threshold:
                    yield candidate

    def classifications(self):
        if not self.subjects:
            return None


class Work(Base):

    __tablename__ = 'works'
    id = Column(Integer, primary_key=True)

    # One Work may have copies scattered across many LicensePools.
    license_pools = relationship("LicensePool", backref="work")

    # A single Work may claim many WorkRecords.
    work_records = relationship("WorkRecord", backref="work")
    
    title = Column(Unicode)
    authors = Column(Unicode)
    languages = Column(Unicode, index=True)
    audience = Column(Unicode, index=True)
    subjects = Column(MutableDict.as_mutable(JSON), default={})
    thumbnail_cover_link = Column(Unicode)
    full_cover_link = Column(Unicode)
    lane = Column(Unicode, index=True)
    quality = Column(Float, index=True)

    def __repr__(self):
        return ('%s "%s" (%s) %s %s (%s wr, %s lp)' % (
            self.id, self.title, self.authors, self.lane, self.languages,
            len(self.work_records), len(self.license_pools))).encode("utf8")

    def all_workrecords(self, _db):
        q, workrecord = WorkRecord.equivalent_to_equivalent_identifiers_query(_db)
        return q.filter(workrecord.work==self).all()

    def similarity_to(self, other_work):
        """How likely is it that this Work describes the same book as the
        given Work (or WorkRecord)?

        This is more accurate than WorkRecord.similarity_to because we
        (hopefully) have a lot of WorkRecords associated with each
        Work. If their metadata has a lot of overlap, the two Works
        are probably the same.
        """
        my_languages = Counter()
        my_authors = Counter()
        total_my_languages = 0
        total_my_authors = 0
        my_titles = []
        other_languages = Counter()
        total_other_languages = 0
        other_titles = []
        other_authors = Counter()
        total_other_authors = 0
        for record in self.work_records:
            if record.languages:
                my_languages[tuple(record.languages)] += 1
                total_my_languages += 1
            my_titles.append(record.title)
            for author in record.authors:
                # TODO: this treats author names as strings that either match
                # or don't. We need to handle author names in a more
                # sophisticated way as per util.
                my_authors[author['name']] += 1
                total_my_authors += 1

        if isinstance(other_work, Work):
            other_work_records = other_work.work_records
        else:
            other_work_records = [other_work]

        for record in other_work_records:
            if record.languages:
                other_languages[tuple(record.languages)] += 1
                total_other_languages += 1
            other_titles.append(record.title)
            for author in record.authors:
                # TODO: this treats author names as strings that either match
                # or don't. We need to handle author names in a more
                # sophisticated way as per util.
                other_authors[author['name']] += 1
                total_other_authors += 1

        title_distance = MetadataSimilarity.histogram_distance(
            my_titles, other_titles)

        my_authors = MetadataSimilarity.normalize_histogram(
            my_authors, total_my_authors)
        other_authors = MetadataSimilarity.normalize_histogram(
            other_authors, total_other_authors)

        author_distance = MetadataSimilarity.counter_distance(
            my_authors, other_authors)

        my_languages = MetadataSimilarity.normalize_histogram(
            my_languages, total_my_languages)
        other_languages = MetadataSimilarity.normalize_histogram(
            other_languages, total_other_languages)

        language_distance = MetadataSimilarity.counter_distance(
            my_languages, other_languages)
        language_factor = 1-language_distance
        title_quotient = 1-title_distance
        author_quotient = 1-author_distance

        return language_factor * (
            (title_quotient * 0.80) + (author_quotient * 0.20))

    def merge_into(self, _db, target_work, similarity_threshold=0.5):
        """This Work ceases to exist and is replaced by target_work.

        The two works must be similar to within similarity_threshold,
        or nothing will happen.
        """
        similarity = self.similarity_to(target_work)
        if similarity < similarity_threshold:
            print "NOT MERGING %r into %r, similarity is only %.3f." % (
                self, target_work, similarity)
        else:
            print "MERGING %r into %r, similarity is %.3f." % (
                self, target_work, similarity)
            target_work.license_pools.extend(self.license_pools)
            target_work.work_records.extend(self.work_records)
            target_work.calculate_presentation(_db)
            print "The resulting work: %r" % target_work
            _db.delete(self)

    def calculate_subjects(self):
        """Consolidate subject information from across WorkRecords."""
        data = {}
        for i in self.work_records:
            data = Classification.classify(i.subjects, data)
        return data

    def calculate_presentation(self, _db):
        """Figure out the 'best' title/author/subjects for this Work.

        For the time being, 'best' means the most common among this
        Work's WorkRecords.
        """

        titles = []
        authors = Counter()
        languages = Counter()
        image_links = Counter()

        shortest_title = ''
        titles = []

        # Find all Open Library WorkRecords that are equivalent to the
        # same OCLC WorkIdentifier as one of this work's WorkRecords.
        equivalent_records = self.all_workrecords(_db)
        for r in equivalent_records:
            titles.append(r.title)
            if r.title and (
                    not shortest_title or len(r.title) < len(shortest_title)):
                shortest_title = r.title

            for a in r.authors:
                authors[a['name']] += 1
            if r.languages:
                languages[tuple(r.languages)] += 1

            if (WorkRecord.THUMBNAIL_IMAGE in r.links and
                WorkRecord.IMAGE in r.links):
                thumb = r.links[WorkRecord.THUMBNAIL_IMAGE][0]['href']
                full = r.links[WorkRecord.IMAGE][0]['href']
                key = (thumb, full)
                image_links[key] += 1

        self.title = MetadataSimilarity.most_common(
            len(shortest_title) * 3, *titles)

        if len(languages) > 1:
            print "%s includes work records from several different languages: %r" % (self.title, languages)
                
        if languages:
            self.languages = languages.most_common(1)[0][0]

        if authors:
            self.authors = authors.most_common(1)[0][0]

        if image_links:
            # Without local copies we have no way of determining which
            # image is the best. But in general, the Open Library ones
            # tend to be higher-quality
            best_index = 0
            items = image_links.most_common()
            for i, link in enumerate(items):
                if 'openlibrary' in link[0][0]:
                    best_index = i
                    break
            self.thumbnail_cover_link, self.full_cover_link = items[best_index][0]

    def calculate_quality(self, _db):
        """Calculate some measure of the quality of a work.

        Higher numbers are better.

        The quality of this quality measure is currently very poor,
        but we will be improving it over time as we have more data.
        """
        # For public domain books, the quality is the number of
        # records we have for it.
        self.quality = len(self.all_workrecords(_db))
        if self.title:
            print "%s %s" % (self.quality, self.title.encode("utf8"))

    def calculate_lane(self):
        """Calculate audience, fiction status, and best lane for this book.

        The quality of this quality measure is currently fairly poor,
        but we will be improving it over time as we have more data.
        """

        print (self.title or "").encode("utf8")
        self.subjects = self.calculate_subjects()
        if 'audience' in self.subjects:
            self.audience = Lane.most_common(self.subjects['audience'])
        else:
            self.audience = Classification.AUDIENCE_ADULT

        self.fiction, self.lane = Lane.best_match(
            self.subjects)
        print " %(lane)s f=%(fiction)s, a=%(audience)s, %(subjects)r" % (
            dict(lane=self.lane, fiction=self.fiction,
                 audience=self.audience, subjects=self.subjects.get('names',{})))
        print

class LicensePool(Base):

    """A pool of undifferentiated licenses for a work from a given source.
    """

    __tablename__ = 'licensepools'
    id = Column(Integer, primary_key=True)

    # A LicensePool may be associated with a Work. (If it's not, no one
    # can check it out.)
    work_id = Column(Integer, ForeignKey('works.id'), index=True)

    # Each LicensePool is associated with one DataSource and one
    # WorkIdentifier, and therefore with one original WorkRecord.
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)
    identifier_id = Column(Integer, ForeignKey('workidentifiers.id'), index=True)

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
        a = 0
        for unassigned in cls.with_no_work(_db):
            etext, new = unassigned.calculate_work(_db)
            a += 1
            print "Created %r" % etext
            if a and not a % 100:
                _db.commit()

    def potential_works(self, _db, similarity_threshold=0.8):
        """Find all existing works that have claimed this pool's 
        work records.

        :return: A 3-tuple ({Work: [WorkRecord]}, [WorkRecord])
        Element 0 is a mapping of Works to the WorkRecords they've claimed.
        Element 1 is a list of WorkRecords that are unclaimed by any Work.
        """
        primary_work_record = self.work_record(_db)

        claimed_records_by_work = defaultdict(list)
        unclaimed_records = []

        # Find all work records connected to this LicensePool's
        # primary work record.
        equivalent_work_records = primary_work_record.equivalent_work_records(
            _db) + [primary_work_record]

        for r in equivalent_work_records:
            if r.work:
                # This work record has been claimed by a Work. This
                # strengthens the tie between this LicensePool and that
                # Work.
                l = claimed_records_by_work[r.work]
                check_against = r.work
            else:
                # This work record has not been claimed by anyone. 
                l = unclaimed_records
                check_against = primary_work_record

            # Apply the similarity threshold filter.
            if check_against.similarity_to(r) >= similarity_threshold:
                l.append(r)
        return claimed_records_by_work, unclaimed_records

    def calculate_work(self, _db, record_similarity_threshold=0.8,
                       work_similarity_threshold=0.8):
        """Find or create a Work for this LicensePool."""
        primary_work_record = self.work_record(_db)
        self.languages = primary_work_record.languages
        if primary_work_record.work is not None:
            # That was a freebie.
            print "ALREADY CLAIMED: %s by %s" % (
                primary_work_record.title, self.work
            )
            self.work = primary_work_record.work
            return primary_work_record.work, False

        # Figure out what existing works have claimed this
        # LicensePool's WorkRecords, and which WorkRecords are still
        # unclaimed.
        claimed, unclaimed = self.potential_works(
            _db, record_similarity_threshold)

        # We're only going to consider records that meet a similarity
        # threshold vis-a-vis this LicensePool's primary work.
        print "Calculating work for %r" % primary_work_record
        print " There are %s unclaimed work records" % len(unclaimed)
        #for i in unclaimed:
        #    print "  %.3f %r" % (
        #        primary_work_record.similarity_to(i), i)
        #print

        # Now we know how many unclaimed WorkRecords this LicensePool
        # will claim if it becomes a new Work. Find all existing Works
        # that claimed *more* WorkRecords than that. These are all
        # better choices for this LicensePool than creating a new
        # Work. In fact, there's a good chance they are all the same
        # Work, and should be merged.
        my_languages = set(self.languages)
        more_popular_choices = [
            (work, len(records))
            for work, records in claimed.items()
            if len(records) > len(unclaimed)
            and work.languages
            and set(work.languages) == my_languages
            and work.similarity_to(primary_work_record) >= work_similarity_threshold
        ]
        for work, records in claimed.items():
            sim = work.similarity_to(primary_work_record)
            if sim < work_similarity_threshold:
                print "REJECTED %r as more popular choice for\n %r (similarity: %.2f)" % (
                    work, primary_work_record, sim
                    )

        if more_popular_choices:
            # One or more Works seem to be better choices than
            # creating a new Work for this LicensePool. Merge them all
            # into the most popular Work.
            by_popularity = sorted(
                more_popular_choices, key=lambda x: x[1], reverse=True)

            # This is the work with the most claimed WorkRecords, so
            # it's the one we'll merge the others into. We chose
            # the most popular because we have the most data for it, so 
            # it's the most accurate choice when calculating similarity.
            work = by_popularity[0][0]
            print "MORE POPULAR CHOICE for %s: %r" % (
                primary_work_record.title.encode("utf8"), work)
            for less_popular, claimed_records in by_popularity[1:]:
                less_popular.merge_into(_db, work, work_similarity_threshold)
            created = False
        else:
            # There is no better choice than creating a brand new Work.
            # print "NEW WORK for %r" % primary_work_record.title
            work = Work()
            _db.add(work)
            _db.flush()
            created = True

        # Associate this LicensePool with the work we chose or
        # created.
        work.license_pools.append(self)

        # Associate the unclaimed WorkRecords with the Work.
        work.work_records.extend(unclaimed)

        # Recalculate the display information for the Work, since the
        # associated WorkRecords have changed.
        # work.calculate_presentation(_db)
        #if created:
        #    print "Created %r" % work
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
    license_pool_id = Column(
        Integer, ForeignKey('licensepools.id'), index=True)

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
