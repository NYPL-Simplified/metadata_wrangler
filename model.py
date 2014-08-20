# encoding: utf-8
from collections import (
    Counter,
    defaultdict,
)
from cStringIO import StringIO
import datetime
import os
from nose.tools import set_trace
import random

from PIL import Image

from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    backref,
    relationship,
)
from sqlalchemy import or_
from sqlalchemy.orm import (
    aliased,
    backref,
)
from sqlalchemy.orm.exc import (
    NoResultFound
)
from sqlalchemy.ext.mutable import (
    MutableDict,
)
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.expression import (
    and_,
    or_,
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
    Enum,
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
    LanguageCodes,
    MetadataSimilarity,
)
from util.summary import SummaryEvaluator

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

DEBUG = False

def production_session():
    url = os.environ['DATABASE_URL']
    print url
    if url.startswith('"'):
        url = url[1:]
    print "ENVIRONMENT: %s" % os.environ['DATABASE_URL'] 
    print "MODIFIED: %s" % url
    return SessionManager.session(url)

class SessionManager(object):

    @classmethod
    def engine(cls, url=None):
        url = url or os.environ['DATABASE_URL']
        return create_engine(url, echo=DEBUG)

    @classmethod
    def initialize(cls, url):
        engine = cls.engine(url)
        Base.metadata.create_all(engine)
        return engine, engine.connect()

    @classmethod
    def session(cls, url):
        engine, connection = cls.initialize(url)
        session = Session(connection)
        print "INITIALIZING DATA"
        cls.initialize_data(session)
        session.commit()
        print "DONE INITIALIZING DATA"
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
        try:
            return create(db, model, create_method, create_method_kwargs, **kwargs)
        except IntegrityError:
            db.rollback()
            return db.query(model).filter_by(**kwargs).one(), False

def create(db, model, create_method='',
           create_method_kwargs=None,
           **kwargs):
    kwargs.update(create_method_kwargs or {})
    created = getattr(model, create_method, model)(**kwargs)
    db.add(created)
    db.flush()
    return created, True

Base = declarative_base()

class Patron(Base):

    __tablename__ = 'patrons'
    id = Column(Integer, primary_key=True)

    # The patron's permanent unique identifier in an external library
    # system, probably never seen by the patron.
    external_identifier = Column(Unicode, unique=True, index=True)

    # An identifier used by the patron that gives them the authority
    # to borrow books. This identifier may change over time.
    authorization_identifier = Column(Unicode, unique=True, index=True)

    # TODO: An identifier used by the patron that authenticates them,
    # but does not give them the authority to borrow books. i.e. their
    # website username.

    # The last time this record was synced up with an external library
    # system.
    last_external_sync = Column(DateTime)

    # The time, if any, at which the user's authorization to borrow
    # books expires.
    authorization_expires = Column(Date, index=True)

    loans = relationship('Loan', backref='patron')

    def works_on_loan(self):
        db = Session.object_session(self)
        loans = db.query(Loan).filter(Loan.patron==self)
        return [loan.license_pool.work for loan in loans]

    @property
    def authorization_is_active(self):
        # Unlike pretty much every other place in this app, I use
        # (server) local time here instead of UTC. This is to make it
        # less likely that a patron's authorization will expire before
        # they think it should.
        if (self.authorization_expires
            and self.authorization_expires 
            < datetime.datetime.now().date()):
            return False
        return True


class Loan(Base):
    __tablename__ = 'loans'
    id = Column(Integer, primary_key=True)
    patron_id = Column(Integer, ForeignKey('patrons.id'), index=True)
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'), index=True)
    start = Column(DateTime)
    end = Column(DateTime)


class DataSource(Base):

    """A source for information about books, and possibly the books themselves."""

    GUTENBERG = "Gutenberg"
    OVERDRIVE = "Overdrive"
    THREEM = "3M"
    OCLC = "OCLC Classify"
    OCLC_LINKED_DATA = "OCLC Linked Data"
    XID = "WorldCat xID"
    AXIS_360 = "Axis 360"
    WEB = "Web"
    OPEN_LIBRARY = "Open Library"
    CONTENT_CAFE = "Content Cafe"
    MANUAL = "Manual intervention"

    __tablename__ = 'datasources'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, index=True)
    offers_licenses = Column(Boolean, default=False)
    primary_identifier_type = Column(String, index=True)
    extra = Column(MutableDict.as_mutable(JSON), default={})

    # One DataSource can generate many WorkRecords.
    work_records = relationship("WorkRecord", backref="data_source")

    # One DataSource can generate many CoverageRecords.
    coverage_records = relationship("CoverageRecord", backref="data_source")

    # One DataSource can generate many IDEquivalencies.
    id_equivalencies = relationship("Equivalency", backref="data_source")

    # One DataSource can grant access to many LicensePools.
    license_pools = relationship(
        "LicensePool", backref=backref("data_source", lazy='joined'))

    # One DataSource can provide many Resources.
    resources = relationship("Resource", backref="data_source")


    @classmethod
    def lookup(cls, _db, name):
        try:
            q = _db.query(cls).filter_by(name=name)
            return q.one()
        except NoResultFound:
            return None

    @classmethod
    def well_known_sources(cls, _db):
        """Make sure all the well-known sources exist."""

        for (name, offers_licenses, primary_identifier_type, refresh_rate) in (
                 (cls.GUTENBERG, True, WorkIdentifier.GUTENBERG_ID, None),
                 (cls.OVERDRIVE, True, WorkIdentifier.OVERDRIVE_ID, 0),
                 (cls.THREEM, True, WorkIdentifier.THREEM_ID, 60*60*6),
                 (cls.AXIS_360, True, WorkIdentifier.AXIS_360_ID, 0),
                 (cls.OCLC_LINKED_DATA, False, WorkIdentifier.OCLC_NUMBER, None),
                 (cls.OCLC, False, WorkIdentifier.OCLC_NUMBER, None),
                 (cls.OPEN_LIBRARY, False, WorkIdentifier.OPEN_LIBRARY_ID, None),
                 (cls.WEB, True, WorkIdentifier.URI, None),
                 (cls.CONTENT_CAFE, False, None, None),
                 (cls.MANUAL, False, None, None),
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


class CoverageRecord(Base):
    """A record of a WorkRecord being used as input into another data source.

    TODO: Should probably be a record of a WorkIdentifier being used as input
    into another source.
    """
    __tablename__ = 'coveragerecords'

    id = Column(Integer, primary_key=True)
    work_record_id = Column(
        Integer, ForeignKey('workrecords.id'), index=True)
    data_source_id = Column(
        Integer, ForeignKey('datasources.id'), index=True)
    date = Column(Date, index=True)


class Equivalency(Base):
    """An assertion that two WorkIdentifiers identify the same work.

    This assertion comes with a 'strength' which represents how confident
    the data source is in the assertion.
    """
    __tablename__ = 'equivalents'

    # 'input' is the ID that was used as input to the datasource.
    # 'output' is the output
    id = Column(Integer, primary_key=True)
    input_id = Column(Integer, ForeignKey('workidentifiers.id'), index=True)
    input = relationship("WorkIdentifier", foreign_keys=input_id)
    output_id = Column(Integer, ForeignKey('workidentifiers.id'), index=True)
    output = relationship("WorkIdentifier", foreign_keys=output_id)

    # Who says?
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)

    # How many distinct votes went into this assertion? This will let
    # us scale the change to the strength when additional votes come
    # in.
    votes = Column(Integer, default=1)

    # How strong is this assertion (-1..1)? A negative number is an
    # assertion that the two WorkIdentifiers do *not* identify the
    # same work.
    strength = Column(Float, index=True)

    def __repr__(self):
        r = u"[%s ->\n %s\n source=%s strength=%.2f votes=%d)]" % (
            repr(self.input).decode("utf8"),
            repr(self.output).decode("utf8"),
            self.data_source.name, self.strength, self.votes
        )
        return r.encode("utf8")

    @classmethod
    def for_identifiers(self, _db, workidentifiers, exclude_ids=None):
        """Find all Equivalencies for the given WorkIdentifiers."""
        if not workidentifiers:
            return []
        if isinstance(workidentifiers[0], WorkIdentifier):
            workidentifiers = [x.id for x in workidentifiers]
        q = _db.query(Equivalency).distinct().filter(
            or_(Equivalency.input_id.in_(workidentifiers),
                Equivalency.output_id.in_(workidentifiers))
        )
        if exclude_ids:
            q = q.filter(~Equivalency.id.in_(exclude_ids))
        return q

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

    OPEN_LIBRARY_ID = "OLID"

    __tablename__ = 'workidentifiers'
    id = Column(Integer, primary_key=True)
    type = Column(String(64), index=True)
    identifier = Column(String, index=True)

    equivalencies = relationship(
        "Equivalency",
        primaryjoin=("WorkIdentifier.id==Equivalency.input_id"),
        backref="input_identifiers",
    )

    def __repr__(self):
        records = self.primarily_identifies
        if records and records[0].title:
            title = u' wr="%s"' % records[0].title
        else:
            title = ""
        return (u"%s/%s ID=%s%s" % (self.type, self.identifier, self.id,
                                    title)).encode("utf8")

    # One WorkIdentifier may serve as the primary identifier for
    # several WorkRecords.
    primarily_identifies = relationship(
        "WorkRecord", backref="primary_identifier"
    )

    # One WorkIdentifier may serve as the identifier for
    # a single LicensePool.
    licensed_through = relationship(
        "LicensePool", backref="identifier", uselist=False, lazy='joined',
    )

    # One WorkIdentifier may serve to identify many Resources.
    resources = relationship(
        "Resource", backref="work_identifier"
    )

    # Type + identifier is unique.
    __table_args__ = (
        UniqueConstraint('type', 'identifier'),
    )

    @classmethod
    def for_foreign_id(cls, _db, foreign_identifier_type, foreign_id,
                       autocreate=True):
        was_new = None
        if autocreate:
            m = get_one_or_create
        else:
            m = get_one
            was_new = False

        result = m(_db, cls, type=foreign_identifier_type,
                   identifier=foreign_id)
        if isinstance(result, tuple):
            return result
        else:
            return result, False

    def equivalent_to(self, data_source, work_identifier, strength):
        """Make one WorkIdentifier equivalent to another.
        
        `data_source` is the DataSource that believes the two 
        identifiers are equivalent.
        """
        _db = Session.object_session(self)
        eq, new = get_one_or_create(
            _db, Equivalency,
            data_source=data_source,
            input=self,
            output=work_identifier,
            create_method_kwargs=dict(strength=strength))
        return eq

    @classmethod
    def recursively_equivalent_identifier_ids(
            self, _db, identifier_ids, levels=5, threshold=0.50):
        """All WorkIdentifier IDs equivalent to the given set of WorkIdentifier
        IDs at the given confidence threshold.

        This is an inefficient but simple implementation, performing
        one SQL query for each level of recursion.

        Four levels is enough to go from a Gutenberg text to an ISBN.
        Gutenberg ID -> OCLC Work IS -> OCLC Number -> ISBN

        Returns a dictionary mapping each ID in the original to a
        dictionary mapping the equivalent IDs to (confidence, strength
        of confidence) 2-tuples.
        """

        precursors = defaultdict(list)
        successors = defaultdict(list)

        seen_equivalency_ids = set([])
        this_round_ids = identifier_ids
        already_checked_ids = set()
        for distance in range(levels):
            next_round_ids = []
            already_checked_ids = already_checked_ids.union(this_round_ids)
            equivalencies = Equivalency.for_identifiers(
                _db, this_round_ids, seen_equivalency_ids)
            for e in equivalencies:
                seen_equivalency_ids.add(e.id)

                # Signal strength decreases monotonically, so
                # if it dips below the threshold, we can
                # ignore it from this point on.

                # I -> O becomes "I is a precursor of O with distance
                # equal to the I->O strength."
                if e.strength > threshold:
                    # print "Strong signal: %r" % e
                    precursors[e.output_id].append((e.input_id, e.strength))
                    successors[e.input_id].append((e.output_id, e.strength))
                else:
                    #print "Ignoring signal below threshold: %r" % e
                    pass

                # A -> ... -> I -> O becomes "A is a precursor of O
                # with strength equal to the A->I strength times the
                # I->O strength."
                for (precursor_id, precursor_strength) in precursors[e.input_id]:
                    total_strength = precursor_strength * e.strength
                    if total_strength >= threshold:
                        precursors[e.output_id].append(
                            (precursor_id, total_strength))
                        successors[precursor_id].append(
                            (e.output_id, total_strength))
                        #print "Confident in %.2f signal %d->\n%r" % (total_strength, e.input_id, e)
                    else:
                        #print "Not confident in %.2f signal %d->\n%r" % (total_strength, e.input_id, e)
                        pass

                if e.output_id not in already_checked_ids:
                    # This is our first time encountering the output
                    # ID of this Equivalency. We will use it as an
                    # input ID in the next round.
                    next_round_ids.append(e.output_id)
            if not next_round_ids:
                # We have achieved transitive closure. There
                # are no more IDs to check.
                break
            #print "Finished round: %r" % this_round_ids
            #print "Next round: %r" % next_round_ids
            this_round_ids = next_round_ids

        # Now that we have a list of successor signals for each
        # identifier ID, we can calculate the average strength of the
        # signal.
        equivalents = defaultdict(dict)
        for id in identifier_ids:
            # Each ID is equivalent to itself.
            equivalents[id][id] = (1, 1000000)
            for successor, strength in successors[id]:
                if successor in equivalents[id]:
                    existing_strength, num_votes = equivalents[id][successor]
                else:
                    existing_strength = 0.0
                    num_votes = 0
                total_strength = (existing_strength * num_votes) + strength
                num_votes += 1
                new_strength = total_strength / num_votes
                equivalents[id][successor] = (new_strength, num_votes)
                for precursor, precursor_strength in precursors[successor]:
                    if precursor in equivalents[id]:
                        existing_strength, num_votes = equivalents[id][precursor]
                    else:
                        existing_strength = 0.0
                        num_votes = 0
                    total_strength = (existing_strength * num_votes) + precursor_strength
                    num_votes += 1
                    new_strength = total_strength / num_votes
                    equivalents[id][precursor] = (new_strength, num_votes)
        return equivalents

    @classmethod
    def recursively_equivalent_identifier_ids_flat(
            cls, _db, identifier_ids, levels=5, threshold=0.5):
        ids = set()
        data = cls.recursively_equivalent_identifier_ids(
            _db, identifier_ids, levels, threshold)
        for equivalents in data.values():
            ids = ids.union(set(equivalents.keys()))
        return ids

    def equivalent_identifier_ids(self, levels=5, threshold=0.5):
        _db = Session.object_session(self)
        return WorkIdentifier.recursively_equivalent_identifier_ids_flat(
            _db, [self.id], levels)

    def add_resource(self, rel, href, data_source, license_pool=None,
                     media_type=None, content=None):
        """Associated a resource with this WorkIdentifier."""
        _db = Session.object_session(self)
        resource, new = get_one_or_create(
            _db, Resource, work_identifier=self,
            rel=rel,
            href=href,
            media_type=media_type,
            content=content,
            create_method_kwargs=dict(
                data_source=data_source,
                license_pool=license_pool))
        if content:
            resource.set_content(content, media_type)
        return resource, new

    @classmethod
    def resources_for_identifier_ids(self, _db, identifier_ids, rel=None):
        resources = _db.query(Resource).filter(
                Resource.work_identifier_id.in_(identifier_ids))
        if rel:
            resources = resources.filter(Resource.rel==rel)
        return resources

    IDEAL_COVER_ASPECT_RATIO = 2.0/3
    IDEAL_IMAGE_HEIGHT = 240
    IDEAL_IMAGE_WIDTH = 160

    @classmethod
    def evaluate_cover_quality(cls, _db, identifier_ids):
        # Find all image resources associated with any of
        # these identifiers.
        images = cls.resources_for_identifier_ids(
            _db, identifier_ids, Resource.IMAGE)
        images = images.filter(Resource.mirrored==True).all()

        champion = None
        # Judge the image resource by its deviation from the ideal
        # aspect ratio, and by its deviation (in the "too small"
        # direction only) from the ideal resolution.
        for r in images:
            if not r.width or not r.height:
                continue
            aspect_ratio = r.width / float(r.height)
            aspect_difference = abs(aspect_ratio-self.IDEAL_COVER_ASPECT_RATIO)
            quality = 1 - aspect_difference
            width_difference = (
                (r.width - self.IDEAL_IMAGE_WIDTH) / self.IDEAL_IMAGE_WIDTH)
            if width_difference < 0:
                # Image is not wide enough.
                quality = quality * (1-width_difference)
            height_difference = (
                (r.height - self.IDEAL_IMAGE_HEIGHT) / self.IDEAL_IMAGE_HEIGHT)
            if height_difference < 0:
                # Image is not tall enough.
                quality = quality * (1-height_difference)
            r.set_estimated_quality(quality)
            if not champion or r.quality > champion.quality:
                champion = r
        return champion

    @classmethod
    def evaluate_summary_quality(cls, _db, identifier_ids):
        """Evaluate the summaries for the given group of WorkIdentifier IDs.

        This is an automatic evaluation based solely on the content of
        the summaries. It will be combined with human-entered ratings
        to form an overall quality score.

        We need to evaluate summaries from a set of WorkIdentifiers
        (typically those associated with a single work) because we
        need to see which noun phrases are most frequently used to
        describe the underlying work.

        :return: The single highest-rated summary Resource.

        """
        evaluator = SummaryEvaluator()
        # Find all rel="description" resources associated with any of
        # these records.
        summaries = cls.resources_for_identifier_ids(
            _db, identifier_ids, Resource.DESCRIPTION)
        summaries = summaries.filter(Resource.content != None).all()

        champion = None
        # Add each resource's content to the evaluator's corpus.
        for r in summaries:
            evaluator.add(r.content)
        evaluator.ready()

        # Then have the evaluator rank each resource.
        scores = dict()
        for r in summaries:
            quality = evaluator.score(r.content)
            scores[r] = quality
            if not champion or quality > champion[1]:
                champion = (r, quality)

        # Scale it so that 
        set_trace()
        return champion

class Contributor(Base):
    """Someone (usually human) who contributes to books."""
    __tablename__ = 'contributors'
    id = Column(Integer, primary_key=True)

    # Standard identifiers for this contributor.
    lc = Column(Unicode, index=True)
    viaf = Column(Unicode, index=True)

    # This is the name we choose to display for this contributor, of
    # all the names we know for them. It may change over time.
    name = Column(Unicode, index=True)
    aliases = Column(ARRAY(Unicode), default=[])

    extra = Column(MutableDict.as_mutable(JSON), default={})

    contributions = relationship("Contribution", backref="contributor")
    work_contributions = relationship("WorkContribution", backref="contributor",
                                      )

    # Types of roles
    AUTHOR_ROLE = "Author"
    UNKNOWN_ROLE = 'Unknown'

    # Extra fields
    BIRTH_DATE = 'birthDate'
    DEATH_DATE = 'deathDate'

    def __repr__(self):
        extra = ""
        if self.lc:
            extra += " lc=%s" % self.lc
        if self.viaf:
            extra += " viaf=%s" % self.viaf
        return (u"Contributor %d (%s)" % (self.id, self.name)).encode("utf8")

    @classmethod
    def lookup(cls, _db, name=None, viaf=None, lc=None, aliases=None,
               extra=None):
        """Find or create a record for the given Contributor."""
        extra = extra or dict()

        create_method_kwargs = {
            Contributor.name.name : name,
            Contributor.aliases.name : aliases,
            Contributor.extra.name : extra
        }

        if not name and not lc and not viaf:
            raise ValueError(
                "Cannot look up a Contributor without any identifying "
                "information whatsoever!")

        if name and not lc and not viaf:
            # We will not create a Contributor based solely on a name
            # unless there is no existing Contributor with that name.
            #
            # If there *are* contributors with that name, we will
            # return all of them.
            #
            # We currently do not check aliases when doing name lookups.
            q = _db.query(Contributor).filter(Contributor.name==name)
            contributors = q.all()
            if contributors:
                return contributors, False
            else:
                try:
                    contributor = Contributor(**create_method_kwargs)
                    _db.add(contributor)
                    _db.flush()
                    contributors = [contributor]
                    new = True
                except IntegrityError:
                    _db.rollback()
                    contributors = q.all()
                    new = False
        else:
            # We are perfecly happy to create a Contributor based solely
            # on lc or viaf.
            query = dict()
            if lc:
                query[Contributor.lc.name] = lc
            if viaf:
                query[Contributor.viaf.name] = viaf

            try:
                contributors, new = get_one_or_create(
                    _db, Contributor, create_method_kwargs=create_method_kwargs,
                    **query)
            except Exception, e:
                set_trace()

        return contributors, new

    def merge_into(self, destination):
        """Two Contributor records should be the same.

        Merge this one into the other one.

        For now, this should only be used when the exact same record
        comes in through two sources. It should not be used when two
        Contributors turn out to represent different names for the
        same human being, e.g. married names or (especially) pen
        names. Just because we haven't thought that situation through
        well enough.
        """
        if self == destination:
            # They're already the same.
            return
        msg = u"MERGING %s into %s" % (
            repr(self).decode("utf8"), 
            repr(destination).decode("utf8"))
        print msg.encode("utf8")
        existing_aliases = set(destination.aliases)
        new_aliases = list(destination.aliases)
        for name in [self.name] + self.aliases:
            if name != destination.name and name not in existing_aliases:
                new_aliases.append(name)
        if new_aliases != destination.aliases:
            destination.aliases = new_aliases
        for k, v in self.extra.items():
            if not k in destination.extra:
                destination.extra[k] = v
        if not destination.lc:
            destination.lc = self.lc
        if not destination.viaf:
            destination.viaf = self.viaf
        _db = Session.object_session(self)
        for contribution in self.contributions:
            # Is the new contributor already associated with this
            # WorkRecord in the given role (in which case we delete
            # the old contribution) or not (in which case we switch the
            # contributor ID)?
            existing_record = _db.query(Contribution).filter(
                Contribution.contributor_id==destination.id,
                Contribution.workrecord_id==contribution.workrecord.id,
                Contribution.role==contribution.role)
            if existing_record.count():
                _db.delete(contribution)
            else:
                contribution.contributor_id = destination.id
        for contribution in self.work_contributions:
            existing_record = _db.query(WorkContribution).filter(
                WorkContribution.contributor_id==destination.id,
                WorkContribution.workrecord_id==contribution.workrecord.id,
                WorkContribution.role==contribution.role)
            if existing_record.count():
                _db.delete(contribution)
            else:
                contribution.contributor_id = destination.id
            contribution.contributor_id = destination.id
        _db.query(Contributor).filter(Contributor.id==self.id).delete()
        _db.commit()


class Contribution(Base):
    """A contribution made by a Contributor to a WorkRecord."""
    __tablename__ = 'contributions'
    id = Column(Integer, primary_key=True)
    workrecord_id = Column(Integer, ForeignKey('workrecords.id'), index=True,
                           nullable=False)
    contributor_id = Column(Integer, ForeignKey('contributors.id'), index=True,
                            nullable=False)
    role = Column(Unicode, index=True, nullable=False)
    __table_args__ = (
        UniqueConstraint('workrecord_id', 'contributor_id', 'role'),
    )


class WorkContribution(Base):
    """A contribution made by a Contributor to a Work."""
    __tablename__ = 'workcontributions'
    id = Column(Integer, primary_key=True)
    work_id = Column(Integer, ForeignKey('works.id'), index=True,
                     nullable=False)
    contributor_id = Column(Integer, ForeignKey('contributors.id'), index=True,
                            nullable=False)
    role = Column(Unicode, index=True, nullable=False)
    __table_args__ = (
        UniqueConstraint('work_id', 'contributor_id', 'role'),
    )


class WorkRecord(Base):

    """A lightly schematized collection of metadata for a work, or an
    edition of a work, or a book, or whatever. If someone thinks of it
    as a "book" with a "title" it can go in here.
    """

    __tablename__ = 'workrecords'
    id = Column(Integer, primary_key=True)

    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)

    # This WorkRecord is associated with one particular
    # identifier--the one used by its data source to identify
    # it. Through the Equivalency class, it is associated with a
    # (probably huge) number of other identifiers.
    primary_identifier_id = Column(
        Integer, ForeignKey('workidentifiers.id'), index=True)

    # A WorkRecord may be associated with a single Work.
    work_id = Column(Integer, ForeignKey('works.id'), index=True)

    # One WorkRecord may have many associated CoverageRecords.
    coverage_records = relationship("CoverageRecord", backref="work_record")

    title = Column(Unicode)
    subtitle = Column(Unicode)
    series = Column(Unicode)

    contributions = relationship("Contribution", backref="workrecord")

    subjects = Column(JSON, default=[])

    language = Column(Unicode, index=True)
    publisher = Column(Unicode)
    imprint = Column(Unicode)

    # `published is the original publication date of the
    # text. `issued` is when made available in this ebook edition. A
    # Project Gutenberg text was likely `published` long before being
    # `issued`.
    issued = Column(Date)
    published = Column(Date)

    extra = Column(MutableDict.as_mutable(JSON), default={})
    
    def __repr__(self):
        return (u"WorkRecord %s (%s/%s/%s)" % (
            self.id, self.title, ", ".join([x.name for x in self.contributors]),
            self.language)).encode("utf8")

    @property
    def contributors(self):
        return [x.contributor for x in self.contributions]

    @property
    def authors(self):
        return [x.contributor for x in self.contributions
                if x.role == Contributor.AUTHOR_ROLE]

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

        work_identifier, ignore = WorkIdentifier.for_foreign_id(
            _db, foreign_id_type, foreign_id)

        # Combine the two to get/create a WorkRecord.
        if create_if_not_exists:
            f = get_one_or_create
            kwargs = dict()
        else:
            f = get_one
            kwargs = dict()
        r = f(_db, WorkRecord, data_source=data_source,
                 primary_identifier=work_identifier,
                 **kwargs)
        return r

    def equivalencies(self, _db):
        """All the direct equivalencies between this record's primary
        identifier and other WorkIdentifiers.
        """
        return self.primary_identifier.equivalencies
        
    def equivalent_identifier_ids(self, levels=3):
        """All WorkIdentifiers equivalent to this record's primary identifier,
        at the given level of recursion."""
        return self.primary_identifier.equivalent_identifier_ids(levels)

    def equivalent_identifiers(self, levels=3, type=None):
        """All WorkIdentifiers equivalent to this
        WorkRecord's primary identifier, at the given level of recursion.
        """
        _db = Session.object_session(self)
        identifier_ids = self.equivalent_identifier_ids(levels)
        q = _db.query(WorkIdentifier).filter(
            WorkIdentifier.id.in_(identifier_ids))
        if type:
            q = q.filter(WorkIdentifier.type==type)
        return q

    def equivalent_work_records(self, levels=5):
        """All WorkRecords whose primary ID is equivalent to this WorkRecord's
        primary ID, at the given level of recursion.

        Five levels is enough to go from a Gutenberg ID to an Overdrive ID
        (Gutenberg ID -> OCLC Work ID -> OCLC Number -> ISBN -> Overdrive ID)
        """
        _db = Session.object_session(self)
        identifier_ids = self.equivalent_identifier_ids(levels)
        return _db.query(WorkRecord).filter(
            WorkRecord.primary_identifier_id.in_(identifier_ids))

    @classmethod
    def missing_coverage_from(
            cls, _db, workrecord_data_source, coverage_data_source):
        """Find WorkRecords from `workrecord_data_source` which have no
        CoverageRecord from `coverage_data_source`.

        e.g.

         gutenberg = DataSource.lookup(_db, DataSource.GUTENBERG)
         oclc_classify = DataSource.lookup(_db, DataSource.OCLC)
         missing_coverage_from(_db, gutenberg, oclc_classify)

        will find WorkRecords that came from Project Gutenberg and
        have never been used as input to the OCLC Classify web
        service.
        """
        join_clause = ((WorkRecord.id==CoverageRecord.work_record_id) &
                       (CoverageRecord.data_source_id==coverage_data_source.id))
        
        q = _db.query(WorkRecord).outerjoin(
            CoverageRecord, join_clause).filter(
                WorkRecord.data_source==workrecord_data_source)
        q2 = q.filter(CoverageRecord.id==None)
        return q2


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
        
    def add_contributor(self, name, roles, aliases=None, lc=None, viaf=None,
                        **kwargs):
        """Assign a contributor to this WorkRecord."""
        _db = Session.object_session(self)
        if isinstance(roles, basestring):
            roles = [roles]            

        # First find or create the Contributor.
        if isinstance(name, Contributor):
            contributor = name
        else:
            contributor, was_new = Contributor.lookup(
                _db, name, lc, viaf, aliases)
            if isinstance(contributor, list):
                # Contributor was looked up/created by name,
                # which returns a list.
                contributor = contributor[0]

        # Then add their Contributions.
        for role in roles:
            get_one_or_create(
                _db, Contribution, workrecord=self, contributor=contributor,
                role=role)
   
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
        if other_record.language == self.language:
            # The books are in the same language. Hooray!
            language_factor = 1
        else:
            if other_record.language and self.language:
                # Each record specifies a different set of languages. This
                # is an immediate disqualification.
                return 0
            else:
                # One record specifies a language and one does not. This
                # is a little tricky. We're going to apply a penalty, but
                # since the majority of records we're getting from OCLC are in
                # English, the penalty will be less if one of the
                # languages is English. It's more likely that an unlabeled
                # record is in English than that it's in some other language.
                if self.language == 'eng' or other_record.language == 'eng':
                    language_factor = 0.80
                else:
                    language_factor = 0.50
       
        title_quotient = MetadataSimilarity.title_similarity(
            self.title, other_record.title)

        author_quotient = MetadataSimilarity.author_similarity(
            self.authors, other_record.authors)
        if author_quotient == 0:
            # The two works have no authors in common. Immediate
            # disqualification.
            return 0

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

    @property
    def best_open_access_link(self):
        """Find the best open-access Resource for this LicensePool."""
        open_access = Resource.OPEN_ACCESS_DOWNLOAD

        best = None
        for l in self.primary_identifier.resources:
            if l.rel != open_access:
                continue
            if l.media_type.startswith("application/epub+zip"):
                best = l
                # A Project Gutenberg-ism: if we find a 'noimages' epub,
                # we'll keep looking in hopes of finding a better one.
                if not 'noimages' in best.href:
                    break
        return best


class Work(Base):

    __tablename__ = 'works'
    id = Column(Integer, primary_key=True)

    # One Work may have copies scattered across many LicensePools.
    license_pools = relationship("LicensePool", backref="work", lazy='joined')

    # A single Work may claim many WorkRecords.
    work_records = relationship("WorkRecord", backref="work")
    
    # A single Work may be built of many Contributions
    contributions = relationship("WorkContribution", backref="work")

    # A Work may be merged into one other Work.
    was_merged_into_id = Column(Integer, ForeignKey('works.id'), index=True)
    was_merged_into = relationship("Work", remote_side = [id])

    title = Column(Unicode)
    authors = Column(Unicode, index=True)
    language = Column(Unicode, index=True)
    summary_id = Column(Integer, ForeignKey('resources.id', use_alter=True, name='fk_works_summary_id'), index=True)
    audience = Column(Unicode, index=True)
    subjects = Column(MutableDict.as_mutable(JSON), default={})

    cover_id = Column(Integer, ForeignKey('resources.id', use_alter=True, name='fk_works_cover_id'), index=True)
    lane = Column(Unicode, index=True)
    quality = Column(Float, index=True)

    def __repr__(self):
        return ('%s "%s" (%s) %s %s (%s wr, %s lp)' % (
            self.id, self.title, self.authors, self.lane, self.language,
            len(self.work_records), len(self.license_pools))).encode("utf8")

    @classmethod
    def search(cls, _db, query, languages, lane=None):
        """Find works that match a search query.
        
        TODO: Current implementation is incredibly bad and does
        a direct database search using ILIKE.
        """
        if isinstance(lane, Lane):
            lane = lane.name

        if isisntance(languages, basestring):
            languages = [languages]

        k = "%" + query + "%"
        q = _db.query(Work).filter(
            Work.language.in_(languages),
            or_(Work.title.ilike(k),
                Work.authors.ilike(k)))
        
        if lane:
            q = q.filter(Work.lane==lane)
        q = q.order_by(Work.quality.desc())
        return q

    @classmethod
    def quality_sample(
            cls, _db, languages, lane, quality_min_start,
            quality_min_rock_bottom, target_size):
        """Get randomly selected Works that meet minimum quality criteria.

        Bring the quality criteria as low as necessary to fill a feed
        of the given size, but not below `quality_min_rock_bottom`.
        """
        if isinstance(lane, Lane):
            lane = lane.name
        if not isinstance(languages, list):
            languages = [languages]
        quality_min = quality_min_start
        previous_quality_min = None
        results = []
        while (quality_min >= quality_min_rock_bottom
               and len(results) < target_size):
            remaining = target_size - len(results)
            # TODO: If the work has multiple languages, in_ will not work.
            query = _db.query(Work).filter(
                Work.language.in_(languages),
                Work.lane==lane,
                Work.quality >= quality_min,
                Work.was_merged_into == None,
            )
            if previous_quality_min is not None:
                query = query.filter(
                    Work.quality < previous_quality_min)
            query = query.order_by(func.random()).limit(remaining)
            results.extend(query.all())

            if quality_min == quality_min_rock_bottom:
                # We can't lower the bar any more.
                break

            # Lower the bar, in case we didn't get enough results.
            previous_quality_min = quality_min
            quality_min *= 0.5
            if quality_min < quality_min_rock_bottom:
                quality_min = quality_min_rock_bottom
        return results


    def all_workrecords(self, recursion_level=3):
        """All WorkRecords identified by a WorkIdentifier equivalent to 
        any of the primary identifiers of this Work's WorkRecords.

        `recursion_level` controls how far to go when looking for equivalent
        WorkIdentifiers.
        """
        _db = Session.object_session(self)
        primary_identifier_ids = [
            x.primary_identifier.id for x in self.work_records]
        identifier_ids = WorkIdentifier.recursively_equivalent_identifier_ids_flat(
            _db, primary_identifier_ids, recursion_level)
        q = _db.query(WorkRecord).filter(
            WorkRecord.primary_identifier_id.in_(identifier_ids))
        return q

    @property
    def language_code(self):
        """A single 2-letter language code for display purposes."""
        if not self.language:
            return None
        language = self.language
        if language in LanguageCodes.three_to_two:
            language = LanguageCodes.three_to_two[language]
        return language

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
            if record.language:
                my_languages[record.language] += 1
                total_my_languages += 1
            my_titles.append(record.title)
            for author in record.authors:
                my_authors[author] += 1
                total_my_authors += 1

        if isinstance(other_work, Work):
            other_work_records = other_work.work_records
        else:
            other_work_records = [other_work]

        for record in other_work_records:
            if record.language:
                other_languages[record.languages] += 1
                total_other_languages += 1
            other_titles.append(record.title)
            for author in record.authors:
                other_authors[author] += 1
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

        if not other_languages or not my_languages:
            language_factor = 1
        else:
            language_distance = MetadataSimilarity.counter_distance(
                my_languages, other_languages)
            language_factor = 1-language_distance
        title_quotient = 1-title_distance
        author_quotient = 1-author_distance

        return language_factor * (
            (title_quotient * 0.80) + (author_quotient * 0.20))

    def merge_into(self, target_work, similarity_threshold=0.5):
        """This Work is replaced by target_work.

        The two works must be similar to within similarity_threshold,
        or nothing will happen.

        All of this work's WorkRecords will be assigned to target_work,
        and it will be marked as merged into target_work.
        """
        _db = Session.object_session(self)
        similarity = self.similarity_to(target_work)
        if similarity < similarity_threshold:
            print "NOT MERGING %r into %r, similarity is only %.3f." % (
                self, target_work, similarity)
        else:
            print "MERGING %r into %r, similarity is %.3f." % (
                self, target_work, similarity)
            target_work.license_pools.extend(self.license_pools)
            target_work.work_records.extend(self.work_records)
            target_work.calculate_presentation()
            print "The resulting work: %r" % target_work
            self.was_merged_into = target_work
            self.license_pools = []
            self.work_records = []

    def gather_presentation_information(self):
        """Consolidate presentation information from multiple sources.

        The main sources are WorkRecord rows and OCLC Linked Data
        documents.
        """
        from integration.oclc import oclc_linked_data
        license_pools = self.license_pools

        license_pool_work_records = set(
            [p.work_record() for p in self.license_pools])

        all_work_records = set(self.all_workrecords(recursion_level=1).all())
        all_work_records = all_work_records.union(license_pool_work_records)

        subject_data = dict()
        authors = Counter()
        languages = Counter()
        image_links = Counter()

        # Go through the privileged subset of work records
        # directly associated with a license pool.
        #
        # These work records set the parameters for the
        # information we display to patrons. For instance, we will
        # look at other records to decide which title is most
        # commonly used, but we will only consider titles that are
        # associated with one license pool or another.
        #
        # Similarly, only work records associated with a license
        # pool are allowed to suggest authors or languages for the
        # work.
        usable_titles = set()
        usable_authors = set()
        usable_languages = set()
        for wr in license_pool_work_records:
            if wr.title:
                usable_titles.add(wr.title)
            for a in wr.contributors:
                usable_authors.add(a)
            if wr.language:
                usable_languages.add(wr.language)

        title_counter = Counter()
        author_counter = Counter()
        language_counter = Counter()
        # Go through all work records to see which titles, authors,
        # and languages are most common.
        for wr in all_work_records:
            if wr.title in usable_titles:
                title_counter[wr.title] += 1
            for a in wr.contributors:
                if a in usable_authors:
                    author_counter[a] += 1
            if wr.language in usable_languages:
                language_counter[wr.language] += 1

        return (title_counter, author_counter, language_counter)

    def calculate_presentation(self):

        titles, authors, languages = self.gather_presentation_information()

        if titles:
            self.title = titles.most_common(1)[0][0]
        if languages:
            self.language = languages.most_common(1)[0][0]
        if authors:
            self.authors = authors.most_common(1)[0][0].name

        # Find all related IDs that might have associated resources.
        _db = Session.object_session(self)
        primary_identifier_ids = [
            x.primary_identifier.id for x in self.work_records]
        identifier_ids = WorkIdentifier.recursively_equivalent_identifier_ids_flat(
            _db, primary_identifier_ids, 5, threshold=0.5)

        self.summary = WorkIdentifier.evaluate_summary_quality(_db, identifier_ids)
        self.cover = WorkIdentifier.evaluate_cover_quality(_db, identifier_ids)

        self.quality = len(self.license_pools) * (
            total_work_records + average_descriptions_per_work_record)

        self.subjects = subjects
        if 'audience' in self.subjects:
            self.audience = Lane.most_common(self.subjects['audience'])
        else:
            self.audience = Classification.AUDIENCE_ADULT

        self.fiction, self.lane = Lane.best_match(
            self.subjects)

        # Now that everything's calculated, print it out.
        if True:
            t = u"%s (by %s)" % (self.title, self.authors)
            print t.encode("utf8")
            print " language=%s" % self.language
            print " quality=%s" % self.quality
            print " %(lane)s f=%(fiction)s, a=%(audience)s, %(subjects)r" % (
                dict(lane=self.lane, fiction=self.fiction,
                     audience=self.audience,
                     subjects=self.subjects.get('names',{})))
            if self.description:
                d = " Description (%.2f) %s" % (
                    score, self.description)
                print d.encode("utf8")
            print

class Resource(Base):
    """An external resource that may be mirrored locally."""

    __tablename__ = 'resources'

    # Some common link relations.
    CANONICAL = "canonical"
    OPEN_ACCESS_DOWNLOAD = "http://opds-spec.org/acquisition/open-access"
    IMAGE = "http://opds-spec.org/image"
    THUMBNAIL_IMAGE = "http://opds-spec.org/image/thumbnail"
    SAMPLE = "http://opds-spec.org/acquisition/sample"
    ILLUSTRATION = "http://library-simplified.com/rel/illustration"
    REVIEW = "http://schema.org/Review"
    DESCRIPTION = "http://schema.org/description"
    AUTHOR = "http://schema.org/author"

    # TODO: Is this the appropriate relation?
    DRM_ENCRYPTED_DOWNLOAD = "http://opds-spec.org/acquisition/"

    # How many votes is the initial quality estimate worth?
    ESTIMATED_QUALITY_WEIGHT = 5

    id = Column(Integer, primary_key=True)

    # A Resource is always associated with some WorkIdentifier.
    work_identifier_id = Column(
        Integer, ForeignKey('workidentifiers.id'), index=True)

    # A Resource may also be associated with some LicensePool which
    # controls scarce access to it.
    license_pool_id = Column(
        Integer, ForeignKey('licensepools.id'), index=True)

    # Who provides this resource?
    data_source_id = Column(
        Integer, ForeignKey('datasources.id'), index=True)

    # Many works may use this resource as their cover image.
    cover_works = relationship("Work", backref="cover", foreign_keys=[Work.cover_id])

    # Many works may use this resource as their summary.
    summary_works = relationship("Work", backref="summary", foreign_keys=[Work.summary_id])

    # The relation between the book identified by the WorkIdentifier
    # and the resource.
    rel = Column(Unicode, index=True)

    # The actual URL to the resource.
    href = Column(Unicode)

    # Whether or not we have a local copy of the representation.
    mirrored = Column(Boolean, index=True)

    # The path to our mirrored representation. This can be converted
    # into a URL for serving to a client. TODO: how?
    mirrored_path = Column(Unicode)

    # The last time we tried to update the mirror.
    mirror_date = Column(DateTime, index=True)

    # The HTTP status code the last time we updated the mirror
    mirror_status = Column(Unicode)

    # A human-readable description of what happened the last time
    # we updated the mirror.
    mirror_exception = Column(Unicode)

    # Sometimes the content of a resource can just be stuck into the
    # database.
    content = Column(Unicode)

    # We need this information to determine the appropriateness of this
    # resource without neccessarily having access to the file.
    media_type = Column(Unicode, index=True)
    language = Column(Unicode, index=True)
    file_size = Column(Integer)
    image_height = Column(Integer, index=True)
    image_width = Column(Integer, index=True)

    # A calculated value for the quality of this resource, based on an
    # algorithmic treatment of its content.
    estimated_quality = Column(Float)

    # The average of human-entered values for the quality of this
    # resource.
    voted_quality = Column(Float)

    # How many votes contributed to the voted_quality value. This lets
    # us scale new votes proportionately while keeping only two piece
    # of information.
    votes_for_quality = Column(Integer)

    # A combination of the calculated quality value and the
    # human-entered quality value.
    quality = Column(Float, index=True)

    def could_not_mirror(self):
        """We tried to mirror this resource and failed."""
        if self.mirrored:
            # We already have a mirrored copy, so just leave it alone.
            return
        self.mirrored = False
        self.mirror_date = datetime.datetime.utcnow()
        self.mirrored_path = None
        self.mirror_status = 404
        self.media_type = None
        self.file_size = None
        self.image_height = None
        self.image_width = None

    def set_content(self, content, media_type):
        """Store the content directly in the database."""
        self.content = content
        self.mirrored = True
        self.mirror_status = 200
        self.media_type = media_type
        self.file_size = len(content)

    def mirrored_to(self, path, media_type, content=None):
        """We successfully mirrored this resource to disk."""
        self.mirrored = True
        self.mirrored_path = path
        self.mirror_status = 200
        self.media_type = media_type

        # If we were provided with the content, make sure the
        # metadata reflects the content.
        #
        # TODO: We don't check the actual file because it's got a
        # variable expansion in it at this point.
        if content is not None:
            self.file_size = len(content)
        if self.media_type.lower().startswith("image/"):
            # Try to load it into PIL and determine height and width.
            try:
                image = Image.open(StringIO(content))
            except IOError, e:
                self.mirror_exception = "Content is not an image."
            self.image_width, self.image_height = image.size

    def set_estimated_quality(self, estimated_quality):
        """Update the estimated quality."""
        self.estimated_quality = estimated_quality
        self.update_quality()

    def add_quality_votes(self, quality, weight=1):
        """Record someone's vote as to the quality of this resource."""
        total_quality = self.voted_quality * self.votes_for_quality
        total_quality += (quality * weight)
        self.votes_for_quality += weight
        self.voted_quality = total_quality / float(self.votes_for_quality)
        self.update_quality()

    def update_quality(self):
        """Combine `estimated_quality` with `voted_quality` to form `quality`.
        """
        estimated_weight = self.ESTIMATED_QUALITY_WEIGHT
        votes_for_quality = self.votes_for_quality or 0
        total_weight = estimated_weight + votes_for_quality

        total_quality = (((self.estimated_quality or 0) * self.ESTIMATED_QUALITY_WEIGHT) + 
                         ((self.voted_quality or 0) * votes_for_quality))
        self.quality = total_quality / float(total_weight)

class WorkFeed(object):

    """Identify a certain page in a certain feed."""

    def __init__(self, languages, lane, order_by):
        if isinstance(lane, type) and issubclass(lane, Lane):
            self.lane = lane.name
        else:
            self.lane = lane
        if isinstance(languages, basestring):
            languages = [languages]
        self.languages = languages
        if not isinstance(order_by, list):
            order_by = [order_by]
        self.order_by = order_by
        # In addition to the given order, we order by author,
        # then title, then work ID.
        for i in (Work.authors, Work.title, Work.id):
            if i not in self.order_by:
                self.order_by.append(i)

    def page_query(self, _db, last_work_seen, page_size):
        """A page of works."""

        query = _db.query(Work).filter(
            Work.languages.in_(self.languages),
            Work.lane==self.lane,
            Work.was_merged_into == None,
        )

        if last_work_seen:
            # Only find works that show up after the last work seen.
            primary_order_field = self.order_by[0]
            last_value = getattr(last_work_seen, primary_order_field.name)

            # This means works where the primary ordering field has a
            # higher value.
            clause = (primary_order_field > last_value)

            base_and_clause = (primary_order_field == last_value)
            for next_order_field in self.order_by[1:]:
                # OR, it means works where all the previous ordering
                # fields have the same value as the last work seen,
                # and this next ordering field has a higher value.
                new_value = getattr(last_work_seen, next_order_field.name)
                clause = or_(clause,
                             and_(base_and_clause, 
                                  (next_order_field > new_value)))
                base_and_clause = and_(base_and_clause,
                                       (next_order_field == new_value))
            query = query.filter(clause)

        return query.order_by(*self.order_by).limit(page_size)

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

    # One LicensePool can have many Loans.
    loans = relationship('Loan', backref='license_pool')

    # One LicensePool can have many CirculationEvents
    circulation_events = relationship(
        "CirculationEvent", backref="license_pool")

    # One LicensePool can control access to many Resources.
    resources = relationship("Resource", backref="license_pool")

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

    def work_record(self):
        """The LicencePool's primary WorkRecord.

        This is (our view of) the book's entry on whatever website
        hosts the licenses.
        """
        _db = Session.object_session(self)
        return _db.query(WorkRecord).filter_by(
            data_source=self.data_source,
            primary_identifier=self.identifier).one()

    @classmethod
    def with_no_work(cls, _db):
        """Find LicensePools that have no corresponding Work."""
        return _db.query(LicensePool).outerjoin(Work).filter(
            Work.id==None).all()

    def add_resource(self, rel, href, data_source, media_type=None,
                     content=None):
        """Associate a Resource with this LicensePool.

        `rel`: The relationship between a LicensePool and the resource
               on the other end of the link.
        `media_type`: Media type of the representation available at the
                      other end of the link.
        """
        return self.identifier.add_resource(
            rel, href, data_source, self, media_type, content)

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

    def loan_to(self, patron):
        _db = Session.object_session(patron)
        kwargs = dict(start=datetime.datetime.utcnow())
        return get_one_or_create(
            _db, Loan, patron=patron, license_pool=self, 
            create_method_kwargs=kwargs)

    @classmethod
    def consolidate_works(cls, _db):
        """Assign a (possibly new) Work to every unassigned LicensePool."""
        a = 0
        for unassigned in cls.with_no_work(_db):
            etext, new = unassigned.calculate_work()
            a += 1
            print "Created %r" % etext
            if a and not a % 100:
                _db.commit()

    def potential_works(self, similarity_threshold=0.8):
        """Find all existing works that have claimed this pool's 
        work records.

        :return: A 3-tuple ({Work: [WorkRecord]}, [WorkRecord])
        Element 0 is a mapping of Works to the WorkRecords they've claimed.
        Element 1 is a list of WorkRecords that are unclaimed by any Work.
        """
        _db = Session.object_session(self)
        primary_work_record = self.work_record()

        claimed_records_by_work = defaultdict(list)
        unclaimed_records = []

        # Find all work records connected to this LicensePool's
        # primary work record.
        equivalent_work_records = primary_work_record.equivalent_work_records()

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

    def calculate_work(self, record_similarity_threshold=0.2,
                       work_similarity_threshold=0.2):
        """Find or create a Work for this LicensePool."""
        primary_work_record = self.work_record()
        self.language = primary_work_record.language
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
            record_similarity_threshold)
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
        more_popular_choices = [
            (work, len(records))
            for work, records in claimed.items()
            if len(records) > len(unclaimed)
            and work.language
            and work.language == self.language
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
                less_popular.merge_into(work, work_similarity_threshold)
            created = False
        else:
            # There is no better choice than creating a brand new Work.
            # print "NEW WORK for %r" % primary_work_record.title
            work = Work()
            _db = Session.object_session(self)
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
        # work.calculate_presentation()
        #if created:
        #    print "Created %r" % work
        # All done!
        return work, created

    @property
    def best_license_link(self):
        """Find the best available licensing link for the work associated
        with this LicensePool.
        """
        wr = self.work_record()
        link = wr.best_open_access_link
        if link:
            return self, link

        # Either this work is not open-access, or there was no epub
        # link associated with it.
        work = self.work
        for pool in work.license_pools:
            wr = pool.work_record()
            link = wr.best_open_access_link
            if link:
                return pool, link
        return self, None


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
    timestamp = Column(DateTime)

    @classmethod
    def stamp(self, _db, service):
        now = datetime.datetime.utcnow()
        stamp, was_new = get_one_or_create(
            _db, Timestamp,
            service=service,
            create_method_kwargs=dict(timestamp=now))
        if not was_new:
            stamp.timestamp = now
        return stamp

class CoverageProvider(object):

    """Run WorkRecords from one DataSource (the input DataSource) through
    code associated with another DataSource (the output
    DataSource). If the code returns success, add a CoverageRecord for
    the WorkRecord and the output DataSource, so that the record
    doesn't get processed next time.
    """

    def __init__(self, service_name, input_source, output_source,
                 workset_size=100):
        self._db = Session.object_session(input_source)
        self.service_name = service_name
        self.input_source = input_source
        self.output_source = output_source
        self.workset_size = workset_size

    @property
    def workrecords_that_need_coverage(self):
        return WorkRecord.missing_coverage_from(
            self._db, self.input_source, self.output_source)

    def run(self):
        remaining = True
        failures = set([])
        while remaining:
            successes = 0
            if len(failures) >= self.workset_size:
                raise Exception(
                    "Number of failures equals workset size, cannot continue.")
            workset = self.workrecords_that_need_coverage.limit(
                self.workset_size)
            remaining = False
            for record in workset:
                if record in failures:
                    continue
                remaining = True
                if self.process_work_record(record):
                    # Success! Now there's coverage! Add a CoverageRecord.
                    successes += 1
                    get_one_or_create(
                        self._db, CoverageRecord,
                        work_record=record,
                        data_source=self.output_source,
                        create_method_kwargs = dict(date=datetime.datetime.utcnow()))
                else:
                    failures.add(record)
            # Commit this workset before moving on to the next one.
            print "Workset processed with %d successes, %d failures." % (
                successes, len(failures))
            self._db.commit()

        # Now that we're done, update the timestamp
        Timestamp.stamp(self._db, self.service_name)
        self._db.commit()

    def process_work_record(self, work_record):
        raise NotImplementedError()


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
