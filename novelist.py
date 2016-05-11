import json
import logging
import requests
import string
import urllib
from nose.tools import set_trace

from core.coverage import (
    CoverageFailure,
    CoverageProvider,
)
from core.metadata_layer import (
    ContributorData,
    IdentifierData,
    LinkData,
    MeasurementData,
    Metadata,
    SubjectData,
)
from core.model import (
    DataSource,
    Hyperlink,
    Identifier,
    Measurement,
    Representation,
    Subject,
)
from core.config import Configuration

class NoveListAPI(object):

    log = logging.getLogger("NoveList API")
    version = "2.2"

    # While the NoveList API doesn't require parameters to be passed via URL,
    # the Representation object needs a unique URL to return the proper data
    # from the database.
    QUERY_ENDPOINT = "http://novselect.ebscohost.com/Data/ContentByQuery?\
            ISBN=%(ISBN)s&ClientIdentifier=%(ClientIdentifier)s&version=%(version)s"
    MAX_REPRESENTATION_AGE = 6*30*24*60*60      # six months

    @classmethod
    def from_config(cls, _db):
        config = Configuration.integration(Configuration.NOVELIST_INTEGRATION)
        profile = config.get(Configuration.NOVELIST_PROFILE)
        password = config.get(Configuration.NOVELIST_PASSWORD)
        if not (profile and password):
            raise ValueError("No NoveList client configured.")
        return cls(_db, profile, password)

    def __init__(self, _db, profile, password):
        self._db = _db
        self.profile = profile
        self.password = password
        self.source = DataSource.lookup(self._db, DataSource.NOVELIST)

    def lookup_equivalent_isbns(self, identifier):
        """Finds NoveList data for all ISBNs equivalent to an identifier.

        :return: a list of Metadata objects
        """

        license_source = DataSource.license_source_for(self._db, identifier)
        # Look up strong ISBN equivalents.
        lookup_metadata =  [self.lookup(eq.output)
                for eq in identifier.equivalencies
                if (eq.data_source==source and eq.strength==1
                    and eq.output.type==Identifier.ISBN)]

        if not lookup_metadata:
            self.log.error("Identifiers without an ISBN equivalent can't \
                    be looked up with NoveList: %r" % identifier)
            return None

        return [metadata for metadata in lookup_metadata if metadata]

    def lookup(self, identifier):
        """Requests NoveList metadata for a particular identifier

        :return: None, a Metadata object, or a list of Metadata objects
        """

        client_identifier = identifier.urn
        if identifier.type != Identifier.ISBN:
            return self.lookup_equivalent_isbns(identifier)

        params = dict(
            ClientIdentifier=client_identifier, ISBN=identifier.identifier,
            version=self.version, profile=self.profile, password=self.password
        )
        url = self._build_query(params)
        self.log.debug("NoveList lookup: %s", url)
        representation, from_cache = Representation.cacheable_post(
            self._db, unicode(url), params, max_age=self.MAX_REPRESENTATION_AGE
        )

        # Confirm that the representation was successful.
        if representation.status_code == 403:
            self._db.delete(representation)
            raise Exception("Invalid NoveList credentials")
        if representation.content.startswith('"Missing'):
            error = representation.content
            self._db.delete(representation)
            raise Exception("Invalid NoveList parameters: %s" % error)

        return self.lookup_info_to_metadata(representation.content)

    @classmethod
    def _build_query(cls, params):
        """Builds a unique and url-encoded query endpoint"""

        for name, value in params.items():
            params[name] = urllib.quote(value)
        return (cls.QUERY_ENDPOINT % params).replace(" ", "")

    def lookup_info_to_metadata(self, lookup_info):
        """Turns a NoveList JSON response into a Metadata object"""

        lookup_info = json.loads(lookup_info)
        book_info = lookup_info['TitleInfo']
        if book_info:
            novelist_identifier = book_info.get('ui')
        if not book_info or not novelist_identifier:
            # NoveList didn't know the ISBN. Delete the cache and return None.
            client_identifier = lookup_info['ClientIdentifier']
            cached = self._db.query(Representation).\
                    filter(Representation.url.like(
                        "%ClientIdentifier="+client_identifier+"%"
                    ))
            for representation in cached.all():
                self.log.info("Deleting cache: %s" % representation.url)
                self._db.delete(representation)
            return None

        primary_identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.NOVELIST_ID, novelist_identifier
        )
        metadata = Metadata(self.source, primary_identifier=primary_identifier)

        # Get the equivalent ISBN identifiers.
        synonymous_ids = book_info.get('manifestations')
        for synonymous_id in synonymous_ids:
            isbn = synonymous_id.get('ISBN')
            if isbn and isbn != primary_identifier.identifier:
                isbn_data = IdentifierData(Identifier.ISBN, isbn)
                metadata.identifiers.append(isbn_data)

        metadata.title = book_info.get('main_title')
        metadata.subtitle = self._subtitle(metadata.title, book_info.get('full_title'))

        author = book_info.get('author')
        if author:
            metadata.contributors.append(ContributorData(sort_name=author))

        description = book_info.get('description')
        if description:
            metadata.links.append(LinkData(
                rel=Hyperlink.DESCRIPTION, content=description,
                media_type=Representation.TEXT_PLAIN
            ))

        audience_level = book_info.get('audience_level')
        if audience_level:
            metadata.subjects.append(SubjectData(
                Subject.FREEFORM_AUDIENCE, audience_level
            ))

        novelist_rating = book_info.get('rating')
        if novelist_rating:
            metadata.measurements.append(MeasurementData(
                Measurement.RATING, novelist_rating
            ))

        # Extract feature content if it is available.
        lexile_info = series_info = goodreads_info = appeals_info = None
        feature_content = lookup_info.get('FeatureContent')
        if feature_content:
            lexile_info = feature_content.get('LexileInfo')
            series_info = feature_content.get('SeriesInfo')
            goodreads_info = feature_content.get('GoodReads')
            appeals_info = feature_content.get('Appeals')

        if series_info:
            metadata.series = series_info['full_title']
            series_titles = series_info.get('series_titles')
            if series_titles:
                [series_volume] = [volume for volume in series_titles
                        if volume.get('full_title')==book_info.get('full_title')]
                series_position = series_volume.get('volume')
                if series_position:
                    if series_position.endswith('.'):
                        series_position = series_position[:-1]
                    metadata.series_position = int(series_position)

        if appeals_info:
            extracted_genres = False
            for appeal in appeals_info:
                genres = appeal.get('genres')
                if genres:
                    for genre in genres:
                        metadata.subjects.append(SubjectData(
                            Subject.TAG, genre['Name']
                        ))
                        extracted_genres = True
                if extracted_genres:
                    break

        if lexile_info:
            metadata.subjects.append(SubjectData(
                Subject.LEXILE_SCORE, lexile_info['Lexile']
            ))

        if goodreads_info:
            metadata.measurements.append(MeasurementData(
                Measurement.RATING, goodreads_info['average_rating']
            ))

        # If nothing interesting comes from the API, ignore it.
        if not (metadata.measurements or metadata.series_position or
                metadata.series or metadata.subjects or metadata.links or
                metadata.subtitle):
            return None

        return metadata

    @classmethod
    def _subtitle(cls, main_title, subtitled_title):
        """Determines whether a subtitle is present and returns it or None"""
        if not subtitled_title:
            return None
        subtitle = subtitled_title.replace(main_title, '')
        while (subtitle and
                (subtitle[0] in string.whitespace+':.')):
            # Trim any leading whitespace or punctuation
            subtitle = subtitle[1:]
        if not subtitle:
            # The main title and the full title were the same.
            return None
        return subtitle


class NoveListCoverageProvider(CoverageProvider):

    def __init__(self, _db, cutoff_time=None):
        self._db = _db
        self.api = NoveListAPI.from_config(self._db)
        self.output_source = DataSource.lookup(self._db, DataSource.NOVELIST)

        super(NoveListCoverageProvider, self).__init__(
            "NoveList Coverage Provider", [Identifier.ISBN],
            self.output_source, workset_size=25
        )

    def process_item(self, identifier):

        novelist_metadata = self.api.lookup(identifier)
        if not novelist_metadata:
            # Either NoveList didn't recognize the identifier or
            # no interesting data came of this. Consider it covered.
            return identifier

        # The metadata returned may be a single object or a list.
        # If it's a list, all of the metadata objects should have the same
        # NoveList identifier.
        if isinstance(novelist_metadata, list):
            if not self._confirm_same_identifier(novelist_metadata):
                return CoverageFailure(
                    self, identifier,
                    "Equivalents returned different NoveList records",
                    transient=True
                )
            # Metadata with the same NoveList id will be identical. Take one.
            novelist_metadata = novelist_metadata[0]

        # Set identifier equivalent to its NoveList ID.
        identifier.equivalent_to(
            self.output_source, novelist_metadata.primary_identifier,
            strength=1
        )

        edition, ignore = novelist_metadata.edition(self._db)
        novelist_metadata.apply(edition)
        return identifier

    def _confirm_same_identifier(self, metadata_objects):
        """Ensures that all metadata objects have the same NoveList ID"""

        novelist_ids = set([metadata.primary_identifier.identifier
                for metadata in metadata_objects])
        return len(novelist_ids)==1
