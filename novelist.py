import json
import logging
import requests
import string
import urllib
from nose.tools import set_trace

from core.coverage import CoverageProvider
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

    def lookup(self, identifier):
        """Returns NoveList metadata for a particular identifier"""

        client_identifier = identifier.urn
        if identifier.type != Identifier.ISBN:
            for equivalency in identifier.equivalencies:
                if (equivalency.strength >= 0.7 and
                    equivalency.output.type == Identifier.ISBN):
                    identifier = equivalency.output
                    break
            else:
                self.log.error("Identifiers without an ISBN equivalent can't \
                    be looked up with NoveList: %r" % identifier)
                return None

        params = dict(
            ClientIdentifier=client_identifier, ISBN=identifier.identifier,
            version=self.version, profile=self.profile, password=self.password
        )
        url = self._build_query(params)
        representation, from_cache = Representation.cacheable_post(
            self._db, unicode(url), params, max_age=self.MAX_REPRESENTATION_AGE
        )
        if representation.status_code == 403:
            self._db.delete(representation)
            raise Exception("Invalid NoveList credentials")

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
        
        urn = urllib.unquote(lookup_info['ClientIdentifier'])
        primary_identifier, ignore = Identifier.parse_urn(self._db, urn)
        metadata = Metadata(
            self._db, self.source, primary_identifier=primary_identifier
        )

        # Get the equivalent ISBN identifiers.
        book_info = lookup_info['TitleInfo']
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

        return metadata

    @classmethod
    def _subtitle(cls, main_title, subtitled_title):
        """Determines whether a subtitle is present and returns it or None"""
        if not subtitled_title:
            return None
        subtitle = subtitled_title.replace(main_title, '')
        while (subtitle and
                (subtitle[0] in string.whitespace or subtitle[0]==":")):
            # Trim any leading whitespace or colons
            subtitle = subtitle[1:]
        if not subtitle:
            # The main title and the full title were the same.
            return None
        return subtitle
