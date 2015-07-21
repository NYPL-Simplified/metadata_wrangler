import datetime
import isbnlib
import json
from nose.tools import set_trace

from core.overdrive import (
    OverdriveAPI
)

from mirror import (
    CoverImageMirror
)
from core.coverage import (
    CoverageProvider,
)

from core.coverage import CoverageProvider
from core.model import (
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    Measurement,
    Representation,
    Subject,
)
from core.monitor import Monitor
from core.util import LanguageCodes

class OverdriveBibliographicMonitor(CoverageProvider):
    """Fill in bibliographic metadata for Overdrive records."""

    def __init__(self, _db):
        self._db = _db
        self.overdrive = OverdriveAPI(self._db)
        self.input_source = DataSource.lookup(_db, DataSource.OVERDRIVE)
        self.output_source = DataSource.lookup(_db, DataSource.OVERDRIVE)
        super(OverdriveBibliographicMonitor, self).__init__(
            "Overdrive Bibliographic Monitor",
            self.input_source, self.output_source)

    @classmethod
    def _add_value_as_resource(cls, input_source, identifier, pool, rel, value,
                               media_type="text/plain", url=None):
        if isinstance(value, str):
            value = value.decode("utf8")
        elif isinstance(value, unicode):
            pass
        else:
            value = str(value)
        pool.add_link(rel, url, input_source, media_type, value)

    @classmethod
    def _add_value_as_measurement(
            cls, input_source, identifier, quantity_measured, value):
        if isinstance(value, str):
            value = value.decode("utf8")
        elif isinstance(value, unicode):
            pass

        value = float(value)
        identifier.add_measurement(
            input_source, quantity_measured, value)

    DATE_FORMAT = "%Y-%m-%d"

    def process_edition(self, edition):
        identifier = edition.primary_identifier
        info = self.overdrive.metadata_lookup(identifier)
        if info.get('errorCode') == 'NotFound':
            # TODO: We need to represent some kind of permanent failure.
            raise Exception("ID not recognized by Overdrive")
        return self.annotate_edition_with_bibliographic_information(
            self._db, edition, info, self.input_source
        )

    media_type_for_overdrive_type = {
        "ebook-pdf-adobe" : "application/pdf",
        "ebook-pdf-open" : "application/pdf",
        "ebook-epub-adobe" : "application/epub+zip",
        "ebook-epub-open" : "application/epub+zip",
    }
        
    @classmethod
    def annotate_edition_with_bibliographic_information(
            cls, _db, wr, info, input_source):

        identifier = wr.primary_identifier
        license_pool = wr.license_pool

        # First get the easy stuff.
        wr.title = info.get('title', None) or wr.title
        wr.subtitle = info.get('subtitle', None)
        wr.series = info.get('series', None)
        wr.publisher = info.get('publisher', None)
        wr.imprint = info.get('imprint', None)

        if 'publishDate' in info:
            wr.published = datetime.datetime.strptime(
                info['publishDate'][:10], cls.DATE_FORMAT)

        languages = [
            LanguageCodes.two_to_three.get(l['code'], l['code'])
            for l in info.get('languages', [])
        ]
        if 'eng' in languages or not languages:
            wr.language = 'eng'
        else:
            wr.language = sorted(languages)[0]

        # TODO: Is there a Gutenberg book with this title and the same
        # author names? If so, they're the same. Merge the work and
        # reuse the Contributor objects.
        #
        # Or, later might be the time to do that stuff.

        for creator in info.get('creators', []):
            name = creator['fileAs']
            display_name = creator['name']
            role = creator['role']
            contributor = wr.add_contributor(name, role)
            contributor.display_name = display_name
            if 'bioText' in creator:
                contributor.extra = dict(description=creator['bioText'])

        for i in info.get('subjects', []):
            c = identifier.classify(
                input_source, Subject.OVERDRIVE, i['value'],
                weight=100
            )

        wr.sort_title = info.get('sortTitle')
        extra = dict()
        if 'grade_levels' in info:
            for i in info['grade_levels']:
                identifier.classify(
                    input_source, Subject.GRADE_LEVEL, i['value'], weight=100)

        if 'ATOS' in info:
            identifier.classify(
                input_source, Subject.ATOS_SCORE, str(info['ATOS']), weight=100)

        if 'lexileScore' in info:
            identifier.classify(
                input_source, Subject.LEXILE_SCORE, str(info['lexileScore']),
                weight=100)

        if 'interestLevel' in info:
            identifier.classify(
                input_source, Subject.INTEREST_LEVEL, info['interestLevel'],
                weight=100)

        if 'awards' in info:
            extra['awards'] = info.get('awards', [])
            num_awards = len(extra['awards'])
            identifier.add_measurement(
                input_source, Measurement.AWARDS, str(num_awards))

        wr.extra = extra

        # Associate the Overdrive Edition with other identifiers
        # such as ISBN.
        medium = Edition.BOOK_MEDIUM
        for format in info.get('formats', []):
            if format['id'].startswith('audiobook-'):
                medium = Edition.AUDIO_MEDIUM
            elif format['id'].startswith('video-'):
                medium = Edition.VIDEO_MEDIUM
            elif format['id'].startswith('ebook-'):
                medium = Edition.BOOK_MEDIUM
            elif format['id'].startswith('music-'):
                medium = Edition.MUSIC_MEDIUM
            else:
                print format['id']
                set_trace()
            for new_id in format.get('identifiers', []):
                t = new_id['type']
                v = new_id['value']
                type_key = None
                if t == 'ASIN':
                    type_key = Identifier.ASIN
                elif t == 'ISBN':
                    type_key = Identifier.ISBN
                    if len(v) == 10:
                        v = isbnlib.to_isbn13(v)
                elif t == 'DOI':
                    type_key = Identifier.DOI
                elif t == 'UPC':
                    type_key = Identifier.UPC
                elif t == 'PublisherCatalogNumber':
                    continue
                if type_key:
                    new_identifier, ignore = Identifier.for_foreign_id(
                        _db, type_key, v)
                    identifier.equivalent_to(
                        input_source, new_identifier, 1)

            # Samples become resources.
            if 'samples' in format:
                if format['id'] == 'ebook-overdrive':
                    # Useless to us.
                    continue
                media_type = cls.media_type_for_overdrive_type.get(
                    format['id'])
                for sample_info in format['samples']:
                    href = sample_info['url']
                    resource, new = license_pool.add_link(
                        Hyperlink.SAMPLE, href, input_source,
                        media_type)

        # Add resources: cover and descriptions

        wr.medium = medium
        def _p(s):
            if not s:
                return 'None'
            if isinstance(s, unicode):
                return s.encode("utf8")
            return s
        
        if medium == Edition.BOOK_MEDIUM:
            print _p(medium), _p(wr.title), _p(wr.author)
        if 'images' in info and 'cover' in info['images']:
            link = info['images']['cover']
            href = OverdriveAPI.make_link_safe(link['href'])
            media_type = link['type']
            license_pool.add_link(Hyperlink.IMAGE, href, input_source, media_type)

        short = info.get('shortDescription')
        full = info.get('fullDescription')

        if full:
            cls._add_value_as_resource(
                input_source, identifier, license_pool,
                Hyperlink.DESCRIPTION, full, "text/html")

        if short and short != full and (not full or not full.startswith(short)):
            cls._add_value_as_resource(
                input_source, identifier, license_pool,
                Hyperlink.SHORT_DESCRIPTION, short, "text/html")

        # Add measurements: rating and popularity
        if info.get('starRating') is not None and info['starRating'] > 0:
            cls._add_value_as_measurement(
                input_source, identifier, Measurement.RATING,
                info['starRating'])

        if info['popularity']:
            cls._add_value_as_measurement(
                input_source, identifier, Measurement.POPULARITY,
                info['popularity'])

        return True


class OverdriveCoverImageMirror(CoverImageMirror):
    """Downloads images from Overdrive and writes them to disk."""

    DATA_SOURCE = DataSource.OVERDRIVE
