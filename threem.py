from nose.tools import set_trace

import datetime
from lxml import etree

from mirror import CoverImageMirror
from core.coverage import CoverageProvider

from core.model import (
    Contributor,
    DataSource,
    Edition,
    Identifier,
    Resource,
)
from core.coverage import CoverageProvider
from core.monitor import Monitor
from core.util.xmlparser import XMLParser
from core.threem import ThreeMAPI as BaseThreeMAPI
from core.util import LanguageCodes

class ThreeMAPI(BaseThreeMAPI):

    def __init__(self, *args, **kwargs):
        super(ThreeMAPI, self).__init__(*args, **kwargs)
        self.item_list_parser = ItemListParser()

    def get_bibliographic_info_for(self, editions):
        results = dict()
        identifiers = []
        edition_for_identifier = dict()
        for edition in editions:
            identifier = edition.primary_identifier
            identifiers.append(identifier)
            edition_for_identifier[identifier] = edition
            data = self.request("/items/%s" % identifier.identifier)
            identifier, raw, cooked = list(self.item_list_parser.parse(data))[0]
            results[identifier] = (edition, cooked)

        return results
      

class ItemListParser(XMLParser):

    DATE_FORMAT = "%Y-%m-%d"
    YEAR_FORMAT = "%Y"

    NAMESPACES = {}

    def parse(self, xml):
        for i in self.process_all(xml, "//Item"):
            yield i

    @classmethod
    def author_names_from_string(cls, string):
        if not string:
            return
        for author in string.split(";"):
            yield author.strip()

    def process_one(self, tag, namespaces):
        def value(threem_key):
            return self.text_of_optional_subtag(tag, threem_key)
        resources = dict()
        identifiers = dict()
        item = { Resource : resources,  Identifier: identifiers,
                 "extra": {} }

        identifiers[Identifier.THREEM_ID] = value("ItemId")
        identifiers[Identifier.ISBN] = value("ISBN13")

        item[Edition.title] = value("Title")
        item[Edition.subtitle] = value("SubTitle")
        item[Edition.publisher] = value("Publisher")
        language = value("Language")
        language = LanguageCodes.two_to_three.get(language, language)
        item[Edition.language] = language

        author_string = value('Authors')
        item[Contributor] = list(self.author_names_from_string(author_string))

        published_date = None
        published = value("PubDate")
        formats = [self.DATE_FORMAT, self.YEAR_FORMAT]
        if not published:
            published = value("PubYear")
            formats = [self.YEAR_FORMAT]

        for format in formats:
            try:
                published_date = datetime.datetime.strptime(published, format)
            except ValueError, e:
                pass

        item[Edition.published] = published_date

        resources[Resource.DESCRIPTION] = value("Description")
        resources[Resource.IMAGE] = value("CoverLinkURL").replace("&amp;", "&")
        resources["alternate"] = value("BookLinkURL").replace("&amp;", "&")

        item['extra']['fileSize'] = value("Size")
        item['extra']['numberOfPages'] = value("NumberOfPages")

        return identifiers[Identifier.THREEM_ID], etree.tostring(tag), item


class ThreeMBibliographicMonitor(CoverageProvider):
    """Fill in bibliographic metadata for 3M records."""

    def __init__(self, _db,
                 account_id=None, library_id=None, account_key=None,
                 batch_size=1):
        self._db = _db
        self.api = ThreeMAPI(_db, account_id, library_id, account_key)
        self.input_source = DataSource.lookup(_db, DataSource.THREEM)
        self.output_source = DataSource.lookup(_db, DataSource.THREEM)
        super(ThreeMBibliographicMonitor, self).__init__(
            "3M Bibliographic Monitor",
            self.input_source, self.output_source)
        self.current_batch = []
        self.batch_size=batch_size

    def process_edition(self, edition):
        self.current_batch.append(edition)
        if len(self.current_batch) >= self.batch_size:
            self.process_batch(self.current_batch)
            self.current_batch = []
        return True

    def commit_workset(self):
        # Process any uncompleted batch.
        self.process_batch(self.current_batch)
        super(ThreeMBibliographicMonitor, self).commit_workset()

    def process_batch(self, batch):
        for edition, info in self.api.get_bibliographic_info_for(
                batch).values():
            self.annotate_edition_with_bibliographic_information(
                self._db, edition, info, self.input_source
            )
            print edition

    def annotate_edition_with_bibliographic_information(
            self, db, edition, info, input_source):

        # ISBN and 3M ID were associated with the work record earlier,
        # so don't bother doing it again.

        pool = edition.license_pool
        identifier = edition.primary_identifier

        edition.title = info[Edition.title]
        edition.subtitle = info[Edition.subtitle]
        edition.publisher = info[Edition.publisher]
        edition.language = info[Edition.language]
        edition.published = info[Edition.published]

        for name in info[Contributor]:
            edition.add_contributor(name, Contributor.AUTHOR_ROLE)

        edition.extra = info['extra']

        # Associate resources with the work record.
        for rel, value in info[Resource].items():
            if rel == Resource.DESCRIPTION:
                href = None
                media_type = "text/html"
                content = value
            else:
                href = value
                media_type = None
                content = None
            identifier.add_resource(rel, href, input_source, pool, media_type, content)


class ThreeMCoverImageMirror(CoverImageMirror):
    """Downloads images from 3M and writes them to disk."""

    ORIGINAL_PATH_VARIABLE = "original_threem_covers_mirror"
    SCALED_PATH_VARIABLE = "scaled_threem_covers_mirror"
    DATA_SOURCE = DataSource.THREEM

    def filename_for(self, resource):
        return resource.identifier.identifier + ".jpg"
