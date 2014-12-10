# encoding: utf-8
import json
import os

from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)

from . import DatabaseTest
from ..core.model import (
    DataSource,
    Identifier,
    Measurement,
    Resource,
)
from ..integration.overdrive import (
    OverdriveAPI,
    OverdriveBibliographicMonitor,
)

class TestOverdrive(DatabaseTest):

    def setup(self):
        super(TestOverdrive, self).setup()
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "overdrive")

    def sample_json(self, filename):
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)

    def test_annotate_edition_with_bibliographic_information(self):

        wr, new = self._edition(with_license_pool=True)
        raw, info = self.sample_json("overdrive_metadata.json")

        input_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        OverdriveBibliographicMonitor.annotate_edition_with_bibliographic_information(
            self._db, wr, info, input_source)

        # Basic bibliographic info.
        eq_("Agile Documentation", wr.title)
        eq_("A Pattern Guide to Producing Lightweight Documents for Software Projects", wr.subtitle)
        eq_("Wiley Software Patterns", wr.series)
        eq_("eng", wr.language)
        eq_("Wiley", wr.publisher)
        eq_("John Wiley & Sons, Inc.", wr.imprint)
        eq_(2005, wr.published.year)
        eq_(1, wr.published.month)
        eq_(31, wr.published.day)

        # Author stuff
        author = wr.author_contributors[0]
        eq_(u"Rüping, Andreas", author.name)
        eq_("Andreas R&#252;ping", author.display_name)
        eq_(set(["Computer Technology", "Nonfiction"]),
            set([c.subject.identifier
                 for c in wr.primary_identifier.classifications]))

        # Related IDs.
        equivalents = [x.output for x in wr.primary_identifier.equivalencies]
        ids = [(x.type, x.identifier) for x in equivalents]
        eq_([("ASIN", "B000VI88N2"), ("ISBN", "9780470856246")],
            sorted(ids))

        # Associated resources.
        resources = wr.primary_identifier.resources
        eq_(3, len(resources))
        long_description = [
            x for x in resources if x.rel==Resource.DESCRIPTION
            and x.href=="tag:full"
        ][0]
        assert long_description.content.startswith("<p>Software documentation")

        short_description = [
            x for x in resources if x.rel==Resource.DESCRIPTION
            and x.href=="tag:short"
        ][0]
        assert short_description.content.startswith("<p>Software documentation")
        assert len(short_description.content) < len(long_description.content)

        image = [x for x in resources if x.rel==Resource.IMAGE][0]
        eq_('http://images.contentreserve.com/ImageType-100/0128-1/%7B3896665D-9D81-4CAC-BD43-FFC5066DE1F5%7DImg100.jpg', image.href)

        measurements = wr.primary_identifier.measurements
        popularity = [x for x in measurements
                      if x.quantity_measured==Measurement.POPULARITY][0]
        eq_(2, popularity.value)

        rating = [x for x in measurements
                  if x.quantity_measured==Measurement.RATING][0]
        eq_(1, rating.value)

        # Un-schematized metadata.

        eq_("eBook", wr.extra['medium'])
        eq_("Agile Documentation A Pattern Guide to Producing Lightweight Documents for Software Projects", wr.sort_title)


    def test_annotate_edition_with_sample(self):
        wr, new = self._edition(with_license_pool=True)
        raw, info = self.sample_json("has_sample.json")

        input_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        OverdriveBibliographicMonitor.annotate_edition_with_bibliographic_information(
            self._db, wr, info, input_source)
        
        i = wr.primary_identifier
        [sample] = [x for x in i.resources if x.rel == Resource.SAMPLE]
        eq_("application/epub+zip", sample.media_type)
        eq_("http://excerpts.contentreserve.com/FormatType-410/1071-1/9BD/24F/82/BridesofConvenienceBundle9781426803697.epub", sample.href)
        eq_(820171, sample.file_size)

    def test_annotate_edition_with_awards(self):
        wr, new = self._edition(with_license_pool=True)
        raw, info = self.sample_json("has_awards.json")

        input_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        OverdriveBibliographicMonitor.annotate_edition_with_bibliographic_information(
            self._db, wr, info, input_source)
        eq_(wr.extra['awards'], [{"source":"The New York Times","value":"The New York Times Best Seller List"}])
