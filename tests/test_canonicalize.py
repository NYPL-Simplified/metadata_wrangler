import logging

from nose.tools import set_trace, eq_

from . import (
    DatabaseTest,
    DummyHTTPClient,
    sample_data,
)

from core.metadata_layer import ContributorData

from test_viaf import MockVIAFClientLookup

from canonicalize import (
    AuthorNameCanonicalizer, 
)



class TestAuthorNameCanonicalizer(DatabaseTest):

    def setup(self):
        super(TestAuthorNameCanonicalizer, self).setup()
        self.log = logging.getLogger("Author Name Canonicalizer Test")
        self.canonicalizer = AuthorNameCanonicalizer(self._db)
        self.viaf_client = MockVIAFClientLookup(self._db, self.log)
        self.canonicalizer.viaf = self.viaf_client
        #self.oclc_client = MockOCLCLinkedData()
        #self.canonicalizer.oclcld = self.oclc_client


    def sample_data(self, filename):
        return sample_data(filename=filename, sample_data_dir="viaf")


    def queue_file_in_mock_http(self, filename):
        h = DummyHTTPClient()
        xml = self.sample_data(filename)
        h.queue_response(200, media_type='text/xml', content=xml)
        return h


    #def queue_viaf_lookup_result():
    #    http = self.queue_file_in_mock_http("mindy_kaling.xml")
    #    lookup = self.viaf_client.lookup_by_viaf(viaf="9581122", do_get=http.do_get)
    #    client.results = [lookup]


    def test_primary_author_name(self):
        # Test our ability to turn a freeform string that identifies
        # one or more people into the likely name of one person.
        m = self.canonicalizer.primary_author_name

        # Test the simplest case.
        eq_("Mindy Kaling", m("Mindy Kaling"))

        # Make sure only the first human's name is used.
        eq_("Mindy Kaling", m("Mindy Kaling, Bob Saget and Co"))
        eq_("Bill O'Reilly", m("Bill O'Reilly with Martin Dugard"))
        eq_("Clare Verbeek",
            m("Clare Verbeek, Thembani Dladla, Zanele Buthelezi"))

        # In most cases, when a sort name is passed in as a display
        # name, the situation is correctly diagnosed and the name is
        # returned as-is.
        for sort_name in (
            'Kaling, Mindy',
            'Tolkien, J. R. R.',
            'van Damme, Jean-Claude',
        ):
            eq_(sort_name, m(sort_name))

        # Similarly when there is no distinction between display
        # and sort name.
        for sort_name in (
            'Cher',
            'Various',
            'Anonymous',
        ):
            eq_(sort_name, m(sort_name))

        # These are not likely to show up in real usage, but we can
        # handle them.
        eq_("Rand, Ayn", m('Rand, Ayn, and Cher'))
        eq_("Rand, Ayn", m('Rand, Ayn, and Kaling, Mindy'))

    def test__canonicalize_single_name(self):
        # For single-named entities, the sort name and display name
        # are identical. We don't need to ask VIAF.
        self.canonicalizer.viaf.queue_lookup("bad data")

        for one_name in (
            'Various',
            'Anonymous',
            'Cher',
        ):
            eq_(
                one_name,
                self.canonicalizer._canonicalize(
                    identifier=None, display_name=one_name
                )
            )

        # We didn't ask the mock VIAF about anything.
        eq_(["bad data"], self.canonicalizer.viaf.results)

    def test_found_contributor(self):
        # If we find a matching contributor already in our database, 
        # then don't bother looking at OCLC or VIAF.
        contributor_1, made_new = self._contributor(sort_name="Zebra, Ant")
        contributor_1.display_name = "Ant Zebra"
        contributor_2, made_new = self._contributor(sort_name="Yarrow, Bloom")
        contributor_2.display_name = "Bloom Yarrow"

        # _canonicalize shouldn't try to contact viaf or oclc, but in case it does, make sure 
        # the contact brings wrong results.
        self.canonicalizer.viaf.queue_lookup([])
        #self.canonicalizer.oclcld.queue_lookup([])
        canonicalized_author = self.canonicalizer._canonicalize(identifier=None, display_name="Ant Zebra")
        eq_(canonicalized_author, contributor_1.sort_name)


    def test_oclc_contributor(self):
        # TODO: make sure isbn ids get directed to OCLC
        pass


    def test_non_isbn_identifier(self):
        # TODO: make sure non-isbn ids get directed to VIAF
        pass






