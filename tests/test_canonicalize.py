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
        # Make sure only the first human's name is used.
        name = "Mindy Kaling, Bob Saget and Co"
        extracted_name = self.canonicalizer.primary_author_name(name)
        eq_("Mindy Kaling", extracted_name)


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






