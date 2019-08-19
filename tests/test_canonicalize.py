import logging

from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
    DummyHTTPClient,
    sample_data,
)

from core.metadata_layer import ContributorData

from .test_viaf import MockVIAFClientLookup

from canonicalize import (
    AuthorNameCanonicalizer,
    CanonicalizationError,
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

        # Test handling of invalid values
        eq_(None, m(None))

        # Corporate names are passed through even if they resemble
        # multiple individual names.
        eq_("Vassar College and its Board of Directors",
            m("Vassar College and its Board of Directors"))

        # Test the simplest case -- one human name.
        eq_("Mindy Kaling", m("Mindy Kaling"))

        # When there are multiple humans, only the first one is used.
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

        # TODO: This is wrong -- we see a single person's sort_name.
        eq_("Madonna, Cher", m('Madonna, Cher'))

        # TODO: This is wrong -- we don't understand that these two
        # people are from the same family.
        eq_("Ryan", m('Ryan and Josh Shook'))

    def test_canonicalize_author_name(self):

        class Mock(AuthorNameCanonicalizer):

            def __init__(self, correct_answers=None):
                self.attempts = []
                self.correct_answers = correct_answers or dict()

            def _canonicalize(self, identifier, name):
                """If there's a known correct answer for this identifier
                and name, return it; otherwise return None.
                """
                key = (identifier, name)
                self.attempts.append(key)
                return self.correct_answers.get(key, None)

            def default_name(self, name):
                """Overriding this method makes it clear whether an answer
                came out of a _canonicalize() call or whether it
                is a default answer.
                """
                return "Default " + name

        assert_raises_regexp(
            CanonicalizationError,
            "Neither useful identifier nor display name was provided.",
            self.canonicalizer.canonicalize_author_name,
            None, ''
        )

        # Test failures by setting up a canonicalizer that doesn't
        # know any answers.
        c = Mock()

        # We call _canonicalize once and when that fails we call
        # default_name.
        eq_("Default Jim Davis",
            c.canonicalize_author_name("An ISBN", "Jim Davis"))
        eq_([("An ISBN", "Jim Davis")], c.attempts)
        c.attempts = []

        # When it looks like there are two authors, we call
        # _canonicalize twice -- once with what appears to be the
        # author's first name, and again with the entire author
        # string.
        eq_("Default Jim Davis and Matt Groening",
            c.canonicalize_author_name(
                "An ISBN", "Jim Davis and Matt Groening"
            )
        )
        eq_(
            [("An ISBN", "Jim Davis"),
             ("An ISBN", "Jim Davis and Matt Groening"),
            ],
            c.attempts
        )

        # Now try a canonicalizer that knows about one correct answer.
        c = Mock({("An ISBN", "Jim Davis") : "Davis, Funky Jim"})

        # This time the _canonicalize() call succeeds and the default
        # code is not triggered.
        eq_("Davis, Funky Jim",
            c.canonicalize_author_name("An ISBN", "Jim Davis")
        )
        eq_(
            [("An ISBN", "Jim Davis")],
             c.attempts
        )
        c.attempts = []

        # If we get an answer with the first part of a suspected multi-part
        # author name, we don't try again with the whole author name.
        eq_("Davis, Funky Jim",
            c.canonicalize_author_name(
                "An ISBN", "Jim Davis and Matt Groening"
            )
        )
        eq_(
            [("An ISBN", "Jim Davis")],
             c.attempts
        )

        # If we don't pass in the key piece of information necessary
        # to unlock the correct answer, we still get the default answer.
        eq_("Default Jim Davis",
            c.canonicalize_author_name("A Different ISBN", "Jim Davis"))

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

    def test_default_name(self):
        # default_name() does a reasonable job of guessing at an
        # author name.
        #
        # It does this primarily by deferring to
        # display_name_to_short_name, though we don't test this.
        m = self.canonicalizer.default_name
        eq_("Davis, Jim", m("Jim Davis"))
        eq_("Davis, Jim", m("Jim Davis and Matt Groening"))
        eq_("Vassar College", m("Vassar College"))

