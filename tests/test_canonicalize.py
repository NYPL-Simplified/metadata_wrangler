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
from core.model import DataSource

from .test_viaf import MockVIAFClientLookup

from canonicalize import (
    AuthorNameCanonicalizer,
    CanonicalizationError,
    SimpleMockAuthorNameCanonicalizer,
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

        # TODO: This is wrong -- we see two names where there's only
        # one.
        eq_("van Damme", m('van Damme, Jean Claude'))

    def test_canonicalize_author_name(self):

        class Mock(AuthorNameCanonicalizer):
            """Mock sort_name_from_services."""

            def __init__(self, correct_answers=None):
                self.attempts = []
                self.correct_answers = correct_answers or dict()

            def sort_name_from_services(self, identifier, name):
                """If there's a known correct answer for this identifier
                and name, return it; otherwise return None.
                """
                key = (identifier, name)
                self.attempts.append(key)
                return self.correct_answers.get(key, None)

            def default_sort_name(self, name):
                """Mocking this method makes it clear whether an answer
                came out of a sort_name_from_services() call or whether it
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

        # We call sort_name_from_services once and when that fails we call
        # default_name.
        eq_("Default Jim Davis",
            c.canonicalize_author_name("Jim Davis", "An ISBN"))
        eq_([("Jim Davis", "An ISBN")], c.attempts)
        c.attempts = []

        # When it looks like there are two authors, we call
        # sort_name_from_services twice -- once with what appears to be the
        # author's first name, and again with the entire author
        # string.
        eq_("Default Jim Davis and Matt Groening",
            c.canonicalize_author_name(
                "Jim Davis and Matt Groening", "An ISBN"
            )
        )
        eq_(
            [("Jim Davis", "An ISBN"),
             ("Jim Davis and Matt Groening", "An ISBN"),
            ],
            c.attempts
        )

        # Now try a canonicalizer that knows about one correct answer.
        c = Mock({("Jim Davis", "An ISBN") : "Davis, Funky Jim"})

        # This time the sort_name_from_services() call succeeds and the default
        # code is not triggered.
        eq_("Davis, Funky Jim",
            c.canonicalize_author_name("Jim Davis", "An ISBN")
        )
        eq_(
            [("Jim Davis", "An ISBN")],
             c.attempts
        )
        c.attempts = []

        # If we get an answer with the first part of a suspected multi-part
        # author name, we don't try again with the whole author name.
        eq_("Davis, Funky Jim",
            c.canonicalize_author_name(
                "Jim Davis and Matt Groening", "An ISBN"
            )
        )
        eq_(
            [("Jim Davis", "An ISBN")],
             c.attempts
        )

        # If we don't pass in the key piece of information necessary
        # to unlock the correct answer, we still get the default answer.
        eq_("Default Jim Davis",
            c.canonicalize_author_name("Jim Davis", "A Different ISBN"))

    def test_sort_name_from_services_single_name(self):
        # For single-named entities, the sort name and display name
        # are identical. We don't need to ask any external services.

        class Mock(AuthorNameCanonicalizer):
            """sort_name_from_services will raise an exception
            if it tries to access any external services.
            """
            def explode(self):
                raise Exception("boom!")

            sort_name_from_database = explode
            sort_name_from_oclc_linked_data = explode
            sort_name_from_viaf_urls = explode
            sort_name_from_viaf = explode

        # We can check these names without causing an exception.
        for one_name in (
            'Various',
            'Anonymous',
            'Cher',
        ):
            eq_(
                one_name,
                self.canonicalizer.sort_name_from_services(
                    identifier=None, display_name=one_name
                )
            )

    def test_sort_name_from_services(self):
        """Verify that sort_name_from_services calls a number of other
        methods trying to get a sort name.
        """
        class Mock(AuthorNameCanonicalizer):
            def __init__(self):
                # Some placeholder objects that will be passed around.
                self.titles_from_database = object()
                self.uris_from_oclc = object()

                self.log = logging.getLogger("unit test")

                # We start out with good return values available from
                # every service. We'll delete these one at a time, to
                # show how sort_name_from_services falls back to one
                # service when another fails to get results.
                self.return_values = dict(
                    sort_name_from_database="good value from database",
                    sort_name_from_oclc_linked_data="good value from OCLC",
                    sort_name_from_viaf_urls="good value from VIAF URLs",
                    sort_name_from_viaf_display_name="good value from VIAF display name",
                )
                self.calls = []

            def sort_name_from_database(self, display_name, identifier):
                m = "sort_name_from_database"
                self.calls.append((m, display_name, identifier))
                return self.return_values.get(m), self.titles_from_database

            def sort_name_from_oclc_linked_data(self, display_name, identifier):
                m = "sort_name_from_oclc_linked_data"
                self.calls.append((m, display_name, identifier))
                return self.return_values.get(m), self.uris_from_oclc

            def sort_name_from_viaf_urls(self, display_name, urls):
                m = "sort_name_from_viaf_urls"
                self.calls.append((m, display_name, urls))
                return self.return_values.get(m)

            def sort_name_from_viaf_display_name(
                self, display_name, known_titles
            ):
                m = "sort_name_from_viaf_display_name"
                self.calls.append((m, display_name, known_titles))
                return self.return_values.get(m)

        # First, verify that sort_name_from_services returns the first
        # usable value returned by one of these methods.
        c = Mock()
        m = c.sort_name_from_services
        args = ("Jim Davis", "An ISBN")
        eq_("good value from database", m(*args))

        del c.return_values['sort_name_from_database']
        eq_("good value from OCLC", m(*args))

        del c.return_values['sort_name_from_oclc_linked_data']
        eq_("good value from VIAF URLs", m(*args))

        del c.return_values['sort_name_from_viaf_urls']
        eq_("good value from VIAF display name", m(*args))

        del c.return_values['sort_name_from_viaf_display_name']

        # This whole time we've been accumulating data in this
        # list. This is the last time we're going to call
        # sort_name_from_services(), so clear it out beforehand. That
        # way we can see a complete list of what methods get called by
        # sort_name_from_services().
        c.calls = []

        # All our attempts fail, so the final result is None.
        eq_(None, m(*args))

        # Let's see the journey we took on the way to this failure.
        (from_database, from_oclc, from_viaf_urls,
         from_viaf_display_name) = c.calls

        # We passed the name and identifier into sort_name_from_database.
        eq_(('sort_name_from_database', 'Jim Davis', 'An ISBN'),
            from_database)

        # Then we passed the same information into
        # sort_name_from_oclc_linked_data.
        eq_(('sort_name_from_oclc_linked_data', 'Jim Davis', 'An ISBN'),
            from_oclc)

        # That returned a bunch of URLs, which we passed into
        # sort_name_from_viaf_urls.
        eq_(('sort_name_from_viaf_urls', 'Jim Davis', c.uris_from_oclc),
            from_viaf_urls)

        # Finally, we called from_viaf_display_name, using the book
        # titles returned by sort_name_from_database.
        eq_(('sort_name_from_viaf_display_name', 'Jim Davis', 
             c.titles_from_database), from_viaf_display_name)

    def test_sort_name_from_database(self):
        # Verify that sort_name_from_database grabs titles and
        # Contributors from the database, then passes them into
        # _sort_name_from_contributor_and_titles.
        
        class Mock(AuthorNameCanonicalizer):

            def __init__(self, _db):
                self._db = _db
                self.calls = []
                self.right_answer = None

            def _sort_name_from_contributor_and_titles(
                self, contributor, known_titles
            ):
                self.calls.append((contributor, known_titles))
                return self.right_answer

        canonicalizer = Mock(self._db)

        input_name = "Display Name"

        # Create a number of contributors with the same display_name
        c1, ignore = self._contributor(sort_name="Zebra, Ant")
        c1.display_name = input_name

        c2, ignore = self._contributor(sort_name="Yarrow, Bloom")
        c2.display_name = input_name

        # These contributors will be ignored -- c3 beacuse it doesn't
        # have a sort name (which is what we're trying to find) and v4
        # because its display name doesn't match.
        c3, ignore = self._contributor(sort_name="will be deleted")
        c3.display_name = input_name
        c3.sort_name = None

        c4, ignore = self._contributor(sort_name="Author, Another")
        c4.display_name = "A Different Display Name"

        # Create two Editions with the same primary Identifier.
        edition = self._edition(
            title="Title 1",
            data_source_name=DataSource.GUTENBERG
        )
        identifier = edition.primary_identifier
        edition2 = self._edition(
            title="Title 2", identifier_type=identifier.type,
            identifier_id=identifier.identifier,
            data_source_name=DataSource.OVERDRIVE
        )

        # Our mocked _sort_name_from_contributor_and_titles will
        # return the answer we specify.
        canonicalizer.right_answer = "Sort Name, The Real"
        answer, titles = canonicalizer.sort_name_from_database(
            input_name, identifier
        )
        eq_("Sort Name, The Real", answer)

        # It also returned all titles associated with the identifier
        # we passed in.
        eq_(set(["Title 1", "Title 2"]), titles)

        # If we don't pass in an Identifier, we get the same answer
        # but no titles.
        answer, titles = canonicalizer.sort_name_from_database(
            input_name, None
        )
        eq_("Sort Name, The Real", answer)
        eq_(set(), titles)
        
        # Now let's get rid of the 'right answer' so we can see
        # everything sort_name_from_database goes through before
        # giving up.
        canonicalizer.right_answer = None
        canonicalizer.calls = []
        answer, titles = canonicalizer.sort_name_from_database(
            input_name, identifier
        )

        # _sort_name_from_contributor_and_titles was called twice, one
        # for each Contributor that looked like it might be a match
        # based on the display_name.
        call1, call2 = canonicalizer.calls
        eq_((c1, titles), call1)
        eq_((c2, titles), call2)

        # Since neither call to _sort_name_from_contributor_and_titles
        # turned up anything, the sort name of the first matching
        # Contributor was used as the answer.
        eq_(c1.sort_name, answer)
        eq_(set(["Title 1", "Title 2"]), titles)

        # If there are no matching Contributors at all,
        # sort_name_from_database returns None.
        eq_((None, set()),
            canonicalizer.sort_name_from_database("Jim Davis", None))

    def test__sort_name_from_contributor_and_titles(self):
        # Verify that _sort_name_from_contributor_and_titles returns a
        # contributor's sort_name only if it looks like they wrote a
        # book with one of the given titles.
        m = AuthorNameCanonicalizer._sort_name_from_contributor_and_titles

        # No contributor -> failure
        eq_(None, m(None, None))
        eq_(None, m(None, ["Title 1", "Title 2"]))

        # This contributor has no contributions at all -> failure
        no_contributions, ignore = self._contributor()
        eq_(None, m(no_contributions, ["Title 1"]))

        # This contributor has an associated edition.
        edition = self._edition(
            title="Adventures of Huckleberry Finn",
            authors="A Display Name"
        )
        [contributor] = edition.contributors
        contributor.sort_name = "Sort Name, An"

        # We'll get None unless we pass in a title that's a
        # substantial match.
        eq_(None, m(contributor, None))
        eq_(None, m(contributor, []))
        eq_(None, m(contributor, ["Title 1", "Title 2"]))

        # If there is a match, we get the answer.
        eq_("Sort Name, An", 
            m(
                contributor,
                ["Adventures of Huckleberry Finn", "Some Other Book"]
            )
        )

        # It doesn't have to be an exact match, but it must be close.
        eq_("Sort Name, An", 
            m(
                contributor,
                ["The Adventures of Huckleberry Finn"]
            )
        )

    def test_sort_name_from_oclc_linked_data(self):
        # We may be able to use OCLC Linked Data to find an author sort
        # name for a given book.
        #
        # TODO Implement as general strategy to bring back OCLC Linked
        # Data.
        pass

    def test_sort_name_from_viaf_urls(self):
        # We may be able to use VIAF lookups to find an author sort name.

        m = self.canonicalizer.sort_name_from_viaf_urls

        # If no VIAF URLs are provided, there's nothing we can do.
        eq_(None, m("Jim Davis", None))
        eq_(
            None,
            m(
                "Jim Davis", ["http://not-a-viaf/", "http://still-not-a-viaf/"]
            )
        )

        # If a VIAF URL is provided, it's passed into the VIAF client.
        # First let's test the success case where we the request gets
        # a good answer.
        davis = ContributorData(sort_name="Davis, Jim (from VIAF)")
        self.viaf_client.queue_lookup([davis, "some other contributor"])
        result = m(
            "Jim Davis",
            [
                "http://not-a-viaf/",
                "http://viaf.org/viaf/1234",
                "http://still-not-a-viaf/"
            ]
        )

        # We ran a VIAF lookup on the ID (derived from the URL) and
        # the working display name (passed in to
        # sort_name_from_viaf_urls).
        lookup = self.viaf_client.viaf_lookups.pop()
        eq_([], self.viaf_client.viaf_lookups)
        ((viaf_id,), kwargs) = lookup
        eq_("1234", viaf_id)
        eq_(dict(working_display_name="Jim Davis"), kwargs)

        # We got the right answer.
        eq_("Davis, Jim (from VIAF)", result)

        # Now test failure cases.
        #

        # VIAF gave some info but it didn't include the sort name.
        davis = ContributorData(display_name="Jim Davis (but you knew that)")
        self.viaf_client.queue_lookup([davis])
        result = m("Jim Davis", ["http://viaf.org/viaf/1234"])

        # The same arguments were passed into lookup_by_viaf().
        lookup = self.viaf_client.viaf_lookups.pop()
        eq_([], self.viaf_client.viaf_lookups)
        ((viaf_id,), kwargs) = lookup
        eq_("1234", viaf_id)
        eq_(dict(working_display_name="Jim Davis"), kwargs)

        # But we didn't get an answer for sort name.
        eq_(None, result)

        # VIAF didn't return any info.
        self.viaf_client.queue_lookup([])
        result = m("Jim Davis", ["http://viaf.org/viaf/1234"])
        eq_(None, result)

    def test_sort_name_from_viaf(self):
        # We may be able to get a sort name by asking VIAF
        # about a display name.

        # Queue up an answer from VIAF.
        davis = ContributorData(sort_name="Davis, Jim (from VIAF)")
        self.viaf_client.queue_lookup([davis, "some other contributor"])

        # Calling sort_name_from_viaf gives us that answer.
        m = self.canonicalizer.sort_name_from_viaf
        eq_("Davis, Jim (from VIAF)",
            m("Jim Davis", ["Garfield Hates Mondays"]))

        # How'd we get it? We called lookup_by_name() with the
        # sort name and a list of titles.
        lookup = self.viaf_client.name_lookups.pop()
        args, kwargs = lookup
        eq_(dict(known_titles=['Garfield Hates Mondays'],
                 display_name='Jim Davis', sort_name=None),
            kwargs
        )

        # If VIAF doesn't give an answer, the result is None.
        self.viaf_client.queue_lookup(None)
        eq_(None, m("Jim Davis", None))

        # Similarly if VIAF gives an answer but it doesn't include a
        # sort_name.
        davis = ContributorData(display_name="Jim Davis (but you knew that)")
        self.viaf_client.queue_lookup([davis])
        eq_(None, m("Jim Davis", None))

    def test_default_sort_name(self):
        # default_sort_name() does a reasonable job of guessing at an
        # author name.
        #
        # It does this primarily by deferring to
        # display_name_to_short_name, though we don't test this
        # explicitly.
        m = self.canonicalizer.default_sort_name
        eq_("Davis, Jim", m("Jim Davis"))
        eq_("Davis, Jim", m("Jim Davis and Matt Groening"))
        eq_("Vassar College", m("Vassar College"))

