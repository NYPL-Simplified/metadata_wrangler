# encoding: utf-8
import logging

from nose.tools import set_trace, eq_

from . import (
    DatabaseTest,
    DummyHTTPClient,
    sample_data,
)

from core.metadata_layer import ContributorData
from core.model import Contributor

from testing import MockVIAFClient
from viaf import (
    NameParser,
    VIAFParser,
    VIAFClient
)

class TestNameParser(object):
    """Test the NameParser class."""

    def test_parse(self):
        """Test the behavior of NameParser.parse.

        In many cases this demonstrates that NameParser.parse doesn't
        do as much as we might like -- it doesn't try to convert sort
        names to display names, and it doesn't handle imprecisely
        recorded birth and death dates.
        """

        def assert_parses_as(to_parse, sort_name=None, birth=None, death=None):
            """Assert that a given string parses into a ContributorData
            object with the given sort_name, birth and death dates.
            """
            sort_name = sort_name or to_parse

            contributor = NameParser.parse(to_parse)
            assert isinstance(contributor, ContributorData)
            eq_(sort_name, contributor.sort_name)
            if birth is not None:
                eq_(birth, contributor.extra[Contributor.BIRTH_DATE])
            if death is not None:
                eq_(death, contributor.extra[Contributor.DEATH_DATE])
        m = assert_parses_as

        # NameParser extracts the birth and/or death dates from these
        # strings.
        m("Baxter, Charles, 1947-", "Baxter, Charles", "1947")
        m("Schlesinger, Arthur M., Jr. (Arthur Meier), 1917-2007",
          "Schlesinger, Arthur M., Jr. (Arthur Meier)", "1917", "2007")
        m("Bstan-ʼdzin-rgya-mtsho, Dalai Lama XIV, 1935-",
          "Bstan-ʼdzin-rgya-mtsho, Dalai Lama XIV", "1935", None)
        m("William, Prince, Duke of Cambridge, 1982-",
          "William, Prince, Duke of Cambridge", "1982")
        m("Windsor, Edward, Duke of, 1894-1972",
          "Windsor, Edward, Duke of", "1894", "1972")
        m("Augustine, of Hippo, Saint, 354-430.",
          "Augustine, of Hippo, Saint", "354", "430")

        # Death year is known but birth year is not.
        m("Mace, Daniel, -1753", "Mace, Daniel", None, "1753")

        # Neither year is known.
        m("Anonymous, ?-?", "Anonymous", None, None)

        # Neither year is known with certainty.
        # It's accurate enough for our purposes so we just go with it.
        m("Bach, P. D. Q., 1807?-1742?", "Bach, P. D. Q.", "1807", "1742")

        # Nameparser doesn't interpret these names as containing any
        # extra data -- they just stored directly in
        # ContributorData.sort_name.
        #
        # In some cases we would rather do without the stuff in
        # parentheses, but without getting more detailed data from
        # VIAF we don't know for sure whether it's a description of
        # the person or part of their name.
        m("Korman, Gordon")
        m("Smythe, J. P. (James P.)")
        m("Bernstein, Albert J.", "Bernstein, Albert J.")
        m("Lifetime Television (Firm)")
        m("Wang, Wei (Writer on the Chinese People's Liberation Army)")

        # TODO: This is definitely a date, but it's too vague to use
        # and we don't parse it out.
        m("Sunzi, active 6th century B.C.")

        # TODO: This is a date but not in the format we expect.
        # m("Fisher, David, 1946 April 16-")

class TestVIAFNameParser(DatabaseTest):
    """Test the name parsing code built into VIAFParser (as opposed to the
    simpler standalone code in the NameParser class).
    """

    def setup(self):
        super(TestVIAFNameParser, self).setup()
        self.parser = VIAFParser()

    def sample_data(self, filename):
        return sample_data(filename, "viaf")

    def test_entry_with_wikipedia_name(self):

        xml = self.sample_data("will_eisner.xml")

        (contributor_data, match_confidences, contributor_titles) = self.parser.parse(xml, None)
        eq_("10455", contributor_data.viaf)
        eq_("Will Eisner", contributor_data.display_name)
        eq_("Eisner", contributor_data.family_name)
        eq_("Will_Eisner", contributor_data.wikipedia_name)

    def test_entry_with_wikipedia_name_that_is_actually_wikidata_id(self):

        xml = self.sample_data("michelle_belanger.xml")

        (contributor_data, match_confidences, contributor_titles) = self.parser.parse(xml, None)
        eq_('38770861', contributor_data.viaf)
        eq_("Michelle A. Belanger", contributor_data.display_name)
        eq_("Belanger", contributor_data.family_name)
        eq_(None, contributor_data.wikipedia_name)

    def test_entry_without_wikipedia_name(self):
        xml = self.sample_data("palmer.xml")

        (contributor_data, match_confidences, contributor_titles) = self.parser.parse(xml)
        eq_("2506349", contributor_data.viaf)
        eq_("Roy Ernest Palmer", contributor_data.display_name)
        eq_("Palmer", contributor_data.family_name)
        eq_(None, contributor_data.wikipedia_name)

    def test_simple_corporate_entry(self):
        xml = self.sample_data("aquarius.xml")
        (contributor_data, match_confidences, contributor_titles) = self.parser.parse(xml)
        eq_("159591140", contributor_data.viaf)
        eq_("Aquarius Paris", contributor_data.display_name)
        eq_("Aquarius", contributor_data.family_name)
        eq_(None, contributor_data.wikipedia_name)

    def test_many_names(self):
        # Even if we pass in "Sam Clemens" as the working name, the
        # family name we get back is "Twain", because we give very
        # high consideration to the Wikipedia name.
        xml = self.sample_data("mark_twain.xml")

        (contributor_data, match_confidences, contributor_titles) = self.parser.parse(xml, working_display_name="Sam Clemens")
        eq_("50566653", contributor_data.viaf)
        eq_("Mark Twain", contributor_data.display_name)
        eq_("Mark_Twain", contributor_data.wikipedia_name)
        eq_("Twain", contributor_data.family_name)


        # Let's try again without the Wikipedia name.
        xml = self.sample_data("mark_twain_no_wikipedia.xml")

        # The author is better known as Mark Twain, so this
        # name wins by popularity if we don't specify a name going in.
        (contributor_data, match_confidences, contributor_titles) = self.parser.parse(xml, None)
        eq_("50566653", contributor_data.viaf)
        eq_("Mark Twain", contributor_data.display_name)
        eq_("Twain", contributor_data.family_name)
        eq_(None, contributor_data.wikipedia_name)

        # NOTE:  Old behavior:  Even if we go in expecting something like "Sam Clemens", we get the consensus result.
        # New behavior:  If the wikipedia name is not there to correct our working_display_name for us,
        # then either "Samuel Langhorne Clemens" or "Mark Twain" is acceptable to us here now.
        (contributor_data, match_confidences, contributor_titles) = self.parser.parse(xml, working_display_name="Samuel Langhorne Clemens")
        eq_("50566653", contributor_data.viaf)
        eq_("Samuel Langhorne Clemens", contributor_data.display_name)
        eq_("Twain, Mark", contributor_data.sort_name)
        eq_("Clemens", contributor_data.family_name)
        eq_(None, contributor_data.wikipedia_name)


    def test_ignore_results_if_author_not_in_viaf(self):
        # This is the VIAF result for searching for "Howard,
        # J. J.". There are lots of results but none of them is the
        # correct one. This test verifies that we ignore all the
        # incorrect results.
        xml = self.sample_data("howard_j_j.xml")
        name = "Howard, J. J."

        contributor_candidates = self.parser.parse_multiple(xml, working_sort_name=name)
        for contributor in contributor_candidates:
            match_quality = self.parser.weigh_contributor(candidate=contributor, working_sort_name=name, strict=True)
            eq_(match_quality, 0)


    def test_multiple_results_with_success(self):
        xml = self.sample_data("lancelyn_green.xml")
        name = "Green, Roger Lancelyn"
        contributor_candidates = self.parser.parse_multiple(xml, working_sort_name=name)
        contributor_candidates = self.parser.order_candidates(working_sort_name=name, contributor_candidates=contributor_candidates)
        (contributor_data, match_confidences, contributor_titles)  = contributor_candidates[0]

        eq_("29620265", contributor_data.viaf)
        eq_("Roger Lancelyn Green", contributor_data.display_name)
        eq_("Green", contributor_data.family_name)
        eq_("Roger_Lancelyn_Green", contributor_data.wikipedia_name)


    def test_multiple_results_with_viaf_number_but_no_name(self):
        # This author's VIAF entry doesn't have any name information
        # we don't already have, but it does have a VIAF entry, and we
        # pick that up.
        xml = self.sample_data("kate_lister.xml")
        name = "Lister, Kate"
        (contributor_data, match_confidences, contributor_titles) = self.parser.parse(xml)
        eq_("68169992", contributor_data.viaf)
        eq_(None, contributor_data.display_name)
        eq_(None, contributor_data.family_name)
        eq_("Lister, Kate", contributor_data.sort_name)
        eq_(None, contributor_data.wikipedia_name)


    def test_library_popularity(self):
        # A good match higher in the list (in terms of library popularity) penalizes a match lower in the list,
        # but a bad match high in the list doesn't penalize matches below it.

        # our correct match Amy Levin is 4th down the list, so without the title, she should be penalized
        xml = self.sample_data("amy_levin_all_viaf.xml")
        name = "Levin, Amy"
        contributor_candidates = self.parser.parse_multiple(xml, working_sort_name=name)
        contributor_candidates = self.parser.order_candidates(working_sort_name=name,
            contributor_candidates=contributor_candidates)
        (contributor_data, match_confidences, contributor_titles) = contributor_candidates[0]
        eq_(contributor_data.viaf, '215866998')
        eq_(match_confidences['library_popularity'], 1)
        for (contributor_data, match_confidences, contributor_titles) in contributor_candidates:
            if contributor_data.viaf == '315591707':
                eq_(match_confidences['library_popularity'], 4)

        # with the title, the right Amy is selected, despite the library unpopularity
        contributor_candidates = self.parser.order_candidates(working_sort_name=name,
            contributor_candidates=contributor_candidates,
            known_titles=["Faithfully Feminist"])
        (contributor_data, match_confidences, contributor_titles) = contributor_candidates[0]
        eq_(contributor_data.viaf, '315591707')
        eq_(match_confidences['library_popularity'], 4)

        for (contributor_data, match_confidences, contributor_titles) in contributor_candidates:
            if contributor_data.viaf == '215866998':
                eq_(match_confidences['library_popularity'], 1)


        # has 10 clusters, first three bearing no resemblance to any John
        xml = self.sample_data("john_jewel_all_viaf.xml")
        name = "Jewel, John"
        contributor_candidates = self.parser.parse_multiple(xml, working_sort_name=name)
        contributor_candidates = self.parser.order_candidates(working_sort_name=name,
            contributor_candidates=contributor_candidates,
            known_titles=["The Apology of the Church of England"])
        (contributor_data, match_confidences, contributor_titles) = contributor_candidates[0]
        eq_("176145857856923020062", contributor_data.viaf)
        eq_(match_confidences['library_popularity'], 4)

        # without using the title, we still get the right John Jewel, even though he's 4th down the list, he hasn't suffered
        contributor_candidates = self.parser.order_candidates(working_sort_name=name,
            contributor_candidates=contributor_candidates)
        (contributor_data, match_confidences, contributor_titles) = contributor_candidates[0]
        eq_("176145857856923020062", contributor_data.viaf)
        eq_(match_confidences['library_popularity'], 4)


    def test_birthdates(self):
        # TODO: waiting on https://github.com/NYPL-Simplified/Simplified/issues/61
        # Good for testing separating authors by birth dates -- VIAF has several Amy Levins, with different birthdates.
        xml = self.sample_data("amy_levin_all_viaf.xml")
        name = "Levin, Amy"
        contributor_candidates = self.parser.parse_multiple(xml, working_sort_name=name)

        contributor_candidates = self.parser.order_candidates(working_sort_name=name,
            contributor_candidates=contributor_candidates)
        (contributor_data, match_confidences, contributor_titles) = contributor_candidates[0]
        eq_(contributor_data.viaf, '215866998')
        # make sure birthdate is 1957

        contributor_candidates = self.parser.order_candidates(working_sort_name=name,
            contributor_candidates=contributor_candidates,
            known_titles=["Faithfully Feminist"])
        (contributor_data, match_confidences, contributor_titles) = contributor_candidates[0]
        eq_(contributor_data.viaf, '315591707')
        # make sure birthdate is 1986


class MockVIAFClientLookup(MockVIAFClient, VIAFClient):
    """A mocked VIAFClient that can queue mocked lookup results and
    still be used to test VIAFClient#process_contributor.
    """
    def __init__(self, _db, log):
        self._db = _db
        self.log = log
        super(MockVIAFClientLookup, self).__init__()


class TestVIAFClient(DatabaseTest):

    def setup(self):
        super(TestVIAFClient, self).setup()
        self.client = VIAFClient(self._db)
        self.log = logging.getLogger("VIAF Client Test")

    def sample_data(self, filename):
        return sample_data(filename, "viaf")

    def queue_file_in_mock_http(self, filename):
        h = DummyHTTPClient()
        xml = self.sample_data(filename)
        h.queue_response(200, media_type='text/xml', content=xml)
        return h

    def test_process_contributor(self):
        client = MockVIAFClientLookup(self._db, self.log)
        contributor = self._contributor()[0]

        # If lookup returns an empty array (as in the case of
        # VIAFParser#parse_multiple), the contributor is not updated.
        client.queue_lookup([])
        client.process_contributor(contributor)
        eq_(contributor.sort_name, '2001')
        eq_(contributor.display_name, None)

        def queue_lookup_result():
            http = self.queue_file_in_mock_http("mindy_kaling.xml")
            lookup = self.client.lookup_by_viaf(viaf="9581122", do_get=http.do_get)
            client.results = [lookup]

        # When lookup is successful, the contributor is updated.
        queue_lookup_result()
        client.process_contributor(contributor)
        eq_(contributor.sort_name, "Kaling, Mindy")
        eq_(contributor.display_name, "Mindy Kaling")

        # If a contributor with the same VIAF number already exists,
        # the original contributor will be updated with VIAF data
        # and the processed contributor will be merged into the original.
        earliest_contributor = contributor
        # Reset the contributors sort name to confirm the data update.
        earliest_contributor.sort_name = None

        # Create a new contributor and contribution to confirm the merge.
        contributor = self._contributor()[0]
        edition = self._edition(authors=contributor.sort_name)
        eq_(edition.contributors, set([contributor]))

        queue_lookup_result()
        client.process_contributor(contributor)
        eq_(earliest_contributor.sort_name, "Kaling, Mindy")
        eq_(edition.contributors, set([earliest_contributor]))
        # The new contributor has been deleted.
        assert contributor not in self._db

        # If the display name of the original contributor is suspiciously
        # different from the VIAF display name, the new contributor will be
        # updated without being merged.
        earliest_contributor.display_name = "Mindy L. Kaling"
        earliest_contributor.sort_name = None
        contributor = self._contributor()[0]
        edition = self._edition(authors=contributor.sort_name)

        queue_lookup_result()
        client.process_contributor(contributor)
        eq_(contributor.viaf, "9581122")
        eq_(contributor.sort_name, "Kaling, Mindy")
        # Earlier contributor has not been updated or merged.
        eq_(earliest_contributor.sort_name, None)
        assert earliest_contributor not in edition.contributors

    def test_lookup_by_viaf(self):
        # there can be one and only one Mindy
        h = self.queue_file_in_mock_http("mindy_kaling.xml")

        contributor_candidate = self.client.lookup_by_viaf(viaf="9581122", do_get=h.do_get)
        (selected_candidate, match_confidences, contributor_titles) = contributor_candidate
        eq_(selected_candidate.viaf, "9581122")
        eq_(selected_candidate.sort_name, "Kaling, Mindy")

    def test_lookup_by_name(self):
        # there can be one and only one Mindy
        h = self.queue_file_in_mock_http("mindy_kaling.xml")

        (selected_candidate,
         match_confidences,
         contributor_titles) = self.client.lookup_by_name(sort_name="Mindy Kaling", do_get=h.do_get)
        eq_(selected_candidate.viaf, "9581122")
        eq_(selected_candidate.sort_name, "Kaling, Mindy")
