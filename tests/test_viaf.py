import logging

from nose.tools import set_trace, eq_

from . import (
    DatabaseTest,
    DummyHTTPClient,
    sample_data,
)

from core.model import (
    Contributor,
)

from viaf import (
    VIAFParser, 
    VIAFClient
)



class TestNameParser(DatabaseTest):

    def setup(self):
        super(TestNameParser, self).setup()
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

        # Even if we go in expecting something like "Sam Clemens",
        # we get the consensus result.
        (contributor_data, match_confidences, contributor_titles) = self.parser.parse(xml, working_display_name="Samuel Langhorne Clemens")
        eq_("50566653", contributor_data.viaf)
        eq_("Mark Twain", contributor_data.display_name)
        eq_("Twain, Mark", contributor_data.sort_name)
        eq_("Twain", contributor_data.family_name)
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



class TestVIAFClient(DatabaseTest):
    def setup(self):
        super(TestVIAFClient, self).setup()
        self.client = VIAFClient(self._db)
        self.log = logging.getLogger("VIAF Client Test")


    def sample_data(self, filename):
        return sample_data(filename, "viaf")


    def test_lookup_by_viaf(self):
        # there can be one and only one Mindy
        h = DummyHTTPClient()
        xml = self.sample_data("mindy_kaling.xml")
        h.queue_response(200, media_type='text/xml', content=xml)

        contributor_candidates = self.client.lookup_by_viaf(viaf="9581122", do_get=h.do_get)
        (selected_candidate, match_confidences, contributor_titles) = contributor_candidates
        eq_(selected_candidate.viaf, "9581122")
        eq_(selected_candidate.sort_name, "Kaling, Mindy")


    def test_lookup_by_name(self):
        # there can be one and only one Mindy
        h = DummyHTTPClient()
        xml = self.sample_data("mindy_kaling.xml")
        h.queue_response(200, media_type='text/xml', content=xml)

        [(selected_candidate, match_confidences, contributor_titles)] = self.client.lookup_by_name(sort_name="Mindy Kaling", do_get=h.do_get)
        eq_(selected_candidate.viaf, "9581122")
        eq_(selected_candidate.sort_name, "Kaling, Mindy")





