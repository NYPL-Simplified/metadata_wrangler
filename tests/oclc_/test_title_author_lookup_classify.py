# encoding: utf-8

from nose.tools import eq_, set_trace

from .. import (
    DatabaseTest,
    sample_data,
)

from core.coverage import CoverageFailure
from core.model import (
    DataSource,
    Identifier,
    Contributor,
    Subject,
)

from oclc.classify import (
    OCLCTitleAuthorLookupXMLParser,
    TitleAuthorLookupCoverageProvider
)
from testing import MockOCLCClassifyAPI


class TestParser(DatabaseTest):

    def sample_data(self, filename):
        return sample_data(filename, "oclc_classify")

    def test_extract_multiple_works(self):
        """We can turn a multi-work response into a list of SWIDs."""
        xml = self.sample_data("multi_work_response.xml")

        status, swids = OCLCTitleAuthorLookupXMLParser.parse(self._db, xml, languages=["eng"])
        eq_(OCLCTitleAuthorLookupXMLParser.MULTI_WORK_STATUS, status)

        eq_(25, len(swids))
        eq_(['10106023', '10190890', '10360105', '105446800', '10798812', '11065951', '122280617', '12468538', '13206523', '13358012', '13424036', '14135019', '1413894', '153927888', '164732682', '1836574', '22658644', '247734888', '250604212', '26863225', '34644035', '46935692', '474972877', '51088077', '652035540'], sorted(swids))

        # For your convenience in verifying what I say in
        # test_extract_multiple_works_with_author_restriction().
        assert '13424036' in swids

    def test_extract_multiple_works_with_title_restriction(self):
        """We can choose to only accept works similar to a given title."""
        xml = self.sample_data("multi_work_response.xml")

        # This will only accept titles that contain exactly the same
        # words as "Dick Moby". Only four titles in the sample data
        # meet that criterion.
        status, swids = OCLCTitleAuthorLookupXMLParser.parse(
            self._db, xml, title="Dick Moby", title_similarity=1)
        eq_(4, len(swids))

        # Stopwords "a", "an", and "the" are removed before
        # consideration.
        status, swids = OCLCTitleAuthorLookupXMLParser.parse(
            self._db, xml, title="A an the Moby-Dick", title_similarity=1)
        eq_(4, len(swids))

        # This is significantly more lax, so it finds more results.
        # The exact number isn't important.
        status, swids = OCLCTitleAuthorLookupXMLParser.parse(
            self._db, xml, title="Dick Moby", title_similarity=0.5)
        assert len(swids) > 4

        # This is so lax as to be meaningless. It accepts everything.
        status, swids = OCLCTitleAuthorLookupXMLParser.parse(
            self._db, xml, title="Dick Moby", title_similarity=0)
        eq_(25, len(swids))

        # This is nearly so lax as to be meaningless, but it does
        # prohibit one work whose title contains ' ; ' (these are
        # usually anthologies) and three works whose titles have no
        # words in common with the title we're looking for.
        status, swids = OCLCTitleAuthorLookupXMLParser.parse(
            self._db, xml, title="Dick Moby", title_similarity=0.00000000001)
        eq_(21, len(swids))

        # Add a semicolon to the title we're looking for, and the
        # work whose title contains ' ; ' is acceptable again.
        status, swids = OCLCTitleAuthorLookupXMLParser.parse(
            self._db, xml, title="Dick ; Moby", title_similarity=0.000000001)
        eq_(22, len(swids))

        # This isn't particularly strict, but none of the books in
        # this dataset have titles that resemble this title, so none
        # of their SWIDs show up here.
        status, swids = OCLCTitleAuthorLookupXMLParser.parse(
            self._db, xml, title="None Of These Words Show Up Whatsoever")
        eq_(0, len(swids))


    def test_extract_multiple_works_with_author_restriction(self):
        """We can choose to only accept works by a given author."""
        xml = self.sample_data("multi_work_response.xml")

        [wrong_author], ignore = Contributor.lookup(self._db, sort_name="Wrong Author")
        status, swids = OCLCTitleAuthorLookupXMLParser.parse(
            self._db, xml, languages=["eng"], authors=[wrong_author])
        # This person is not listed as an author of any work in the dataset,
        # so none of those works were picked up.
        eq_(0, len(swids))

        [melville], ignore = Contributor.lookup(self._db, sort_name="Melville, Herman")
        status, swids = OCLCTitleAuthorLookupXMLParser.parse(
            self._db, xml, languages=["eng"], authors=[melville])

        # We picked up 11 of the 25 works in the dataset.
        eq_(11, len(swids))

        # The missing works (as you can verify by looking at
        # oclc_multi_work_response.xml) either don't credit Herman
        # Melville at all (the 1956 Gregory Peck movie "Moby Dick"),
        # credit him as "Associated name" rather than as an author
        # (four books about "Moby Dick"), or credit him as an author
        # but not as the primary author (academic works and adaptations).
        for missing in '10798812', '13424036', '22658644', '250604212', '474972877', '13358012', '153927888', '13206523', '46935692', "14135019", "51088077", "105446800", "164732682", "26863225":
            assert missing not in swids

    def test_primary_author_name(self):
        melville = OCLCTitleAuthorLookupXMLParser.primary_author_from_author_string(self._db, "Melville, Herman, 1819-1891 | Hayford, Harrison [Associated name; Editor] | Parker, Hershel [Editor] | Tanner, Tony [Editor; Commentator for written text; Author of introduction; Author] | Cliffs Notes, Inc. | Kent, Rockwell, 1882-1971 [Illustrator]")
        eq_("Melville, Herman", melville.sort_name)

        eq_(None, OCLCTitleAuthorLookupXMLParser.primary_author_from_author_string(
            self._db,
            "Melville, Herman, 1819-1891 [Author] | Hayford, Harrison [Associated name; Editor]"))

    def test_extract_single_work(self):
        """We can turn a single-work response into a single Edition.
        """

        xml = self.sample_data("single_work_response.xml")

        status, records = OCLCTitleAuthorLookupXMLParser.parse(
            self._db, xml, languages=["eng"])
        eq_(OCLCTitleAuthorLookupXMLParser.SINGLE_WORK_DETAIL_STATUS, status)

        # We expect 1 work record for the OCLC work. The two
        # edition records do not become work records.
        eq_(1, len(records))

        # Work and edition both have a primary identifier.
        work = records[0]
        work_id = work.primary_identifier
        eq_(Identifier.OCLC_WORK, work_id.type)
        eq_('4687', work_id.identifier)

        eq_("Moby Dick", work.title)

        work_contributors = [x.sort_name for x in work.contributors]

        # The work has a ton of contributors, collated from all the
        # editions.
        eq_(set([
            'Cliffs Notes, Inc.',
            'Kent, Rockwell',
            'Hayford, Harrison',
            'Melville, Herman',
            'Parker, Hershel',
            'Tanner, Tony',
             ]), set(work_contributors))

        # Most of the contributors have LC and VIAF numbers, but two
        # (Cliffs Notes and Rockwell Kent) do not.
        eq_(
            [None, None, u'n50025038', u'n50050335', u'n79006936',
             u'n79059764'],
            sorted([x.lc for x in work.contributors])
        )
        eq_(
            [None, None, u'27068555', u'34482742', u'4947338', u'51716047'],
            sorted([x.viaf for x in work.contributors]))

        # Only two of the contributors are considered 'authors' by
        # OCLC. Herman Melville is the primary author, and Tony Tanner is
        # also credited as an author.
        primary_author = sorted(
            [x.contributor.sort_name for x in work.contributions
             if x.role==Contributor.PRIMARY_AUTHOR_ROLE])[0]
        other_author = sorted(
            [x.contributor.sort_name for x in work.contributions
             if x.role==Contributor.AUTHOR_ROLE])[0]

        eq_("Melville, Herman", primary_author)
        eq_("Tanner, Tony", other_author)

        # The work has no language specified. The edition does have
        # a language specified.
        eq_(None, work.language)

        classifications = work.primary_identifier.classifications
        [[subject, weight]] = [(c.subject, c.weight) for c in classifications
                             if c.subject.type == Subject.DDC]
        eq_("813.3", subject.identifier)
        eq_(21183, weight)

        [[subject, weight]] = [(c.subject, c.weight) for c in classifications
                        if c.subject.type == Subject.LCC]
        eq_("PS2384", subject.identifier)
        eq_(22460, weight)

        fast = sorted(
            [(c.subject.name, c.subject.identifier, c.weight)
             for c in classifications if c.subject.type == Subject.FAST])

        expect = [
            ('Ahab, Captain (Fictitious character)', '801923', 29933),
            ('Mentally ill', '1016699', 17294),
            ('Moby Dick (Melville, Herman)', '1356235', 4512),
            ('Sea stories', '1110122', 6893),
            ('Ship captains', '1116147', 19086),
            ('Whales', '1174266', 31482),
            ('Whaling', '1174284', 32058),
            ('Whaling ships', '1174307', 18913)
        ]
        eq_(expect, fast)

    def test_missing_work_id(self):

        # This document contains a work that has a number of editions,
        # but there's no work ID. We use the document anyway.
        xml = self.sample_data("missing_pswid.xml")

        status, [record] = OCLCTitleAuthorLookupXMLParser.parse(
            self._db, xml, languages=["eng"])
        eq_(OCLCTitleAuthorLookupXMLParser.SINGLE_WORK_DETAIL_STATUS, status)
        eq_("The Europeans. Washington Square.", record.title)

    def test_no_contributors(self):
        # This document has no contributors listed.
        xml = self.sample_data("single_work_no_authors.xml")

        status, records = OCLCTitleAuthorLookupXMLParser.parse(
            self._db, xml, languages=["eng"])
        eq_(OCLCTitleAuthorLookupXMLParser.SINGLE_WORK_DETAIL_STATUS, status)
        # We parsed the work, but it had no contributors listed.
        eq_([set()], [r.contributors for r in records])


class TestAuthorParser(DatabaseTest):

    MISSING = object()

    def assert_author(self, result, name, role=Contributor.AUTHOR_ROLE,
                      birthdate=None, deathdate=None):
        contributor, roles = result
        eq_(contributor.sort_name, name)
        if role:
            if not isinstance(role, list) and not isinstance(role, tuple):
                role = [role]
            eq_(role, roles)
        if birthdate is self.MISSING:
            assert Contributor.BIRTH_DATE not in contributor.extra
        elif birthdate:
            eq_(birthdate, contributor.extra[Contributor.BIRTH_DATE])
        if deathdate is self.MISSING:
            assert Contributor.DEATH_DATE not in contributor.extra
        elif deathdate:
            eq_(deathdate, contributor.extra[Contributor.DEATH_DATE])

    def assert_parse(self, string, name, role=Contributor.AUTHOR_ROLE,
                     birthdate=None, deathdate=None):
        [res] = OCLCTitleAuthorLookupXMLParser.parse_author_string(self._db, string)
        self.assert_author(res, name, role, birthdate, deathdate)

    def test_authors(self):

        self.assert_parse(
            "Carroll, Lewis, 1832-1898",
            "Carroll, Lewis", Contributor.PRIMARY_AUTHOR_ROLE, "1832", "1898")

        self.assert_parse(
            "Kent, Rockwell, 1882-1971 [Illustrator]",
            "Kent, Rockwell", "Illustrator",
            "1882", "1971")

        self.assert_parse(
            u"Карролл, Лувис, 1832-1898.",
            u"Карролл, Лувис", Contributor.PRIMARY_AUTHOR_ROLE,
            birthdate="1832", deathdate="1898")

        kerry, melville = OCLCTitleAuthorLookupXMLParser.parse_author_string(
            self._db,
            "McSweeney, Kerry, 1941- | Melville, Herman, 1819-1891")
        self.assert_author(kerry, "McSweeney, Kerry",
                           Contributor.PRIMARY_AUTHOR_ROLE,
                           birthdate="1941", deathdate=self.MISSING)

        self.assert_author(
            melville, "Melville, Herman", Contributor.AUTHOR_ROLE,
            birthdate="1819", deathdate="1891")


        # Check out this mess.
        s = "Sunzi, active 6th century B.C. | Giles, Lionel, 1875-1958 [Writer of added commentary; Translator] | Griffith, Samuel B. [Editor; Author of introduction; Translator] | Cleary, Thomas F., 1949- [Editor; Translator] | Sawyer, Ralph D. [Editor; Author of introduction; Translator] | Clavell, James"
        sunzi, giles, griffith, cleary, sawyer, clavell = (
            OCLCTitleAuthorLookupXMLParser.parse_author_string(self._db, s))

        # This one could be better.
        self.assert_author(sunzi, "Sunzi, active 6th century B.C.",
                           Contributor.PRIMARY_AUTHOR_ROLE)
        self.assert_author(giles, "Giles, Lionel",
                           ["Writer of added commentary", "Translator"],
                           "1875", "1958")
        self.assert_author(griffith, "Griffith, Samuel B.",
                           ["Editor", "Author of introduction", "Translator"],
                           self.MISSING, self.MISSING)
        self.assert_author(
            cleary, "Cleary, Thomas F.", ["Editor", "Translator"],
            "1949", self.MISSING)

        self.assert_author(
            sawyer, "Sawyer, Ralph D.", ["Editor", "Author of introduction",
                                         "Translator"],
            self.MISSING, self.MISSING)

        # Once contributors start getting explicit roles, a
        # contributor with no explicit role is treated as 'unknown'
        # rather than 'author.'
        self.assert_author(
            clavell, "Clavell, James", [Contributor.UNKNOWN_ROLE],
            self.MISSING, self.MISSING)

        # These are titles we don't parse as well as we ought, but
        # we are able to handle them without crashing.
        self.assert_parse(
            u"梅爾維爾 (Melville, Herman), 1819-1891",
            u"梅爾維爾 (Melville, Herman)", Contributor.PRIMARY_AUTHOR_ROLE,
            birthdate="1819", deathdate="1891")

        self.assert_parse(
            u"卡洛爾 (Carroll, Lewis), (英), 1832-1898",
            u"卡洛爾 (Carroll, Lewis), (英)", Contributor.PRIMARY_AUTHOR_ROLE,
            birthdate="1832", deathdate="1898")

        s = u"杜格孫 (Dodgson, Charles Lutwidge,1832-1896)"
        self.assert_parse(s, s, Contributor.PRIMARY_AUTHOR_ROLE)


class TestTitleAuthorLookupCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestTitleAuthorLookupCoverageProvider, self).setup()
        self.edition, ignore = self._edition(
            with_license_pool=True, data_source_name=DataSource.GUTENBERG
        )
        self.identifier = self.edition.primary_identifier
        self.api = MockOCLCClassifyAPI()
        self.provider = TitleAuthorLookupCoverageProvider(self._db, api=self.api, collection=self._collection())

    def sample_data(self, filename):
        return sample_data(filename, 'oclc_classify')

    def test_oclc_safe_title(self):
        # Returns an empty string when passed None.
        eq_(self.provider.oclc_safe_title(None), '')

        # Returns the original title if it has no special characters.
        title = 'The Curious Incident of the Dog in the Night-Time'
        eq_(self.provider.oclc_safe_title(title), title)

        # Returns a title without special characters otherwise.
        title = '3 Blind Mice & Other Tales: A Bedtime Reader'
        expected = '3 Blind Mice  Other Tales A Bedtime Reader'
        eq_(self.provider.oclc_safe_title(title), expected)

    def test_process_item_without_book_information(self):
        def process_item():
            lookup = self.sample_data('jane_eyre.xml')
            self.api.queue_lookup(lookup)
            return self.provider.process_item(self.identifier)

        # Create an edition without a title
        self.edition.title = None

        result = process_item()
        eq_(True, isinstance(result, CoverageFailure))
        eq_(True, result.exception.endswith('title and author!'))

        # Create an edition without an author
        self.edition.title = u"Jane Eyre"
        self._db.delete(self.edition.contributions[0])
        self._db.commit()

        result = process_item()
        eq_(True, isinstance(result, CoverageFailure))
        eq_(True, result.exception.endswith('title and author!'))

        # Create an edition with both a title and author
        bronte = self._contributor(sort_name="Bronte, Charlotte")[0]
        self.edition.add_contributor(bronte, Contributor.AUTHOR_ROLE)

        result = process_item()
        eq_(result, self.identifier)

    def test_process_item_when_parsing_error_occurs(self):
        class AlwaysErrorsClassifyProvider(TitleAuthorLookupCoverageProvider):
            def parse_edition_data(self, *args, **kwargs):
                raise IOError('It broke!')

        provider = AlwaysErrorsClassifyProvider(self._db, api=self.api)
        self.api.queue_lookup(self.sample_data('jane_eyre.xml'))
        result = provider.process_item(self.identifier)

        eq_(True, isinstance(result, CoverageFailure))
        eq_(self.identifier, result.obj)
        eq_('It broke!', result.exception)
        eq_(provider.data_source, result.data_source)
