# encoding: utf-8

import pkgutil
import StringIO
from integration.oclc import (
    OCLCXMLParser,
)
from nose.tools import set_trace, eq_

from model import (
    Author,
    SubjectType,
    WorkIdentifier,
    WorkRecord,
    )

from tests.db import (
    setup_module,
    teardown_module,
    DatabaseTest,
)

class TestParser(DatabaseTest):

    def test_extract_multiple_works(self):
        """We can turn a multi-work response into a list of SWIDs."""
        xml = pkgutil.get_data(
            "tests.integration",
            "files/oclc_multi_work_response.xml")

        status, swids = OCLCXMLParser.parse(self._db, xml, languages=["eng"])
        eq_(OCLCXMLParser.MULTI_WORK_STATUS, status)

        eq_(25, len(swids))
        eq_(['10106023', '10190890', '10360105', '105446800', '10798812', '11065951', '122280617', '12468538', '13206523', '13358012', '13424036', '14135019', '1413894', '153927888', '164732682', '1836574', '22658644', '247734888', '250604212', '26863225', '34644035', '46935692', '474972877', '51088077', '652035540'], sorted(swids))
        
        # For your convenience in verifying what I say in
        # test_extract_multiple_works_with_author_restriction().
        assert '13424036' in swids

    def test_extract_multiple_works_with_title_restriction(self):
        """We can choose to only accept works similar to a given title."""
        xml = pkgutil.get_data(
            "tests.integration",
            "files/oclc_multi_work_response.xml")

        # This will only accept titles that contain exactly the same
        # words as "Dick Moby". Only four titles in the sample data
        # meet that criterion.
        status, swids = OCLCXMLParser.parse(
            self._db, xml, title="Dick Moby", title_similarity=1)
        eq_(4, len(swids))

        # Stopwords "a", "an", and "the" are removed before
        # consideration.
        status, swids = OCLCXMLParser.parse(
            self._db, xml, title="A an the Moby-Dick", title_similarity=1)
        eq_(4, len(swids))

        # This is significantly more lax, so it finds more results.
        # The exact number isn't important.
        status, swids = OCLCXMLParser.parse(
            self._db, xml, title="Dick Moby", title_similarity=0.5)
        assert len(swids) > 4

        # This is so lax as to be meaningless. It accepts everything.
        status, swids = OCLCXMLParser.parse(
            self._db, xml, title="Dick Moby", title_similarity=0)
        eq_(25, len(swids))

        # This isn't particularly strict, but none of the books in
        # this dataset have titles that resemble this title, so none
        # of their SWIDs show up here.
        status, swids = OCLCXMLParser.parse(
            self._db, xml, title="None Of These Words Show Up Whatsoever")
        eq_(0, len(swids))


    def test_extract_multiple_works_with_author_restriction(self):
        """We can choose to only accept works by a given author."""
        xml = pkgutil.get_data(
            "tests.integration",
            "files/oclc_multi_work_response.xml")

        status, swids = OCLCXMLParser.parse(
            self._db, xml, languages=["eng"], authors=["No Such Person"])
        # This person is not listed as an author of any work in the dataset,
        # so none of those works were picked up.
        eq_(0, len(swids))

        status, swids = OCLCXMLParser.parse(
            self._db, xml, languages=["eng"], authors=["Herman Melville"])
        
        # We picked up 20 of the 25 works in the dataset.
        eq_(20, len(swids))

        # The missing five (as you can verify by looking at
        # oclc_multi_work_response.xml) either don't credit Herman
        # Melville at all (the 1956 Gregory Peck movie "Moby Dick"),
        # or credit him as "Associated name" rather than as an author
        # (four books about "Moby Dick").
        for missing in '10798812', '13424036', '22658644', '250604212', '474972877':
            assert missing not in swids

    def test_extract_single_work(self):
        """We can turn a single-work response into a list of WorkRecords.

        One record for the OCLC Work ID, and one for each OCLC Number.
        """

        xml = pkgutil.get_data(
            "tests.integration",
            "files/oclc_single_work_response.xml")

        status, records = OCLCXMLParser.parse(
            self._db, xml, languages=["eng"])
        eq_(OCLCXMLParser.SINGLE_WORK_DETAIL_STATUS, status)

        # We expect 3 work records: one for the work and two for
        # English editions. (In the real response there are 25
        # editions; I cut them to make the test run faster.)
        eq_(3, len(records))

        # Work and edition both have a primary identifier.
        work = records[0]
        work_id = work.primary_identifier
        eq_(WorkIdentifier.OCLC_WORK, work_id.type)
        eq_('4687', work_id.identifier)

        edition = records[1]
        edition_id = edition.primary_identifier
        eq_(WorkIdentifier.OCLC_NUMBER, edition_id.type)
        eq_('47010459', edition_id.identifier)

        # The edition is identified with the work, and vice versa.
        assert edition_id in work.equivalent_identifiers
        assert work_id in edition.equivalent_identifiers

        eq_("Moby Dick", work.title)
        eq_("Moby Dick", edition.title)

        # The work has a ton of contributors, collated from all the
        # editions.
        work_contributors = sorted([x['name'] for x in work.authors])
        eq_(['Cliffs Notes, Inc.', 
             'Hayford, Harrison', 
             'Kent, Rockwell', 
             'Melville, Herman',
             'Parker, Hershel', 
             'Tanner, Tony',
             ], work_contributors)

        # But only some of them are considered 'authors' by OCLC.
        work_authors = sorted([x['name'] for x in work.authors
                               if Author.AUTHOR_ROLE in x['roles']])
        eq_(['Melville, Herman', 'Tanner, Tony'], work_authors)

        # The edition only has one contributor.
        edition_authors = sorted([x['name'] for x in edition.authors])
        eq_(['Melville, Herman'], edition_authors)

        eq_([], work.languages)
        eq_(["eng"], edition.languages)

        [ws] = work.subjects[SubjectType.DDC]
        eq_("813.3", ws['id'])
        eq_(21183, ws['weight'])
        eq_("813.3", edition.subjects[SubjectType.DDC][0]['id'])

        [ws] = work.subjects[SubjectType.LCC]
        eq_("PS2384", ws['id'])
        eq_(22460, ws['weight'])
        eq_("PS2384", edition.subjects[SubjectType.LCC][0]['id'])

        fast = sorted(
            [(x['value'], x['id'], x['weight'])
             for x in work.subjects[SubjectType.FAST]])

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

        # If we were to parse the same data looking for Spanish works,
        # we would get 2 work records: one for the work and one
        # for a Spanish edition that didn't show up in the English
        # list.
        status, records = OCLCXMLParser.parse(
            self._db, xml, languages=["spa"])
        eq_(2, len(records))
        eq_(["spa"], records[1].languages)

class TestAuthorParser(object):

    MISSING = object()

    def assert_author(self, author, name, role=Author.AUTHOR_ROLE, 
                      birthdate=None, deathdate=None):
        eq_(author[Author.NAME], name)
        if role:
            if not isinstance(role, list) and not isinstance(role, tuple):
                role = [role]
            eq_(role, author[Author.ROLES])
        if birthdate is self.MISSING:
            assert Author.BIRTH_DATE not in author
        elif birthdate:
            eq_(birthdate, author[Author.BIRTH_DATE])
        if deathdate is self.MISSING:
            assert Author.DEATH_DATE not in author
        elif deathdate:
            eq_(deathdate, author[Author.DEATH_DATE])

    def assert_parse(self, string, name, role=Author.AUTHOR_ROLE, 
                     birthdate=None, deathdate=None):
        [author] = OCLCXMLParser.parse_author_string(string)
        self.assert_author(author, name, role, birthdate, deathdate)

    def test_authors(self):

        self.assert_parse(
            "Carroll, Lewis, 1832-1898",
            "Carroll, Lewis", Author.AUTHOR_ROLE, "1832", "1898")

        self.assert_parse(
            "Kent, Rockwell, 1882-1971 [Illustrator]",
            "Kent, Rockwell", Author.ILLUSTRATOR_ROLE,
            "1882", "1971")

        self.assert_parse(
            u"Карролл, Лувис, 1832-1898.",
            u"Карролл, Лувис", Author.AUTHOR_ROLE, birthdate="1832",
            deathdate="1898")

        kerry, melville = OCLCXMLParser.parse_author_string(
            "McSweeney, Kerry, 1941- | Melville, Herman, 1819-1891")
        self.assert_author(kerry, "McSweeney, Kerry", Author.AUTHOR_ROLE,
                           birthdate="1941", deathdate=self.MISSING)

        self.assert_author(
            melville, "Melville, Herman", Author.AUTHOR_ROLE,
            birthdate="1819", deathdate="1891")


        # Check out this mess.
        s = "Sunzi, active 6th century B.C. | Giles, Lionel, 1875-1958 [Writer of added commentary; Translator] | Griffith, Samuel B. [Editor; Author of introduction; Translator] | Cleary, Thomas F., 1949- [Editor; Translator] | Sawyer, Ralph D. [Editor; Author of introduction; Translator] | Clavell, James"
        sunzi, giles, griffith, cleary, sawyer, clavell = (
            OCLCXMLParser.parse_author_string(s))

        # This one could be better.
        self.assert_author(sunzi, "Sunzi, active 6th century B.C.",
                           Author.AUTHOR_ROLE)
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
            clavell, "Clavell, James", [Author.UNKNOWN_ROLE],
            self.MISSING, self.MISSING)

        # These are titles we don't parse as well as we ought, but
        # we are able to handle them without crashing.
        self.assert_parse(
            u"梅爾維爾 (Melville, Herman), 1819-1891",
            u"梅爾維爾 (Melville, Herman)", birthdate="1819", deathdate="1891")

        self.assert_parse(
            u"卡洛爾 (Carroll, Lewis), (英), 1832-1898",
            u"卡洛爾 (Carroll, Lewis), (英)", birthdate="1832", deathdate="1898")

        s = u"杜格孫 (Dodgson, Charles Lutwidge,1832-1896)"
        self.assert_parse(s, s)
