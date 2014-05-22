import pkgutil
import StringIO
from integration.oclc import (
    OCLCXMLParser,
)
from nose.tools import set_trace, eq_

from model import (
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

        status, swids = OCLCXMLParser.parse(self._db, xml)
        eq_(OCLCXMLParser.MULTI_WORK_STATUS, status)

        eq_(['10106023', '10190890', '10360105', '105446800', '10798812', '11065951', '122280617', '12468538', '13206523', '13358012', '13424036', '14135019', '1413894', '153927888', '164732682', '1836574', '22658644', '247734888', '250604212', '26863225', '34644035', '46935692', '474972877', '51088077', '652035540'], sorted(swids))

    def test_extract_single_work(self):
        """We can turn a single-work response into a list of WorkRecords.

        One record for the OCLC Work ID, and one for each OCLC Number.
        """

        xml = pkgutil.get_data(
            "tests.integration",
            "files/oclc_single_work_response.xml")

        status, records = OCLCXMLParser.parse(self._db, xml)
        eq_(OCLCXMLParser.SINGLE_WORK_DETAIL_STATUS, status)

        # We expect 3 work records: one for the work and two for editions. (In the real response
        # there are 25 editions; I cut them to make the test run faster.)
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

        work_authors = sorted([x['name'] for x in work.authors])
        edition_authors = sorted([x['name'] for x in edition.authors])
        # The work has a ton of authors, collated from all the
        # editions.
        eq_(['Cliffs Notes, Inc.', 
             'Hayford, Harrison [Associated name; Editor]', 
             'Kent, Rockwell, 1882-1971 [Illustrator]', 
             'Melville, Herman, 1819-1891',
             'Parker, Hershel [Editor]', 
             'Tanner, Tony [Editor; Commentator for written text; Author of introduction; Author]',
             ], work_authors)
        # The edition only has one author.
        eq_(['Melville, Herman, 1819-1891'], edition_authors)

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
