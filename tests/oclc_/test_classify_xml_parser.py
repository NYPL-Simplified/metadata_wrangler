# encoding: utf-8

import json
from nose.tools import set_trace, eq_
from .. import (
    DatabaseTest,
    sample_data
)
from lxml import etree
from core.model import Contributor
from oclc.classify import OCLCClassifyXMLParser

class TestOCLCClassifyXMLParser(DatabaseTest):

    def sample_data(self, filename):
        return sample_data(filename, 'oclc_classify')

    def test_parse(self):
        parser = OCLCClassifyXMLParser()
        xml = self.sample_data("single_work_response.xml")
        tree = etree.fromstring(xml, parser=etree.XMLParser(recover=True))
        identifier = self._identifier()
        result = parser.parse(self._db, tree, identifier)

        eq_(identifier, result.identifiers)

        # Contributors
        [parker, tanner, hayford, melville] = result.contributors
        eq_('4947338', parker.viaf)
        eq_('n50050335', parker.lc)
        eq_([Contributor.EDITOR_ROLE], parker.roles)

        eq_('51716047', tanner.viaf)
        eq_('n79059764', tanner.lc)
        eq_(set([Contributor.UNKNOWN_ROLE, Contributor.EDITOR_ROLE,
                 Contributor.INTRODUCTION_ROLE, Contributor.AUTHOR_ROLE]),
            tanner.roles
        )

        eq_('34482742', hayford.viaf)
        eq_('n50025038', hayford.lc)
        eq_(set([Contributor.ASSOCIATED_ROLE, Contributor.EDITOR_ROLE]),
            hayford.roles)

        eq_('27068555', melville.viaf)
        eq_('n79006936', melville.lc)
        eq_([Contributor.AUTHOR_ROLE], melville.roles)
        eq_({'deathDate': '1891', 'birthDate': '1819'}, melville.extra)


        # Measurements
        def get_measurement(quantity):
            [measurement] = [m.value for m in result.measurements if m.quantity_measured == quantity]
            return measurement

        eq_(46983, get_measurement("holdings"))
        eq_(2781, get_measurement("editions"))

        # Subjects
        def get_subjects(type):
            for s in result.subjects:
                if s.type == type:
                    yield s

        [ddc] = get_subjects("DDC")
        eq_("813.3", ddc.identifier)
        eq_(21183, ddc.weight)

        [lcc] = get_subjects("LCC")
        eq_("PS2384", lcc.identifier)
        eq_(22460, lcc.weight)

        fasts = list(get_subjects("FAST"))
        eq_(['1174284', '1174266', '801923', '1116147', '1174307', '1016699', '1110122', '1356235'], [x.identifier for x in fasts])
        eq_([32058, 31482, 29933, 19086, 18913, 17294, 6893, 4512], [x.weight for x in fasts])
        eq_(['Whaling', 'Whales', 'Ahab, Captain (Fictitious character)', 'Ship captains', 'Whaling ships', 'Mentally ill', 'Sea stories', 'Moby Dick (Melville, Herman)'],
            [x.name for x in fasts])
