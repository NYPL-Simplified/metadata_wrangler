# encoding: utf-8

import json
from nose.tools import set_trace, eq_
from .. import (
    DatabaseTest,
    sample_data
)
from lxml import etree
from core.model import Contributor, DataSource, Identifier
from core.metadata_layer import Metadata
from oclc.classify import OCLCClassifyXMLParser

class TestOCLCClassifyXMLParser(DatabaseTest):

    parser =  OCLCClassifyXMLParser()

    def sample_data(self, filename):
        return sample_data(filename, 'oclc_classify')

    def tree(self, filename):
        xml = self.sample_data(filename)
        return etree.fromstring(xml, parser=etree.XMLParser(recover=True))

    def test_initial_look_up(self):
        single_tree = self.tree("single_work_response.xml")
        code, [owi_data] = self.parser.initial_look_up(single_tree)
        eq_(self.parser.SINGLE_WORK_DETAIL_STATUS, code)
        eq_(Identifier.OCLC_WORK, owi_data.type)
        eq_(None, owi_data.identifier)
        eq_(1, owi_data.weight)

        multi_tree = self.tree("multi_work_with_owis.xml")
        code, owi_data = self.parser.initial_look_up(multi_tree)
        eq_(self.parser.MULTI_WORK_STATUS, code)
        eq_(2, len(owi_data))
        [id_1, id_2] = owi_data

        eq_(Identifier.OCLC_WORK, id_1.type)
        eq_("48446512", id_1.identifier)
        eq_(1, id_1.weight)

        eq_(Identifier.OCLC_WORK, id_2.type)
        eq_("48525129", id_2.identifier)
        eq_(1, id_2.weight)

    def test_parse(self):
        identifier = self._identifier()
        tree = self.tree("single_work_response.xml")
        metadata = Metadata(
            data_source=DataSource.OCLC,
            primary_identifier=identifier
        )
        result = self.parser.parse(tree, metadata)
        eq_([identifier], result.identifiers)

        # Contributors
        [parker, tanner, hayford, melville] = result.contributors
        eq_('4947338', parker.viaf)
        eq_('n50050335', parker.lc)
        eq_([Contributor.EDITOR_ROLE], parker.roles)

        eq_('51716047', tanner.viaf)
        eq_('n79059764', tanner.lc)
        eq_(set([Contributor.UNKNOWN_ROLE, Contributor.EDITOR_ROLE,
                 Contributor.INTRODUCTION_ROLE, Contributor.AUTHOR_ROLE]),
            set(tanner.roles)
        )

        eq_('34482742', hayford.viaf)
        eq_('n50025038', hayford.lc)
        eq_(set([Contributor.ASSOCIATED_ROLE, Contributor.EDITOR_ROLE]),
            set(hayford.roles))

        eq_('27068555', melville.viaf)
        eq_('n79006936', melville.lc)
        eq_([Contributor.AUTHOR_ROLE], melville.roles)
        eq_({'deathDate': '1891', 'birthDate': '1819'}, melville.extra)


        # Measurements
        def get_measurement(quantity):
            [measurement] = [m.value for m in result.measurements if m.quantity_measured == self.parser.MEASUREMENT_MAPPING[quantity]]
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
