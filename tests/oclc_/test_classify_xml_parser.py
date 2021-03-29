# encoding: utf-8

import json
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
        assert self.parser.SINGLE_WORK_DETAIL_STATUS == code
        assert Identifier.OCLC_WORK == owi_data.type
        assert None == owi_data.identifier
        assert 1 == owi_data.weight

        multi_tree = self.tree("multi_work_with_owis.xml")
        code, owi_data = self.parser.initial_look_up(multi_tree)
        assert self.parser.MULTI_WORK_STATUS == code
        assert 2 == len(owi_data)
        [id_1, id_2] = owi_data

        assert Identifier.OCLC_WORK == id_1.type
        assert "48446512" == id_1.identifier
        assert 1 == id_1.weight

        assert Identifier.OCLC_WORK == id_2.type
        assert "48525129" == id_2.identifier
        assert 1 == id_2.weight

    def test_parse(self):
        identifier = self._identifier()
        tree = self.tree("single_work_response.xml")
        metadata = Metadata(
            data_source=DataSource.OCLC,
            primary_identifier=identifier
        )
        result = self.parser.parse(tree, metadata)
        assert [identifier] == result.identifiers

        # Contributors
        [parker, tanner, hayford, melville] = result.contributors
        assert '4947338' == parker.viaf
        assert 'n50050335' == parker.lc
        assert [Contributor.EDITOR_ROLE] == parker.roles

        assert '51716047' == tanner.viaf
        assert 'n79059764' == tanner.lc
        assert (set([Contributor.UNKNOWN_ROLE, Contributor.EDITOR_ROLE,
                 Contributor.INTRODUCTION_ROLE, Contributor.AUTHOR_ROLE]) ==
            set(tanner.roles)
        )

        assert '34482742' == hayford.viaf
        assert 'n50025038' == hayford.lc
        assert (set([Contributor.ASSOCIATED_ROLE, Contributor.EDITOR_ROLE]) ==
            set(hayford.roles))

        assert '27068555' == melville.viaf
        assert 'n79006936' == melville.lc
        assert [Contributor.AUTHOR_ROLE] == melville.roles
        assert {'deathDate': '1891', 'birthDate': '1819'} == melville.extra


        # Measurements
        def get_measurement(quantity):
            [measurement] = [m.value for m in result.measurements if m.quantity_measured == self.parser.MEASUREMENT_MAPPING[quantity]]
            return measurement

        assert 46983 == get_measurement("holdings")
        assert 2781 == get_measurement("editions")

        # Subjects
        def get_subjects(type):
            for s in result.subjects:
                if s.type == type:
                    yield s

        [ddc] = get_subjects("DDC")
        assert "813.3" == ddc.identifier
        assert 21183 == ddc.weight

        [lcc] = get_subjects("LCC")
        assert "PS2384" == lcc.identifier
        assert 22460 == lcc.weight

        fasts = list(get_subjects("FAST"))
        assert ['1174284', '1174266', '801923', '1116147', '1174307', '1016699', '1110122', '1356235'] == [x.identifier for x in fasts]
        assert [32058, 31482, 29933, 19086, 18913, 17294, 6893, 4512] == [x.weight for x in fasts]
        assert (['Whaling', 'Whales', 'Ahab, Captain (Fictitious character)', 'Ship captains', 'Whaling ships', 'Mentally ill', 'Sea stories', 'Moby Dick (Melville, Herman)'] ==
            [x.name for x in fasts])
