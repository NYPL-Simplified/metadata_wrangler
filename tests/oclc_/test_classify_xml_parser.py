# encoding: utf-8

import json
from nose.tools import set_trace, eq_
from .. import (
    DatabaseTest,
    sample_data
)
from lxml import etree
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

        expected_viafs = ['4947338', '51716047', '34482742', '27068555']
        actual_viafs = [x.viaf for x in result.contributors]
        eq_(set(expected_viafs), set(actual_viafs))

        expected_lcs = ['n50050335', 'n79059764', 'n50025038', 'n79006936']
        actual_lcs = [x.lc for x in result.contributors]
        eq_(set(expected_lcs), set(actual_lcs))

        actual_names = [x.sort_name for x in result.contributors]
        assert "Melville, Herman" in actual_names

        editor = next(contributor for contributor in result.contributors if contributor.viaf == "4947338")
        assert "Editor" in editor.roles

        # Measurements

        expected_holdings = 46983
        actual_holdings = next(m.value for m in result.measurements if m.quantity_measured == "holdings")
        eq_(expected_holdings, actual_holdings)

        expected_editions = 2781
        actual_editions = next(m.value for m in result.measurements if m.quantity_measured == "editions")
        eq_(expected_editions, actual_editions)

        # Subjects

        expected_ddc_id = "813.3"
        actual_ddc_id = next(s.identifier for s in result.subjects if s.type == "DDC")
        expected_ddc_weight = 21183
        actual_ddc_weight = next(s.weight for s in result.subjects if s.type == "DDC")
        eq_(expected_ddc_id, actual_ddc_id)
        eq_(expected_ddc_weight, actual_ddc_weight)

        expected_lcc_id = "PS2384"
        actual_lcc_id = next(s.identifier for s in result.subjects if s.type == "LCC")
        expected_lcc_weight = 22460
        actual_lcc_weight = next(s.weight for s in result.subjects if s.type == "LCC")
        eq_(expected_lcc_id, actual_lcc_id)
        eq_(expected_lcc_weight, actual_lcc_weight)

        fast_data = [s for s in result.subjects if s.type == "FAST"]

        expected_fast_ids = ['1174284', '1174266', '801923', '1116147', '1174307', '1016699', '1110122', '1356235']
        actual_fast_ids = [x.identifier for x in fast_data]
        eq_(set(expected_fast_ids), set(actual_fast_ids))

        expected_fast_weights = [32058, 31482, 29933, 19086, 18913, 17294, 6893, 4512]
        actual_fast_weights = [x.weight for x in fast_data]
        expected_weights_with_ids = zip(expected_fast_ids, expected_fast_weights)
        actual_weights_with_ids = zip(actual_fast_ids, actual_fast_weights)
        eq_(set(expected_weights_with_ids), set(actual_weights_with_ids))

        subject_name_1 = next(item.name for item in fast_data if item.identifier == expected_fast_ids[0])
        eq_("Whaling", subject_name_1)
        subject_name_2 = next(item.name for item in fast_data if item.identifier == expected_fast_ids[7])
        eq_("Moby Dick (Melville, Herman)", subject_name_2)
