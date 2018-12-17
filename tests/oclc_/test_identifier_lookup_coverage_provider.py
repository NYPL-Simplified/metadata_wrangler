# encoding: utf-8

import json
from nose.tools import set_trace, eq_
from .. import (
    DatabaseTest,
    sample_data
)
from lxml import etree
from core.model import Contributor, Identifier
from core.metadata_layer import IdentifierData
from oclc.classify import (
    IdentifierLookupCoverageProvider,
    OCLCClassifyXMLParser,
)

class MockParser(OCLCClassifyXMLParser):
    def __init__(self):
        self.call_count = 0

    def parse(self, db, tree, identifiers):
        self.call_count += 1
        return dict(
            identifiers=identifiers,
        )

class MockParserSingle(MockParser):
    def initial_look_up(self, db, tree):
        return 2, []

class MockParserMulti(MockParser):
    def initial_look_up(self, db, tree):
        results = []
        owi_numbers = ["48446512", "48525129"]
        for number in owi_numbers:
            data = IdentifierData(Identifier.OCLC_WORK, number)
            results.append(data)
        return 4, results

class MockProvider(IdentifierLookupCoverageProvider):

    def _single(self, db, tree, identifier):
        self.called_with = dict(tree=tree, identifier=identifier)

    def _multiple(self, db, owi_data, identifier):
        self.called_with = dict(owi_data=owi_data, identifier=identifier)

class MockProviderSingle(MockProvider):
    def _get_tree(self, isbn):
        xml = sample_data("single_work_with_isbn.xml", "oclc_classify")
        return etree.fromstring(xml, parser=etree.XMLParser(recover=True))

class MockProviderMulti(MockProvider):
    def _get_tree(self, isbn):
        xml = sample_data("multi_work_with_owis.xml", "oclc_classify")
        return etree.fromstring(xml, parser=etree.XMLParser(recover=True))

class TestIdentifierLookupCoverageProvider(DatabaseTest):

    SINGLE_ISBN = "9781620281932"
    MULTI_ISBN = "0345391837"

    def _tree(self, type):
        if type == "single":
            return MockProviderSingle(self._default_collection)._get_tree(self.SINGLE_ISBN)
        else:
            return MockProviderMulti(self._default_collection)._get_tree(self.MULTI_ISBN)

    def _id(self, type):
        if type == "single":
            return self._identifier(Identifier.ISBN, self.SINGLE_ISBN)
        else:
            return self._identifier(Identifier.ISBN, self.MULTI_ISBN)

    def test_process_item_single(self):
        provider = MockProviderSingle(self._default_collection)
        provider.parser = MockParserSingle()
        id = self._id("single")

        result = provider.process_item(id)
        eq_(etree.tostring(provider.called_with["tree"]), etree.tostring(provider._get_tree(self.SINGLE_ISBN)))
        eq_(provider.called_with["identifier"], id)
        eq_(result, id)

    def test_process_item_multi(self):
        provider = MockProviderMulti(self._default_collection)
        provider.parser = MockParserMulti()
        id = self._id("multi")

        result = provider.process_item(id)
        eq_([x.identifier for x in provider.called_with["owi_data"]], ["48446512", "48525129"])
        eq_(provider.called_with["identifier"], id)
        eq_(result, id)

    def test__single(self):
        provider = IdentifierLookupCoverageProvider(self._default_collection)
        provider.parser = MockParserSingle()
        tree, identifier = self._tree("single"), self._id("single")

        result = provider._single(self._db, tree, identifier)[0]
        result_ids = result.get('identifiers')
        eq_(len(result_ids), 1)

        eq_(result_ids[0].type, Identifier.ISBN)
        eq_(result_ids[0].identifier, self.SINGLE_ISBN)

        eq_(provider.parser.call_count, 1)

    def test__multiple(self):
        provider = IdentifierLookupCoverageProvider(self._default_collection)
        provider.parser = MockParserMulti()
        tree, identifier = self._tree("multi"), self._id("multi")

        code, owi_data = provider.parser.initial_look_up(self._db, tree)
        result_1, result_2 = provider._multiple(self._db, owi_data, identifier)

        result_1_ids = result_1.get("identifiers")
        eq_(len(result_1_ids), 2)

        isbn_1 = result_1_ids[0]
        eq_(isbn_1.type, Identifier.ISBN)
        eq_(isbn_1.identifier, self.MULTI_ISBN)
        owi_1 = result_1_ids[1]
        eq_(owi_1.type, Identifier.OCLC_WORK)
        eq_(owi_1.identifier, "48446512")

        result_2_ids = result_2.get("identifiers")
        eq_(len(result_2_ids), 2)

        isbn_2 = result_2_ids[0]
        eq_(isbn_2.type, Identifier.ISBN)
        eq_(isbn_2.identifier, self.MULTI_ISBN)
        owi_2 = result_2_ids[1]
        eq_(owi_2.type, Identifier.OCLC_WORK)
        eq_(owi_2.identifier, "48525129")

        eq_(provider.parser.call_count, 2)
