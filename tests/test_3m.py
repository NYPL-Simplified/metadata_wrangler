from nose.tools import set_trace, eq_
import datetime
import os
from ..core.model import (
    Contributor,
    Resource,
    Identifier,
    Edition,
)
from ..integration.threem import (
    ItemListParser,
)

class TestItemListParser(object):

    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "3m")

    def get_data(self, filename):
        path = os.path.join(self.resource_path, filename)
        return open(path).read()

    def text_parse_author_string(cls):
        authors = ItemListParser.author_names_from_string(
            "Walsh, Jill Paton; Sayers, Dorothy L.")
        eq_(authors, ["Walsh, Jill Paton", "Sayers, Dorothy L."])

    def test_item_list(cls):
        data = self.get_data("3m_item_metadata_list.xml")
        
        data = [(id, raw, cooked)
                for (id, raw,cooked)  in ItemListParser().parse(data)]

        # There should be 25 items in the list.
        eq_(25, len(data))

        # Do a spot check of the first item in the list
        id, raw, cooked = data[0]

        eq_("ddf4gr9", id)

        assert raw.startswith("<Item")

        eq_(id, cooked[Identifier][Identifier.THREEM_ID])
        eq_("9781250015280", cooked[Identifier][Identifier.ISBN])
        eq_("The Incense Game", cooked[Edition.title])
        eq_("A Novel of Feudal Japan", cooked[Edition.subtitle])
        eq_(["Rowland, Laura Joh"], cooked[Contributor])
        eq_("eng", cooked[Edition.language])
        eq_("St. Martin's Press", cooked[Edition.publisher])
        eq_("1.2 MB", cooked['extra']['fileSize'])
        eq_("304", cooked['extra']['numberOfPages'])
        eq_(datetime.datetime(year=2012, month=9, day=17), cooked[Edition.published])

        summary = cooked[Resource][Resource.DESCRIPTION]
        assert summary.startswith("<b>Winner")

        # Check the links

        image = cooked[Resource][Resource.IMAGE]
        assert image.startswith("http://ebook.3m.com/delivery")

        alternate = cooked[Resource]["alternate"]
        assert alternate.startswith("http://ebook.3m.com/library")
