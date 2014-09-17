from nose.tools import set_trace, eq_
import datetime
import pkgutil
from model import (
    Edition,
    Event,
    LicensedWork,
)
from integration.threem import (
    CirculationParser,
    EventParser,
    ItemListParser,
)

class Test3MEventParser(object):

    # Sample event feed to test out the parser.
    TWO_EVENTS = """<LibraryEventBatch xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <PublishId>1b0d6667-a10e-424a-9f73-fb6f6d41308e</PublishId>
  <PublishDateTimeInUTC>2014-04-14T13:59:05.6920303Z</PublishDateTimeInUTC>
  <LastEventDateTimeInUTC>2014-04-03T00:00:34</LastEventDateTimeInUTC>
  <Events>
    <CloudLibraryEvent>
      <LibraryId>test-library</LibraryId>
      <EventId>event-1</EventId>
      <EventType>CHECKIN</EventType>
      <EventStartDateTimeInUTC>2014-04-03T00:00:23</EventStartDateTimeInUTC>
      <EventEndDateTimeInUTC>2014-04-03T00:00:23</EventEndDateTimeInUTC>
      <ItemId>theitem1</ItemId>
      <ISBN>900isbn1</ISBN>
      <PatronId>patronid1</PatronId>
      <EventPublishDateTimeInUTC>2014-04-14T13:59:05</EventPublishDateTimeInUTC>
    </CloudLibraryEvent>
    <CloudLibraryEvent>
      <LibraryId>test-library</LibraryId>
      <EventId>event-2</EventId>
      <EventType>CHECKOUT</EventType>
      <EventStartDateTimeInUTC>2014-04-03T00:00:34</EventStartDateTimeInUTC>
      <EventEndDateTimeInUTC>2014-04-02T23:57:37</EventEndDateTimeInUTC>
      <ItemId>theitem2</ItemId>
      <ISBN>900isbn2</ISBN>
      <PatronId>patronid2</PatronId>
      <EventPublishDateTimeInUTC>2014-04-14T13:59:05</EventPublishDateTimeInUTC>
    </CloudLibraryEvent>
  </Events>
</LibraryEventBatch>
"""

    def test_parse_event_batch(self):
        # Parsing the XML gives us two events.
        event1, event2 = EventParser().process_all(self.TWO_EVENTS)

        # Events have been tagged as originating from 3M.
        for i in event1, event2:
            eq_(EventParser.EVENT_SOURCE, event1[Event.SOURCE])

        # Source ID, patron ID, and ISBN have bene picked up.
        for k, prefix in (
                (Event.PATRON_ID, "patronid"),
                (Event.SOURCE_BOOK_ID, "theitem"),
                (LicensedWork.ISBN, "900isbn")):
            eq_(prefix + "1", event1[k])
            eq_(prefix + "2", event2[k])

        # The event name has been translated.
        eq_(Event.CHECKIN, event1[Event.EVENT_TYPE])
        eq_(Event.CHECKOUT, event2[Event.EVENT_TYPE])

        # The first event has no end time, since its start and end
        # times were identical.
        assert not Event.END_TIME in event1
        assert Event.END_TIME in event2

        # Verify that start and end time were parsed correctly.
        correct_start = datetime.datetime(2014, 4, 3, 0, 0, 34)
        correct_end = datetime.datetime(2014, 4, 2, 23, 57, 37)
        eq_(correct_start, event2[Event.START_TIME])
        eq_(correct_end, event2[Event.END_TIME])


class Test3MCirculationParser(object):

    # Sample circulation feed for testing the parser.

    TWO_CIRCULATION_STATUSES = """
<ArrayOfItemCirculation xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<ItemCirculation>
  <ItemId>item1</ItemId>
  <ISBN13>900isbn1</ISBN13>
  <TotalCopies>2</TotalCopies>
  <AvailableCopies>0</AvailableCopies>
  <Checkouts>
    <Patron>
      <PatronId>patron1</PatronId>
      <EventStartDateInUTC>2014-03-24T13:10:51</EventStartDateInUTC>
      <EventEndDateInUTC>2014-04-15T13:10:51</EventEndDateInUTC>
      <Position>1</Position>
    </Patron>
  </Checkouts>
  <Holds/>
  <Reserves>
    <Patron>
      <PatronId>patron2</PatronId>
      <EventStartDateInUTC>2014-03-24T13:10:51</EventStartDateInUTC>
      <EventEndDateInUTC>2014-04-15T13:10:51</EventEndDateInUTC>
      <Position>1</Position>
    </Patron>
  </Reserves>
</ItemCirculation>

<ItemCirculation>
  <ItemId>item2</ItemId>
  <ISBN13>900isbn2</ISBN13>
  <TotalCopies>1</TotalCopies>
  <AvailableCopies>0</AvailableCopies>
  <Checkouts>
    <Patron>
      <PatronId>patron3</PatronId>
      <EventStartDateInUTC>2014-04-23T22:14:02</EventStartDateInUTC>
      <EventEndDateInUTC>2014-05-14T22:14:02</EventEndDateInUTC>
      <Position>1</Position>
    </Patron>
  </Checkouts>
  <Holds>
    <Patron>
      <PatronId>patron4</PatronId>
      <EventStartDateInUTC>2014-04-24T18:10:44</EventStartDateInUTC>
      <EventEndDateInUTC>2014-04-24T18:11:02</EventEndDateInUTC>
      <Position>1</Position>
    </Patron>
  </Holds>
  <Reserves/>
</ItemCirculation>
</ArrayOfItemCirculation>
"""

    def test_parse_circulation_batch(self):
        event1, event2 = CirculationParser().process_all(
            self.TWO_CIRCULATION_STATUSES)
        eq_(event1[LicensedWork.SOURCE_ID], 'item1')
        eq_(event1[LicensedWork.ISBN], '900isbn1')
        eq_(event1[LicensedWork.AVAILABLE], 0)
        eq_(event1[LicensedWork.OWNED], 2)
        eq_(event1[LicensedWork.RESERVES], 1)
        eq_(event1[LicensedWork.CHECKOUTS], 1)
        eq_(event1[LicensedWork.HOLDS], 0)


class TestItemListParser(object):

    def text_parse_author_string(cls):
        authors = ItemListParser.parse_author_string(
            "Walsh, Jill Paton; Sayers, Dorothy L.")
        eq_(authors, [dict(name="Walsh, Jill Paton"),
                      dict(name="Sayers, Dorothy L.")])

    def test_item_list(cls):
        data = pkgutil.get_data(
            "tests.integration",
            "files/3m_item_metadata_list.xml")
        
        data = [(id, raw, cooked)
                for (id, raw,cooked)  in ItemListParser().parse(data)]

        # There should be 25 items in the list.
        eq_(25, len(data))

        # Do a spot check of the first item in the list
        id, raw, cooked = data[0]

        eq_("ddf4gr9", id)

        assert raw.startswith("<Item")

        eq_(id, cooked[Edition.SOURCE_ID])
        eq_("The Incense Game", cooked[Edition.TITLE])
        eq_("A Novel of Feudal Japan", cooked[Edition.SUBTITLE])
        eq_("9781250015280", cooked[Edition.ISBN])
        eq_([dict(name="Rowland, Laura Joh")], cooked[Edition.AUTHOR])
        eq_("en", cooked[Edition.LANGUAGE])
        eq_("St. Martin's Press", cooked[Edition.PUBLISHER])
        eq_("1.2 MB", cooked[Edition.FILE_SIZE])
        eq_("304", cooked[Edition.NUMBER_OF_PAGES])
        eq_("2012-09-17", cooked[Edition.DATE_PUBLISHED])

        assert cooked[Edition.SUMMARY][Edition.TEXT_VALUE].startswith(
            "<b>Winner")

        # Check the links

        l1 = cooked[Edition.LINKS][Edition.IMAGE][0]['href']
        assert l1.startswith("http://ebook.3m.com/delivery")

        l2 = cooked[Edition.LINKS]['alternate'][0]['href']
        assert l2.startswith("http://ebook.3m.com/library")
