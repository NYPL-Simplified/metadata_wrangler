from nose.tools import set_trace

import re


class LaneData(object):
    sublanes = set([])
    LCC = []
    DDC = []
    OVERDRIVE = []
    name = None

    @classmethod
    def self_and_sublanes(cls, nemesis=None):
        yield cls
        for sl in cls.sublanes:
            if sl is nemesis:
                continue
            for l in sl.self_and_sublanes(nemesis):
                yield l

class Unclassified(LaneData):
    name = "Unclassified"
    sublanes = set([])
LaneData.sublanes.add(Unclassified)

class Humor(LaneData):
    name = "Humor"
    sublanes = set([])
LaneData.sublanes.add(Humor)

class FineArts(LaneData):
    name = "Fine Arts"
    sublanes = set([])
LaneData.sublanes.add(FineArts)

class Poetry(LaneData):
    name = "Poetry"
    sublanes = set([])
FineArts.sublanes.add(Poetry)

class Drama(LaneData):
    name = "Drama"
    sublanes = set([])
FineArts.sublanes.add(Drama)

class Nonfiction(LaneData):
    name = "Nonfiction"
    sublanes = set()
LaneData.sublanes.add(Nonfiction)

class Travel(LaneData):
    name = "Travel"
    sublanes = set([])
Nonfiction.sublanes.add(Travel)

class History(Nonfiction):
    name = "History"    
    sublanes = set([])
Nonfiction.sublanes.add(History)

class Biography(Nonfiction):
    name = "Biography"
    sublanes = set([])
Nonfiction.sublanes.add(Biography)

class Reference(Nonfiction):
    name = "Reference"
    sublanes = set([])
Nonfiction.sublanes.add(Reference)

class Philosophy(Nonfiction):
    name = "Philosophy"
    sublanes = set([])
Nonfiction.sublanes.add(Philosophy)

class Religion(Nonfiction):
    name = "Religion"
    sublanes = set([])
Nonfiction.sublanes.add(Religion)

class Science(Nonfiction):
    name = "Science"
    sublanes = set([])
Nonfiction.sublanes.add(Science)

class Cooking(Nonfiction):
    name = "Cooking"
    sublanes = set([])
Nonfiction.sublanes.add(Cooking)

class Fiction(LaneData):
    name = "Fiction"
    sublanes = set()
LaneData.sublanes.add(Fiction)

class Adventure(LaneData):
    name = "Adventure"
    sublanes = set([])
Fiction.sublanes.add(Adventure)

class Romance(Fiction):
    name = "Romance"
    sublanes = set([])
Fiction.sublanes.add(Romance)

class Fantasy(Fiction):
    name = "Fantasy"
    sublanes = set([])
Fiction.sublanes.add(Fantasy)

class ScienceFiction(Fiction):
    name = "Science Fiction"
    sublanes = set([])
Fiction.sublanes.add(ScienceFiction)

class Mystery(Fiction):
    name = "Mystery"
    sublanes = set([])
Fiction.sublanes.add(Mystery)

class Horror(Fiction):
    name = "Horror"
    sublanes = set([])
Fiction.sublanes.add(Horror)

class Periodicals(Fiction):
    name = "Periodicals"
    sublanes = set([])
LaneData.sublanes.add(Periodicals)

# A work that's considered to be fiction will never be filed under
# nonfiction, and vice versa.
Fiction.nemesis = Nonfiction
Nonfiction.nemesis = Fiction

