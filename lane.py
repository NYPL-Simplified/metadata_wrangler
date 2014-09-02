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

    @classmethod
    def best_match(cls, subjects):
        if 'fiction' in subjects:
            fiction = (cls.most_common(subjects['fiction']) == True)
        else:
            fiction = False

        codes = subjects.get('codes', {})
        names = subjects.get('names', {})

        # For each lane, how close are we to being in that lane?        
        if fiction:
            top_lane = Fiction
        else:
            top_lane = Nonfiction

        best_affinity = 0
        best_lane = top_lane
        for lane in cls.self_and_sublanes(top_lane.nemesis):
            if lane is Unclassified or lane is Lane:
                continue
            affinity = lane.affinity(subjects)
            #if affinity > 0:
            #    print " Affinity for %s: %.2f" % (lane.name, affinity)
            if affinity > best_affinity:
                best_lane = lane
                best_affinity = affinity
        return fiction, best_lane.name

    @classmethod
    def most_common(cls, h):
        champ = None
        for k, v in h.items():
            if not k:
                # "None" is not a valid choice here.
                continue
            if not champ or v > champ[1]:
                champ = (k, v)
        if not champ:
            return None
        return champ[0]

    @classmethod
    def affinity(cls, subjects):
        lcsh_names = subjects.get('names', {}).get('LCSH', {})
        fast_names = subjects.get('names', {}).get('FAST', {})
        total = 0
        possibilities = lcsh_names.items() + fast_names.items()

        for k, v in possibilities:
            for kw in cls.KEYWORDS:
                if isinstance(kw, basestring):
                    match = (kw == k)
                else:
                    match = kw.search(k)
                if match:
                    total += v

        lcc_codes = subjects.get('codes', {}).get('LCC', {})
        for k, v in lcc_codes.items():
            match = False
            for check in cls.LCC:
                if isinstance(check, basestring):
                    match = (k == check)
                else:
                    match = check.match(k)
                if match:
                    break
            if match:
                total += v

        ddc_codes = subjects.get('codes', {}).get('DDC', {})
        for k, v in ddc_codes.items():
            match = False
            for check in cls.DDC:
                if isinstance(check, int):
                    match = (k == check)
                else:
                    match = (k in check)
                if match:
                    break
            if match:
                total += v

        overdrive_codes = subjects.get('codes', {}).get('Overdrive', {})
        for k, v in overdrive_codes.items():
            match = False
            for check in cls.OVERDRIVE:
                match = (k in check)
                if match:
                    break
            if match:
                total += v

        return total

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

