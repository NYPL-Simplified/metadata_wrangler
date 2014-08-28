from nose.tools import set_trace

import re

def make_kw(*l):
    res = []
    for i in l:
        res.append(re.compile(r'\b%s\b' % i, re.I))
    return res

class Lane(object):
    sublanes = set([])
    LCC = []
    DDC = []
    OVERDRIVE = []
    KEYWORDS = []
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

class Unclassified(Lane):
    name = "Unclassified"
    sublanes = set([])
Lane.sublanes.add(Unclassified)

class Humor(Lane):
    name = "Humor"
    KEYWORDS = make_kw(
        "humor", "wit", "humorous", "satire", "comedies", "comedy")
    sublanes = set([])
Lane.sublanes.add(Humor)

class Poetry(Lane):
    name = "Poetry"
    KEYWORDS = make_kw("poetry")
    sublanes = set([])
Lane.sublanes.add(Poetry)

class Drama(Lane):
    name = "Drama"
    KEYWORDS = make_kw("drama")
    sublanes = set([])
Lane.sublanes.add(Drama)

class Nonfiction(Lane):
    name = "Nonfiction"
    sublanes = set()
Lane.sublanes.add(Nonfiction)

class History(Nonfiction):
    name = "History"
    KEYWORDS = make_kw("histories") + ["history"]
    LCC = ["AZ", re.compile("C.*"), re.compile("D.*"), 
           re.compile("E.*"), re.compile("F.*"),
           "KBR", "LA",
    ]
    DDC = [900, 904, 909, range(930, 941), 950, 960, 970, 980, 990]
    OVERDRIVE = ["History"]
    sublanes = set([])
Nonfiction.sublanes.add(History)

class Biography(Nonfiction):
    name = "Biography"
    KEYWORDS = make_kw("biography")
    DDC = [920, "B"]
    OVERDRIVE = ["Biography & Autobiography"]
    sublanes = set([])
Nonfiction.sublanes.add(Biography)

class Reference(Nonfiction):
    name = "Reference"
    KEYWORDS = make_kw("dictionaries", "handbooks", "manuals", "catalogs",
                        "encyclopedias")
    sublanes = set([])
Nonfiction.sublanes.add(Reference)

class Philosophy(Nonfiction):
    name = "Philosophy"
    LCC = [
        "B", "BC", "BD"
    ]
    DDC = [100, 101, range(140, 150), range(180, 201)]
    KEYWORDS = make_kw("philosophy")
    sublanes = set([])
Nonfiction.sublanes.add(Philosophy)

class Religion(Nonfiction):
    name = "Religion"
    LCC = [
        "KB", "KBM", "KBP", "KBR", "KBU",
        "BL", "BM", "BP", "BQ", "BR", "BS", "BT", "BV",
        "BX"
    ]
    DDC = [range(200,300)]
    KEYWORDS = make_kw("sermons", "bible", "christianity", "islam",
                       "judaism", "religious", "religion", "church",
                       "theology", "theological", 'biblical')
    sublanes = set([])
Nonfiction.sublanes.add(Religion)

class Science(Nonfiction):
    name = "Science"
    sublanes = set([])
    LCC = [re.compile("Q.*"), re.compile("R.*"), 
           re.compile("S.*"), re.compile("T[A-P].*"),
    ]
    KEYWORDS = make_kw("science", "aeronautics", "medicine", "evolution",
                        "mathematics", 'natural history')
Nonfiction.sublanes.add(Science)

class Cooking(Nonfiction):
    name = "Cooking"
    LCC = ["TX"]
    KEYWORDS = make_kw("cooking", "baking", "food", "home economics",
                       "cookbook")
    sublanes = set([])
Nonfiction.sublanes.add(Cooking)

class Fiction(Lane):
    name = "Fiction"
    sublanes = set()
Lane.sublanes.add(Fiction)

class Romance(Fiction):
    name = "Romance"
    KEYWORDS = make_kw("love stories", "romances")
    OVERDRIVE = ["Romance"]
    sublanes = set([])
Fiction.sublanes.add(Romance)

class Fantasy(Fiction):
    name = "Fantasy"
    KEYWORDS = make_kw("fantasy")
    sublanes = set([])
Fiction.sublanes.add(Fantasy)

class ScienceFiction(Fiction):
    name = "Science Fiction"
    KEYWORDS = make_kw("science fiction")
    sublanes = set([])
Fiction.sublanes.add(ScienceFiction)

class Mystery(Fiction):
    name = "Mystery"
    KEYWORDS = make_kw(
        "mystery and detective stories",
        "detective and mystery stories",
        "crime",
    )
    OVERDRIVE = ["Mystery"]
    sublanes = set([])
Fiction.sublanes.add(Mystery)

class Horror(Fiction):
    name = "Horror"
    KEYWORDS = make_kw("horror", "ghost stories")
    sublanes = set([])
Fiction.sublanes.add(Horror)

class Periodicals(Fiction):
    name = "Periodicals"
    KEYWORDS = make_kw("periodicals")
    sublanes = set([])
Lane.sublanes.add(Periodicals)

# A work that's considered to be fiction will never be filed under
# nonfiction, and vice versa.
Fiction.nemesis = Nonfiction
Nonfiction.nemesis = Fiction

# Fiction (catch-all, Popular literature)
# Adventure (Adventure stories, Western stories, Adventure and adventurers, Sea stories, War stories)
# Travel (Travel, Description and travel, Voyages and travels, Discovery and exploration)
# Philosophy (Political science)
# Periodicals
