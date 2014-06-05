from pdb import set_trace

import re

def make_kw(*l):
    res = []
    for i in l:
        res.append(re.compile(r'\b%s\b' % i, re.I))
    return res

class Lane(object):
    sublanes = set([])
    DDC = []
    KEYWORDS = []

    @classmethod
    def best_match(cls, subjects):
        fiction = cls.most_common(subjects['fiction'])
        codes = subjects['codes']
        names = subjects['names']

        # For each lane, how close are we to being in that lane?        
        if fiction:
            top_lane = Fiction
        else:
            top_lane = Nonfiction

        affinities = dict()
        for lane in set([top_lane]).union(top_lane.sublanes).union(cls.sublanes):
            if lane is top_lane.nemesis:
                continue
            affinity = lane.affinity(subjects)
            if affinity > 0:
                affinities[lane] = affinity
        best_lane = cls.most_common(affinities)
        if not best_lane:
            best_lane = Unclassified
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
                if kw.search(k):
                    total += v
                    
        ddc_codes = subjects.get('codes', {}).get('DDC', {})
        for k, v in ddc_codes.items():
            set_trace()

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
        return total

class Unclassified(Lane):
    name = "Unclassified"
Lane.sublanes.add(Unclassified)

class Humor(Lane):
    name = "Humor"
    KEYWORDS = make_kw(
        "humor", "wit", "humorous", "satire", "comedies", "comedy")
Lane.sublanes.add(Humor)

class Poetry(Lane):
    name = "Poetry"
    KEYWORDS = make_kw("poetry")
Lane.sublanes.add(Poetry)

class Drama(Lane):
    name = "Drama"
    KEYWORDS = make_kw("drama")
Lane.sublanes.add(Drama)

class Nonfiction(Lane):
    name = "Nonfiction"
    sublanes = set()
Lane.sublanes.add(Nonfiction)

class History(Nonfiction):
    name = "History"
    KEYWORDS = make_kw("history")
    DDC = [900, 904, 909, range(930, 940), 950, 960, 970, 980, 990]
Nonfiction.sublanes.add(History)

class Biography(Nonfiction):
    name = "Biography"
    KEYWORDS = make_kw("biography")
    DDC = [920, "B"]
Nonfiction.sublanes.add(Biography)

class Reference(Nonfiction):
    name = "Reference"
    KEYWORDS = make_kw("dictionaries", "handbooks", "manuals", "catalogs",
                        "encyclopedias")
Nonfiction.sublanes.add(Reference)

class Religion(Nonfiction):
    name = "Religion"
    KEYWORDS = make_kw("sermons", "bible", "christianity", "islam",
                "judaism", "religious")
Nonfiction.sublanes.add(Religion)

class Science(Nonfiction):
    name = "Science"
    KEYWORDS = make_kw("science", "aeronautics", "medicine", "evolution",
                        "mathematics", 'natural history')
Nonfiction.sublanes.add(Science)

class Cooking(Nonfiction):
    name = "Cooking"
    KEYWORDS = make_kw("cooking", "food")
Nonfiction.sublanes.add(Cooking)

class Fiction(Lane):
    name = "Fiction"
    sublanes = set()
Lane.sublanes.add(Fiction)

class Romance(Fiction):
    name = "Romance"
    KEYWORDS = make_kw("love stories", "romances")
Fiction.sublanes.add(Romance)

class Fantasy(Fiction):
    name = "Fantasy"
    KEYWORDS = make_kw("fantasy")
Fiction.sublanes.add(Fantasy)

class ScienceFiction(Fiction):
    name = "Science Fiction"
    KEYWORDS = make_kw("science fiction")
Fiction.sublanes.add(ScienceFiction)

class Mystery(Fiction):
    name = "Mystery"
    KEYWORDS = make_kw(
        "mystery and detective stories",
        "detective and mystery stories",
        "crime",
    )
Fiction.sublanes.add(Mystery)

class Horror(Fiction):
    name = "Horror"
    KEYWORDS = make_kw("horror")
Fiction.sublanes.add(Horror)

# A work that's considered to be fiction will never be filed under
# nonfiction, and vice versa.
Fiction.nemesis = Nonfiction
Nonfiction.nemesis = Fiction

# Fiction (catch-all, Popular literature)
# Adventure (Adventure stories, Western stories, Adventure and adventurers, Sea stories, War stories)
# Travel (Travel, Description and travel, Voyages and travels, Discovery and exploration)
# Philosophy (Political science)
# Periodicals
