# encoding: utf-8

# SQL to find commonly used DDC classifications
# select count(workrecords.id) as c, subjects.identifier from workrecords join workidentifiers on workrecords.primary_identifier_id=workidentifiers.id join classifications on workidentifiers.id=classifications.work_identifier_id join subjects on classifications.subject_id=subjects.id where subjects.type = 'DDC' and not subjects.identifier like '8%' group by subjects.identifier order by c desc;

# SQL to find commonly used classifications not assigned to a genre 
# select count(workidentifiers.id) as c, subjects.type, substr(subjects.identifier, 0, 20) as i, substr(subjects.name, 0, 20) as n from workidentifiers join classifications on workidentifiers.id=classifications.work_identifier_id join subjects on classifications.subject_id=subjects.id where subjects.genre_id is null and subjects.fiction is null group by subjects.type, i, n order by c desc;

import json
import pkgutil
from collections import (
    Counter,
    defaultdict,
)
from nose.tools import set_trace
import re
from sqlalchemy.sql.expression import and_

class GenreData(object):
    subgenres = set([])
    LCC = []
    DDC = []
    OVERDRIVE = []
    name = None

    @classmethod
    def self_and_subgenres(cls, nemesis=None):
        yield cls
        for sl in cls.subgenres:
            if sl is nemesis:
                continue
            for l in sl.self_and_subgenres(nemesis):
                yield l

genre_structure = {
    "Art, Architecture, & Design" : [
        "Architecture",
        "Art",
        "Art Criticism & Theory",
        "Design",
        "Fashion",
        "Art History",
        "Photography",
    ],
    "Biography & Memoir" : [],
    "Business & Economics" : [
        "Economics",
        "Management & Leadership",
        "Personal Finance & Investing",
        "Real Estate",
    ],
    # UNUSED: Children
    "Classics & Poetry" : [
        "Classics",
        "Poetry",
    ],
    "Crafts, Cooking & Garden" : [
        "Antiques & Collectibles",
        "Bartending & Cocktails",
        "Cooking",
        "Crafts, Hobbies, & Games",
        "Gardening",
        "Health & Diet",
        "House & Home",
        "Pets",
        "Vegetarian & Vegan",
    ],
    "Crime, Thrillers & Mystery" : [
        "Action & Adventure",
        "Espionage",
        "Hard Boiled",
        "Legal Thrillers",
        "Military Thrillers",
        "Mystery",
        "Police Procedurals",
        "Supernatural Thrillers",
        "Thrillers",
        "True Crime",
        "Women Detectives",
    ],
    "Criticism & Philosophy" : [
        "Language Arts & Disciplines",
        "Literary Criticism",
        "Philosophy",
    ],
    # Not included: Fiction General
    "Graphic Novels & Comics" : [
        "Literary",
        "Manga",
        "Superhero",
    ],
    "Historical Fiction" : [],
    "History" : [
        "African History",
        "Ancient History",
        "Asian History",
        "Civil War History",
        "European History",
        "Latin American History",
        "Medieval History",
        "Middle East History",
        "Military History",
        "Modern History",
        "Renaissance History",
        "United States History",
        "World History",
    ],
    "Humor & Entertainment" : [
        "Dance",
        "Drama",
        "Film & TV",
        "Humor",
        "Music",
        "Performing Arts",
    ],
    "Literary Fiction" : ["Literary Collections"],
    "Parenting & Family" : [
        "Education",
        "Family & Relationships",
        "Parenting",
    ],
    "Periodicals" : [],
    "Politics & Current Events" : [
        "Political Science",
    ],
    "Reference" : [
        "Dictionaries",
        "Encyclopedias",
        "Foreign Language Study",
        "Law",
        "Study Aids",
    ],
    "Religion & Spirituality" : [
        "Body, Mind, & Spirit",
        "Buddhism",
        "Christianity",
        "Hinduism",
        "Islam",
        "Judaism",
        "New Age",
        "Religious Fiction",
    ],
    "Romance & Erotica" : [
        "Contemporary Romance",
        "Erotica",
        "Historical Romance",
        "Paranormal Romance",
        "Regency Romance",
        "Romance",
        "Suspense Romance",

    ],
    "Science Fiction & Fantasy" : [
        "Epic Fantasy",
        "Fantasy",
        "Horror",
        "Military",
        "Movies/Gaming",
        "Science Fiction",
        "Space Opera",
        "Urban Fantasy",
    ],
    "Science, Technology, & Nature" : [
        "Computers",
        "Mathematics",
        "Medical",
        "Nature",
        "Psychology",
        "Science",
        "Social Science",
        "Technology & Engineering",
    ],
    "Self-Help" : [],
    "Travel, Adventure & Sports" : [
        "Sports",
        "Transportation",
        "Travel",
    ],
    "African-American" : [],
    # Not included: Young Adult.
}

class GenreData(object):
    def __init__(self, name, subgenres, parent=None):
        self.name = name
        self.parent = parent
        self.subgenres = []
        for sub in subgenres:
            self.subgenres.append(GenreData(sub, [], self))

    @property
    def variable_name(self):
        return self.name.replace("-", "_").replace(", & ", "_").replace(", ", "_").replace(" & ", "_").replace(" ", "_")

genres = dict()
namespace = globals()
for name, subgenres in genre_structure.items():
    genre = GenreData(name, subgenres, genres)
    genres[genre.name] = genre
    namespace[genre.variable_name] = genre
    for sub in genre.subgenres:
        if sub.name in genres:
            raise ValueError("Duplicate genre name! %s" % sub.name)
        genres[sub.name] = sub
        namespace[sub.variable_name] = sub

# Some of the genres should contain fiction by default; others should
# contain nonfiction by default.
fiction_genredata = set([
    Crime_Thrillers_Mystery,
    Historical_Fiction,
    Literary_Fiction,
    Romance_Erotica,
    Science_Fiction_Fantasy,
])
nonfiction_genredata = set([
    Art_Architecture_Design,
    Biography_Memoir,
    Business_Economics, 
    Crafts_Cooking_Garden,
    Criticism_Philosophy,
    History,
    Humor_Entertainment,
    Parenting_Family,
    Politics_Current_Events,
    Reference,
    Religion_Spirituality,
    Science_Technology_Nature,
    Self_Help,
    Travel_Adventure_Sports,
])

fiction_genres = set([])
nonfiction_genres = set([])

# The subgenres of fiction are fiction (with rare exceptions).
nonfiction_subgenres_of_fiction = [True_Crime]
for genre in list(fiction_genredata):
    fiction_genres.add(genre.name)
    for subgenre in genre.subgenres:
        if subgenre not in nonfiction_subgenres_of_fiction:
            fiction_genres.add(subgenre.name)

# Similarly for nonfiction.
fiction_subgenres_of_nonfiction = [Religious_Fiction]
for genre in list(nonfiction_genredata):
    nonfiction_genres.add(genre.name)
    for subgenre in genre.subgenres:
        if subgenre not in fiction_subgenres_of_nonfiction:
            nonfiction_genres.add(subgenre.name)

# Humor includes both fiction and nonfiction.
nonfiction_genres.remove(Humor.name)

class AssignSubjectsToGenres(object):

    def __init__(self, _db):
        self._db = _db

    def run(self, force=False):
        from model import (
            Genre,
            Subject,
        )
        q = self._db.query(Subject).filter(Subject.locked==False)
        if not force:
            q = q.filter(Subject.checked==False)
        counter = 0
        for subject in q:
            subject.checked = True
            classifier = Classification.classifiers.get(
                subject.type, None)
            if not classifier:
                continue
            genredata, audience, fiction = classifier.classify(subject)
            if genredata:
                genre, was_new = Genre.lookup(self._db, genredata.name, True)
                subject.genre = genre
            if audience:
                subject.audience = audience
            if fiction:
                subject.fiction = fiction
            if genredata or audience or fiction:
                print subject
            counter += 1
            if not counter % 100:
                print "!", counter
                self._db.commit()

class Classification(object):

    AUDIENCE_CHILDREN = "Children"
    AUDIENCE_YOUNG_ADULT = "Young Adult"
    AUDIENCE_ADULT = "Adult"

    # TODO: This is currently set in model.py in the Subject class.
    classifiers = dict()

    @classmethod
    def name_for(cls, identifier):
        """Look up a human-readable name for the given identifier."""
        return None

    @classmethod
    def classify(cls, subject):
        """Try to determine genre, audience, and fiction status
        for the given Subject.
        """
        identifier = cls.scrub_identifier(subject.identifier)
        if subject.name:
            name = cls.scrub_name(subject.name)
        else:
            name = identifier
        return (cls.genre(identifier, name),
                cls.audience(identifier, name),
                cls.is_fiction(identifier, name))

    @classmethod
    def scrub_identifier(cls, identifier):
        """Prepare an identifier from within a call to classify().
        
        This may involve data normalization, conversion to lowercase,
        etc.
        """
        return identifier.lower()

    @classmethod
    def scrub_name(cls, name):
        """Prepare a name from within a call to classify()."""
        return name.lower()

    @classmethod
    def genre(cls, identifier, name):
        """Is this identifier associated with a particular Genre?"""
        return None

    @classmethod
    def is_fiction(cls, identifier, name):
        """Is this identifier+name particularly indicative of fiction?
        How about nonfiction?
        """
        n = name.lower()
        if "nonfiction" in n:
            return False
        if "fiction" in n:
            return True
        return None

    @classmethod
    def audience(cls, identifier, name):
        """What does this identifier+name say about the audience for
        this book?
        """
        n = name.lower()
        if 'juvenile' in n:
            return cls.AUDIENCE_CHILDREN
        elif 'young adult' in n or "YA" in name:
            return cls.AUDIENCE_YOUNG_ADULT
        return None


class OverdriveClassification(Classification):

    # Any classification that includes the string "Fiction" will be
    # counted as fiction. This is just the leftovers.
    FICTION = set([
        "Short Stories",
        "Fantasy",
        "Horror",
        "Mystery",
        "Romance",
        "Western",
        "Suspense",
        "Thriller",
        "Science Fiction & Fantasy",
        ])

    GENRES = {
        African_American : ["African American Fiction", "African American Nonfiction", "Urban Fiction", ],
        Antiques_Collectibles : "Antiques",
        Architecture : "Architecture",
        Art : "Art",
        Biography_Memoir : "Biography & Autobiography",
        Business_Economics : ["Business", "Marketing & Sales", "Careers"],
        Christianity : ["Christian Fiction", "Christian Nonfiction"],
        Computers : "Computer Technology",
        Cooking : "Cooking & Food",
        Crafts_Hobbies_Games : ["Crafts", "Games"],
        Drama : "Drama",
        Education : "Education",
        Erotica : "Erotic Literature",
        Fantasy : "Fantasy",
        Foreign_Language_Study : "Foreign Language Study",
        Gardening : "Gardening",
        Graphic_Novels_Comics : "Comic and Graphic Books",
        Health_Diet : "Health & Fitness",
        Historical_Fiction : "Historical Fiction",
        History : "History",
        Horror : "Horror",
        House_Home : u"Home Design & Décor",
        Humor : ["Humor (Fiction)", "Humor (Nonfiction)"],
        Humor_Entertainment : "Entertainment",
        Judaism : "Judaica",
        Language_Arts_Disciplines : ["Language Arts", "Grammar & Language Usage"],
        Law : "Law",
        Literary_Collections : "Literary Anthologies",
        Literary_Criticism : ["Literary Criticism", "Criticism"],
        Management_Leadership : "Management",
        Mathematics : "Mathematics",
        Medical : "Medical",
        Military_History : "Military",
        Music : "Music",
        Mystery : "Mystery",
        Nature : "Nature",
        New_Age : "New Age",
        Parenting_Family : "Family & Relationships",
        Performing_Arts : "Performing Arts",
        Personal_Finance_Investing : "Finance",
        Pets : "Pets",
        Philosophy : ["Philosophy", "Ethics"],
        Photography : "Photography",
        Poetry : "Poetry",
        Politics_Current_Events : ["Politics", "Current Events"],
        Psychology : ["Psychology", "Psychiatry", "Psychiatry & Psychology"],
        Reference : "Reference",
        Religion_Spirituality : "Religion & Spirituality",
        Romance : "Romance",
        Science : ["Science", "Physics", "Chemistry"],
        Science_Fiction : "Science Fiction",
        Science_Fiction_Fantasy : "Science Fiction & Fantasy",
        Self_Help : ["Self-Improvement", "Self-Help", "Self Help"],
        Social_Science : "Sociology",
        Sports : "Sports & Recreations",
        Study_Aids : "Study Aids & Workbooks",
        Technology_Engineering : ["Technology", "Engineering"],
        Thrillers : ["Suspense", "Thriller"],
        Transportation : "Transportation",
        Travel : ["Travel", "Travel Literature"],
        True_Crime : "True Crime",
    }

    @classmethod
    def scrub_identifier(cls, identifier):
        return identifier

    @classmethod
    def is_fiction(cls, identifier, name):
        if (identifier in cls.FICTION
            or "Fiction" in identifier
            or "Literature" in identifier):
            # "Literature" on Overdrive seems to be synonymous with fiction,
            # but not necessarily "Literary Fiction".
            return True
        return False

    @classmethod
    def audience(cls, identifier, name):
        if ("Juvenile" in identifier or "Picture Book" in identifier
            or "Beginning Reader" in identifier):
            return cls.AUDIENCE_CHILDREN
        elif "Young Adult" in identifier:
            return cls.AUDIENCE_YOUNG_ADULT
        return cls.AUDIENCE_ADULT

    @classmethod
    def genre(cls, identifier, name):
        for l, v in cls.GENRES.items():
            if identifier == v or (isinstance(v, list) and identifier in v):
                return l
        return None


class DeweyDecimalClassification(Classification):

    NAMES = None
    FICTION = set([813, 823, 833, 843, 853, 863, 873, 883, "FIC", "E", "F"])
    NONFICTION = set(["J", "B"])

    # 791.4572 and 791.4372 is for recordings. 741.59 is for comic
    #  adaptations? This is a good sign that a workidentifier should
    #  not be considered, actually.
    # 398 - Folklore
    # 428.6 - Primers, Readers, i.e. collections of stories
    # 700 - Arts - full of distinctions

    GENRES = {
        African_History : [range(960, 970)],
        Architecture : [range(710, 720), range(720, 730)],
        Art : [range(700, 710), range(730, 770), 774, 776],
        Asian_History : [range(950, 960), 995, 996, 997],
        Biography_Memoir : ["B", 920],
        Business_Economics : [range(330, 340)],
        Christianity : [range(220, 230), range(230, 290)],
        Cooking : [range(640, 642)],
        Crafts_Hobbies_Games : [790, 793, 794, 795],
        Drama : [812, 822, 832, 842, 852, 862, 872, 882],
        European_History : [range(940, 950)],
        History : [900],
        Islam : [297],
        Judaism : [296],
        Language_Arts_Disciplines : [range(410, 430)],
        Latin_American_History : [range(981, 990)],
        Law : [range(340, 350)],
        Management_Leadership : [658],        
        Mathematics : [range(510, 520)],
        Medical : [range(610, 620),],
        Military_History : [range(355, 360)],
        Music : [range(780, 789)],
        Periodicals : [range(50, 60), 105, 205, 304, 405, 505, 605, 705, 805, 905],
        Philosophy : [range(160, 200)],
        Photography : [771, 772, 773, 775, 778, 779],
        Poetry : [811, 821, 831, 841, 851, 861, 871, 874, 881, 884],
        Political_Science : [range(320, 330), range(351, 355)],
        Psychology : [range(150, 160)],
        Reference : [range(10, 20), range(30, 40), 103, 203, 303, 403, 503, 603, 703, 803, 903],
        Religion_Spirituality : [range(200, 220), 290, 292, 293, 294, 295, 299,],
        Science : [500, 501, 502, range(506, 510), range(520, 530), range(530, 540), range(540, 550), range(550, 560), range(560, 570), range(570, 580), range(580, 590), range(590, 600),],
        Sports : [range(796, 800)],
        Technology_Engineering : [600, 601, 602, 604, range(606, 610), range(610, 640), range(660, 670), range(670, 680), range(681, 690), range(690, 700),],
        Travel : [range(910, 920)],
        United_States_History : [range(973,980)],
        World_History : [909],
    }

    @classmethod
    def _load(cls):
        cls.NAMES = json.loads(
            pkgutil.get_data("resources", "dewey_1000.json"))

        # Add some other values commonly found in MARC records.
        cls.DEWEY["B"] = "Biography"
        cls.DEWEY["E"] = "Juvenile Fiction"
        cls.DEWEY["F"] = "Fiction"
        cls.DEWEY["FIC"] = "Juvenile Fiction"
        cls.DEWEY["J"] = "Juvenile Nonfiction"
        cls.DEWEY["Y"] = "Young Adult"

    @classmethod
    def name_for(cls, identifier):
        return cls.NAMES.get(identifier, None)

    @classmethod
    def scrub_identifier(cls, identifier):
        if isinstance(identifier, int):
            identifier = str(identifier).zfill(3)

        identifier = identifier.upper()

        if identifier.startswith('[') and identifier.endswith(']'):
            # This is just bad data.
            identifier = identifier[1:-1]

        if identifier.startswith('C') or identifier.startswith('A'):
            # A work from our Canadian neighbors or our Australian
            # friends.
            identifier = identifier[1:]
        elif identifier.startswith("NZ"):
            # A work from the good people of New Zealand.
            identifier = identifier[2:]

        # Trim everything after the first period. We don't know how to
        # deal with it.
        if '.' in identifier:
            identifier = identifier.split('.')[0]
        try:
            identifier = int(identifier)
        except ValueError:
            pass
        return identifier

    @classmethod
    def is_fiction(cls, identifier, name):
        """Is the given DDC classification likely to contain fiction?"""
        if identifier == 'Y':
            # Inconsistently used for young adult fiction and
            # young adult nonfiction.
            return None

        if (isinstance(identifier, basestring) and (
                identifier.startswith('Y') or identifier.startswith('J'))):
            # Young adult/children's literature--not necessarily fiction
            identifier = identifier[1:]
            try:
                identifier = int(identifier)
            except ValueError:
                pass

        if identifier in cls.FICTION:
            return True
        if identifier in cls.NONFICTION:
            return False

        # TODO: Make NONFICTION more comprehensive and return None
        # if not in there.
        return False

    @classmethod
    def audience(cls, identifier, name):
        if identifier in ('e', 'fic'):
            # Juvenile fiction
            return cls.AUDIENCE_CHILDREN

        if isinstance(identifier, basestring) and identifier.startswith('j'):
            return cls.AUDIENCE_CHILDREN

        if isinstance(identifier, basestring) and identifier.startswith('y'):
            return cls.AUDIENCE_YOUNG_ADULT

        return cls.AUDIENCE_ADULT

    @classmethod
    def genre(cls, identifier, name):
        for genre, identifiers in cls.GENRES.items():
            if identifier == identifiers or identifier in identifiers:
                return genre
        return None
    

class LCCClassification(Classification):

    TOP_LEVEL = re.compile("^([A-Z]{1,2})")
    FICTION = set(["P", "PN", "PQ", "PR", "PS", "PT", "PZ"])
    JUVENILE = set(["PZ"])

    GENRES = {

        # Unclassified/complicated stuff.
        # "America": E11-E143
        # Ancient_History: D51-D90
        # Angling: SH401-SH691
        # Civil_War_History: E456-E655
        # Folklore: GR
        # Geography: leftovers of G
        # Islam: BP1-BP253
        # Latin_American_History: F1201-F3799
        # Manners and customs: GT
        # Medieval History: D111-D203
        # Military_History: D25-D27
        # Modern_History: ???
        # Renaissance_History: D219-D234 (1435-1648, so roughly)
        # Sports: GV557-1198.995
        # TODO: E and F are actually "the Americas".
        # United_States_History is E151-E909, F1-F975 but not E456-E655
        African_History : ["DT"],
        Ancient_History : ["DE"],
        Architecture : ["NA"],
        Art_Criticism_Theory : ["BH"],
        Asian_History : ["DS", "DU"],
        Biography_Memoir : ["CT"],
        Business_Economics : ["HB", "HC", "HF", "HJ"],      
        Christianity : ["BR", "BS", "BT", "BV", "BX"],
        Cooking : ["TX"],
        Crafts_Hobbies_Games : ["TT", "GV"],
        Education : ["L"],
        European_History : ["DA", "DAW", "DB", "DD", "DF", "DG", "DH", "DJ", "DK", "DL", "DP", "DQ", "DR"],
        Islam : ["BP"],
        Judaism : ["BM"],
        Language_Arts_Disciplines : ["Z"],
        Mathematics : ["QA", "HA", "GA"],
        Medical: ["QM", "R"],
        Military_History: ["U", "V"],
        Music: ["M"],
        Parenting_Family : ["HQ"],
        Periodicals : ["AP", "AN"],
        Philosophy : ["BC", "BD", "BJ"],
        Photography: ["TR"],
        Political_Science : ["J", "HX"],
        Psychology : ["BF"],
        Reference : ["AE", "AG", "AI"],
        Religion_Spirituality : ["BL", "BQ"],
        Science : ["QB", "QC", "QD", "QE", "QH", "QK", "QL", "QR", "CC", "GB", "GC", "QP"],
        Social_Science : ["HD", "HE", "HF", "HM", "HN", "HS", "HT", "HV", "GN", "GF"],
        Sports: ["SK"],
        World_History : ["CB"],
    }

    LEFTOVERS = dict(
        B=Philosophy,
        T=Technology_Engineering,
        Q=Science,
        S=Science,
        H=Social_Science,
        D=History,
        N=Art,
        L=Education,
        E=United_States_History,
        F=United_States_History,
        BP=Religion_Spirituality,
    )

    NAMES = {}
    @classmethod
    def _load(cls):
        cls.NAMES = json.loads(
            pkgutil.get_data("resources", "lcc_one_level.json"))

    @classmethod
    def scrub_identifier(cls, identifier):
        return identifier.upper()

    @classmethod
    def name_for(cls, identifier):
        return cls.NAMES.get(identifier, None)

    @classmethod
    def is_fiction(cls, identifier, name):
        return identifier.startswith("P")

    @classmethod
    def genre(cls, identifier, name):
        for genre, strings in cls.GENRES.items():
            for s in strings:
                if identifier.startswith(s):
                    return genre
        for prefix, genre in cls.LEFTOVERS.items():
            if identifier.startswith(prefix):
                return genre
        return None

    @classmethod
    def audience(cls, identifier, name):
        if identifier.startswith("PZ"):
            return cls.AUDIENCE_CHILDREN
        # Everything else is implicitly for adults.
        return cls.AUDIENCE_ADULT

def match_kw(*l):
    """Turn a list of strings into a regular expression which matches
    any of those strings, so long as there's a word boundary on both ends.
    """
    any_keyword = "|".join([keyword for keyword in l])
    with_boundaries = r'\b(%s)\b' % any_keyword
    return re.compile(with_boundaries, re.I)

class KeywordBasedClassification(Classification):

    """Classify a book based on keywords."""
    
    FICTION_INDICATORS = match_kw(
        "fiction", "stories", "tales", "literature",
        "bildungsromans",
    )
    NONFICTION_INDICATORS = match_kw(
        "history", "biography", "histories", "biographies", "autobiography",
        "autobiographies")
    JUVENILE_INDICATORS = match_kw(
        "for children", "children's", "juvenile",
        "nursery rhymes")
    YOUNG_ADULT_INDICATORS = match_kw("young adult", "ya")

    # These identifiers indicate that the string "children" or
    # "juvenile" in the identifier does not actually mean the work is
    # _for_ children.
    JUVENILE_BLACKLIST = set([
        "military participation",
        "children's accidents",
        "children's voices",
        "juvenile delinquency",
        "children's television workshop",
    ])

    GENRES = {
        # Adventure : match_kw(
        #     "adventure",
        #     "western stories",
        #     "adventurers",
        #     "sea stories",
        #     "war stories",
        # ),
        Biography_Memoir : match_kw(
            "autobiographies",
            "autobiography",
            "biographies",
            "biography",
        ),
        Cooking : match_kw(
            "baking",
            "cookbook",
            "cooking",
            "food",
            "home economics",
        ),
        Drama : match_kw(
            "drama",
            "plays",
        ),
        Fantasy : match_kw(
            "fantasy",
        ),
        History : match_kw(
            "histories",
            "history",
        ),
        Horror : match_kw(
            "ghost stories",
            "horror",
        ),
        Humor : match_kw(
            "comedies",
            "comedy",
            "humor",
            "humorous",
            "satire",
            "wit",
        ),
        Mystery : match_kw(
            "crime",
            "detective",
            "murder",
            "mystery",
            "mysteries",
        ),
        Periodicals : match_kw(
            "periodicals",
        ),
        Philosophy : match_kw(
            "philosophy",
            "political science",
        ),
        Poetry : match_kw(
            "poetry",
        ),
        Political_Science : match_kw(
            "politics", "goverment",
        ),
        Reference : match_kw(
            "catalogs",
            "dictionaries",
            "encyclopedias",
            "handbooks",
            "manuals",
        ),
        Religion_Spirituality : match_kw(
            "religion",
            "religious",
        ),
        Christianity : match_kw(
            "bible",
            "sermons",
            "theological",
            "theology",
            'biblical',
            "christianity",
            "church",
        ),
        Islam : match_kw('islam'),
        Judaism : match_kw('judaism'),
        Erotica : match_kw(
            'erotic',
        ),
        Romance : match_kw(
            "love stories",
            "romance",
            "romances",
        ),
        Medical : match_kw("medicine", "medical"),
        Mathematics : match_kw("mathematics"),
        Computers : match_kw(
            "computer",
            "software",
        ),
        Military_History : match_kw(
            "military science",
            "warfare",
            "military",
        ),
        Science : match_kw(
            "aeronautics",
            "evolution",
            "natural history",
            "science",
        ),
        Science_Fiction : match_kw(
            "science fiction",
        ),
        Travel : match_kw(
            "discovery",
            "exploration",
            "travel",
            "travels",
            "voyages",
        ),
        
    }

    @classmethod
    def is_fiction(cls, identifier, name):
        if not name:
            return None
        if (cls.FICTION_INDICATORS.search(name)):
            return True
        if (cls.NONFICTION_INDICATORS.search(name)):
            return False
        return None

    @classmethod
    def audience(cls, identifier, name):
        if name is None:
            return None
        if cls.JUVENILE_INDICATORS.search(name):
            use = cls.AUDIENCE_CHILDREN
        elif cls.YOUNG_ADULT_INDICATORS.search(name):
            use = cls.AUDIENCE_YOUNG_ADULT
        else:
            return None

        # It may be for kids, or it may be about kids, e.g. "juvenile
        # delinquency".
        for i in cls.JUVENILE_BLACKLIST:
            if i in name:
                return None
        return use

    @classmethod
    def genre(cls, identifier, name):
        match_against = [name]
        for genre, keywords in cls.GENRES.items():
            if keywords.search(name):
                return genre
        return None

class LCSHClassification(KeywordBasedClassification):
    pass

class FASTClassification(KeywordBasedClassification):
    pass
