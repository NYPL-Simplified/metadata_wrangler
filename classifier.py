# encoding: utf-8

# "literary history" != "history"
# "Investigations -- nonfiction" != "Mystery"

# SQL to find commonly used DDC classifications
# select count(editions.id) as c, subjects.identifier from editions join identifiers on workrecords.primary_identifier_id=workidentifiers.id join classifications on workidentifiers.id=classifications.work_identifier_id join subjects on classifications.subject_id=subjects.id where subjects.type = 'DDC' and not subjects.identifier like '8%' group by subjects.identifier order by c desc;

# SQL to find commonly used classifications not assigned to a genre 
# select count(identifiers.id) as c, subjects.type, substr(subjects.identifier, 0, 20) as i, substr(subjects.name, 0, 20) as n from workidentifiers join classifications on workidentifiers.id=classifications.work_identifier_id join subjects on classifications.subject_id=subjects.id where subjects.genre_id is null and subjects.fiction is null group by subjects.type, i, n order by c desc;

import json
import pkgutil
from collections import (
    Counter,
    defaultdict,
)
from nose.tools import set_trace
import re
from sqlalchemy.sql.expression import and_

# This is the large-scale structure of our classification system,
# taken from Zola. 
#
# "Children" and "Young Adult" are not here--they are the 'audience' facet
# of a genre.
#
# "Fiction" is not here--it's a seprate facet.
#
# If the name of a genre is a 2-tuple, the second item in the tuple is
# whether or not the genre contains fiction by default. If the name of
# a genre is a string, the genre inherits the default fiction status
# of its parent, or (if a top-level genre) is nonfiction by default.
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
    ("Classics & Poetry", None) : [
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
    ("Crime, Thrillers & Mystery", True) : [
        "Action & Adventure",
        "Espionage",
        "Hard Boiled",
        "Legal Thrillers",
        "Military Thrillers",
        "Mystery",
        "Police Procedurals",
        "Supernatural Thrillers",
        "Thrillers",
        ("True Crime", False),
        "Women Detectives",
    ],
    "Criticism & Philosophy" : [
        "Language Arts & Disciplines",
        "Literary Criticism",
        "Philosophy",
    ],
    ("Graphic Novels & Comics", True) : [
        "Literary",
        "Manga",
        "Superhero",
    ],
    ("Historical Fiction", True) : [],
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
        ("Humor", None),
        "Music",
        "Performing Arts",
    ],
    ("Literary Fiction", True) : ["Literary Collections"],
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
        ("Religious Fiction", True),
    ],
    ("Romance & Erotica", True) : [
        "Contemporary Romance",
        "Erotica",
        "Historical Romance",
        "Paranormal Romance",
        "Regency Romance",
        "Romance",
        "Suspense Romance",
    ],
    ("Science Fiction & Fantasy", True) : [
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
    ("African-American", None) : [],
    ("LGBT", None) : [],
}

class GenreData(object):
    def __init__(self, name, is_fiction, parent=None):
        self.name = name
        self.parent = parent
        self.is_fiction = is_fiction
        self.subgenres = []

    @property
    def variable_name(self):
        return self.name.replace("-", "_").replace(", & ", "_").replace(", ", "_").replace(" & ", "_").replace(" ", "_").replace("/", "_")

    @classmethod
    def populate(cls, namespace, genres, source):
        """Create a GenreData object for every genre and subgenre in the given
        dictionary.
        """
        for name, subgenres in source.items():
            # Nonfiction is the default, because genres of
            # nonfiction outnumber genres of fiction.
            default_to_fiction=False
            cls.add_genre(
                namespace, genres, name, subgenres, default_to_fiction, None)

    @classmethod
    def add_genre(cls, namespace, genres, name, subgenres, default_to_fiction,
                  parent):
        """Create a GenreData object. Add it to a dictionary and a namespace.
        """
        if isinstance(name, tuple):
            name, default_to_fiction = name
        if name in genres:
            raise ValueError("Duplicate genre name! %s" % name)

        # Create the GenreData object.
        genre_data = GenreData(name, default_to_fiction)
        if parent:
            parent.subgenres.append(genre_data)

        # Add the genre to the given dictionary, keyed on name.
        genres[genre_data.name] = genre_data

        # Convert the name to a Python-safe variable name,
        # and add it to the given namespace.
        namespace[genre_data.variable_name] = genre_data

        # Do the same for subgenres.
        for sub in subgenres:
            cls.add_genre(namespace, genres, sub, [], default_to_fiction,
                          genre_data)

genres = dict()
GenreData.populate(globals(), genres, genre_structure)

class Classifier(object):

    """Turn an external classification into an internal genre, an
    audience, and a fiction status.
    """

    AUDIENCE_CHILDREN = "Children"
    AUDIENCE_YOUNG_ADULT = "Young Adult"
    AUDIENCE_ADULT = "Adult"

    # Classification schemes with associated classifiers.
    LCC = "LCC"
    LCSH = "LCSH"
    DDC = "DDC"
    OVERDRIVE = "Overdrive"
    FAST = "FAST"
    TAG = "tag"

    # TODO: This is currently set in model.py in the Subject class.
    classifiers = dict()

    @classmethod
    def lookup(cls, scheme):
        """Look up a classifier for a classification scheme."""
        return cls.classifiers.get(scheme, None)

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


class OverdriveClassifier(Classifier):

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
        Classics : "Classic Literature",
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


class DeweyDecimalClassifier(Classifier):

    NAMES = json.loads(
        pkgutil.get_data("resources", "dewey_1000.json"))

    # Add some other values commonly found in MARC records.
    NAMES["B"] = "Biography"
    NAMES["E"] = "Juvenile Fiction"
    NAMES["F"] = "Fiction"
    NAMES["FIC"] = "Juvenile Fiction"
    NAMES["J"] = "Juvenile Nonfiction"
    NAMES["Y"] = "Young Adult"

    FICTION = set([813, 823, 833, 843, 853, 863, 873, 883, "FIC", "E", "F"])
    NONFICTION = set(["J", "B"])

    # 791.4572 and 791.4372 is for recordings. 741.59 is for comic
    #  adaptations? This is a good sign that a identifier should
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

        # TODO: Make NONFICTION more comprehensive and return None if
        # not in there, instead of always returning False. Or maybe
        # returning False is fine here, who knows.
        return False

    @classmethod
    def audience(cls, identifier, name):
        if identifier in ('E', 'FIC'):
            # Juvenile fiction
            return cls.AUDIENCE_CHILDREN

        if isinstance(identifier, basestring) and identifier.startswith('J'):
            return cls.AUDIENCE_CHILDREN

        if isinstance(identifier, basestring) and identifier.startswith('Y'):
            return cls.AUDIENCE_YOUNG_ADULT

        return cls.AUDIENCE_ADULT

    @classmethod
    def genre(cls, identifier, name):
        for genre, identifiers in cls.GENRES.items():
            if identifier == identifiers or identifier in identifiers:
                return genre
        return None
    

class LCCClassifier(Classifier):

    TOP_LEVEL = re.compile("^([A-Z]{1,2})")
    FICTION = set(["PN", "PQ", "PR", "PS", "PT", "PZ"])
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

    NAMES = json.loads(
        pkgutil.get_data("resources", "lcc_one_level.json"))

    @classmethod
    def scrub_identifier(cls, identifier):
        return identifier.upper()

    @classmethod
    def name_for(cls, identifier):
        return cls.NAMES.get(identifier, None)

    @classmethod
    def is_fiction(cls, identifier, name):
        if identifier == 'P':
            return True
        if not identifier.startswith('P'):
            return False
        for i in cls.FICTION:
            if identifier.startswith(i):
                return True
        return False

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
    if not l:
        return None
    any_keyword = "|".join([keyword for keyword in l])
    with_boundaries = r'\b(%s)\b' % any_keyword
    return re.compile(with_boundaries, re.I)

class KeywordBasedClassifier(Classifier):

    """Classify a book based on keywords."""
    
    FICTION_INDICATORS = match_kw(
        "fiction", "stories", "tales", "literature",
        "bildungsromans", "fictitious",
    )
    NONFICTION_INDICATORS = match_kw(
        "history", "biography", "histories", "biographies", "autobiography",
        "autobiographies", "nonfiction")
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
        "missing children",
    ])

    GENRES = {
        Action_Adventure : match_kw(
             "adventure",
             "adventure stories",
             "adventure fiction",
             "western stories",
             "adventurers",
             "sea stories",
             "war stories",
        ),
        African_American : match_kw(
            "african americans",
            "african american",
            "african-american",
            "african-americans",
            "urban fiction",
        ),

        African_History: match_kw(
        ),

        Ancient_History: match_kw(
        ),

        Antiques_Collectibles: match_kw(
        ),

        Architecture: match_kw(
        ),

        Art: match_kw(
        ),

        Art_Architecture_Design: match_kw(
        ),

        Art_Criticism_Theory: match_kw(
        ),

        Art_History: match_kw(
        ),

        Asian_History: match_kw(
        ),

        Bartending_Cocktails: match_kw(
        ),

        Biography_Memoir : match_kw(
            "autobiographies",
            "autobiography",
            "biographies",
            "biography",
        ),

        Body_Mind_Spirit: match_kw(
        ),

        Buddhism: match_kw(
        ),

        Business_Economics: match_kw(
        ),

        Christianity : match_kw(
            "schema:creativework:bible",
            "bible",
            "sermons",
            "theological",
            "theology",
            'biblical',
            "christian",
            "christianity",
            "catholic",
            "protestant",
            "catholicism",
            "protestantism",
            "church",
        ),

        Civil_War_History: match_kw(
            "american civil war",
            "1861-1865",
        ),

        Classics: match_kw(
        ),

        Classics_Poetry: match_kw(
        ),

        Classifier: match_kw(
        ),

        Computers : match_kw(
            "computer",
            "computers",
            "hardware",
            "software",
        ),

        Contemporary_Romance: match_kw(
            "contemporary romance",
        ),

        Cooking : match_kw(
            "baking",
            "cookbook",
            "cooking",
            "food",
            "home economics",
        ),

        Crafts_Cooking_Garden: match_kw(
        ),

        Crafts_Hobbies_Games: match_kw(
        ),

        Crime_Thrillers_Mystery: match_kw(
        ),

        Criticism_Philosophy: match_kw(
        ),

        Dance: match_kw(
        ),

        Design: match_kw(
        ),

        Dictionaries: match_kw(
            "dictionaries",
            "dictionary",
        ),

        Economics: match_kw(
        ),

        Education: match_kw(
        ),

        Encyclopedias: match_kw(
            "encyclopaedias",
            "encyclopaedia",
            "encyclopedias",
            "encyclopedia",            
        ),

        Epic_Fantasy: match_kw(
        ),

        Erotica: match_kw(
        ),

        Espionage: match_kw(
        ),

        # TODO: history _plus_ a place
        European_History: match_kw(
        ),

        Family_Relationships: match_kw(
        ),

        Drama : match_kw(
            "drama",
            "plays",
        ),

        Erotica : match_kw(
            'erotic',
            'erotica',
        ),

        Fantasy : match_kw(
            "fantasy",
            "magic",
            "wizards",
            "fairies",
            "witches",
            "dragons",
            "sorcery",
        ),

        Fashion: match_kw(
            "fashion",
        ),

        Film_TV: match_kw(
        ),

        Foreign_Language_Study: match_kw(
        ),

        Gardening: match_kw(
        ),

        Graphic_Novels_Comics: match_kw(
            "comics",
            "comic books",
            "graphic novels",
        ),

        Hard_Boiled: match_kw(
        ),

        Health_Diet: match_kw(
        ),

        Hinduism: match_kw(
        ),

        Historical_Fiction : match_kw(
            "historical fiction",
        ),

        Historical_Romance: match_kw(
            "historical romance",
        ),

        History : match_kw(
            "histories",
            "history",
        ),

        Horror : match_kw(
            "ghost stories",
            "horror",
            "vampires",
            "paranormal fiction",
            "occult fiction",
        ),

        House_Home: match_kw(
        ),

        Humor : match_kw(
            "comedies",
            "comedy",
            "humor",
            "humorous",
            "satire",
            "wit",
        ),

        Humor_Entertainment: match_kw(
        ),

        # These might be a problem because they might pick up
        # hateful books. Not sure if this will be a problem.
        Islam : match_kw('islam', 'islamic', 'muslim', 'muslims'),

        Judaism: match_kw(
            'judaism', 'jewish', 'kosher', 'jews',
        ),

        LGBT: match_kw(
            'lesbian',
            'lesbians',
            'gay',
            'bisexual',
            'transgender',
            'transsexual',
            'transsexuals',
            'homosexual',
            'homosexuals',
            'homosexuality',
            'queer',
        ),

        Language_Arts_Disciplines: match_kw(
        ),

        Latin_American_History: match_kw(
        ),

        Law: match_kw(
        ),

        Legal_Thrillers: match_kw(
        ),

        Literary: match_kw(
        ),

        Literary_Collections: match_kw(
        ),

        Literary_Criticism: match_kw(
            "criticism, interpretation",
        ),

        Literary_Fiction: match_kw(
            "literary",
            "literary fiction",
        ),

        Management_Leadership: match_kw(
        ),

        Manga: match_kw(
            "manga",
        ),

        Mathematics : match_kw("mathematics"),

        Medical : match_kw("medicine", "medical"),

        Medieval_History: match_kw(
        ),

        Middle_East_History: match_kw(
        ),

        Military: match_kw(
        ),

        Military_History : match_kw(
            "military science",
            "warfare",
            "military",
            "1939-1945",
        ),

        Military_Thrillers: match_kw(
            "military thrillers",
        ),

        Modern_History: match_kw(
            "1900 - 1999",
        ),

        Movies_Gaming: match_kw(
            "film",
            "movies",
            "games",
            "video games",
            "motion picture",
            "motion pictures",
        ),

        Music: match_kw(
            "music",
        ),

        Mystery : match_kw(
            "crime",
            "detective",
            "murder",
            "mystery",
            "mysteries",
            "private investigators",
            "holmes, sherlock",
            "poirot, hercule",
            "schema:person:holmes, sherlock",
        ),

        Nature : match_kw(
            "nature",
        ),

        New_Age: match_kw(
            "new age",
        ),

        Paranormal_Romance : match_kw(
            "paranormal romance",
        ),

        Parenting_Family: match_kw(
        ),

        Performing_Arts: match_kw(
        ),

        Periodicals : match_kw(
            "periodicals",
        ),

        Personal_Finance_Investing: match_kw(
            "personal finance",
            "investing",
        ),

        Pets: match_kw(
            "pets",
            "dogs",
            "cats",
        ),

        Philosophy : match_kw(
            "philosophy",

        ),

        Photography: match_kw(
            "photography",
        ),

        Police_Procedurals: match_kw(
            "police procedural",
            "police procedurals",
        ),

        Poetry : match_kw(
            "poetry",
        ),

        Political_Science : match_kw(
            "political science",
            "goverment",
            "political economy",
        ),

        Politics_Current_Events: match_kw(
            "politics",
            "current events",
        ),

        Psychology: match_kw(
            "psychology",
            "psychiatry",
            "psychological aspects",
            "psychiatric",
        ),

        Real_Estate: match_kw(
            "real estate",
        ),

        Reference : match_kw(
            "catalogs",
            "handbooks",
            "manuals",
        ),

        Regency_Romance: match_kw(
            "regency romance",
        ),

        Religion_Spirituality : match_kw(
            "religion",
            "religious",
        ),

        Religious_Fiction: match_kw(
            "christian fiction",
            "religious fiction",
        ),

        Renaissance_History: match_kw(
        ),

        Romance : match_kw(
            "love stories",
            "romance",
            "romances",
        ),

        Romance_Erotica: match_kw(
        ),

        Science : match_kw(
            "aeronautics",
            "evolution",
            "natural history",
            "science",
        ),

        Science_Fiction : match_kw(
            "science fiction",
            "time travel",
        ),

        Science_Fiction_Fantasy: match_kw(
            "science fiction and fantasy",
            "science fiction & fantasy",
        ),

        Science_Technology_Nature: match_kw(
        ),

        Self_Help: match_kw(
        ),

        Social_Science: match_kw(
        ),

        Space_Opera: match_kw(
            "space opera",
        ),

        Sports: match_kw(
            "sports",
        ),

        Study_Aids: match_kw(
        ),

        Superhero: match_kw(
            "superhero",
            "superheroes",
        ),

        Supernatural_Thrillers: match_kw(
        ),

        Suspense_Romance : match_kw(
            "romantic suspense",
        ),

        Technology_Engineering: match_kw(
        ),

        Thrillers: match_kw(
            "thriller",
            "thrillers",
            "suspense",
        ),

        Transportation: match_kw(
        ),

        Travel : match_kw(
            "discovery",
            "exploration",
            "travel",
            "travels",
            "voyages",
        ),

        True_Crime: match_kw(
            "true crime",
        ),

        United_States_History: match_kw(
            "united states history",
            "u.s. history",
            "american revolution",
            "1775-1783",
        ),

        Urban_Fantasy: match_kw(
            "urban fantasy",
        ),

        Vegetarian_Vegan: match_kw(
            "vegetarian",
            "vegan",
            "veganism",
            "vegetarianism",
        ),

        Women_Detectives : match_kw(
            "women detectives",
            "women private investigators",
            "women sleuths",
        ),
        
        World_History: match_kw(
            "world history",
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
            if keywords and keywords.search(name):
                return genre
        return None

class LCSHClassifier(KeywordBasedClassifier):
    pass

class FASTClassifier(KeywordBasedClassifier):
    pass

class TAGClassifier(KeywordBasedClassifier):
    pass

# Make a dictionary of classification schemes to classifiers.
Classifier.classifiers[Classifier.DDC] = DeweyDecimalClassifier
Classifier.classifiers[Classifier.LCC] = LCCClassifier
Classifier.classifiers[Classifier.FAST] = FASTClassifier
Classifier.classifiers[Classifier.LCSH] = LCSHClassifier
Classifier.classifiers[Classifier.TAG] = TAGClassifier
Classifier.classifiers[Classifier.OVERDRIVE] = OverdriveClassifier
