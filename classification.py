import json
import pkgutil
from collections import (
    Counter,
    defaultdict,
)
from nose.tools import set_trace
import re

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
        "Criticism & Theory",
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
        "Africa",
        "Ancient",
        "Asia",
        "Civil War",
        "Europe",
        "Latin America",
        "Medieval",
        "Middle East",
        "Military History",
        "Modern",
        "Renaissance",
        "United States",
        "World",
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
    def __init__(self, name, subgenres, storage, parent=None):
        self.name = name
        self.parent = parent
        self.subgenres = []
        storage[self.name] = self
        for sub in subgenres:
            self.subgenres.append(GenreData(sub, [], storage, self))

genres = dict()
for genre, subgenres in genre_structure.items():
    GenreData(genre, subgenres, genres)

# Now make a bunch of constants
Biography = genres['Biography & Memoir']
Cooking = genres["Cooking"]
Family = genres["Family & Relationships"]
History = genres["History"]
Mystery = genres["Mystery"]
Politics = genres["Politics & Current Events"]
Romance = genres["Romance"]
Religion = genres['Religion']
AfricanAmerican = genres["African-American"]

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
            q = q.filter(Subject.genre==None)
        counter = 0
        for subject in q:
            classifier = Classification.classifiers.get(
                subject.type, None)
            if not classifier:
                continue
            genredata, audience, fiction = classifier.classify(subject)
            if genredata:
                genre = Genre.lookup(self._db, genredata)
                subject.genre = genre
            if audience:
                subject.audience = audience
            if fiction:
                subject.fiction = fiction
            if genredata or audience or fiction:
                print subject
            counter += 1
            if not counter % 100:
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
        AfricanAmerican : [
            "African American Fiction",
            "African American Nonfiction",
            "Urban Fiction",
        ],
        AntiquesAndCollectibles : "Antiques",
        Biography : "Biography & Autobiography",
        Business : ["Business", "Marketing & Sales"],
        Christianity : ["Christian Fiction", "Christian Nonfiction"],
        Computers : "Computer Technology",
        Cooking : "Cooking & Food",
        Erotica : "Erotic Literature",
        Family : "Family & Relationships",
        Fantasy : "Fantasy",,
        Health : "Health & Fitness",
        HistoricalFiction : "Historical Fiction",
        History : "History",
        Horror : "Horror",
        Humor : ["Humor (Fiction)", "Humor (Nonfiction)"],
        Mystery : "Mystery",
        Politics : ["Politics", "Current Events"],
        Religion : "Religion & Spirituality",
        Romance : "Romance",
        Science : ["Science", "Physics", "Chemistry"],
        ScienceFiction : "Science Fiction",
        ScienceFictionAndFantasy : "Science Fiction & Fantasy",
        SelfHelp : "Self-Improvement",
        SocialScience : "Sociology",
        Suspense : "Suspense",
        Thriller : "Thriller",
        Travel : ["Travel", "Travel Literature"],
        Reference : "Reference",
        PersonalFinance : "Finance",
        Business : "Careers",
        MilitaryHistory : "Military",
        PerformingArts : "Performing Arts",
        Art : "Art",
        Sports : "Sports & Recreations",
        CraftsHobbiesGames : ["Crafts", "Games"],
        Nature : "Nature",
        LiteraryCriticism : ["Literary Criticism", "Criticism"],
        Education : "Education",
        NewAge : "New Age",
        Music : "Music",
        TrueCrime : "True Crime",
        HouseAndHome : "Home Design & Décor",
        Philosophy : "Philosophy",
        Psychology : "Psychology",
        LanguageArts : ["Language Arts", "Grammar & Language Usage"],
        Drama : "Drama",
        Poetry : "Poetry",
        Medical : "Medical",
        Pets : "Pets",
        StudyAids : "Study Aids & Workbooks",
        Gardening : "Gardening",
        ForeignLanguageStudy : "Foreign Language Study",
        Comics : "Comic and Graphic Books",
        Mathematics : "Mathematics",
        Architecture : "Architecture",
        Technology : "Technology",
        Photography : "Photography",
        Law : "Law",
        SelfHelp : "Self Help",
        Transportation : "Transportation",
        Management : "Management",
        LiteraryCollections : "Literary Anthologies",
    }

    @classmethod
    def scrub_identifier(cls, identifier):
        return identifier

    def fiction(cls, identifier):
        if (identifier in cls.FICTION
            or "Fiction" in identifier
            or "Literature" in identifier):
            # "Literature" on Overdrive seems to be synonymous with fiction,
            # but not necessarily "Literary Fiction".
            return True
        return False

    def audience(cls, identifier):
        if ("Juvenile" in identifier or "Picture Book" in identifier
            or "Beginning Reader" in identifier):
            return cls.AUDIENCE_CHILDREN
        elif "Young Adult" in identifier:
            return cls.AUDIENCE_YOUNG_ADULT
        return cls.ADULT

    def genre(cls, identifier):
        for l, v in cls.GENRES.items():
            if identifier in v:
                return v
        return None


class DeweyDecimalClassification(Classification):

    NAMES = None
    FICTION = set([800, 810, 811, 812, 813, 817, 820, 821, 822, 823, 827])

    GENRES = {
        Humor : set(
            [817, 827, 837, 847, 857, 867, 877, 887]
        ),
        History : set(
            range(930, 941) + [900, 904, 909, 950, 960, 970, 980, 990]
        ),
        Biography : set(
            [920, "B"]
        ),
        Philosophy : set(
            range(140, 150) + range(180, 201) + [100, 101]
        ),
        Religion : set(
            range(200,300)
        ),
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

        identifier = identifier.lower()

        if identifier.startswith('[') and identifier.endswith(']'):
            # This is just bad data.
            identifier = identifier[1:-1]

        if identifier.startswith('c') or identifier.startswith('a'):
            # A work from our Canadian neighbors or our Australian
            # friends.
            identifier = identifier[1:]
        elif identifier.startswith("nz"):
            # A work from the good people of New Zealand.
            identifier = identifier[2:]

        # Trim everything after the first period. We don't know how to
        # deal with it.
        if '.' in identifier:
            identifier = identifier.split('.')[0]
        return identifier

    @classmethod
    def is_fiction(cls, identifier, name):
        """Is the given DDC classification likely to contain fiction?"""
        if identifier in ('e', 'fic'):
            # Juvenile fiction
            return True

        if identifier == 'j':
            # Juvenile non-fiction
            return False

        if identifier.startswith('f'):
            # Adult fiction
            return True

        if identifier == 'b':
            # Biography
            return False

        if identifier == 'y':
            # Inconsistently used for young adult fiction and
            # young adult nonfiction.
            return None

        if identifier.startswith('y') or identifier.startswith('j'):
            # Young adult/children's literature--not necessarily fiction
            identifier = identifier[1:]

        try:
            identifier = int(identifier)
        except Exception, e:
            return False
        if identifier in cls.FICTION:
            return True
        if identifier >= 830 and identifier <= 899:
            # TODO: make this less of a catch-all.
            return True

        return False

    @classmethod
    def audience(cls, identifier, name):
        if identifier in ('e', 'fic'):
            # Juvenile fiction
            return cls.AUDIENCE_CHILDREN

        if identifier.startswith('j'):
            return cls.AUDIENCE_CHILDREN

        if identifier.startswith('y'):
            return cls.AUDIENCE_YOUNG_ADULT

        return cls.AUDIENCE_ADULT

    @classmethod
    def genre(cls, identifier, name):

        if identifier not in ('e', 'fic', 'j', 'b', 'y'):
            # Strip off everything except the three-digit number.
            identifier = identifier[-3:]
            try:
                # Turn a three-digit number into a top-level code.
                identifier = int(identifier)
                identifiers = [identifier, identifier / 100 * 100]
            except ValueError, e:
                # Oh well, try a lookup, maybe it'll work. (Probably not.)
                pass
        for genre, identifiers in cls.GENRES.items():
            if identifier in identifiers:
                return genre
        return None
    

class LCCClassification(Classification):

    TOP_LEVEL = re.compile("^([A-Z]{1,2})")
    FICTION = set(["P", "PN", "PQ", "PR", "PS", "PT", "PZ"])
    JUVENILE = set(["PZ"])

    GENRES = {
        Cooking : (
            [], set(["TX"])
        ),

        FineArts : (
            [re.compile("[MN].*")],
            [],
        ),

        History : (
            [re.compile("[CDEF].*")],
            set(["AZ", "KBR", "LA"]),
        ),

        Philosophy : (
            [],
            set(["B", "BC", "BD"]),
        ),

        Reference : (
            [],
            set(["AE", "AG", "AI", "AY"])
        ),

        Religion : (
            [],
            set([
                "KB", "KBM", "KBP", "KBR", "KBU",
                "BL", "BM", "BP", "BQ", "BR", "BS", "BT", "BV",
                "BX",
            ])
        ),

        Science: (
            [re.compile("[QRS].*"), re.compile("T[A-P].*")],
            [],
        ),
    }

    NAMES = {}
    @classmethod
    def _load(cls):
        cls.NAMES = json.loads(
            pkgutil.get_data("resources", "lcc_one_level.json"))

    @classmethod
    def scrub_identifier(cls, identifier):
        # We don't currently have an understanding of anything
        # beyond the first two characters of an LCC identifier.
        return identifier[:2].upper()

    @classmethod
    def name_for(cls, identifier):
        return cls.NAMES.get(identifier, None)

    @classmethod
    def is_fiction(cls, identifier, name):
        return identifier in cls.FICTION

    @classmethod
    def genre(cls, identifier, name):
        for genre, (res, strings) in cls.GENRES.items():
            if identifier in strings:
                return genre
            for r in res:
                if r.match(identifier):
                    return genre
        return None

    @classmethod
    def audience(cls, identifier, name):
        if identifier in cls.JUVENILE:
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
    JUVENILE_INDICATORS = match_kw("for children", "children's", "juvenile")
    YOUNG_ADULT_INDICATORS = match_kw("young adult", "ya")

    GENRES = {
        Adventure : match_kw(
            "adventure",
            "western stories",
            "adventurers",
            "sea stories",
            "war stories",
        ),
        Biography : match_kw(
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
        Reference : match_kw(
            "catalogs",
            "dictionaries",
            "encyclopedias",
            "handbooks",
            "manuals",
        ),
        Religion : match_kw(
            "bible",
            "christianity",
            "church",
            "islam",
            "judaism",
            "religion",
            "religious",
            "sermons",
            "theological",
            "theology",
            'biblical',
        ),
        Romance : match_kw(
            "love stories",
            "romance",
            "romances",
        ),
        Science : match_kw(
            "aeronautics",
            "evolution",
            "mathematics",
            "medicine",
            "natural history",
            "science",
        ),
        ScienceFiction : match_kw(
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
            return cls.AUDIENCE_CHILDREN
        if cls.YOUNG_ADULT_INDICATORS.search(name):
            return cls.AUDIENCE_YOUNG_ADULT
        return None

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
