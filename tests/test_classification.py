"""Test logic surrounding classification schemes."""

from nose.tools import eq_, set_trace

from classification import (
    Classification,
    DeweyDecimalClassification as DDC,
    LCCClassification as LCC,
    LCSHClassification as LCSH,
    )

class TestDewey(object):

    def test_lookup(self):
        """Do a simple spot check lookup."""
        eq_("General statistics of Europe",
            DDC.lookup("314"))

    def test_nonexistent_lookup(self):
        eq_(None, DDC.lookup("no-such-key"))

    def test_names(self):

        child = Classification.AUDIENCE_CHILDREN
        adult = Classification.AUDIENCE_ADULT

        eq_([('B', 'Biography', child, False)], list(DDC.names("JB")))
        eq_([('FIC', 'Juvenile Fiction', child, True)], list(DDC.names("FIC")))
        eq_([('FIC', 'Juvenile Fiction', child, True)], list(DDC.names("Fic")))
        eq_([('E', 'Juvenile Fiction', child, True)], list(DDC.names("E")))

        eq_(
            [('400', u'Language', child, False),
             ('405', u'Serial publications', child, False)],
            list(DDC.names("J405"))
        )

        eq_(
            [('400', u'Language', adult, False),
             ('406', u'Organizations & management', adult, False)],
            list(DDC.names("406"))
        )

        eq_(
            [('400', u'Language', adult, False)],
            list(DDC.names("400"))
        )

        eq_(
            [('400', u'Language', adult, False)],
            list(DDC.names("400"))
        )

        eq_(
            [("600", u"Technology", adult, False),
             ("616", u"Diseases", adult, False)],
            list(DDC.names("616.9940092")))

    def test_is_fiction(self):
        eq_(False, DDC.is_fiction("B"))
        eq_(True, DDC.is_fiction("E"))
        eq_(True, DDC.is_fiction("FIC"))
        eq_(True, DDC.is_fiction("Fic"))
        eq_(False, DDC.is_fiction(615))
        eq_(True, DDC.is_fiction(800))
        eq_(True, DDC.is_fiction(891))
        eq_(False, DDC.is_fiction(814))

class TestLCC(object):
    def test_lookup(self):
        """Do a simple spot check lookup."""
        eq_("Local government.  Municipal government",
            LCC.lookup("JS"))

    def test_nonexistent_lookup(self):
        eq_(None, LCC.lookup("no-such-key"))

    def test_names(self):

        child = Classification.AUDIENCE_CHILDREN
        adult = Classification.AUDIENCE_ADULT

        eq_([('P', u'LANGUAGE AND LITERATURE', 'Adult', True), 
             ('PR', u'English literature', 'Adult', True)], 
            list(LCC.names("PR")))
        eq_([('P', u'LANGUAGE AND LITERATURE', 'Children', True), 
             ('PZ', u'Fiction and juvenile belles lettres', 'Children', True)],
            list(LCC.names("PZ")))
        eq_([('E', u'HISTORY OF THE AMERICAS', 'Adult', False)], 
            list(LCC.names("E176.8")))
        eq_([('P', u'LANGUAGE AND LITERATURE', 'Adult', True), 
             ('PS', u'American literature', 'Adult', True)], 
            list(LCC.names("PS2384.M6")))
        eq_([('P', u'LANGUAGE AND LITERATURE', 'Children', True), 
             ('PZ', u'Fiction and juvenile belles lettres', 'Children', True)],
            list(LCC.names("PZ8.G882")))
        eq_([('P', u'LANGUAGE AND LITERATURE', 'Adult', True), 
             ('PS', u'American literature', 'Adult', True)], 
            list(LCC.names("PS2384 M68 2003")))
        eq_([('P', u'LANGUAGE AND LITERATURE', 'Adult', True), 
             ('PA', u'Classical philology', 'Adult', False)], 
            list(LCC.names("PA7.C234937")))
        eq_([('P', u'LANGUAGE AND LITERATURE', 'Adult', True),
             ('PN', u'Literature (General)', 'Adult', True)], 
            list(LCC.names("PN1997")))
        eq_([('J', u'POLITICAL SCIENCE', 'Adult', False)],
            list(LCC.names("J821.8 CARRIKK")))
        eq_([('D', u'WORLD HISTORY AND HISTORY OF EUROPE, ASIA, AFRICA, AUSTRALIA, NEW ZEALAND, ETC.', 'Adult', False), 
             ('DC', u'History of France', 'Adult', False)],
            list(LCC.names("DC235")))

        # This showed up in real data, but it's not a valid LCC
        # classification.
        eq_([], list(LCC.names("W85-12")))

    def test_is_fiction(self):
        eq_(False, LCC.is_fiction("A"))
        eq_(False, LCC.is_fiction("AB"))
        eq_(True, LCC.is_fiction("P"))
        eq_(False, LCC.is_fiction("PA"))
        eq_(True, LCC.is_fiction("PN"))
        eq_(True, LCC.is_fiction("PQ"))
        eq_(True, LCC.is_fiction("PT"))
        eq_(True, LCC.is_fiction("PZ"))



class TestLCSH(object):

    def test_is_fiction(self):
        eq_(True, LCSH.is_fiction("Science fiction"))
        eq_(True, LCSH.is_fiction("Science fiction, American"))
        eq_(True, LCSH.is_fiction("Fiction"))
        eq_(True, LCSH.is_fiction("Historical fiction"))
        eq_(True, LCSH.is_fiction("Biographical fiction"))
        eq_(True, LCSH.is_fiction("Detective and mystery stories"))
        eq_(True, LCSH.is_fiction("Horror tales"))
        eq_(True, LCSH.is_fiction("Classical literature"))
        eq_(False, LCSH.is_fiction("History and criticism"))
        eq_(False, LCSH.is_fiction("Biography"))
        eq_(None, LCSH.is_fiction("Kentucky"))
        eq_(None, LCSH.is_fiction("Social life and customs"))


    def test_audience(self):
        child = Classification.AUDIENCE_CHILDREN
        eq_(child, LCSH.audience("Children's stories"))
        eq_(child, LCSH.audience("Picture books for children"))
        eq_(child, LCSH.audience("Juvenile fiction"))
        eq_(child, LCSH.audience("Juvenile poetry"))
        eq_(None, LCSH.audience("Runaway children"))
        eq_(None, LCSH.audience("Humor"))


class TestClassifier(object):


    def test_misc(self):
        adult = Classification.AUDIENCE_ADULT
        child = Classification.AUDIENCE_CHILDREN

        data = {"DDC": [{"id": "813.4", "weight": 137}], "LCC": [{"id": "PR9199.2.B356", "weight": 48}], "FAST": [{"weight": 103, "id": "1719440", "value": "Mackenzie, Alexander, 1764-1820"}, {"weight": 25, "id": "969633", "value": "Indians of North America"}, {"weight": 22, "id": "1064447", "value": "Pioneers"}, {"weight": 17, "id": "918556", "value": "Explorers"}, {"weight": 17, "id": "936416", "value": "Fur traders"}, {"weight": 17, "id": "987694", "value": "Kings and rulers"}, {"weight": 7, "id": "797462", "value": "Adventure stories"}, {"weight": 5, "id": "1241420", "value": "Rocky Mountains"}]}
        classified = Classification.classify(data, True)

        # This is pretty clearly fiction intended for an adult
        # audience.
        assert classified['audience'][Classification.AUDIENCE_ADULT] > 0.6
        assert classified['audience'][Classification.AUDIENCE_CHILDREN] == 0
        assert classified['fiction'][True] > 0.6
        assert classified['fiction'][False] == 0

        # Its LCC classifications are heavy on the literature.
        names = classified['names']
        eq_(0.5, names['LCC']['LANGUAGE AND LITERATURE'])
        eq_(0.5, names['LCC']['English literature'])

        # Alexander Mackenzie is more closely associated with this work
        # than the Rocky Mountains.
        assert (names['FAST']['Mackenzie, Alexander, 1764-1820'] > 
                names['FAST']['Rocky Mountains'])

        # But the Rocky Mountains ain't chopped liver.
        assert names['FAST']['Rocky Mountains'] > 0

    def test_lcsh(self):

        adult = Classification.AUDIENCE_ADULT
        child = Classification.AUDIENCE_CHILDREN

        data = {"LCC": [{"id": "NC"}], "LCSH": [{"id": "World War, 1914-1918"}, {"id": "World War, 1914-1918 -- Pictorial works"}, {"id": "Illustrated books"}]}

        classified = Classification.classify(data, True)

        # We're not sure it's nonfiction, but we have no indication
        # whatsoever that it's fiction.
        assert classified['fiction'][True] == 0
        assert classified['fiction'][False] > 0.3

        # We're not sure its for adults, but we have no indication
        # whatsoever that it's for children.
        assert classified['audience'][child] == 0
        assert classified['audience'][adult] > 0.3

        # It's more closely associated with "World War, 1914-1918"
        # than with any other LCSH classification.
        champ = None
        for k, v in classified['codes']['LCSH'].items():
            if not champ or v > champ[1]:
                champ = (k,v)
        eq_(champ, ("World War, 1914-1918", 0.5))

