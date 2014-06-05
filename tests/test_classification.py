"""Test logic surrounding classification schemes."""

from nose.tools import eq_, set_trace

from classification import (
    Classification,
    DeweyDecimalClassification as DDC,
    LCCClassification as LCC,
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
