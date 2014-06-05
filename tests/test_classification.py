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
