from nose.tools import eq_, set_trace
from util import (
    LanguageCodes,
    MetadataSimilarity,
)

class TestLanguageCodes(object):

    def test_lookups(self):
        c = LanguageCodes
        eq_("eng", c.two_to_three['en'])
        eq_("en", c.three_to_two['eng'])
        eq_(["English"], c.english_names['en'])
        eq_(["English"], c.english_names['eng'])

        eq_("spa", c.two_to_three['es'])
        eq_("es", c.three_to_two['spa'])
        eq_(["Spanish", "Castilian"], c.english_names['es'])
        eq_(["Spanish", "Castilian"], c.english_names['spa'])

        eq_("chi", c.two_to_three['zh'])
        eq_("zh", c.three_to_two['chi'])
        eq_(["Chinese"], c.english_names['zh'])
        eq_(["Chinese"], c.english_names['chi'])

        eq_(None, c.two_to_three['nosuchlanguage'])
        eq_(None, c.three_to_two['nosuchlanguage'])
        eq_([], c.english_names['nosuchlanguage'])


class TestMetadataSimilarity(object):

    def test_identity(self):
        """Verify that we ignore the order of words in titles/authors,
        as well as non-alphanumeric characters."""

        eq_(1, MetadataSimilarity.title("foo bar", "foo bar"))
        eq_(1, MetadataSimilarity.title("foo bar", "bar, foo"))
        eq_(1, MetadataSimilarity.title("foo bar.", "FOO BAR"))

        a1 = dict(name="Foo Bar", alternateName=["baz Quux"])
        a2 = dict(name="Bar Foo", alternateName=["QUUX, baz"])
        a3 = dict(name="BAR FOO", alternateName=["baz (QuuX)"])

        eq_(1, MetadataSimilarity.authors([a1], [a2]))
        eq_(1, MetadataSimilarity.authors([a1], [a3]))
        eq_(1, MetadataSimilarity.authors([a2], [a3]))
