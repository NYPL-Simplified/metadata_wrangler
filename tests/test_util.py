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

    def test_author_found_in(self):
        eq_(True, MetadataSimilarity.author_found_in(
            "Herman Melville", [dict(name="Melville, Herman"),
                                dict(name="Someone else")]))

        eq_(False, MetadataSimilarity.author_found_in(
            "Herman Melville", [dict(name="Someone else")]))

        eq_(False, MetadataSimilarity.author_found_in(
            "No Such Person", {'roles': ['Author'], 'deathDate': '1891', 'name': 'Melville, Herman', 'birthDate': '1819'}, {'name': 'Tanner, Tony', 'roles': ['Editor', 'Commentator for written text', 'Author of introduction', 'Author']}))

        eq_(True, MetadataSimilarity.author_found_in(
            "Lewis Carroll", [dict(name="Someone else"),
                              dict(name="Charles Dodgson",
                                   alternateName=["Lewis Carroll"])]))


    # def test_not_quite_identity(self):
    #     main = "The Adventures of Huckleberry Finn (Tom Sawyer's Comrade)"
    #     print MetadataSimilarity.title(
    #         "The Adventures of Huckleberry Finn",
    #         "The Adventures of Tom Sawyer")
    #     for expect, i in [
    #             (0.4444444444444445, "Adventures of Huckleberry Finn"),
    #             (0.4, "The adventures of Huck Finn"),
    #             (0.5555555555555556, "The adventures of Tom Sawyer"),
    #           ]:
    #         eq_(expect, MetadataSimilarity.title(main, i))

    #     [{"alternateName": ["Twain, Mark (Samuel Clemens)", "Clemens, Samuel Langhorne"], "name": "Twain, Mark"}]

    #     "Alice in Wonderland"
    #     "Alice's adventures in Wonderland"
    #     "Alice's adventures in Wonderland; and, Through the looking-glass and what Alice found there"
    #     "Through the looking-glass and what Alice found there"
    #     "The annotated Alice : Alice's adventures in Wonderland &amp; Through the looking-glass"
    #     'The nursery "Alice,"'


    #     "Alice in Zombieland"
    #     [{"roles": ["Author"], "name": "Cook, Nickolas", "birthDate": "1969"}]

    #     "Moby-Dick, or, The whale"
    #     "Moby Dick; notes"
    #     "Moby Dick; or, The white whale."
