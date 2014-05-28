"Miscellaneous utilities"
from nose.tools import set_trace
import collections
import pkgutil
import re

class LanguageCodes(object):
    """Convert between ISO-639-2 and ISO-693-1 language codes.

    The data file comes from
    http://www.loc.gov/standards/iso639-2/ISO-639-2_utf-8.txt
    """

    two_to_three = collections.defaultdict(lambda: None)
    three_to_two = collections.defaultdict(lambda: None)
    english_names = collections.defaultdict(list)

    data = pkgutil.get_data(
        "resources", "ISO-639-2_utf-8.txt")

    for i in data.split("\n"):
        (alpha_3, terminologic_code, alpha_2, names,
         french_names) = i.strip().split("|")
        names = [x.strip() for x in names.split(";")]
        if alpha_2:
            three_to_two[alpha_3] = alpha_2
            english_names[alpha_2] = names
            two_to_three[alpha_2] = alpha_3
        english_names[alpha_3] = names


class MetadataSimilarity(object):
    """Estimate how similar two bits of metadata are."""

    SEPARATOR = re.compile("\W")

    @classmethod
    def _wordbag(cls, s):
        return set([x.strip().lower() for x in cls.SEPARATOR.split(s) if x.strip()])

    @classmethod
    def _wordbags_for_author(cls, author):
        from model import Author
        bags = [cls._wordbag(author[Author.NAME])]
        if Author.ALTERNATE_NAME in author:
            for pseudonym in author[Author.ALTERNATE_NAME]:
                bags.append(cls._wordbag(pseudonym))
        return bags

    @classmethod
    def _matching_author_in(cls, to_match, authors):
        for author in authors:
            for name in author:
                if name in to_match:
                    return name
        return None

    @classmethod
    def _word_match_proportion(cls, s1, s2, stopwords):
        """What proportion of words do s1 and s2 share, considered as wordbags?"""
        b1 = cls._wordbag(s1) - stopwords
        b2 = cls._wordbag(s2) - stopwords
        total_words = len(b1.union(b2))
        shared_words = len(b1.intersection(b2))
        return shared_words/float(total_words)

    @classmethod
    def title_similarity(cls, title1, title2):
        return cls._word_match_proportion(title1, title2, set(['a', 'the', 'an']))

    @classmethod
    def author_found_in(cls, author_name, find_among):
        a_bags = [cls._wordbag(author_name)]
        b_bags = [cls._wordbags_for_author(a) for a in find_among]
        return cls._matching_author_in(a_bags, b_bags) is not None

    @classmethod
    def authors(cls, authors1, authors2):
        """For each author in authors1, find a matching author in
        authors2, and vice versa. Quotient is the % of authors
        that match."""

        # First, convert the author dicts to lists of wordbags.
        a1 = [cls._wordbags_for_author(a) for a in authors1]
        a2 = [cls._wordbags_for_author(a) for a in authors2]

        attempts = 0
        successes = 0
        matches_found = []
        for author in a1:
            attempts += 1
            success = cls._matching_author_in(author, a2)
            if success:
                successes += 1
                matches_found.append(success)

        for author in a2:
            if author in matches_found:
                # We already matched this author from the other record
                # with an author from the current record. Don't check
                # it again.
                continue
            attempts += 1
            success = cls._matching_author_in(author, a1)
            if success:
                successes += 1

        return float(successes) / attempts
