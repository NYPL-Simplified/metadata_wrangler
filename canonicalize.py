"""Use external services to canonicalize names."""
from nose.tools import set_trace
from oclc import OCLCLinkedData
from core.util import MetadataSimilarity
from core.util.personal_names import (
    display_name_to_sort_name,
    is_corporate_name,
)
from viaf import VIAFClient
from core.model import (
    Contributor,
    Identifier,
)
import re

class CanonicalizationError(Exception):
    pass

class AuthorNameCanonicalizer(object):

    """Does whatever it takes to find the name of a book's primary author
    in canonicalized ("Picoult, Jody") form.
    """

    VIAF_ID = re.compile("^http://viaf.org/viaf/([0-9]+)$")

    def __init__(self, _db, oclcld=None, viaf=None):
        self._db = _db
        self.oclcld = oclcld or OCLCLinkedData(_db)
        self.viaf = viaf or VIAFClient(_db)

    @classmethod
    def primary_author_name(self, author_name):
        """From an 'author' name that may contain multiple people, extract
        just the first name.

        This is intended to extract e.g. "Bill O'Reilly" from
        "Bill O'Reilly with Martin Dugard".

        TODO: When the author is "Ryan and Josh Shook" I really have no clue
        what to do.
        """
        if is_corporate_name(author_name):
            return author_name
        for splitter in (' with ', ' and '):
            if splitter in author_name:
                author_name = author_name.split(splitter)[0]
        author_name = author_name.split(", ")[0]

        return author_name

    def canonicalize(self, identifier, display_name):
        """Canonicalize a book's primary author given an identifier and a
        display name.

        We are not interested in finding out which individual the
        author is, we just want to know what their name looks like in
        "Picoult, Jody" format.

        TODO: VIAF recognizes "D.H. Lawrence" and "D H Lawrence" but
        not "DH Lawrence".  NYT commonly formats names like "DH
        Lawrence".
        """    
        if identifier.type != Identifier.ISBN:
            # The only identifier we can do a useful lookup on is
            # ISBN.
            identifier = None
        if not identifier and not display_name:
            raise CanonicalizationError(
                "Neither useful identifier nor display name was provided.")

        # From an author name that potentially names multiple people,
        # extract only the first name.
        shortened_name = self.primary_author_name(display_name)

        # If we can canonicalize that shortened name, great. If not,
        # try again with the full name.
        for n in shortened_name, display_name:
            v = self._canonicalize(identifier, n)
            if v:
                return v

        # All our techniques have failed. Let's just convert the
        # shortened display name to a sort name and hope for the best.
        sort_name = display_name_to_sort_name(shortened_name)
        print "FAILURE on %s, going with %s" % (
            display_name, sort_name)
        return sort_name


    def _canonicalize(self, identifier, display_name):
        # The best outcome would be that we already have a Contributor
        # with this exact display name and a known sort name.
        contributors = self._db.query(Contributor).filter(
            Contributor.display_name==display_name).filter(
                Contributor.name != None).all()
        sort_name = None
        if contributors and False:
            # Yes, awesome. Use this name.
            return contributors[0].name

        # Looking in the database didn't work. Let's ask OCLC
        # Linked Data about this ISBN and see if it gives us an
        # author.
        uris = None
        if identifier:
            sort_name, uris = self.sort_name_from_oclc_linked_data(
                identifier, display_name)
        if sort_name:
            return sort_name

        # Nope. If OCLC Linked Data gave us any VIAF IDs, look them up
        # and see if we can get a sort name out of them.
        if uris:
            for uri in uris:
                m = self.VIAF_ID.search(uri)
                if m:
                    viaf_id = m.groups()[0]
                    viaf_id, display_name, family_name, sort_name, wikipedia_name = (
                        self.viaf.lookup_by_viaf(
                            viaf_id, working_display_name=display_name))
                    if sort_name:
                        return sort_name

        # Nope. If we were given a display name, let's ask VIAF about it
        # and see what it says.
        if display_name:
            sort_name = self.sort_name_from_viaf(display_name)
        return sort_name

    def sort_name_from_oclc_linked_data(
            self, identifier, display_name):
        """Try to find an author sort name for this book from
        OCLC Linked Data.
        """
        def comparable_name(s):
            return s.replace(",", "").replace(".", "")

        test_working_display_name = comparable_name(display_name)

        if identifier.type != Identifier.ISBN:
            # We have no way of telling OCLC Linked Data which book
            # we're talking about. Don't bother.
            return None

        try:
            works = list(self.oclcld.oclc_works_for_isbn(identifier))
        except IOError, e:
            works = []
        shortest_candidate = None
        uris = []
        for work in works:
            graph = self.oclcld.graph(work)
            # TODO: Unroll this. We should try the creator names, then
            # the creator URIs, then the contributor names, then the
            # contributor URIs.
            for field_name in ('creator', 'contributor'):
                names, new_uris = self.oclcld.creator_names(graph, field_name)
                if field_name == 'creator':
                    # Contributor URIs have too much junk in them to
                    # be trustworthy.
                    uris.extend(new_uris)
                for name in names:
                    if name.endswith(','):
                        name = name[:-1]
                    test_name = comparable_name(name)
                    sim = MetadataSimilarity.title_similarity(
                        test_name, test_working_display_name)
                    if sim > 0.6:
                        if (not shortest_candidate 
                            or len(name) < len(shortest_candidate)):
                            shortest_candidate = name
        return shortest_candidate, uris

    def sort_name_from_viaf(self, display_name):
        viaf, display_name, family_name, sort_name, wikipedia_name = (
                self.viaf.lookup_by_name(None, display_name))
        return sort_name

