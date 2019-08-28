"""Use external services to canonicalize names."""
import logging
import os
import re

from nose.tools import set_trace
from oclc.linked_data import OCLCLinkedData
from viaf import VIAFClient, MockVIAFClient

from core.model import (
    Contributor,
    Identifier,
)

from core.util.personal_names import (
    contributor_name_match_ratio, 
    display_name_to_sort_name,
    is_corporate_name,
    name_tidy, 
)
from core.util.titles import (
    title_match_ratio, 
)



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
        self.log = logging.getLogger("Author name canonicalizer")

    @classmethod
    def primary_author_name(self, author_name):
        """From an 'author' name that may contain multiple people, extract
        just the first name.

        This is intended to extract e.g. "Bill O'Reilly" from
        "Bill O'Reilly with Martin Dugard".

        TODO: Cases we can't handle:
         Madonna, Cher
         Ryan and Josh Shook
         van Damme, Jean Claude
        """
        if not author_name:
            return None
        if is_corporate_name(author_name):
            return author_name
        for splitter in (' with ', ' and '):
            if splitter in author_name:
                author_name = author_name.split(splitter)[0]

        author_names = author_name.split(", ")
        if len(author_names) == 2 and any(
            ' ' not in name for name in author_names
        ):
            # There are two putative author names, and one of them doesn't
            # have a space in it. The most likely scenario is that
            # this is the sort name of a single person
            # (e.g. "Tolkien, J. R. R."), not two different display names.
            # In that situation the best we can do is return the
            # sort name as-is.
            pass
        else:
            # Either there is no comma here, or the comma really does seem to
            # separate multiple peoples' names. Pick the first one.
            author_name = author_names[0]

        if author_name.endswith(','):
            # Sometimes peoples' sort names end with a period, but
            # commas, not so much.
            author_name = author_name[:-1]
        return author_name

    def canonicalize_author_name(self, display_name, identifier=None):
        """Canonicalize a book's primary author given an identifier and a
        display name.

        We are not interested in finding out which individual the
        author is, we just want to know what their name looks like in
        "Picoult, Jody" format.

        TODO: VIAF recognizes "D.H. Lawrence" and "D H Lawrence" but
        not "DH Lawrence".  NYT commonly formats names like "DH
        Lawrence".
        """
        if not identifier and not display_name:
            raise CanonicalizationError(
                "Neither useful identifier nor display name was provided."
            )

        # From an author name that potentially names multiple people,
        # extract only the first name.
        shortened_name = self.primary_author_name(display_name)

        # If we can canonicalize that shortened name, great. If not,
        # try again with the full name.
        candidates = [shortened_name]
        if display_name != shortened_name:
            candidates.append(display_name)

        # Run each potential name through various services that might
        # give us a sort name.
        for n in candidates:
            v = self.sort_name_from_services(n, identifier)
            if v:
                return v

        # All our techniques have failed. Woe! Let's just make a best
        # guess at the sort name.
        return self.default_sort_name(display_name)

    def default_sort_name(self, display_name):
        """Make a guess at the sort name for the given `display_name."""
        shortened_name = self.primary_author_name(display_name)
        return display_name_to_sort_name(shortened_name)

    def sort_name_from_services(self, display_name, identifier=None):
        """Try to find a sort name for the given author by asking various
        knowledgeable sources: our own database, OCLC Linked Data,
        and VIAF.

        :param display_name: The display name for which we're trying
            to find a sort name.

        :param identifier: An Identifier corresponding to a book we
            know this person wrote.
        """
        self.log.debug("Attempting to canonicalize %s", display_name)
        if not ' ' in display_name:
            # This is a one-named entity, like 'Cher' or 'Various'.
            # The display name and sort name are identical.
            return display_name

        # The best outcome would be that we already have a Contributor
        # with this exact display name and a known sort name. Then we
        # can reuse the information.
        sort_name, known_titles = self.sort_name_from_database(
            display_name, identifier
        )
        if sort_name:
            return sort_name

        # Looking in the database didn't work. Let's ask OCLC
        # Linked Data about this ISBN and see if it gives us an
        # author.
        uris = None
        if identifier:
            # TODO: We don't have good test coverage from
            # sort_name_from_oclc_linked_data, because it brings in
            # all the complexity of our OCLC Linked Data client. If
            # there is some kind of problem in there, we don't want it
            # to crash this whole method, so we catch and ignore
            # exceptions.
            try:
                sort_name, uris = self.sort_name_from_oclc_linked_data(
                    display_name, identifier
                )
            except Exception as e:
                self.log.error(
                    "Exception in sort_name_from_oclc_linked_data",
                    exc_info=e
                )
        if sort_name:
            return sort_name

        # Nope. If OCLC Linked Data gave us any VIAF IDs, look them up
        # and see if we can get a sort name out of them.
        sort_name = self.sort_name_from_viaf_urls(display_name, uris)
        if sort_name:
            return sort_name

        # Nope. If we were given a display name, let's ask VIAF about
        # _that_ and see what it says.
        if display_name:
            sort_name = self.sort_name_from_viaf_display_name(
                display_name, known_titles
            )

        return sort_name

    def sort_name_from_database(self, display_name, identifier=None):
        """Try to find an author sort name for this book from
        data already in the database.

        :param display_name: The display name for which we're trying
            to find a sort name.

        :param identifier: An Identifier corresponding to a book we
            know this person wrote.

        :return: A 2-tuple (sort_name, known_titles). `sort_name` is a
            known sort name for this person; known_titles is a set
            containing the titles of books we know they wrote.
        """
        sort_name = None
        known_titles = set()

        # Look up the names of books associated with `identifier.
        # This might help us distinguish between two people with
        # similar names.
        if identifier:
            editions = identifier.primarily_identifies
            for edition in editions:
                if edition.title:
                    known_titles.add(edition.title)

        # Find all Contributors with this display name.
        contributors = self._db.query(
            Contributor
        ).filter(
            Contributor.display_name==display_name
        ).filter(
            Contributor.sort_name != None
        ).order_by(
            Contributor.id
        )

        fallback_sort_name = None
        for contributor in contributors:
            sort_name = self._sort_name_from_contributor_and_titles(
                contributor, known_titles
            )
            if sort_name:
                # We've found a sort_name based on display_name
                # and a title match!
                break
            elif not fallback_sort_name:
                # If we can't match on name + title, we'll pick an
                # author based solely on name.
                fallback_sort_name = contributor.sort_name
        sort_name = sort_name or fallback_sort_name
        return sort_name, known_titles

    @classmethod
    def _sort_name_from_contributor_and_titles(cls, contributor, known_titles):
        """Return `contributor.sort_name` only if the Contributor is
        associated with an Edition whose title is similar to one of
        the `known_titles`.
        """
        if not contributor or not known_titles:
            return
        for contribution in contributor.contributions:
            if (contribution.edition and known_titles):
                check_against_title = contribution.edition.title
                if not check_against_title:
                    continue
                max_title_match_ratio = max(
                    title_match_ratio(title, check_against_title)
                    for title in known_titles
                )
                if max_title_match_ratio >= 80:
                    # 80% similarity -- close enough.
                    return contributor.sort_name

    def sort_name_from_oclc_linked_data(self, display_name, identifier):
        """Try to find an author sort name for this book from
        OCLC Linked Data.

        :param display_name: The display name for which we're trying
            to find a sort name.        

        :param identifier: An Identifier for a book we know was
            written by this person. Must be of Identifier.ISBN type.

        TODO This method is missing test coverage.
        """
        if display_name:
            test_working_display_name = name_tidy(display_name)
        else:
            test_working_display_name = None

        if (not identifier or identifier.type != Identifier.ISBN):
            # We have no way of telling OCLC Linked Data which book
            # we're talking about. Don't bother.
            return None, None

        try:
            self.log.debug(
                "Asking OCLC about works for ISBN %s", identifier
            )
            works = list(self.oclcld.oclc_works_for_isbn(identifier))
        except IOError, e:
            self.log.error(
                "OCLC errored out: %s", e, exc_info=e
            )
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
                    #if name.endswith(','):
                    #    name = name[:-1]
                    #test_name = comparable_name(name)
                    test_name = name_tidy(name)

                    match_ratio = contributor_name_match_ratio(test_name, test_working_display_name, normalize_names=False)
                    if (match_ratio > 60):
                        if (not shortest_candidate
                            or len(test_name) < len(shortest_candidate)):
                            shortest_candidate = test_name

        return shortest_candidate, uris

    def sort_name_from_viaf_urls(self, working_display_name, viaf_urls):
        if not viaf_urls:
            return None
        for uri in viaf_urls:
            m = self.VIAF_ID.search(uri)
            if not m:
                continue
            [viaf_id] = m.groups()
            contributors = self.viaf.lookup_by_viaf(
                viaf_id, working_display_name=working_display_name
            )
            if contributors:
                contributor = contributors[0]
                return contributor.sort_name
        return None

    def sort_name_from_viaf(self, display_name, known_titles=None):
        """
        Ask VIAF about the contributor, looking them up by name, 
        rather than any numeric id.

        :param display_name: Author name in First Last format.
        :param known_titles: A list of titles we know this author wrote 
            (helps better match the VIAF results if there's more than one matching VIAF author record).
        :return: Author name in Last, First format.
        """
        sort_name = None
        
        viaf_contributor = self.viaf.lookup_by_name(
            sort_name=None, display_name=display_name,
            known_titles=known_titles
        )

        if viaf_contributor:
            contributor_data = viaf_contributor[0]
            sort_name = contributor_data.sort_name
            self.log.debug(
                "Asked VIAF for sort name for %s. Response: %s",
                display_name, sort_name
            )
        return sort_name



class MockAuthorNameCanonicalizer(AuthorNameCanonicalizer):

    """Mocks the services used by the author name canonicalizer, but
    leaves the logic alone.
    """

    def __init__(self, _db, oclcld=None, viaf=None):
        super(MockAuthorNameCanonicalizer, self).__init__(_db)
        self._db = _db
        self.viaf = viaf or MockVIAFClient(_db)
        self.oclcld = oclcld or MockOCLCLinkedData(_db)
        self.log = logging.getLogger("Mocked Author Name Canonicalizer")
        self.responses = []
        self.requests = []
        self.non_response_results = [] # all mocked results that are not http response objects
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "canonicalizer")


    def queue_response(self, status_code, headers={}, content=None):
        from testing import MockRequestsResponse
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )


    def _make_request(self, url, *args, **kwargs):
        self.requests.append([url, args, kwargs])
        response = self.responses.pop()
        return HTTP._process_response(
            url, response, kwargs.get('allowed_response_codes'),
            kwargs.get('disallowed_response_codes')
        )


    def get_data(self, filename):
        # returns contents of sample file as string and as dict
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)


    def queue_non_response_result(self, dummy_result):
        self.non_response_results.insert(dummy_result)


    def sort_name_from_oclc_linked_data(self, identifier, display_name):
        """
        Skip calling parent sort_name_from_oclc_linked_data for now.  It contains http 
        calls it'd be hard to mock.  Return a dummy response.
        """
        uris = ["http://viaf.org/viaf/9581122"]
        if len(self.non_response_results) > 0:
            return self.non_response_results.pop()
        else:
            return None, uris


    def sort_name_from_viaf(self, display_name, known_titles=None):
        """
        Skip calling parent sort_name_from_viaf for now.  It contains http 
        calls it'd be hard to mock.  Return a dummy response.
        """
        if len(self.non_response_results) > 0:
            return self.non_response_results.pop()
        else:
            return None


class SimpleMockAuthorNameCanonicalizer(AuthorNameCanonicalizer):
    """Mocks the entire logic of AuthorNameCanonicalizer, allowing
    the tester to simply define the 'correct' answers ahead of time.
    """
    def __init__(self):
        self.mapping = {}
        self.canonicalize_author_name_calls = []

    def register(self, display_name, identifier, value):
        """Register the canonical author name for an
        (identifier, display_name) pair.
        """
        self.mapping[(display_name, identifier)] = value

    def canonicalize_author_name(self, display_name, identifier):
        """Record the fact that the method was called, and return
        the predefined 'correct' answer.
        """
        self.canonicalize_author_name_calls.append((display_name, identifier))
        return self.mapping.get((display_name, identifier), None)
