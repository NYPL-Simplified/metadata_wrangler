import logging
import re
import urllib

from lxml import etree
from nose.tools import set_trace
from sqlalchemy.orm.session import Session

from core.coverage import (
    IdentifierCoverageProvider,
    CoverageFailure,
)
from core.metadata_layer import (
    ContributorData,
    IdentifierData,
    MeasurementData,
    Metadata,
    ReplacementPolicy,
    SubjectData,
)
from core.model import (
    get_one_or_create,
    Contribution,
    Contributor,
    DataSource,
    Edition,
    Identifier,
    Measurement,
    Representation,
    Subject,
)
from core.util import MetadataSimilarity
from core.util.xmlparser import XMLParser
from coverage_utils import MetadataWranglerBibliographicCoverageProvider
from viaf import NameParser as VIAFNameParser

class OCLC(object):
    """Repository for OCLC-related constants."""
    EDITION_COUNT = "OCLC.editionCount"
    HOLDING_COUNT = "OCLC.holdings"
    FORMAT = "OCLC.format"

class OCLCClassifyXMLParser(XMLParser):
    """Turn the output of the OCLC Classify API into a Metadata object
    (if the ISBN identifies a single work) or a list of OCLC Work IDs
    (if the ISBN identifies multiple works).

    The Metadata object includes ContributorData for the book's
    contributors, SubjectData representing the most common
    classifications for the book, and MeasurementData describing how
    many libraries have collected the book.
    """

    # OCLC in-representation 'status codes'
    SINGLE_WORK_SUMMARY_STATUS = 0
    SINGLE_WORK_DETAIL_STATUS = 2
    MULTI_WORK_STATUS = 4
    NO_INPUT_STATUS = 100
    INVALID_INPUT_STATUS = 101
    NOT_FOUND_STATUS = 102
    UNEXPECTED_ERROR_STATUS = 200

    NAMESPACES = {'oclc' : 'http://classify.oclc.org'}
    ROLES = re.compile("\[([^]]+)\]$")
    LIFESPAN = re.compile("([0-9]+)-([0-9]*)[.;]?$")

    CLASSIFIERS = {"fast": Subject.FAST, "lcc": Subject.LCC, "ddc": Subject.DDC}

    @classmethod
    def initial_look_up(cls, tree):
        """Extract the internal 'status code' and any OCLC Work IDs
        from a preparsed XML document.

        :param tree: An XML document parsed with etree
        :return A 2-tuple (code, [owis]). Each item in `owis` is
            an IdentifierData describing an OCLC Work ID.
        """
        code = int(cls._xpath1(tree, "oclc:response").get('code'))
        return code, cls._owi_data(tree)

    @classmethod
    def _owi_data(cls, tree):
        """Extract OCLC Work IDs from a preparsed XML document.

        :param tree: An XML document parsed with etree
        :return A list of IdentifierData
        """
        results = []
        tags = cls._xpath(tree, "//oclc:work")

        owi_numbers = [tag.get("owi") for tag in tags]
        for number in owi_numbers:
            data = IdentifierData(Identifier.OCLC_WORK, number)
            results.append(data)
        return results

    @classmethod
    def parse(cls, tree, metadata):
        """Process a preparsed XML document and annotate a
        preexisting Metadata with the result.

        :param tree: An XML document parsed with etree
        :param metadata: A Metadata to be improved with the information
            from the XML document.
        """
        # If the Metadata already has ContributorData objects, do
        # nothing -- otherwise we're likely to add duplicate or
        # irrelevant information.
        if not metadata.contributors:
            metadata.contributors = cls.contributors(tree)

        # SubjectData and MeasurementData are additive. If two OWIs
        # have the same classification the Metadata should get one
        # SubjectData with the total weight.

        # Build a dictionary mapping (type, identifier) to SubjectData,
        # so that add_subjects() has an easier job.
        existing_subjects = dict()
        for subject in metadata.subjects:
            existing_subjects[(subject.type, subject.identifier)] = subject
        cls.add_subjects(tree, metadata, existing_subjects)

        # Similarly for MeasurementData and add_measurements().
        existing_measurements = dict()
        for measurement in metadata.measurements:
            existing_measurements[measurement.quantity_measured] = measurement
        cls.add_measurements(tree, metadata, existing_measurements)

        return metadata

    # CONTRIBUTORS:

    @classmethod
    def contributors(cls, tree):
        """Returns a list of ContributorData objects"""
        results = []
        tags = cls._xpath(tree, "//oclc:authors/oclc:author")
        for tag in tags:
            contributor, default_role_used = NameParser.parse(tag.text)
            results.append(cls._add_lc_viaf(contributor, tag))

        return results

    @classmethod
    def _add_lc_viaf(cls, author, tag):
        """Annotate a ContributorData object with LC and VIAF identifiers
        found in an <author> tag from an OCLC CLassify document.
        """
        author.lc = tag.get("lc") or None
        author.viaf = tag.get("viaf") or None
        return author

    # MEASUREMENTS:

    @classmethod
    def add_measurements(cls, tree, metadata, existing_measurements):
        """Extract MeasurementData from a parsed XML document
        and update a list of existing MeasurementData.
        """
        tags = cls._xpath(tree, "//oclc:work")
        for measurement in cls.get_measurements(tags):
            key = measurement.quantity_measured
            if key in existing_measurements:
                # We don't need another MeasurementData -- just add it to
                # the preexisting value.
                existing_measurements[key].value += measurement.value
            else:
                # We do need another MeasurementData -- add it to the
                # Metadata.
                metadata.measurements.append(measurement)

    # Maps OCLC attribute names to the link relations we use when
    # creating Measurements.
    MEASUREMENT_MAPPING = {
        "holdings" : Measurement.HOLDINGS,
        "eholdings" : Measurement.HOLDINGS,
        "editions" : Measurement.PUBLISHED_EDITIONS,
    }

    @classmethod
    def get_measurements(cls, work_tags):
        """Extract MeasurementData representing values
        measured from a list of <work> tags.

        The MeasurementData returned are totals; their number
        is independent of the number of <work> tags.

        :param work_tags: A list of preparsed <work> tags.
        :return: A list of MeasurementData objects
        """
        # Start with all possible values set to zero.
        data = dict()
        for rel in set(cls.MEASUREMENT_MAPPING.values()):
            data[rel] = 0
        # In some cases, there are different versions of the book, each
        # listed under a different work tag with its own measurements; for each
        # type of measurement, we need to add up the numbers for each work tag.
        for work_tag in work_tags:
            cls._update_data(work_tag, data)

        measurement_data_objects = cls.make_measurement_data(data)
        return measurement_data_objects

    @classmethod
    def _update_data(cls, work_tag, data):
        """Update a dictionary based on integer values found in
        a <work> tag.

        :param work_tag: A preparsed <work> tag.
        :param data: A dictionary containing totals.
        """
        for key, rel in cls.MEASUREMENT_MAPPING.items():
            if work_tag.get(key):
                data[rel] += int(work_tag.get(key))

    @classmethod
    def make_measurement_data(cls, data):
        """Convert a dictionary containing totals to a list of
        MeasurementData objects.
        """
        results = []
        for item in data:
            results.append(
                MeasurementData(
                    quantity_measured=item,
                    value=data[item]
                )
            )
        return results

    # SUBJECTS

    @classmethod
    def add_subjects(cls, tree, metadata, existing_subjects):
        """Extract SubjectData from a parsed XML document
        and update a list of existing SubjectData.
        """
        classifiers = cls.get_classifier_names_and_parent_tags(tree)
        subjects = cls.get_subjects(classifiers)
        for subject in subjects:
            key = (subject.type, subject.identifier)
            if key in existing_subjects:
                # We don't need another SubjectData -- just bump up
                # the weight on this old one.
                existing_subjects[key].weight += subject.weight
            else:
                # We do need another SubjectData -- add it to the
                # Metadata.
                metadata.subjects.append(subject)

        return subjects

    @classmethod
    def get_classifier_names_and_parent_tags(cls, tree):
        """Extracts the <ddc>, <lcc>, and <fast> tags from an XML
        document representing a single work.

        :return: A dictionary mapping a classifier name to all the
        tags representing subjects for that classifier.
        """
        mapping = dict()
        for name in cls.CLASSIFIERS.keys():
            tags = cls._xpath(tree, "//oclc:%s" % name)
            if tags:
                mapping[name] = tags
        return mapping

    @classmethod
    def get_subjects(cls, classifiers):
        """Convert a list of tags from an XML document into a corresponding
        list of SubjectData objects.

        :param classifiers: A dictionary mapping classifier names to
        etree tags. The classifier name is the type of classifier--DDC, LCC, or
        Fast.

        The element tags are formatted such that the names are buried
        in a longer string (e.g. "{http://classify.oclc.org}ddc"), so
        it's easiest to just keep track of them separately, rather
        than having to extract them every time.
        """
        subjects = []
        for classifier_name, classifier_tags in classifiers.items():
            for tag in classifier_tags:
                subtags = cls._extract_subtags(classifier_name, tag)
                subjects.extend(
                    cls.make_subject_data(classifier_name, subtags)
                )
        return subjects

    @classmethod
    def _extract_subtags(cls, classifier_name, classifier_tag):
        """
        :param classifier_name: the name of a classifier type (e.g. "ddc")
        :param classifier_tag: An etree tag containing classification
           data.
        """

        # Some classifier types use 'heading', some use 'mostPopular'.
        # Try it both ways and see which one works.
        matches = None
        for item in ("heading", "mostPopular"):
            search_term = "//oclc:%s//oclc:%s" % (classifier_name, item)
            matches = cls._xpath(classifier_tag, search_term)
            if matches:
                break
        return matches

    @classmethod
    def make_subject_data(cls, classifier_name, classifier_tags):
        """Create a SubjectData object for every tag.

        :param classifier_name: The name of the classifier to use
        when determining what this SubjectData means.

        :param classifier_tags: A list of etree tags, each containing
        a single classification.
        """
        results = []
        for tag in classifier_tags:
            weight, identifier, name = cls._parse_subject_tag(tag)
            subject_data = SubjectData(
                type=cls.CLASSIFIERS[classifier_name],
                weight=weight,
                identifier=identifier,
                name=name,
            )
            results.append(subject_data)
        return results


    @classmethod
    def _parse_subject_tag(cls, tag):
        """Extract from an etree tag the information necessary to
        create a SubjectData.
        """
        # The names of the attributes vary depending on which type of classifier
        # the tag is for.  Only Fast tags have a name.
        weight = tag.get("holdings") or tag.get("heldby")
        identifier = tag.get("ident") or tag.get("nsfa") or tag.get("sfa")
        name = tag.text or None
        return int(weight), identifier, name


class NameParser(VIAFNameParser):
    """Parse the name format used by OCLC Classify.

    This is like the VIAF format with the addition of optional roles
    for authors.

    Example:
     Giles, Lionel, 1875-1958 [Writer of added commentary; Translator]

    In addition, multiple authors are sometimes given in one string,
    separated by the pipe character.

    Example:
     Leodhas, Sorche Nic, 1898-1969 | Ness, Evaline [Illustrator]
    """

    ROLES = re.compile("\[([^]]+)\]$")

    # Map the roles defined in OCLC Classify to the constants
    # defined in Contributor.
    ROLE_MAPPING = {
        "Author": Contributor.AUTHOR_ROLE,
        "Translator": Contributor.TRANSLATOR_ROLE,
        "Illustrator": Contributor.ILLUSTRATOR_ROLE,
        "Editor": Contributor.EDITOR_ROLE,
        "Unknown": Contributor.UNKNOWN_ROLE,
        "Contributor": Contributor.CONTRIBUTOR_ROLE,
        "Author of introduction": Contributor.INTRODUCTION_ROLE,
        "Other": Contributor.UNKNOWN_ROLE,
        "Creator": Contributor.AUTHOR_ROLE,
        "Artist": Contributor.ARTIST_ROLE,
        "Associated name": Contributor.ASSOCIATED_ROLE,
        "Photographer": Contributor.PHOTOGRAPHER_ROLE,
        "Compiler": Contributor.COMPILER_ROLE,
        "Adapter": Contributor.ADAPTER_ROLE,
        "Editor of compilation": Contributor.EDITOR_ROLE,
        "Narrator": Contributor.NARRATOR_ROLE,
        "Author of afterword, colophon, etc.": Contributor.AFTERWORD_ROLE,
        "Performer": Contributor.PERFORMER_ROLE,
        "Author of screenplay": Contributor.AUTHOR_ROLE,
        "Writer of added text": Contributor.AUTHOR_ROLE,
        "Composer": Contributor.COMPOSER_ROLE,
        "Lyricist": Contributor.LYRICIST_ROLE,
        "Author of dialog": Contributor.AUTHOR_ROLE,
        "Film director": Contributor.DIRECTOR_ROLE,
        "Actor": Contributor.ACTOR_ROLE,
        "Musician": Contributor.MUSICIAN_ROLE,
        "Filmmaker": Contributor.DIRECTOR_ROLE,
        "Producer": Contributor.PRODUCER_ROLE,
        "Director": Contributor.DIRECTOR_ROLE,
    }

    @classmethod
    def parse_multiple(cls, author_string):
        """Parse a list of people.

        :return: A list of ContributorData objects.
        """

        # We start off assuming that someone with no explicit role
        # is the primary author.
        default_role = Contributor.PRIMARY_AUTHOR_ROLE
        contributors = []
        if not author_string:
            return contributors
        for author in author_string.split("|"):
            contributor, default_role_used = cls.parse(author, default_role)
            contributors.append(contributor)

            # Start using a new default role if necessary.
            default_role = cls._default_role_transition(
                contributor.roles, default_role_used
            )
        return contributors

    @classmethod
    def _default_role_transition(cls, contributor_roles,
                                 contributor_role_is_default):
        """Modify the default role to be used from this point on.

        :param contributor_roles: The list of roles assigned to the
        most recent contributor.

        :param contributor_role_is_default: If this is True, the
        contributor's roles came from the default. If this is False,
        they were explicitly specified in the original.

        :return: The role to use in the future when a user has no
        explicitly specified role.
        """
        if Contributor.PRIMARY_AUTHOR_ROLE in contributor_roles:
            # There can only be one primary author. No matter what,
            # the default role becomes AUTHOR_ROLE beyond this point.
            return Contributor.AUTHOR_ROLE

        if not any(
            role in contributor_roles for role in Contributor.AUTHOR_ROLES
        ):
            # The current contributor is not any kind of author. If
            # we see someone with no explicit role after this point,
            # it's probably because their role is so minor as to not
            # be worth mentioning, not because it's so major that we
            # can assume they're an author.
            return Contributor.UNKNOWN_ROLE

        # The only possibility now is that the user has one of the
        # AUTHOR_ROLES other than PRIMARY_AUTHOR.
        #
        # Now it matters whether the roles were assigned explicitly or
        # whether the default was used.
        if contributor_role_is_default:
            # This author-like contributor was not given an explicit
            # role, so if the next contributor has no explicit role
            # they are probably also an author.
            return Contributor.AUTHOR_ROLE
        else:
            # This author-like contributor was given an explicit role.
            # If the next contributor has no explicit role given, we
            # can assume they're not an author -- if they were, they
            # would also have an explicit role.
            return Contributor.UNKNOWN_ROLE


    @classmethod
    def parse(cls, string, default_role=Contributor.AUTHOR_ROLE):
        """Parse the a person's name as found in OCLC Classify into a
        ContributorData object.

        :return: A 2-tuple (Contributor, default_role_used).
        default_role_used is true if the Contributor was assigned
        the default role, as opposed to that role being
        explicitly specified.
        """
        string = string.strip()
        name_without_roles, roles, default_role_used = cls._parse_roles(
            string, default_role
        )
        contributor = VIAFNameParser.parse(name_without_roles)
        contributor.roles = roles
        return contributor, default_role_used

    @classmethod
    def _parse_roles(cls, name, default_role=Contributor.AUTHOR_ROLE):
        """Remove role information from a person's name.

        :param name: A person's name as given by OCLC classify.
        :param default_role: If no role is present inside the person's
            name, use this role.
        :return: A 3-tuple (name_without_roles, roles,
            default_role_used).  `name_without_roles` is the person's
            name with the role information removed. `roles` is a list
            of Contributor role constants corresponding to the removed
            information. `default_role_used` indicates whether
            role information was found in the person's name or whether
            `roles` is the `default_role` passed in.
        """
        default_role_used = False
        name_without_roles = name
        match = cls.ROLES.search(name)
        if match:
            name_without_roles = name[:match.start()].strip()
            role_string = match.groups()[0]
            roles = list(set(cls._map_roles(role_string.split(";"))))
        elif default_role:
            roles = [default_role]
            default_role_used = True
        else:
            roles = []

        return name_without_roles, roles, default_role_used

    @classmethod
    def _map_roles(cls, roles):
        """Map the names of roles from OCLC Classify to the corresponding
        Contributor constants.

        Roles that don't have a mapping will become UNKNOWN_ROLE.

        :yield: A sequence of Contributor constants.
        """
        for role in roles:
            role = role.strip()
            if role in cls.ROLE_MAPPING:
                yield cls.ROLE_MAPPING[role]
            else:
                yield Contributor.UNKNOWN_ROLE


class OCLCTitleAuthorLookupXMLParser(XMLParser):
    # OCLC in-representation 'status codes'
    SINGLE_WORK_SUMMARY_STATUS = 0
    SINGLE_WORK_DETAIL_STATUS = 2
    MULTI_WORK_STATUS = 4
    NO_INPUT_STATUS = 100
    INVALID_INPUT_STATUS = 101
    NOT_FOUND_STATUS = 102
    UNEXPECTED_ERROR_STATUS = 200

    INTS = set([OCLC.HOLDING_COUNT, OCLC.EDITION_COUNT])

    NAMESPACES = {'oclc' : 'http://classify.oclc.org'}

    LIST_TYPE = "works"
    log = logging.getLogger("OCLC XML Parser")

    @classmethod
    def parse(cls, _db, xml, **restrictions):
        """Turn XML data from the OCLC lookup service into a list of SWIDs
        (for a multi-work response) or a list of Edition
        objects (for a single-work response).
        """
        tree = etree.fromstring(xml, parser=etree.XMLParser(recover=True))
        response = cls._xpath1(tree, "oclc:response")
        representation_type = int(response.get('code'))

        workset_record = None
        editions = []
        edition_records = []

        if representation_type == cls.UNEXPECTED_ERROR_STATUS:
            raise IOError("Unexpected error from OCLC API: %s" % xml)
        elif representation_type in (
                cls.NO_INPUT_STATUS, cls.INVALID_INPUT_STATUS):
            return representation_type, []
        elif representation_type == cls.SINGLE_WORK_SUMMARY_STATUS:
            raise IOError("Got single-work summary from OCLC despite requesting detail: %s" % xml)

        # The real action happens here.
        if representation_type == cls.SINGLE_WORK_DETAIL_STATUS:
            authors_tag = cls._xpath1(tree, "//oclc:authors")

            work_tag = cls._xpath1(tree, "//oclc:work")
            if work_tag is not None:
                author_string = work_tag.get('author')
                primary_author = cls.primary_author_from_author_string(_db, author_string)

            existing_authors = cls.extract_authors(
                _db, authors_tag, primary_author=primary_author)

            # The representation lists a single work, its authors, its editions,
            # plus summary classification information for the work.
            edition, ignore = cls.extract_edition(
                _db, work_tag, existing_authors, **restrictions)
            if edition:
                cls.log.info("EXTRACTED %r", edition)
            records = []
            if edition:
                records.append(edition)
            else:
                # The work record itself failed one of the
                # restrictions. None of its editions are likely to
                # succeed either.
                return representation_type, records

        elif representation_type == cls.MULTI_WORK_STATUS:
            # The representation lists a set of works that match the
            # search query.
            cls.log.debug("Extracting SWIDs from search results.")
            records = cls.extract_swids(_db, tree, **restrictions)
        elif representation_type == cls.NOT_FOUND_STATUS:
            # No problem; OCLC just doesn't have any data.
            records = []
        else:
            raise IOError("Unrecognized status code from OCLC API: %s (%s)" % (
                representation_type, xml))

        return representation_type, records

    @classmethod
    def extract_swids(cls, _db, tree, **restrictions):
        """Turn a multi-work response into a list of SWIDs."""

        swids = []
        for work_tag in cls._xpath(tree, "//oclc:work"):
            # We're not calling extract_basic_info because we care about
            # the info, we're calling it to make sure this work meets
            # the restriction. If this work meets the restriction,
            # we'll store its info when we look up the SWID.
            response = cls._extract_basic_info(
                _db, work_tag, **restrictions)
            if response:
                title, author_names, language = response
                # TODO: 'swid' is what it's called in older representations.
                # That code can be removed once we replace all representations.
                work_identifier = work_tag.get('wi') or work_tag.get('swid')
                cls.log.debug(
                    "WORK ID %s (%s, %r, %s)",
                    work_identifier, title, author_names, language
                )
                swids.append(work_identifier)
        return swids

    ROLES = re.compile("\[([^]]+)\]$")
    LIFESPAN = re.compile("([0-9]+)-([0-9]*)[.;]?$")

    @classmethod
    def extract_authors(cls, _db, authors_tag, primary_author=None):
        results = []
        if authors_tag is not None:
            for author_tag in cls._xpath(authors_tag, "//oclc:author"):
                lc = author_tag.get('lc', None)
                viaf = author_tag.get('viaf', None)
                contributor, roles, default_role_used = cls._parse_single_author(
                    _db, author_tag.text, lc=lc, viaf=viaf,
                    primary_author=primary_author)
                if contributor:
                    results.append(contributor)

        return results

    @classmethod
    def _contributor_match(cls, contributor, name, lc, viaf):
        return (
            contributor.sort_name == name
            and (lc is None or contributor.lc == lc)
            and (viaf is None or contributor.viaf == viaf)
        )

    @classmethod
    def _parse_single_author(cls, _db, author,
                             lc=None, viaf=None,
                             existing_authors=[],
                             default_role=Contributor.AUTHOR_ROLE,
                             primary_author=None):
        default_role_used = False
        # First find roles if present
        # "Giles, Lionel, 1875-1958 [Writer of added commentary; Translator]"
        author = author.strip()
        m = cls.ROLES.search(author)
        if m:
            author = author[:m.start()].strip()
            role_string = m.groups()[0]
            roles = [x.strip() for x in role_string.split(";")]
        elif default_role:
            roles = [default_role]
            default_role_used = True
        else:
            roles = []

        # Author string now looks like
        # "Giles, Lionel, 1875-1958"
        m = cls.LIFESPAN.search(author)
        kwargs = dict()
        if m:
            author = author[:m.start()].strip()
            birth, death = m.groups()
            if birth:
                kwargs[Contributor.BIRTH_DATE] = birth
            if death:
                kwargs[Contributor.DEATH_DATE] = death

        # Author string now looks like
        # "Giles, Lionel,"
        if author.endswith(","):
            author = author[:-1]

        contributor = None
        if not author:
            # No name was given for the author.
            return None, roles, default_role_used

        if primary_author and author == primary_author.sort_name:
            if Contributor.AUTHOR_ROLE in roles:
                roles.remove(Contributor.AUTHOR_ROLE)
            if Contributor.UNKNOWN_ROLE in roles:
                roles.remove(Contributor.UNKNOWN_ROLE)
            roles.insert(0, Contributor.PRIMARY_AUTHOR_ROLE)

        if existing_authors:
            # Calling Contributor.lookup will result in a database
            # hit, and looking up a contributor based on name may
            # result in multiple results (see below). We'll have no
            # way of distinguishing between those results. If
            # possible, it's much more reliable to look through
            # existing_authors (the authors derived from an entry's
            # <authors> tag).
            for x in existing_authors:
                if cls._contributor_match(x, author, lc, viaf):
                    contributor = x
                    break
            if contributor:
                was_new = False

        if not contributor:
            contributor, was_new = Contributor.lookup(
                _db, author, viaf, lc, extra=kwargs)
        if isinstance(contributor, list):
            # We asked for an author based solely on the name, which makes
            # Contributor.lookup() return a list.
            if len(contributor) == 1:
                # Fortunately, either the database knows about only
                # one author with that name, or it didn't know about
                # any authors with that name and it just created one,
                # so we can unambiguously use it.
                contributor = contributor[0]
            else:
                # Uh-oh. The database knows about multiple authors
                # with that name.  We have no basis for deciding which
                # author we mean. But we would prefer to identify with
                # an author who has a known LC or VIAF number.
                #
                # This should happen very rarely because of our check
                # against existing_authors above. But it will happen
                # for authors that have a work in Project Gutenberg.
                with_id = [x for x in contributor if x.lc is not None
                           or x.viaf is not None]
                if with_id:
                    contributor = with_id[0]
                else:
                    contributor = contributor[0]
        return contributor, roles, default_role_used

    @classmethod
    def primary_author_from_author_string(cls, _db, author_string):
        # If the first author mentioned in the author string
        # does not have an explicit role set, treat them as the primary
        # author.
        if not author_string:
            return None
        authors = author_string.split("|")
        if not authors:
            return None
        author, roles, default_role_used = cls._parse_single_author(
            _db, authors[0], default_role=Contributor.PRIMARY_AUTHOR_ROLE)
        if roles == [Contributor.PRIMARY_AUTHOR_ROLE]:
            return author
        return None

    @classmethod
    def parse_author_string(cls, _db, author_string, existing_authors=[],
                            primary_author=None):
        default_role = Contributor.PRIMARY_AUTHOR_ROLE
        authors = []
        if not author_string:
            return authors
        for author in author_string.split("|"):
            author, roles, default_role_used = cls._parse_single_author(
                _db, author, existing_authors=existing_authors,
                default_role=default_role,
                primary_author=primary_author)
            if roles:
                if Contributor.PRIMARY_AUTHOR_ROLE in roles:
                    # That was the primary author.  If we see someone
                    # with no explicit role after this point, they're
                    # just a regular author.
                    default_role = Contributor.AUTHOR_ROLE
                elif not default_role_used:
                    # We're dealing with someone whose role was
                    # explicitly specified. If we see someone with no
                    # explicit role after this point, it's probably
                    # because their role is so minor as to not be
                    # worth mentioning, not because it's so major that
                    # we can assume they're an author.
                    default_role = Contributor.UNKNOWN_ROLE
            roles = roles or [default_role]
            if author:
                authors.append((author, roles))
        return authors

    @classmethod
    def _extract_basic_info(cls, _db, tag, existing_authors=None,
                            **restrictions):
        """Extract information common to work tag and edition tag."""
        title = tag.get('title')
        author_string = tag.get('author')
        authors_and_roles = cls.parse_author_string(
            _db, author_string, existing_authors)
        if 'language' in tag.keys():
            language = tag.get('language')
        else:
            language = None

        if title and 'title' in restrictions:
            must_resemble_title = restrictions['title']
            threshold = restrictions.get('title_similarity', 0.25)
            similarity = MetadataSimilarity.title_similarity(
                must_resemble_title, title)
            if similarity < threshold:
                # The title of the book under consideration is not
                # similar enough to the given title.
                cls.log.debug(
                    "FAILURE TO RESEMBLE: %s vs %s (%.2f)",
                    title, must_resemble_title, similarity
                )
                return None

            # The semicolon is frequently used to separate multiple
            # works in an anthology. If there is no semicolon in the
            # original title, do not consider titles that contain
            # semicolons.
            if (not ' ; ' in must_resemble_title
                and ' ; ' in title and threshold > 0):
                cls.log.debug(
                    "SEMICOLON DISQUALIFICATION: %s", title
                )
                return None

        # Apply restrictions. If they're not met, return None.
        if 'language' in restrictions and language:
            # We know which language this record is for. Match it
            # against the language used in the Edition we're
            # matching against.
            restrict_to_language = set(restrictions['language'])
            if language != restrict_to_language:
                # This record is for a book in a different language
                cls.log.debug(
                    "WRONG LANGUAGE: %s", language
                )
                return None

        if 'authors' in restrictions:
            restrict_to_authors = restrictions['authors']
            if restrict_to_authors and isinstance(restrict_to_authors[0], Contributor):
                restrict_to_authors = [x.sort_name for x in restrict_to_authors]
            primary_author = None

            for a, roles in authors_and_roles:
                if Contributor.PRIMARY_AUTHOR_ROLE in roles:
                    primary_author = a
                    break
            if (not primary_author
                or (primary_author not in restrict_to_authors
                    and primary_author.sort_name not in restrict_to_authors)):
                    # None of the given authors showed up as the
                    # primary author of this book. They may have had
                    # some other role in it, or the book may be about
                    # them, or incorporate their work, but this book
                    # is not *by* them.
                return None

        author_names = ", ".join([x.sort_name for x, y in authors_and_roles])

        return title, authors_and_roles, language

    UNUSED_MEDIA = set([
        "itemtype-intmm",
        "itemtype-msscr",
        "itemtype-artchap-artcl",
        "itemtype-jrnl",
        "itemtype-map",
        "itemtype-vis",
        "itemtype-jrnl-digital",
        "itemtype-image-2d",
        "itemtype-artchap-digital",
        "itemtype-intmm-digital",
        "itemtype-archv",
        "itemtype-msscr-digital",
        "itemtype-game",
        "itemtype-web-digital",
        "itemtype-map-digital",
    ])

    @classmethod
    def extract_edition(cls, _db, work_tag, existing_authors, **restrictions):
        """Create a new Edition object with information about a
        work (identified by OCLC Work ID).
        """
        # TODO: 'pswid' is what it's called in older representations.
        # That code can be removed once we replace all representations.
        oclc_work_id = unicode(work_tag.get('owi') or work_tag.get('pswid'))
        # if oclc_work_id:
        #     print " owi: %s" % oclc_work_id
        # else:
        #     print " No owi in %s" % etree.tostring(work_tag)


        if not oclc_work_id:
            raise ValueError("Work has no owi")

        item_type = work_tag.get("itemtype")
        if (item_type.startswith('itemtype-book')
            or item_type.startswith('itemtype-compfile')):
            medium = Edition.BOOK_MEDIUM
        elif item_type.startswith('itemtype-audiobook') or item_type.startswith('itemtype-music'):
            # Pretty much all Gutenberg texts, even the audio texts,
            # are based on a book, and the ones that aren't
            # (recordings of individual songs) probably aren't in OCLC
            # anyway. So we just want to get the books.
            medium = Edition.AUDIO_MEDIUM
            medium = None
        elif item_type.startswith('itemtype-video'):
            #medium = Edition.VIDEO_MEDIUM
            medium = None
        elif item_type in cls.UNUSED_MEDIA:
            medium = None
        else:
            medium = None

        # Only create Editions for books with a recognized medium
        if medium is None:
            return None, False

        result = cls._extract_basic_info(_db, work_tag, existing_authors, **restrictions)
        if not result:
            # This record did not meet one of the restrictions.
            return None, False

        title, authors_and_roles, language = result

        # Record some extra OCLC-specific information
        editions = work_tag.get('editions')
        holdings = work_tag.get('holdings')

        # Get an identifier for this work.
        identifier, ignore = Identifier.for_foreign_id(
            _db, Identifier.OCLC_WORK, oclc_work_id
        )

        data_source = DataSource.lookup(_db, DataSource.OCLC)
        identifier.add_measurement(data_source, Measurement.HOLDINGS, holdings)
        identifier.add_measurement(
            data_source, Measurement.PUBLISHED_EDITIONS, editions)


        # Create a Edition for source + identifier
        edition, new = get_one_or_create(
            _db, Edition,
            data_source=data_source,
            primary_identifier=identifier,
            create_method_kwargs=dict(
                title=title,
                language=language,
            )
        )

        # Get the most popular Dewey and LCC classification for this
        # work.
        for tag_name, subject_type in (
                ("ddc", Subject.DDC),
                ("lcc", Subject.LCC)):
            tag = cls._xpath1(
                work_tag,
                "//oclc:%s/oclc:mostPopular" % tag_name)
            if tag is not None:
                id = tag.get('nsfa') or tag.get('sfa')
                weight = int(tag.get('holdings'))
                identifier.classify(
                    data_source, subject_type, id, weight=weight)

        # Find FAST subjects for the work.
        for heading in cls._xpath(
                work_tag, "//oclc:fast//oclc:heading"):
            id = heading.get('ident')
            weight = int(heading.get('heldby'))
            value = heading.text
            identifier.classify(
                data_source, Subject.FAST, id, value, weight)

        # Associate the authors with the Edition.
        for contributor, roles in authors_and_roles:
            edition.add_contributor(contributor, roles)
        return edition, new

    @classmethod
    def extract_edition_record(cls, _db, edition_tag,
                               existing_authors,
                               **restrictions):
        """Create a new Edition object with information about an
        edition of a book (identified by OCLC Number).
        """
        oclc_number = unicode(edition_tag.get('oclc'))
        try:
            int(oclc_number)
        except ValueError, e:
            # This record does not have a valid OCLC number.
            return None, False

        # Fill in some basic information about this new record.
        result = cls._extract_basic_info(
            _db, edition_tag, existing_authors, **restrictions)
        if not result:
            # This record did not meet one of the restrictions.
            return None, False

        title, authors_and_roles, language = result

        # Add a couple extra bits of OCLC-specific information.
        extra = {
            OCLC.HOLDING_COUNT : edition_tag.get('holdings'),
            OCLC.FORMAT : edition_tag.get('itemtype'),
        }

        # Get an identifier for this edition.
        identifier, ignore = Identifier.for_foreign_id(
            _db, Identifier.OCLC_NUMBER, oclc_number
        )

        # Create a Edition for source + identifier
        data_source = DataSource.lookup(_db, DataSource.OCLC)
        edition_record, new = get_one_or_create(
            _db, Edition,
            data_source=data_source,
            primary_identifier=identifier,
            create_method_kwargs=dict(
                title=title,
                language=language,
                subjects=subjects,
                extra=extra,
            )
        )

        subjects = {}
        for subject_type, oclc_code in (
                (Subject.LCC, "050"),
                (Subject.DDC, "082")):
            classification = cls._xpath1(edition_tag,
                "oclc:classifications/oclc:class[@tag=%s]" % oclc_code)
            if classification is not None:
                value = classification.get("nsfa") or classification.get('sfa')
                identifier.classify(data_source, subject_type, value)

        # Associated each contributor with the new record.
        for author, roles in authors_and_roles:
            edition_record.add_contributor(author, roles)
        return edition_record, new

class OCLCClassifyAPI(object):

    BASE_URL = 'http://classify.oclc.org/classify2/Classify?'

    NO_SUMMARY = '&summary=false'

    def __init__(self, _db):
        self._db = _db

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.OCLC)

    def query_string(self, **kwargs):
        args = dict()
        for k, v in kwargs.items():
            if isinstance(v, unicode):
                v = v.encode("utf8")
            args[k] = v
        return urllib.urlencode(sorted(args.items()))

    def lookup_by(self, **kwargs):
        """Perform an OCLC Classify lookup."""
        query_string = self.query_string(**kwargs)
        url = self.BASE_URL + query_string
        return self._make_request(url)

    def _make_request(self, url):
        representation, cached = Representation.get(self._db, url)
        return representation.content


class MockOCLCClassifyAPI(OCLCClassifyAPI):

    def __init__(self, _db):
        super(MockOCLCClassifyAPI, self).__init__(_db)
        self.requests = []
        self.responses = []

    def queue_response(self, content):
        self.responses.append(content)

    def _make_request(self, url):
        self.requests.append(url)
        return self.responses.pop(0)


class OCLCLookupCoverageProvider(MetadataWranglerBibliographicCoverageProvider):

    def __init__(self, collection, api=None, **kwargs):
        super(OCLCLookupCoverageProvider, self).__init__(
            collection, registered_only=True, **kwargs
        )
        self.api = api or OCLCClassifyAPI(self._db)


class IdentifierLookupCoverageProvider(OCLCLookupCoverageProvider):
    """Does identifier (specifically, ISBN) lookups using OCLC Classify.
    """
    SERVICE_NAME = "OCLC Classify Identifier Lookup"
    INPUT_IDENTIFIER_TYPES = [Identifier.ISBN]
    DATA_SOURCE_NAME = DataSource.OCLC

    parser = OCLCClassifyXMLParser()

    def _get_tree(self, **kwargs):
        """Look up either an ISBN or an OWI, and return a tree generated from the resulting XML."""
        xml = self.api.lookup_by(**kwargs)
        return etree.fromstring(xml, parser=etree.XMLParser(recover=True))

    def process_item(self, identifier):
        """Ask OCLC Classify about a single ISBN. Create an Edition based on
        what it says. This may involve consolidating information from
        multiple OWIs.

        TODO: Ideally we would create a distinct Metadata for each
        OWI, and consolidate them into a presentation edition for the
        ISBN. Unfortunately, a presentation edition only takes into account
        information from different sources for the same Identifier.

        Subject classification can be gathered across Identifiers, but
        information such as author and measurements currently can not.
        """
        metadata_list = []
        failure = None

        # Start with an empty Metadata. We're going to fill this out.
        metadata = Metadata(
            data_source=DataSource.OCLC,
            primary_identifier=identifier
        )
        try:
            tree = self._get_tree(isbn=identifier.identifier)
            code, owi_data = self.parser.initial_look_up(tree)
            if code in [self.parser.SINGLE_WORK_DETAIL_STATUS, self.parser.SINGLE_WORK_SUMMARY_STATUS]:
                metadata = self._single(tree, metadata)
            elif code == self.parser.MULTI_WORK_STATUS:
                metadata = self._multiple(owi_data, metadata)
            elif code == self.parser.NOT_FOUND_STATUS:
                message = ("The work with %s %s was not found." % (identifier.type, identifier.identifier))
                return self.failure(identifier, message)
            self._apply(metadata)
            return identifier

        except IOError as e:
            return self.failure(identifier, e.message)

    def _single(self, tree, metadata):
        """In the case of a single work response, annotate
        `metadata` with the information it returned.
        """
        return self.parser.parse(tree, metadata)

    def _multiple(self, owi_data, metadata):
        """In the case of a multi-work response, the document we got is
        itself useless for annotating `metadata`.

        Instead, for each <work> tag, we get a more complete document
        by looking up the OWI, and annotate `metadata` based on
        that.
        """
        for item in owi_data:
            tree_from_owi = self._get_tree(owi=item.identifier)
            metadata = self.parser.parse(tree_from_owi, metadata)
        return metadata

    def _apply(self, metadata):
        """Create an edition based on a Metadata object we
        obtained by parsing a tree.
        """
        edition = self.edition(metadata.primary_identifier)
        metadata.apply(
            edition, collection=None, replace=self.replacement_policy
        )


class TitleAuthorLookupCoverageProvider(IdentifierCoverageProvider):
    """Does title/author lookups using OCLC Classify.

    NOTE: This code is no longer used. It was designed to get extra
    metadata for titles from Project Gutenberg/Standard
    Ebooks/unglue.it/Feedbooks, where the title and author are known
    but there is no ISBN associated with the work.

    Most of these data sources provide adequate metadata, except for
    Project Gutenberg, which (generally speaking) we no longer use.
    So for now we're focused on coverage providers that are more
    reliable and give bigger bang for the (processing time) buck.
    """

    # Strips most non-alphanumerics from the title.
    # 'Alphanumerics' includes alphanumeric characters
    # for any language, so this shouldn't affect
    # titles in non-Latin languages.
    #
    # OCLC has trouble recognizing non-alphanumerics in titles,
    # especially colons.
    NON_TITLE_SAFE = re.compile("[^\w\-' ]", re.UNICODE)

    SERVICE_NAME = "OCLC Classify Coverage Provider"
    INPUT_IDENTIFIER_TYPES = [Identifier.GUTENBERG_ID, Identifier.URI]
    DATA_SOURCE_NAME = DataSource.OCLC

    def __init__(self, _db, api=None, **kwargs):
        super(TitleAuthorLookupCoverageProvider, self).__init__(
            _db, registered_only=True, **kwargs
        )
        self.api = api or OCLCClassifyAPI(self._db)

    def oclc_safe_title(self, title):
        if not title:
            return ''
        return self.NON_TITLE_SAFE.sub("", title)

    def get_bibliographic_info(self, identifier):
        """Find any local source for this Identifier that lists title, author
        and language, so we can do a lookup based on that information.
        """
        _db = Session.object_session(identifier)
        editions = _db.query(Edition).join(Edition.contributions).filter(
            Edition.primary_identifier==identifier
        ).filter(Edition.title != None).filter(
            Edition.language != None).filter(
                Contribution.role.in_(Contributor.AUTHOR_ROLES)
            ).all()
        if not editions:
            return None, None, None
        edition = editions[0]

        title = self.oclc_safe_title(edition.title)
        authors = edition.author_contributors
        if len(authors) == 0:
            # Should never happen.
            author = ''
        else:
            author = authors[0].sort_name
        language = edition.language

        # Log the info
        def _f(s):
            if not s:
                return ''
            if isinstance(s, unicode):
                return s.encode("utf8")
            return s
        self.log.info(
            '%s "%s" "%s" %r', _f(edition.primary_identifier.identifier),
            _f(title), _f(author), _f(language)
        )

        return title, author, language

    def parse_edition_data(self, xml, edition, title, language):
        """Transforms the OCLC XML files into usable bibliographic records,
        including making additional API calls as necessary.
        """
        parser = OCLCTitleAuthorLookupXMLParser()
        # For now, the only restriction we apply is the language
        # restriction. If we know that a given OCLC record is in a
        # different language from this record, there's no need to
        # even import that record. Restrictions on title and
        # author will be applied statistically, when we calculate
        # works.
        restrictions = dict(language=language,
                            title=title,
                            authors=edition.author_contributors)
        # These representation types shouldn't occur, but if they do there's
        # either nothing we need to do about them or nothing we can do.
        ignored_representation_types = [
            parser.NOT_FOUND_STATUS, parser.INVALID_INPUT_STATUS,
            parser.NO_INPUT_STATUS
        ]

        # Turn the raw XML into some number of bibliographic records.
        representation_type, records = parser.parse(
            self._db, xml, **restrictions
        )

        if representation_type == parser.MULTI_WORK_STATUS:
            # `records` contains a bunch of SWIDs, not
            # Editions. Do another lookup to turn each SWID
            # into a set of Editions.
            swids = records
            records = []
            for swid in swids:
                swid_xml = self.api.lookup_by(wi=swid)
                representation_type, editions = parser.parse(
                    self._db, swid_xml, **restrictions
                )
                if representation_type == parser.SINGLE_WORK_DETAIL_STATUS:
                    records.extend(editions)
                elif representation_type in ignored_representation_types:
                    pass
                else:
                    raise IOError(
                        "Got unexpected representation type from \
                        lookup: %s" % representation_type
                    )
        return records

    def merge_contributors(self, edition, records):
        """Connect the Gutenberg book to the OCLC works looked up by
        title/author. Hopefully we can also connect the Gutenberg book
        to an author who has an LC and VIAF.
        """
        # First, find any authors associated with this book that
        # have not been given VIAF or LC IDs.
        gutenberg_authors_to_merge = [
            x for x in edition.author_contributors if not x.viaf or not x.lc
        ]
        gutenberg_names = set([x.sort_name for x in edition.author_contributors])
        for r in records:
            if gutenberg_authors_to_merge:
                oclc_names = set([x.sort_name for x in r.author_contributors])
                if gutenberg_names == oclc_names:
                    # Perfect overlap. We've found an OCLC record
                    # for a book written by exactly the same
                    # people as the Gutenberg book. Merge each
                    # Gutenberg author into its OCLC equivalent.
                    for gutenberg_author in gutenberg_authors_to_merge:
                        oclc_authors = [x for x in r.author_contributors
                                        if x.sort_name == gutenberg_author.sort_name]
                        if len(oclc_authors) == 1:
                            oclc_author = oclc_authors[0]
                            if oclc_author != gutenberg_author:
                                gutenberg_author.merge_into(oclc_author)
                                gutenberg_authors_to_merge.remove(
                                    gutenberg_author)

            # Now that we've (perhaps) merged authors, calculate the
            # similarity between the two records.
            strength = edition.similarity_to(r)
            if strength > 0:
                edition.primary_identifier.equivalent_to(
                    self.data_source, r.primary_identifier, strength
                )

    def process_item(self, identifier):
        edition = self.edition(identifier)
        if isinstance(edition, CoverageFailure):
            return edition

        # Perform a title/author lookup.
        title, author, language = self.get_bibliographic_info(identifier)
        if not (title and author):
            e = 'Cannot lookup edition without title and author!'
            return self.failure(identifier, e)
        xml = self.api.lookup_by(title=title, author=author)

        try:
            records = self.parse_edition_data(xml, edition, title, language)
        except IOError as e:
            return self.failure(identifier, e.message)

        self.merge_contributors(edition, records)
        self.log.info("Created %s records(s).", len(records))
        return identifier
