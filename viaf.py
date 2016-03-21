import os
from nose.tools import set_trace
from lxml import etree
import logging
import re

from collections import Counter, defaultdict

from core.model import (
    Contributor,
    DataSource,
    Representation,
)

from core.util.xmlparser import (
    XMLParser,
)
from core.util.personal_names import display_name_to_sort_name

class VIAFParser(XMLParser):

    NAMESPACES = {'ns2' : "http://viaf.org/viaf/terms#"}

    log = logging.getLogger("VIAF Parser")

    @classmethod
    def name_matches(cls, n1, n2):
        return n1.replace(".", "").lower() == n2.replace(".", "").lower()

    def info(self, contributor, viaf, display_name, family_name, wikipedia_name):
        """For the given Contributor, find:

        * A VIAF ID
        * A display name (that can go on a book cover)
        * A family name (to have a short way of referring to the author)
        * A Wikipedia name (so we can get access to Wikipedia,
          Wikidata, WikiQuotes, etc.)

        :return: a 3-tuple (display name, family name, Wikipedia name)
        """
        self.log.info("Starting point for lookup: %s" % contributor.name)

        if not contributor.viaf:
            contributor.viaf = viaf

        if not display_name or not family_name:
            default_family, default_display = contributor.default_names(None)
            if not display_name:
                display_name = default_display
            if not family_name:
                family_name = default_family

        self.log.info("VIAF ID: %s" % viaf)
        if wikipedia_name:
            self.log.info("Wikipedia name: %s" % wikipedia_name)
        self.log.info("Display name: %s" % display_name)
        self.log.info("Family name: %s" % family_name)
        return viaf, display_name, family_name, wikipedia_name

    def sort_names_for_cluster(self, cluster):
        """Find all sort names for the given cluster."""
        for tag in ('100', '110'):
            for data_field in self._xpath(
                    cluster, './/*[local-name()="datafield"][@dtype="MARC21"][@tag="%s"]' % tag):
                for potential_match in self._xpath(
                        data_field, '*[local-name()="subfield"][@code="a"]'):
                    yield potential_match.text

    def cluster_has_record_for_named_author(
            self, cluster, working_sort_name, working_display_name):

        # If we have a sort name to look for, and it's in this cluster's
        # sort names, great.
        if working_sort_name:
            for potential_match in self.sort_names_for_cluster(cluster):
                if self.name_matches(potential_match, working_sort_name):
                    return True

        # If we have a display name to look for, and this cluster's
        # Wikipedia name converts to the display name, great.
        if working_display_name:
            wikipedia_name = self.extract_wikipedia_name(cluster)
            if wikipedia_name:
                display_name = self.wikipedia_name_to_display_name(
                    wikipedia_name)
                if self.name_matches(display_name, working_display_name):
                    return True

        # If there are UNIMARC records, and every part of the UNIMARC
        # record matches the sort name or the display name, great.
        unimarcs = self._xpath(cluster, './/*[local-name()="datafield"][@dtype="UNIMARC"]')
        candidates = []
        for unimarc in unimarcs:
            (possible_given, possible_family,
             possible_extra, possible_sort_name) = self.extract_name_from_unimarc(unimarc)
            if working_sort_name:
                if self.name_matches(possible_sort_name, working_sort_name):
                    return True

            for name in (working_sort_name, working_display_name):
                if not name:
                    continue
                if (possible_given and possible_given in name
                    and possible_family and possible_family in name and (
                        not possible_extra or possible_extra in name)):
                    return True

        # Last-ditch effort. Guess at the sort name and see if *that's* one
        # of the cluster sort names.
        if working_display_name and not working_sort_name:
            test_sort_name = display_name_to_sort_name(working_display_name)
            for potential_match in self.sort_names_for_cluster(cluster):
                if self.name_matches(potential_match, test_sort_name):
                    return True

        return False

    def parse_multiple(
            self, xml, working_sort_name=None, working_display_name=None,
            strict=True):
        """Parse a VIAF response containing multiple clusters into a
        VIAF ID + name 5-tuple.
        """
        tree = etree.fromstring(xml, parser=etree.XMLParser(recover=True))
        viaf_id = None
        for cluster in self._xpath(tree, '//*[local-name()="VIAFCluster"]'):
            viaf, display, family, sort, wikipedia = self.extract_viaf_info(
                cluster, working_sort_name, working_display_name, strict)
            if display:
                return viaf, display, family, sort, wikipedia
            # We couldn't find a display name, but can we at least
            # determine that this is an acceptable VIAF ID for this
            # name?
            if viaf:
                viaf_id = viaf

        # We could not find any names for this author, but hopefully
        # we at least found a VIAF ID.
        return viaf_id, None, None, None, None

    def parse(self, xml, working_sort_name=None, working_display_name=None,
              strict=False):
        """Parse a VIAF response containing a single cluster into a name
        3-tuple."""
        tree = etree.fromstring(xml, parser=etree.XMLParser(recover=True))
        return self.extract_viaf_info(
            tree, working_sort_name, working_display_name, strict=strict)

    wikidata_id = re.compile("^Q[0-9]")

    def extract_wikipedia_name(self, cluster):
        """Extract Wiki name from a single VIAF cluster."""
        for source in self._xpath(cluster, './/*[local-name()="sources"]/*[local-name()="source"]'):
            if source.text.startswith("WKP|"):
                # This could be a Wikipedia page, which is great,or it
                # could be a Wikidata ID, which we don't want.
                potential_wikipedia = source.text[4:]
                if not self.wikidata_id.search(potential_wikipedia):
                    return potential_wikipedia

    def sort_names_by_popularity(self, cluster):
        sort_name_popularity = Counter()
        for possible_sort_name in self.sort_names_for_cluster(cluster):
            if possible_sort_name.endswith(","):
                possible_sort_name = possible_sort_name[:-1]
            sort_name_popularity[possible_sort_name] += 1
        return sort_name_popularity

    def extract_viaf_info(self, cluster, working_sort_name=None,
                          working_display_name=False, strict=False):
        """Extract name info from a single VIAF cluster."""
        display_name = None
        sort_name = working_sort_name
        family_name = None
        wikipedia_name = None

        # If we're not sure that this is even the right cluster for
        # the given author, make sure that one of the working names
        # shows up in a name record.
        if strict:
            if not self.cluster_has_record_for_named_author(
                cluster, working_sort_name, working_display_name):
                return None, None, None, None, None

        # Get the VIAF ID for this cluster, just in case we don't have one yet.
        viaf_tag = self._xpath1(cluster, './/*[local-name()="viafID"]')
        if viaf_tag is None:
            viaf_id = None
        else:
            viaf_id = viaf_tag.text

        # If we don't have a working sort name, find the most popular
        # sort name in this cluster and use it as the sort name.
        sort_name_popularity = self.sort_names_by_popularity(cluster)

        # Does this cluster have a Wikipedia page?
        wikipedia_name = self.extract_wikipedia_name(cluster)
        if wikipedia_name:
            display_name = self.wikipedia_name_to_display_name(wikipedia_name)
            working_display_name = display_name
            # TODO: There's a problem here when someone's record has a
            # Wikipedia page other than their personal page (e.g. for
            # a band they're in.)

        unimarcs = self._xpath(cluster, './/*[local-name()="datafield"][@dtype="UNIMARC"]')
        candidates = []
        for unimarc in unimarcs:
            (possible_given, possible_family,
             possible_extra, possible_sort_name) = self.extract_name_from_unimarc(unimarc)
            # Some part of this name must also show up in the original
            # name for it to even be considered. Otherwise it's a
            # better bet to try to munge the original name.
            for v in (possible_given, possible_family, possible_extra):
                if not v:
                    continue
                if not working_sort_name or v in working_sort_name:
                    self.log.debug(
                        "FOUND %s in %s", v, working_sort_name
                    )
                    candidates.append((possible_given, possible_family,
                                       possible_extra))
                    if possible_sort_name and possible_sort_name.endswith(","):
                        possible_sort_name = sort_name[:-1]
                        sort_name_popularity[possible_sort_name] += 1
                    break
            else:
                self.log.debug(
                    "EXCLUDED %s/%s/%s for lack of resemblance to %s",
                    possible_given, possible_family, possible_extra,
                    working_sort_name
                )
                pass

        if sort_name_popularity and not sort_name:
            sort_name, ignore = sort_name_popularity.most_common(1)[0]

        if display_name:
            parts = display_name.split(" ")
            if len(parts) == 2:
                # Pretty clearly given name+family name.
                # If it gets more complicated than this we can't
                # be confident.
                candidates.append(parts + [None])

        display_nameparts = self.best_choice(candidates)
        if display_nameparts[1]: # Family name
            family_name = display_nameparts[1]

        v = (
            viaf_id,
            display_name or self.combine_nameparts(*display_nameparts) or working_display_name,
            family_name,
            sort_name or working_sort_name,
            wikipedia_name)
        return v

    def wikipedia_name_to_display_name(self, wikipedia_name):
        "Convert 'Bob_Jones_(Author)' to 'Bob Jones'"
        display_name = wikipedia_name.replace("_", " ")
        if ' (' in display_name:
            display_name = display_name[:display_name.rindex(' (')]
        return display_name

    def best_choice(self, possibilities):
        """Return the best (~most popular) choice among the given names.

        :param possibilities: A list of (given, family, extra) 3-tuples.
        """
        if not possibilities:
            return None, None, None
        elif len(possibilities) == 1:
            # There is only one choice. Use it.
            return possibilities[0]

        # There's more than one choice, so it's gonna get
        # complicated. First, find the most common family name.
        family_names = Counter()
        given_name_for_family_name = defaultdict(Counter)
        extra_for_given_name_and_family_name = defaultdict(Counter)
        for given_name, family_name, name_extra in possibilities:
            self.log.debug(
                "POSSIBILITY: %s/%s/%s",
                given_name, family_name, name_extra
            )
            if family_name:
                family_names[family_name] += 1
                if given_name:
                    given_name_for_family_name[family_name][given_name] += 1
                    extra_for_given_name_and_family_name[(family_name, given_name)][name_extra] += 1
        if not family_names:
            # None of these are useful.
            return None, None, None
        family_name = family_names.most_common(1)[0][0]

        given_name = None
        name_extra = None

        # Now find the most common given name, given the most
        # common family name.
        given_names = given_name_for_family_name[family_name]
        if given_names:
            given_name = given_names.most_common(1)[0][0]
            extra = extra_for_given_name_and_family_name[
                (family_name, given_name)]
            if extra:
                name_extra, count = extra.most_common(1)[0]

                # Don't add extra stuff on to the name if it's a
                # viable option.
                if extra[None] == count:
                    name_extra = None
        return given_name, family_name, name_extra

    def remove_commas_from(self, namepart):
        """Strip dangling commas from a namepart."""
        if namepart.endswith(","):
            namepart = namepart[:-1]
        if namepart.startswith(","):
            namepart = namepart[1:]
        return namepart.strip()

    def extract_name_from_unimarc(self, unimarc):
        """Turn a UNIMARC tag into a 4-tuple:
         (given name, family name, extra, sort name)
        """
        # Only process author names and corporate names.
        #if unimarc.get('tag') not in ('100', '110'):
        #    return None, None, None, None
        #if unimarc.get('tag') == '110':
        #    set_trace()
        data = dict()
        sort_name_in_progress = []
        for (code, key) in (
                ('a', 'family'),
                ('b', 'given'),
                ('c', 'extra'),
                ):
            value = self._xpath1(unimarc, 'ns2:subfield[@code="%s"]' % code)
            if value is not None and value.text:
                value = value.text
                value = self.remove_commas_from(value)
                sort_name_in_progress.append(value)
                data[key] = value
        return (data.get('given', None), data.get('family', None),
                data.get('extra', None), ", ".join(sort_name_in_progress))

    @classmethod
    def combine_nameparts(self, given, family, extra):
        """Turn a (given name, family name, extra) 3-tuple into a
        display name.
        """
        if not given and not family:
            return None
        if family and not given:
            display_name = family
        elif given and not family:
            display_name = given
        else:
            display_name = given + ' ' + family
        if extra and not extra.startswith('pseud'):
            if family and given:
                display_name += ', ' + extra
            else:
                display_name += ' ' + extra
        return display_name


class VIAFClient(object):

    MAX_AGE = 3600 * 24 * 180
    MAX_AGE = 0

    LOOKUP_URL = 'http://viaf.org/viaf/%(viaf)s/viaf.xml'
    SEARCH_URL = 'http://viaf.org/viaf/search?query=local.names+%3D+%22{sort_name}%22&maximumRecords=5&startRecord=1&sortKeys=holdingscount&local.sources=lc&httpAccept=text/xml'
    SUBDIR = "viaf"

    MEDIA_TYPE = Representation.TEXT_XML_MEDIA_TYPE

    def __init__(self, _db):
        self._db = _db
        self.data_source = DataSource.lookup(self._db, DataSource.VIAF)
        self.parser = VIAFParser()
        self.log = logging.getLogger("VIAF Client")

    def process_contributor(self, contributor):

        if contributor.viaf:
            # We can look them up by VIAF.
            v = self.lookup_by_viaf(
                contributor.viaf, contributor.name, contributor.display_name)
        else:
            v = self.lookup_by_name(contributor.name, contributor.display_name)

        viaf, display_name, family_name, sort_name, wikipedia_name = v
        contributor.viaf = viaf
        contributor.display_name = display_name
        contributor.family_name = family_name
        contributor.wikipedia_name = wikipedia_name

        # Is there already another contributor with this VIAF?
        if contributor.viaf is not None:
            duplicates = self._db.query(Contributor).filter(
                Contributor.viaf==contributor.viaf).filter(
                    Contributor.id != contributor.id).all()
            if duplicates:
                if duplicates[0].display_name == contributor.display_name:
                    contributor.merge_into(duplicates[0])
                else:
                    d1 = contributor.display_name
                    if isinstance(d1, unicode):
                        d1 = d1.encode("utf8")
                    d2 = duplicates[0].display_name
                    if isinstance(d2, unicode):
                        d2 = d2.encode("utf8")

                    self.log.warn(
                        "POSSIBLE SPURIOUS AUTHOR MERGE: %s => %s", d1, d2
                    )
                    # TODO: This might be okay or it might be a
                    # problem we need to address. Whatever it is,
                    # don't merge the records.
                    pass

    def lookup_by_viaf(self, viaf, working_sort_name=None,
                       working_display_name=None):
        url = self.LOOKUP_URL % dict(viaf=viaf)
        r, cached = Representation.get(self._db, url)

        xml = r.content
        return self.parser.parse(xml, working_sort_name, working_display_name)

    def lookup_by_name(self, sort_name, display_name=None, strict=True):
        name = sort_name or display_name
        url = self.SEARCH_URL.format(sort_name=name.encode("utf8"))
        r, cached = Representation.get(self._db, url)
        xml = r.content
        v = self.parser.parse_multiple(
            xml, sort_name, display_name, strict)
        if not any(v):
            # Delete the representation so it's not cached.
            self._db.query(Representation).filter(Representation.id==r.id).delete()
        return v

