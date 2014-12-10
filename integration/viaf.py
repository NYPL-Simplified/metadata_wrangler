import os
from nose.tools import set_trace
from lxml import etree
import re

from collections import Counter, defaultdict

from sqlalchemy.sql.expression import (
    or_,
)

from ..core.model import (
    Contributor,
    Contribution,
    DataSource,
    Edition,
    Representation,
)

from ..core.util.xmlparser import (
    XMLParser,
)

class VIAFParser(XMLParser):

    NAMESPACES = {'ns2' : "http://viaf.org/viaf/terms#"}

    def info(self, contributor, xml, from_multiple):
        """For the given Contributor, find:

        * A VIAF ID
        * A display name (that can go on a book cover)
        * A family name (to have a short way of referring to the author)
        * A Wikipedia name (so we can get access to Wikipedia,
          Wikidata, WikiQuotes, etc.)

        :return: a 3-tuple (display name, family name, Wikipedia name)
        """
        if not xml:
            display, family = contributor.default_names(None)
            return display, family, None
        print "Starting point: %s" % contributor.name

        if from_multiple:
            viaf, display_name, family_name, wikipedia_name = self.parse_multiple(
                xml, contributor.name)
        else:
            viaf, display_name, family_name, wikipedia_name = self.parse(
                xml, contributor.name)

        if not contributor.viaf:
            contributor.viaf = viaf
        
        if not display_name or not family_name:
            default_family, default_display = contributor.default_names(None)
            if not display_name:
                display_name = default_display
            if not family_name:
                family_name = default_family

        print " VIAF ID: %s" % viaf
        if wikipedia_name:
            print " Wikipedia name: %s" % wikipedia_name
        print " Display name: %s" % display_name
        print " Family name: %s" % family_name
        print
        return viaf, display_name, family_name, wikipedia_name

    def cluster_has_record_for_named_author(self, cluster, name):

        for data_field in self._xpath(
                cluster, './/*[local-name()="datafield"][@dtype="MARC21"][@tag="100"]'):
            for potential_match in self._xpath(
                    data_field, '*[local-name()="subfield"][@code="a"]'):
                if potential_match.text == name:
                    return True

        unimarcs = self._xpath(cluster, './/*[local-name()="datafield"][@dtype="UNIMARC"]')
        candidates = []
        for unimarc in unimarcs:
            (possible_given, possible_family,
             possible_extra) = self.extract_name_from_unimarc(unimarc)
            if (possible_given and possible_given in name
                and possible_family and possible_family in name and (
                    not possible_extra or possible_extra in name)):
                return True
        return False

    def parse_multiple(self, xml, working_name=None):
        """Parse a VIAF response containing multiple clusters into a
        VIAF ID + name 4-tuple.
        """
        tree = etree.fromstring(xml, parser=etree.XMLParser(recover=True))
        viaf_id = None
        for cluster in self._xpath(tree, '//*[local-name()="VIAFCluster"]'):
            viaf, display, family, wikipedia = self.extract_viaf_info(
                cluster, working_name, strict=True)
            if display:
                return viaf, display, family, wikipedia
            # We couldn't find a display name, but can we at least
            # determine that this is an acceptable VIAF ID for this
            # name?
            if viaf:
                viaf_id = viaf

        # We could not find any names for this author, but hopefully
        # we at least found a VIAF ID.
        return viaf_id, None, None, None

    def parse(self, xml, working_name=None):
        """Parse a VIAF response containing a single cluster into a name
        3-tuple."""
        tree = etree.fromstring(xml, parser=etree.XMLParser(recover=True))
        return self.extract_viaf_info(tree, working_name)
        
    def extract_viaf_info(self, cluster, working_name=None, strict=False):
        """Extract name info from a single VIAF cluster."""
        display_name = None
        family_name = None
        wikipedia_name = None

        # If we're not sure that this is even the right cluster for
        # the given author, make sure that the working name shows up
        # in a name record.
        if strict and not self.cluster_has_record_for_named_author(
                cluster, working_name):
            return None, None, None, None

        # Get the VIAF ID for this cluster, just in case we don't have one yet.
        viaf_tag = self._xpath1(cluster, './/*[local-name()="viafID"]')
        if viaf_tag is None:
            viaf_id = None
        else:
            viaf_id = viaf_tag.text


        # Does this cluster have a Wikipedia page?
        for source in self._xpath(cluster, './/*[local-name()="sources"]/*[local-name()="source"]'):
            if source.text.startswith("WKP|"):
                # Jackpot!
                wikipedia_name = source.text[4:]
                display_name = wikipedia_name.replace("_", " ")
                if ' (' in display_name:
                    display_name = display_name[:display_name.rindex(' (')]
                working_name = wikipedia_name

        unimarcs = self._xpath(cluster, './/*[local-name()="datafield"][@dtype="UNIMARC"]')
        candidates = []
        for unimarc in unimarcs:
            (possible_given, possible_family,
             possible_extra) = self.extract_name_from_unimarc(unimarc)
            # Some part of this name must also show up in the original
            # name for it to even be considered. Otherwise it's a
            # better bet to try to munge the original name.
            for v in (possible_given, possible_family, possible_extra):
                if (not working_name) or (v and v in working_name):
                    # print "FOUND %s in %s" % (v, working_name)
                    candidates.append((possible_given, possible_family,
                                       possible_extra))
                    break
            else:
                #print "  EXCLUDED %s/%s/%s for lack of resemblance to %s" % (
                #    possible_given, possible_family, possible_extra,
                #    working_name)
                pass

        display_nameparts = self.best_choice(candidates)
        if display_nameparts[1]: # Family name
            family_name = display_nameparts[1]
        return (
            viaf_id,
            display_name or self.combine_nameparts(*display_nameparts),
            family_name,
            wikipedia_name)

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
            #print "  POSSIBILITY: %s/%s/%s" % (
            #    given_name, family_name, name_extra)
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
        """Turn a UNIMARC tag into a 3-tuple:
         (given name, family name, extra)
        """
        data = dict()
        for (code, key) in (
                ('a', 'family'),
                ('b', 'given'),
                ('c', 'extra'),
                ):
            value = self._xpath1(unimarc, 'ns2:subfield[@code="%s"]' % code)
            if value is not None and value.text:
                value = value.text
                value = self.remove_commas_from(value)
                data[key] = value
        return (data.get('given', None), data.get('family', None),
                data.get('extra', None))

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

    LOOKUP_URL = 'http://viaf.org/viaf/%(viaf)s/viaf.xml'
    SEARCH_URL = 'http://viaf.org/viaf/search?query=local.names+%3D+%22{sort_name}%22&maximumRecords=5&startRecord=1&sortKeys=holdingscount&local.sources=lc&httpAccept=text/xml'
    SUBDIR = "viaf"

    def __init__(self, _db):
        self._db = _db
        self.data_source = DataSource.lookup(self._db, DataSource.VIAF)
        self.parser = VIAFParser()

    def run(self, force=False):
        a = 0
        # Ignore editions from OCLC
        oclc_linked_data = DataSource.lookup(
            self._db, DataSource.OCLC_LINKED_DATA)
        oclc_search = DataSource.lookup(
            self._db, DataSource.OCLC)
        ignore_editions_from = [oclc_linked_data.id, oclc_search.id]
        must_have_roles = [
            Contributor.PRIMARY_AUTHOR_ROLE, Contributor.AUTHOR_ROLE]
        candidates = self._db.query(Contributor)
        candidates = candidates.join(Contributor.contributions)
        candidates = candidates.join(Contribution.edition)
        candidates = candidates.filter(~Edition.data_source_id.in_(ignore_editions_from))
        candidates = candidates.filter(Contribution.role.in_(must_have_roles))
        if not force:
            something_is_missing = or_(
                Contributor.display_name==None,
                Contributor.viaf==None)
            # Only process authors that haven't been processed yet.
            candidates = candidates.filter(something_is_missing)
        for contributor in candidates:
            if contributor.viaf:
                # A VIAF ID is the most reliable way we have of identifying
                # a contributor.
                viafs = [x.strip() for x in contributor.viaf.split("|")]
                # Sometimes there are multiple VIAF IDs.
                for v in viafs:
                    self.fill_contributor_info_from_viaf(contributor, v)
                    if contributor.wikipedia_name or contributor.family_name:
                        # Good enough.
                        break
            elif contributor.name:
                self.fill_contributor_info_from_name(contributor)

            if not contributor.display_name:
                # We could not find a VIAF record for this contributor,
                # or none of the records were good enough. Use the
                # default name.
                print "BITTER FAILURE for %s" % contributor.name
                contributor.family_name, contributor.display_name = (
                    contributor.default_names())
            a += 1
            if not a % 10:
                print a
                self._db.commit()
        self._db.commit()

    def fill_contributor_info_from_viaf(self, contributor, viaf):
        url = self.LOOKUP_URL % dict(viaf=viaf)
        r, cached = Representation.get(self._db, url, data_source=self.data_source)

        xml = r.content
        viaf, display_name, family_name, wikipedia_name = self.parser.info(
            contributor, xml, False)
        contributor.display_name = display_name
        contributor.family_name = family_name
        contributor.wikipedia_name = wikipedia_name

    def fill_contributor_info_from_name(self, contributor):
        url = self.SEARCH_URL.format(sort_name=contributor.name.encode("utf8"))
        r, cached = Representation.get(
            self._db, url, data_source=self.data_source)
        xml = r.content
        viaf, display_name, family_name, wikipedia_name = self.parser.info(
            contributor, xml, True)
        contributor.viaf = viaf
        contributor.display_name = display_name
        contributor.family_name = family_name
        contributor.wikipedia_name = wikipedia_name

        # Is there already a contributor with this VIAF?
        if contributor.viaf is not None:
            duplicates = self._db.query(Contributor).filter(
                Contributor.viaf==contributor.viaf).all()
            if duplicates:
                if duplicates[0].display_name != contributor.display_name:
                    set_trace()
                contributor.merge_into(duplicates[0])
                
        
