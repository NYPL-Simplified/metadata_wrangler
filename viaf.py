import os
from nose.tools import set_trace
from lxml import etree
import logging
from fuzzywuzzy import fuzz
import re

from collections import Counter, defaultdict

from core.metadata_layer import (
    ContributorData,
    Metadata, 
)

from core.model import (
    Contributor,
    DataSource,
    Representation,
)

from core.util.personal_names import normalize_contributor_name_for_matching;

from core.util.xmlparser import (
    XMLParser,
)
from core.util.personal_names import display_name_to_sort_name



class VIAFParser(XMLParser):

    NAMESPACES = {'ns2' : "http://viaf.org/viaf/terms#"}

    log = logging.getLogger("VIAF Parser")
    wikidata_id = re.compile("^Q[0-9]")



    @classmethod
    def prepare_contributor_name_for_matching(cls, name):
        """
        Normalize the special characters and inappropriate spacings away.
        Put the name into title, first, middle, last, suffix, nickname order, 
        and lowercase.
        """
        return normalize_contributor_name_for_matching(name)


    @classmethod
    def contributor_name_match_ratio(cls, name1, name2):
        """
        Returns a number between 0 and 100, representing the percent 
        match (Levenshtein Distance) between name1 and name2, 
        after each has been normalized.
        """
        name1 = cls.prepare_contributor_name_for_matching(name1)
        name2 = cls.prepare_contributor_name_for_matching(name2)
        match_ratio = fuzz.ratio(name1, name2)
        print "name1=%s, name2=%s" % (name1, name2)
        print "match_ratio=%s" % match_ratio
        return match_ratio


    @classmethod
    def name_matches(cls, n1, n2):
        """ Returns true if n1 and n2 are identical strings.  
        Ignores case, periods, commas, spaces after single-letter initials.
        """ 
        return n1.replace(".", "").lower() == n2.replace(".", "").lower()


    def alternate_name_forms_for_cluster(self, cluster):
        """Find all pseudonyms in the given cluster."""
        for tag in ('400', '700'):
            for data_field in self._xpath(
                    cluster, './/*[local-name()="datafield"][@dtype="MARC21"][@tag="%s"]' % tag):
                for potential_match in self._xpath(
                        data_field, '*[local-name()="subfield"][@code="a"]'):
                    yield potential_match.text


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
        """  TODO: fill in

        Short-circuit the xml parsing process -- if found an author name 
        match, stop parsing and return the match.

        :return: a dictionary containing description of xml field 
        that matched author name searched for.
        """
        match_confidences = {}

        # If we have a sort name to look for, and it's in this cluster's
        # sort names, great.
        if working_sort_name:
            for potential_match in self.sort_names_for_cluster(cluster):
                match_confidence = self.contributor_name_match_ratio(potential_match, working_sort_name)
                match_confidences["sort_name"] = match_confidence
                # fuzzy match filter may not always give a 100% match, so cap arbitrarily at 90% as a "sure match"
                if match_confidence > 90:
                    return match_confidences

        # If we have a display name to look for, and this cluster's
        # Wikipedia name converts to the display name, great.
        if working_display_name:
            wikipedia_name = self.extract_wikipedia_name(cluster)
            if wikipedia_name:
                display_name = self.wikipedia_name_to_display_name(wikipedia_name)
                match_confidence = self.contributor_name_match_ratio(display_name, working_display_name)
                match_confidences["display_name"] = match_confidence
                if match_confidence > 90:
                    return match_confidences

        # If there are UNIMARC records, and every part of the UNIMARC
        # record matches the sort name or the display name, great.
        unimarcs = self._xpath(cluster, './/*[local-name()="datafield"][@dtype="UNIMARC"]')
        candidates = []
        for unimarc in unimarcs:
            (possible_given, possible_family,
             possible_extra, possible_sort_name) = self.extract_name_from_unimarc(unimarc)
            if working_sort_name:
                match_confidence = self.contributor_name_match_ratio(possible_sort_name, working_sort_name)
                match_confidences["unimarc"] = match_confidence
                if match_confidence > 90:
                    return match_confidences

            for name in (working_sort_name, working_display_name):
                if not name:
                    continue
                if (possible_given and possible_given in name
                    and possible_family and possible_family in name and (
                        not possible_extra or possible_extra in name)):
                    match_confidences["unimarc"] = 90
                    return match_confidences

        # Last-ditch effort. Guess at the sort name and see if *that's* one
        # of the cluster sort names.
        if working_display_name and not working_sort_name:
            test_sort_name = display_name_to_sort_name(working_display_name)
            for potential_match in self.sort_names_for_cluster(cluster):
                match_confidence = self.contributor_name_match_ratio(potential_match, test_sort_name)
                match_confidences["guessed_sort_name"] = match_confidence
                if match_confidence > 90:
                    return match_confidences

        # OK, last last-ditch effort.  See if the alternate name forms (pseudonyms) are it.
        if working_sort_name:
            for potential_match in self.alternate_name_forms_for_cluster(cluster):
                match_confidence = self.contributor_name_match_ratio(potential_match, working_sort_name)
                match_confidences["alternate_name"] = match_confidence
                if match_confidence > 90:
                    return match_confidences
        
        return match_confidences



    def parse_multiple(
            self, xml, working_sort_name=None, working_display_name=None):
        """ Parse a VIAF response containing multiple clusters into 
        contributors and titles.

        NOTE:  No longer performs quality judgements on whether the contributor found is good enough.

        :return: a list of tuples, each tuple containing: 
        - a ContributorData object filled with VIAF id, display, sort, family, 
        and wikipedia names, or None on error.
        - a list of work titles ascribed to this Contributor.
        """

        tree = etree.fromstring(xml, parser=etree.XMLParser(recover=True))

        # each contributor_candidate entry contains 3 objects:
        # a contributor_data, a dictionary of search match confidence weights, 
        # and a list of metadata objects representing authored titles.
        contributor_candidates = []
        for cluster in self._xpath(tree, '//*[local-name()="VIAFCluster"]'):
            '''
            dir(cluster)=['__class__', '__contains__', '__copy__', '__deepcopy__', '__delattr__', '__delitem__', '__doc__', 
            '__format__', '__getattribute__', '__getitem__', '__hash__', '__init__', '__iter__', '__len__', '__new__', 
            '__nonzero__', '__reduce__', '__reduce_ex__', '__repr__', '__reversed__', '__setattr__', '__setitem__', 
            '__sizeof__', '__str__', '__subclasshook__', '_init', 
            'addnext', 'addprevious', 'append', 'attrib', 'base', 'clear', 'cssselect', 'extend', 'find', 'findall', 
            'findtext', 'get', 'getchildren', 'getiterator', 'getnext', 'getparent', 'getprevious', 'getroottree', 
            'index', 'insert', 'items', 'iter', 'iterancestors', 'iterchildren', 'iterdescendants', 'iterfind', 
            'itersiblings', 'itertext', 'keys', 'makeelement', 'nsmap', 'prefix', 'remove', 'replace', 'set', 'sourceline', 
            'tag', 'tail', 'text', 'values', 'xpath']
            print "dir(cluster)={}".format(dir(cluster))
            '''
            contributor_data, match_confidences, contributor_titles = self.extract_viaf_info(
                cluster, working_sort_name, working_display_name)
            
            if not contributor_data:
                print "why don't I have contributor_data for this cluster?: {}".format(cluster)
                set_trace()
                continue

            if contributor_data.display_name or contributor_data.viaf:
                contributor_candidate = (contributor_data, match_confidences, contributor_titles)
                contributor_candidates.append(contributor_candidate)
            
        # We could not find any names or viaf ids for this author.
        return contributor_candidates


    def parse(self, xml, working_sort_name=None, working_display_name=None):
        """ Parse a VIAF response containing a single cluster.

        NOTE:  No longer performs quality judgements on whether the contributor found is good enough.

        :return: a ContributorData object filled with display, sort, family, 
        and wikipedia names, and a list of titles this author has written.
        Return None on error.
        """
        tree = etree.fromstring(xml, parser=etree.XMLParser(recover=True))
        return self.extract_viaf_info(
            tree, working_sort_name, working_display_name, data_source)


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
                          working_display_name=False):
        """ Extract name info from a single VIAF cluster.

        :return: a tuple containing: 
        - ContributorData object filled with display, sort, family, and wikipedia names.
        - dictionary of ways the xml cluster data matched the names searched for.
        - list of titles attributed to the contributor in the cluster.
        or Nones on error.
        """
        contributor_data = ContributorData(sort_name=working_sort_name)
        contributor_titles = []
        match_confidences = {}

        # Find out if one of the working names shows up in a name record.
        match_confidences = self.cluster_has_record_for_named_author(
                cluster, working_sort_name, working_display_name)        

        # Get the VIAF ID for this cluster, just in case we don't have one yet.
        viaf_tag = self._xpath1(cluster, './/*[local-name()="viafID"]')
        if viaf_tag is None:
            contributor_data.viaf = None
        else:
            contributor_data.viaf = viaf_tag.text

        # If we don't have a working sort name, find the most popular
        # sort name in this cluster and use it as the sort name.
        sort_name_popularity = self.sort_names_by_popularity(cluster)

        # Does this cluster have a Wikipedia page?
        contributor_data.wikipedia_name = self.extract_wikipedia_name(cluster)
        if contributor_data.wikipedia_name:
            contributor_data.display_name = self.wikipedia_name_to_display_name(contributor_data.wikipedia_name)
            working_display_name = contributor_data.display_name
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
                        possible_sort_name = contributor_data.sort_name[:-1]
                        sort_name_popularity[possible_sort_name] += 1
                    break
            else:
                self.log.debug(
                    "EXCLUDED %s/%s/%s for lack of resemblance to %s",
                    possible_given, possible_family, possible_extra,
                    working_sort_name
                )
                pass

        if sort_name_popularity and not contributor_data.sort_name:
            contributor_data.sort_name, ignore = sort_name_popularity.most_common(1)[0]

        if contributor_data.display_name:
            parts = contributor_data.display_name.split(" ")
            if len(parts) == 2:
                # Pretty clearly given name+family name.
                # If it gets more complicated than this we can't
                # be confident.
                candidates.append(parts + [None])

        display_nameparts = self.best_choice(candidates)
        if display_nameparts[1]: # Family name
            contributor_data.family_name = display_nameparts[1]

        contributor_data.display_name = contributor_data.display_name or self.combine_nameparts(*display_nameparts) or working_display_name


        # Now go through the title elements, and make a list.
        titles = self._xpath(cluster, './/*[local-name()="titles"]/*[local-name()="work"]/*[local-name()="title"]')
        for title in titles:
            #set_trace()
            print u"title={}".format(title.text)
            contributor_titles.append(title.text)

        return contributor_data, match_confidences, contributor_titles


    def wikipedia_name_to_display_name(self, wikipedia_name):
        """ Convert 'Bob_Jones_(Author)' to 'Bob Jones'. """
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


    @classmethod
    def weigh_contributor(cls, candidate, working_sort_name, known_title=None, strict=False):
        """ TODO: doc

        Find the top-most (most popular in libraries) author who corresponds the 
        best to the working_sort_name.

        """
        (contributor, match_confidences, titles) = candidate
        if not match_confidences:
            # we didn't get it from the xml, but we'll add to it now
            match_confidences = {}

        # If we're not sure that this is even the right cluster for
        # the given author, make sure that one of the working names
        # shows up in a name record.
        if strict:
            if len(match_confidences) == 0:
                return 0

        # Assign weights to fields matched in the xml.  
        # The fuzzy matching returned a number between 0 and 100, 
        # now tell the system that we find sort_name to be a more reliable indicator 
        # than unimarc flags.  
        # Weights are cumulative -- if both the sort and display name match, that helps us 
        # be extra special sure.  But what to do if unimarc tags match and sort_name doesn't? 
        # Here's where the strict tag comes in.  With strict, a failed sort_name match says "no" 
        # to any other suggestions of a possible fit.
        match_confidences["total"] = 0
        if match_confidences["sort_name"]:
            if strict and match_confidences["sort_name"] < 90:
                return 0
            match_confidences["total"] += 0.8 * match_confidences["sort_name"]
        if match_confidences["display_name"]:
            match_confidences["total"] += 0.7 * match_confidences["display_name"]
        if match_confidences["unimarc"]:
            match_confidences["total"] += 0.6 * match_confidences["unimarc"]
        if match_confidences["guessed_sort_name"]:
            match_confidences["total"] += 0.5 * match_confidences["guessed_sort_name"]
        if match_confidences["alternate_name"]:
            match_confidences["total"] += 0.4 * match_confidences["alternate_name"]

        # Add in some data quality evidence.  We want the contributor to have recognizable 
        # data to work with.
        if contributor_data.display_name:
            return 100

        if contributor_data.viaf:
            return 100



    def order_candidates(self, contributor_candidates, working_sort_name, 
                        known_title=None, strict=False):
        """
        Accepts a list of tuples, each tuple containing: 
        - a ContributorData object filled with VIAF id, display, sort, family, 
        and wikipedia names, or None on error.
        - a list of work titles ascribed to this Contributor.

        For each contributor, determines how likely that contributor is to 
        be the one being searched for (how well they correspond to the 
        working_sort_name and known_title.

        Assumes the contributor_candidates list was generated off an xml 
        that was is in popularity order.  I.e., the author id that 
        appears in most libraries when searching for working_sort_name is on top.
        Assumes the xml's order is preserved in the contributor_candidates list.

        :return: the list of tuples, ordered by percent match, in descending order 
        (top match first).
        """
        
        # higher score for better match, so to have best match first, do desc order.
        contributor_candidates.sort(key=lambda x: self.weigh_contributor(x, working_sort_name=working_sort_name, 
            known_title=known_title, strict=strict), reverse=True)
        return contributor_candidates



class VIAFClient(object):

    LOOKUP_URL = 'http://viaf.org/viaf/%(viaf)s/viaf.xml'
    SEARCH_URL = 'http://viaf.org/viaf/search?query=local.names+%3D+%22{sort_name}%22&maximumRecords=5&startRecord=1&sortKeys=holdingscount&local.sources=lc&httpAccept=text/xml'
    SUBDIR = "viaf"

    MEDIA_TYPE = Representation.TEXT_XML_MEDIA_TYPE

    def __init__(self, _db):
        self._db = _db
        self.parser = VIAFParser()
        self.log = logging.getLogger("VIAF Client")

    @property
    def data_source(self):
        return DataSource.lookup(self._db, DataSource.VIAF)

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

