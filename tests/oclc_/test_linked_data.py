# encoding: utf-8

import json
from nose.tools import set_trace, eq_

from core.model import (
    Contributor,
    DataSource,
    Equivalency,
    Identifier,
    LicensePool,
    Subject,
)
from core.metadata_layer import (
    ContributorData,
    IdentifierData,
    Metadata,
)
from core.coverage import CoverageFailure

from oclc.linked_data import (
    OCLCLinkedData,
    LinkedDataCoverageProvider,
)

from testing import (
    MockOCLCLinkedDataAPI,
    MockVIAFClient,
)

from .. import (
    DatabaseTest,
    sample_data
)


class TestOCLCLinkedData(DatabaseTest):

    def sample_data(self, filename):
        return sample_data(filename, 'oclc')

    def test_creator_names_picks_up_contributors(self):
        graph = json.loads(
            self.sample_data("no_author_only_contributor.jsonld"))['@graph']

        eq_(([], []), OCLCLinkedData.creator_names(graph))
        eq_((['Thug Kitchen LLC.'], []),
            OCLCLinkedData.creator_names(graph, 'contributor'))

    def test_creator_names_gathers_external_uris(self):
        graph = json.loads(
            self.sample_data("creator_includes_viaf_uris.jsonld"))['@graph']

        names, uris = OCLCLinkedData.creator_names(graph)
        eq_([], names)
        eq_(set(["http://id.loc.gov/authorities/names/n2013058227",
                 "http://viaf.org/viaf/221233754",
                 "http://viaf.org/viaf/305306689"]),
            set(uris))

    def test_extract_contributor(self):
        # It pulls relevant contributor data out of an OCLC person entity graph.
        sloane_info = json.loads(self.sample_data('sloane_crosley.jsonld'))['@graph'][1]
        result = OCLCLinkedData.extract_contributor(sloane_info)
        eq_(result['family_name'], 'Crosley')
        eq_(result['display_name'], 'Sloane Crosley')

        flanagan_info = json.loads(self.sample_data('john_flanagan_multiname.jsonld'))
        flanagan_info = flanagan_info['@graph'][1]
        result = OCLCLinkedData.extract_contributor(flanagan_info)
        eq_(result['family_name'], 'Flanagan')
        eq_(result['display_name'], 'John Anthony Flanagan')
        eq_(result['extra']['birthDate'], '1944')

        # TODO: Modify the contributor extraction to handle cases where
        # maiden names are included and/or multiple name options are the
        # same except for capitalization.
        rice_info = json.loads(self.sample_data('anne_rice.jsonld'))
        result = OCLCLinkedData.extract_contributor(rice_info['@graph'][1])
        eq_(result['family_name'], "O'Brien Rice")
        eq_(result['display_name'], "Anne O'Brien Rice")
        eq_(result['extra']['birthDate'], '1941')

    def test_extract_useful_data(self):
        subgraph = json.loads(
            self.sample_data('galapagos.jsonld')
        )['@graph']
        [book] = [book for book in OCLCLinkedData.books(subgraph)]

        (oclc_id_type,
         oclc_id,
         titles,
         descriptions,
         subjects,
         creator_uris,
         publishers,
         publication_dates,
         example_uris) = OCLCLinkedData.extract_useful_data(subgraph, book)

        eq_(Identifier.OCLC_NUMBER, oclc_id_type)
        eq_(u"11866009", oclc_id)
        eq_([u"Galápagos : a novel"], titles)
        eq_(1, len(descriptions))

        # Even though there are 11 links in the books "about" list,
        # "http://subject.example.wo/internal_lookup" does not get included as
        # a subject because it doesn't have an internal lookup.
        eq_(1, len(subjects[Subject.DDC]))
        eq_(1, len(subjects[Subject.FAST]))
        eq_(4, len(subjects[Subject.TAG]))
        eq_(1, len(subjects[Subject.PLACE]))
        # Meanwhile, the made-up LCSH subject that also doesn't have an
        # internal lookup is included because its details can be parsed from
        # the url: "http://id.loc.gov/authorities/subjects/sh12345678"
        eq_(3, len(subjects[Subject.LCSH]))

        eq_(1, len(creator_uris))
        eq_(["Delacorte Press/Seymour Lawrence"], publishers)
        eq_(["1985"], publication_dates)
        eq_(2, len(example_uris))

    def test_book_info_to_metadata(self):
        oclc = OCLCLinkedData(self._db)
        subgraph = json.loads(self.sample_data("galapagos.jsonld"))['@graph']
        [book] = [book for book in oclc.books(subgraph)]

        metadata_obj = OCLCLinkedData(self._db).book_info_to_metadata(
            subgraph, book
        )

        # A metadata object is returned, with the proper OCLC identifier.
        eq_(True, isinstance(metadata_obj, Metadata))
        eq_(Identifier.OCLC_NUMBER, metadata_obj.primary_identifier.type)
        eq_(u"11866009", metadata_obj.primary_identifier.identifier)

        # It has publication information & ISBNs
        eq_(u"Galápagos : a novel", metadata_obj.title)
        eq_(u'Delacorte Press/Seymour Lawrence', metadata_obj.publisher)
        eq_(1985, metadata_obj.published.year)
        eq_(1, len(metadata_obj.links))
        assert "ghost of a shipbuilder" in metadata_obj.links[0].content
        eq_(4, len(metadata_obj.identifiers))

        eq_(1, len(metadata_obj.contributors))
        [viaf] = [c.viaf for c in metadata_obj.contributors]
        eq_(u"71398958", viaf)
        eq_(10, len(metadata_obj.subjects))

        # Make sure a book with no English title doesn't break anything.
        subgraph[14]['name']['@language'] = 'fr'
        [book] = [book for book in oclc.books(subgraph)]

        metadata_obj = OCLCLinkedData(self._db).book_info_to_metadata(
            subgraph, book
        )

        # The metadata has no title.
        eq_(None, metadata_obj.title)


class TestLinkedDataCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestLinkedDataCoverageProvider, self).setup()
        self.provider = LinkedDataCoverageProvider(self._default_collection)

    def test_new_isbns(self):
        existing_id = self._identifier()
        metadata = Metadata(
            DataSource.lookup(self._db, DataSource.GUTENBERG),
            identifiers=[
                IdentifierData(type=Identifier.OCLC_WORK, identifier="abra"),
                IdentifierData(type=existing_id.type, identifier=existing_id.identifier),
                IdentifierData(type=Identifier.ISBN, identifier="kadabra"),
            ]
        )

        eq_(2, self.provider.new_isbns(metadata))

    def test_set_equivalence(self):
        edition = self._edition()
        edition.title = "The House on Mango Street"
        edition.add_contributor(Contributor(viaf="112460612"), Contributor.AUTHOR_ROLE)
        identifier = edition.primary_identifier

        i1 = self._identifier()
        identifierdata1 = IdentifierData(type=i1.type, identifier=i1.identifier)
        good_metadata = Metadata(
            DataSource.lookup(self._db, DataSource.GUTENBERG),
            primary_identifier = identifierdata1,
            title = "The House on Mango Street",
            contributors = [Contributor(viaf="112460612")]
        )

        i2 = self._identifier()
        identifierdata2 = IdentifierData(type=i2.type, identifier=i2.identifier)
        bad_metadata = Metadata(
            DataSource.lookup(self._db, DataSource.GUTENBERG),
            primary_identifier = identifierdata2,
            title = "Calvin & Hobbes",
            contributors = [Contributor(viaf="101010")]
        )

        self.provider.set_equivalence(identifier, good_metadata)
        self.provider.set_equivalence(identifier, bad_metadata)
        equivalencies = Equivalency.for_identifiers(self._db, [identifier]).all()

        # The identifier for the bad metadata isn't made equivalent
        eq_([i1], [x.output for x in equivalencies])
        eq_([1], [x.strength for x in equivalencies])

        # But if the existing identifier has no editions, they're made equivalent.
        identifier = self._identifier()
        self.provider.set_equivalence(identifier, bad_metadata)
        equivalencies = Equivalency.for_identifiers(self._db, [identifier]).all()
        eq_([i2], [x.output for x in equivalencies])
        eq_([1], [x.strength for x in equivalencies])

    def test_process_item_exception(self):
        class DoomedOCLCLinkedData(OCLCLinkedData):
            def info_for(self, identifier):
                raise IOError("Exception!")

        provider = LinkedDataCoverageProvider(
            self._default_collection, api=DoomedOCLCLinkedData(self._db)
        )

        edition = self._edition()
        identifier = edition.primary_identifier

        result = provider.process_item(identifier)
        assert isinstance(result, CoverageFailure)
        assert "Exception!" in result.exception

    def test_process_item_exception_missing_isbn(self):
        class DoomedOCLCLinkedData(OCLCLinkedData):
            def info_for(self, identifier):
                raise IOError("Tried, but couldn't find location")

        provider = LinkedDataCoverageProvider(
            self._default_collection, api=DoomedOCLCLinkedData(self._db)
        )

        edition = self._edition()
        identifier = edition.primary_identifier

        result = provider.process_item(identifier)
        assert isinstance(result, CoverageFailure)
        assert "OCLC doesn't know about this ISBN" in result.exception

    def test_viaf_authors_get_viaf_lookup(self):
        # TODO: The code this calls could be refactored quite a bit --
        # we don't really need to test all of process_item() here.
        # But ATM it does seem to be our only test of process_item().

        oclc = MockOCLCLinkedDataAPI()
        viaf = MockVIAFClient()
        provider = LinkedDataCoverageProvider(
            self._default_collection, api=oclc
        )
        provider.viaf = viaf

        # Here's a placeholder that will be filled in with information from
        # OCLC Linked Data.
        edition = self._edition()
        for i in edition.contributions:
            self._db.delete(i)
        self._db.commit()
        identifier = edition.primary_identifier

        # OCLC Linked Data is going to mention two authors -- one with
        # a sort name + VIAF, and one with a VIAF but no sort name.
        contributor1 = ContributorData(viaf="1")
        contributor2 = ContributorData(viaf="2", sort_name="Jordan, Robert")
        contributor3 = ContributorData(sort_name="Rice, Anne", display_name="Anne Rice")
        idata = IdentifierData(type=identifier.type,
                               identifier=identifier.identifier)
        metadata = Metadata(
            DataSource.OCLC_LINKED_DATA,
            contributors=[contributor1, contributor2, contributor3],
            primary_identifier=idata,
            title=u"foo"
        )
        oclc.queue_info_for(metadata)

        # Our OCLC Linked Data client is going to try to fill in the
        # data, asking VIAF about the contributors that have VIAF data,
        # and not those who do not.
        lookup1 = (ContributorData(
                  viaf="1", display_name="Display Name",
                  family_name="Family", sort_name="Name, Sort",
                  wikipedia_name="Wikipedia_Name"), None, None)
        lookup2 = (ContributorData(
                   viaf="2", wikipedia_name="Robert_Jordan_(Author)",
                   biography="That guy."), None, None)
        viaf.queue_lookup(lookup1, lookup2, "Unrequested lookup")

        provider.process_item(identifier)


        # Both VIAF-identified authors have had their information updated
        # with the VIAF results.
        filled_in = sorted(
            [(x.sort_name, x.display_name, x.viaf, x.wikipedia_name, x.biography)
             for x in edition.contributors]
        )
        eq_(
            [(u'Jordan, Robert', None, u'2', u'Robert_Jordan_(Author)', u'That guy.'),
            (u'Name, Sort', u'Display Name', u'1', u'Wikipedia_Name', None),
            (u'Rice, Anne', u'Anne Rice', None, None, None)],
            filled_in
        )
        # The author without VIAF data didn't request a VIAF lookup.
        # Instead, that result is still in the mock VIAF queue.
        eq_(viaf.results, ["Unrequested lookup"])

    def test_calculate_work_for_isbn(self):
        identifier = self._identifier()

        # With a non-ISBN identifier, nothing happens
        self.provider.calculate_work_for_isbn(identifier)
        eq_(None, identifier.work)

        # With an ISBN identifier without a LicensePool, nothing happens.
        identifier.type = Identifier.ISBN
        self.provider.calculate_work_for_isbn(identifier)
        eq_(None, identifier.work)

        # If there's a LicensePool and an edition, a work is created.
        edition, pool = self._edition(
            identifier_type=Identifier.ISBN, with_license_pool=True
        )
        identifier = edition.primary_identifier
        self.provider.calculate_work_for_isbn(identifier)

        work = identifier.work
        assert work
        eq_(edition.title, work.title)
        eq_(edition.author, work.author)

        # If there are two LicensePools, all of them get the same work.
        edition, pool = self._edition(
            identifier_type=Identifier.ISBN, with_license_pool=True
        )
        identifier = edition.primary_identifier

        ignore, other_pool = self._edition(
            data_source_name=DataSource.OCLC, with_license_pool=True
        )
        other_pool.identifier = identifier
        self.provider.calculate_work_for_isbn(identifier)

        work = identifier.work
        assert work
        eq_(work, pool.work)
        eq_(work, other_pool.work)

    def test_generate_edition(self):
        # Create an ISBN with a LicensePool.
        identifier = self._identifier(identifier_type=Identifier.ISBN)
        lp = LicensePool.for_foreign_id(
            self._db, self.provider.data_source, identifier.type,
            identifier.identifier, collection=self._default_collection
        )[0]

        # Create editions and equivalencies for some OCLC equivalent identifiers.
        number_ed = self._edition(identifier_type=Identifier.OCLC_NUMBER)
        work_id_ed = self._edition(identifier_type=Identifier.OCLC_WORK)

        identifier.equivalent_to(
            self.provider.data_source, number_ed.primary_identifier, 1
        )
        identifier.equivalent_to(
            self.provider.data_source, work_id_ed.primary_identifier, 1
        )
        self._db.commit()

        number_ed_info = (number_ed.title, number_ed.author)
        work_id_ed_info = (work_id_ed.title, work_id_ed.author)

        def presentation_edition_info():
            return (lp.presentation_edition.title, lp.presentation_edition.author)

        # generate_edition sets a presentation_edition
        self.provider.generate_edition(identifier)
        assert presentation_edition_info() in [number_ed_info, work_id_ed_info]

        # (Remove the generated presentation_edition for next portion of the test.)
        combined_edition = lp.presentation_edition
        lp.presentation_edition = None
        for contribution in combined_edition.contributions:
            self._db.delete(contribution)
        self._db.delete(combined_edition)

        # When only one edition has title and author, that edition becomes the
        # the presentation edition.
        for contribution in work_id_ed.contributions:
            work_id_ed.author = None
            self._db.delete(contribution)
        self._db.commit()

        self.provider.generate_edition(identifier)
        eq_(number_ed_info, presentation_edition_info())
