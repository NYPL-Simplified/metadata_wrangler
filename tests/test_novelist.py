from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)

from . import DatabaseTest, sample_data

from core.config import (
    Configuration,
    temp_config,
)
from core.metadata_layer import Metadata
from core.model import (
    get_one_or_create,
    DataSource,
    Identifier,
    Representation,
)
from novelist import (
    NoveListAPI,
    NoveListCoverageProvider,
)


class TestNoveListAPI(DatabaseTest):
    """Tests the NoveList API service object"""

    def setup(self):
        super(TestNoveListAPI, self).setup()
        with temp_config() as config:
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PROFILE : "library",
                Configuration.NOVELIST_PASSWORD : "yep"
            }
            self.novelist = NoveListAPI.from_config(self._db)

    def sample_data(self, filename):
        return sample_data(filename, 'novelist')

    def test_from_config(self):
        """Confirms that NoveListAPI can be built from config successfully"""

        with temp_config() as config:
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PROFILE : "library",
                Configuration.NOVELIST_PASSWORD : "yep"
            }
            novelist = NoveListAPI.from_config(self._db)
            eq_(True, isinstance(novelist, NoveListAPI))
            eq_("library", novelist.profile)
            eq_("yep", novelist.password)

            # Without either configuration value, an error is raised.
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PROFILE : "library"
            }
            assert_raises(ValueError, NoveListAPI.from_config, self._db)
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PASSWORD : "yep"
            }
            assert_raises(ValueError, NoveListAPI.from_config, self._db)

    def test_lookup(self):
        source = DataSource.lookup(self._db, DataSource.NOVELIST)
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        # Without an ISBN-equivalent identifier, lookup returns None
        eq_(None, self.novelist.lookup(identifier))

        # With invalid credentials, lookup raises an error
        isbn = self._identifier(identifier_type=Identifier.ISBN)
        identifier.equivalent_to(source, isbn, 1)
        self._db.commit()
        assert_raises(Exception, self.novelist.lookup, identifier)

    def test_lookup_info_to_metadata(self):
        # Basic book information is returned
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.ISBN, "9780804171335"
        )
        bad_character = self.sample_data("a_bad_character.json")
        metadata = self.novelist.lookup_info_to_metadata(bad_character)

        eq_(True, isinstance(metadata, Metadata))
        eq_(Identifier.NOVELIST_ID, metadata.primary_identifier.type)
        eq_('10392078', metadata.primary_identifier.identifier)
        eq_("A bad character", metadata.title)
        eq_(None, metadata.subtitle)
        eq_(1, len(metadata.contributors))
        [contributor] = metadata.contributors
        eq_("Kapoor, Deepti", contributor.sort_name)
        eq_(4, len(metadata.identifiers))
        eq_(4, len(metadata.subjects))
        eq_(2, len(metadata.measurements))
        ratings = sorted(metadata.measurements, key=lambda m: m.value)
        eq_(2, ratings[0].value)
        eq_(3.27, ratings[1].value)

        # Confirm that Lexile and series data is extracted with a
        # different sample.
        vampire = self.sample_data("vampire_kisses.json")
        metadata = self.novelist.lookup_info_to_metadata(vampire)
        [lexile] = filter(lambda s: s.type=='Lexile', metadata.subjects)
        eq_(u'630', lexile.identifier)
        eq_(u'Vampire kisses manga', metadata.series)
        # The full title should be selected, since every volume
        # has the same main title: 'Vampire kisses'
        eq_(u'Vampire kisses: blood relatives. Volume 1', metadata.title)
        eq_(1, metadata.series_position)

    def test_lookup_info_to_metadata_ignores_empty_responses(self):
        """API requests that return no data result return None"""
        null_response = self.sample_data("null_data.json")
        # Cached empty Representations are deleted based on their
        # unique URL, so this Representation needs one.
        test_url = self.novelist._build_query({
            'ISBN' : '4', 'version' : '2.2',
            'ClientIdentifier' : 'http%3A//www.gutenberg.org/ebooks/1001'
        })
        rep, ignore = get_one_or_create(
            self._db, Representation, url=test_url, content=null_response
        )

        # When a response has no bibliographic information, None is
        # returned and the Representation is deleted.
        result = self.novelist.lookup_info_to_metadata(null_response)
        eq_(None, result)
        eq_([], self._db.query(Representation).all())

        # This also happens when NoveList indicates with an empty
        # response that it doesn't know the ISBN.
        empty_response = self.sample_data("unknown_isbn.json")
        result = self.novelist.lookup_info_to_metadata(empty_response)
        eq_(None, result)

    def test_scrub_subtitle(self):
        """Unnecessary title segments are removed from subtitles"""

        scrub = self.novelist._scrub_subtitle
        eq_(None, scrub(None))
        eq_(None, scrub('[electronic resource]'))
        eq_(None, scrub('[electronic resource] :  '))
        eq_('A Biomythography', scrub('[electronic resource] :  A Biomythography'))


class TestNoveListCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestNoveListCoverageProvider, self).setup()
        with temp_config() as config:
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PROFILE : "library",
                Configuration.NOVELIST_PASSWORD : "yep"
            }
            self.novelist = NoveListCoverageProvider(self._db)

    def test_confirm_same_identifier(self):
        source = DataSource.lookup(self._db, DataSource.NOVELIST)
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.NOVELIST_ID, '84752928'
        )
        unmatched_identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.NOVELIST_ID, '23781947'
        )
        metadata = Metadata(source, primary_identifier=identifier)
        match = Metadata(source, primary_identifier=identifier)
        mistake = Metadata(source, primary_identifier=unmatched_identifier)

        eq_(False, self.novelist._confirm_same_identifier([metadata, mistake]))
        eq_(True, self.novelist._confirm_same_identifier([metadata, match]))
