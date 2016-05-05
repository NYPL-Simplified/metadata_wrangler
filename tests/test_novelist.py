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
    DataSource,
    Identifier,
)
from novelist import NoveListAPI


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
        identifier = self._identifier()
        # Without an ISBN-equivalent identifier, lookup returns None
        eq_(None, self.novelist.lookup(identifier))

        # With invalid credentials, lookup raises an error
        isbn = self._identifier(identifier_type=Identifier.ISBN)
        identifier.equivalent_to(source, isbn, 1)
        self._db.commit()
        assert_raises(Exception, self.novelist.lookup, identifier)

    def test_lookup_info_to_metadata(self):
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.ISBN, "9780804171335"
        )
        bad_character = self.sample_data("a_bad_character.json")
        metadata = self.novelist.lookup_info_to_metadata(bad_character)

        eq_(True, isinstance(metadata, Metadata))
        eq_(identifier, metadata.primary_identifier)
        eq_("A bad character", metadata.title)
        eq_(None, metadata.subtitle)
        eq_(1, len(metadata.contributors))
        [contributor] = metadata.contributors
        eq_("Kapoor, Deepti", contributor.sort_name)
        eq_(3, len(metadata.identifiers))
        eq_(4, len(metadata.subjects))
        eq_(2, len(metadata.measurements))
        ratings = sorted(metadata.measurements, key=lambda m: m.value)
        eq_(2, ratings[0].value)
        eq_(3.27, ratings[1].value)

    def test_lookup_info_to_metadata_extracts_lexile(self):
        vampire = self.sample_data("vampire_kisses.json")
        metadata = self.novelist.lookup_info_to_metadata(vampire_kisses)
        [lexile] = filter(lambda s: s.type=='Lexile', metadata.subjects)
        eq_(u'630', lexile.identifier)
