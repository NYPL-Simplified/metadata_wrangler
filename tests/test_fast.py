from nose.tools import (
    eq_,
    set_trace,
)
from contextlib import contextmanager
from io import StringIO
import os

from fast import (
    FASTNames,
    LCSHNames,
)

BASE_DIR = os.path.split(__file__)[0]

class MockFASTNames(FASTNames):
    """Works just like FASTNames, but writes consolidated output to a
    String object rather than a file on disk.
    """

    def __init__(self):
        self.output_consolidated_file = None

    @contextmanager
    def consolidated_output_filehandle(self, path):
        # Create a String object to stand in for the
        # consolidated file that would otherwise be written to
        # disk.
        self.output_consolidated_file = StringIO()
        yield self.output_consolidated_file

class MockLCSHNames(MockFASTNames, LCSHNames):
    """Works just like LCSHNames, but writes consolidated output to a
    StringIO object rather than a file on disk.
    """

class TestFASTNames(object):

    def test_from_data_directory_not_consolidated(self):
        # Load FAST data from a number of gzipped files in N-Triples
        # format.
        not_consolidated = MockFASTNames.from_data_directory(
            os.path.join(BASE_DIR, "files/fast/not-consolidated")
        )
        eq_(
            {
                '1726280': 'Filmed roundtables',
                '631903': 'New Yorker (Fireboat)',
                '1750175': 'Short stories, American'
            },
            not_consolidated
        )
        
        # A consolidated file was written to "disk" in CSV format.
        output = not_consolidated.output_consolidated_file
        expect = '631903,New Yorker (Fireboat)\r\n1750175,"Short stories, American"\r\n1726280,Filmed roundtables\r\n'
        eq_(expect, output.getvalue())

    def test_from_data_directory_consolidated(self):
        # Load FAST data from a single CSV file created by an earlier call
        # to write_consolidated_file().
        consolidated = MockFASTNames.from_data_directory(
            os.path.join(BASE_DIR, "files/fast/consolidated")
        )
        eq_({'identifier1': 'name1', 'identifier2': 'name2'},
            consolidated)

        # Since data was loaded in from a consolidated file,
        # no new consolidated file was created.
        eq_(None, consolidated.output_consolidated_file)

class TestLCSHNames(object):

    def test_from_data_directory_not_consolidated(self):
        # Load FAST data from a number of ZIP archives containing data
        # in N-Triples format.
        not_consolidated = MockLCSHNames.from_data_directory(
            os.path.join(BASE_DIR, "files/fast/not-consolidated")
        )
        eq_(
            {
                'sj00001253': 'Ceratopsians',
                'gf2008025611': 'Abstract films'
            },
            not_consolidated
        )
        
        # A consolidated file was written to "disk" in CSV format.
        output = not_consolidated.output_consolidated_file
        expect = 'sj00001253,Ceratopsians\r\ngf2008025611,Abstract films\r\n'
        eq_(expect, output.getvalue())
