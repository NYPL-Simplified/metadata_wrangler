import json
from StringIO import StringIO
from collections import defaultdict
import os
import re
from nose.tools import set_trace

from model import (
    CoverageProvider,
    Identifier,
    Edition,
    DataSource,
)
import subprocess
import tempfile

class GutenbergIllustratedDataProvider(object):
    """Manage the command-line Gutenberg Illustrated program.

    Given the path to a Project Gutenberg HTML mirror, this class can
    invoke Gutenberg Illustrated to generate covers and then upload
    the covers to S3.
    """

    IMAGE_EXTENSIONS = ['jpg', 'png', 'jpeg', 'gif']

    gutenberg_id_res = [
        re.compile(".*/([0-9]+)-h"),
        re.compile(".*/([0-9]+)"),
    ]

    # These authors can be treated as if no author was specified
    ignorable_authors = ['Various']

    # These regular expressions match text that can be removed from a
    # title when calculating the short display title.
    short_display_title_removable = [
        re.compile(",? or,? the london chi?ar[ia]vari\.?", re.I),
        re.compile("\([^0-9XVI]+\)$"),
        re.compile("[(\[]?of [0-9XVI]+[\])]$"),
    ]

    # These regular expressions indicate the end of a short display title.
    short_display_title_stoppers = [
        re.compile('(?<!Operation): '), 
        re.compile('; '),
        re.compile(r",? and other\b", re.I),
        re.compile(', A Novel'),
    ]

    @classmethod
    def short_display_title(self, title):
        """Turn a title into a short display title suitable for use with
        Gutenberg Illustrated.

        It's okay if the title is short to begin with; Gutenberg
        Illustrated will decide which version of the title to use.
        """
        orig = title
        for u in self.short_display_title_removable:
            title = u.sub("", title)
        for stop_at in self.short_display_title_stoppers:
            m = stop_at.search(title)
            if not m:
                continue
            title = title[:m.start()]

        title = title.strip()
        if title == orig:
            return None
        return title


    @classmethod
    def author_string(cls, names):
        if not names:
            return ''

        if len(names) == 1:
            name = names[0]
            if name in cls.ignorable_authors:
                return ''
            return name

        before_ampersand = names[:-1]
        after_ampersand = names[-1]
        return ", ".join(before_ampersand) + " & " + after_ampersand

    @classmethod
    def data_for_edition(cls, edition):
        short_names = []
        long_names = []
        primary_author = None
        other_authors = []
        for a in edition.author_contributors:
            if a.name in cls.ignorable_authors:
                continue
            if a.family_name:
                short_names.append(a.family_name)
            if a.display_name:
                long_names.append(a.display_name)
        short_name = cls.author_string(short_names)
        long_name = cls.author_string(long_names)

        d = dict(
            authors_short=short_name or "",
            authors_long=long_name or "",
#            identifier=gid,
            title=edition.title or "",
            title_short=cls.short_display_title(edition.title) or "",
            subtitle=edition.subtitle or "",
            identifier_type = Identifier.GUTENBERG_ID,
#            illustrations=container,
        )
        return d


    @classmethod
    def is_usable_image_name(cls, filename):
        if not '.' in filename:
            return False
        name, extension = filename.lower().rsplit('.', 1)
        if not extension in cls.IMAGE_EXTENSIONS:
            return False

        if name.endswith('thumb') or name.endswith('th') or name.endswith('tn'):
            # No thumbnails.
            return False

        if 'cover' in name:
            # Don't coverize something that's already a cover.
            return False
        return True

    @classmethod
    def illustrations_from_file_list(cls, paths):
        seen_ids = set()
        images_for_work = defaultdict(list)
        gid = container = working_directory = None
        for i in paths:
            i = i.strip()
            if i.endswith("images:") and not i.endswith("page-images:"):
                working_directory = i[:-1]
                for r in cls.gutenberg_id_res:
                    gid = r.search(i)
                    if gid:
                        gid = gid.groups()[0]
                        container = images_for_work[gid]
                        break
                continue

            if i:
                if container is not None and cls.is_usable_image_name(i):
                    container.append(os.path.join(working_directory, i))
                continue

            # At this point we know that we've encountered a blank line.

            if gid is None or container is None:
                # We're not equipped to handle this.
                continue

            # Look up Gutenberg info.
            if gid in seen_ids:
                # This happens very rarely, when an anthology book includes
                # another book's directory wholesale. But the illustrations
                # should be the same in either case,
                continue

            if container:
                yield gid, container
            gid = container = working_directory = None

        if container:
            yield gid, container


class GutenbergIllustratedCoverageProvider(CoverageProvider):

    DESTINATION_DIRECTORY = "Gutenberg Illustrated"
    FONT_FILENAME = "AvenirNext-Bold-14.vlw"

    def __init__(self, _db, data_directory, binary_path):

        self.gutenberg_mirror = os.path.join(
            data_directory, "gutenberg-mirror") + "/"
        self.file_list = os.path.join(self.gutenberg_mirror, "ls-R")
        self.binary_path = binary_path
        binary_directory = os.path.split(self.binary_path)[0]
        self.font_path = os.path.join(
            binary_directory, 'data', self.FONT_FILENAME)
        self.output_directory = os.path.join(
            data_directory, self.DESTINATION_DIRECTORY) + "/"

        input_source = DataSource.lookup(_db, DataSource.GUTENBERG)
        output_source = DataSource.lookup(
            _db, DataSource.GUTENBERG_COVER_GENERATOR)
        super(GutenbergIllustratedCoverageProvider, self).__init__(
            "Gutenberg Illustrated", input_source, output_source)

        # Load the illustration lists from the Gutenberg ls-R file.
        self.illustration_lists = dict()
        for (gid, illustrations) in GutenbergIllustratedDataProvider.illustrations_from_file_list(
                open(self.file_list)):
            if gid not in self.illustration_lists:
                self.illustration_lists[gid] = illustrations

    IMAGE_CUTOFF_SIZE = 10 * 1024

    def apply_size_filter(self, illustrations):
        large_enough = []
        for i in illustrations:
            path = os.path.join(self.gutenberg_mirror, i)
            if not os.path.exists(path):
                print "ERROR: could not find illustration %s" % path
                continue
            file_size = os.stat(path).st_size
            if file_size < self.IMAGE_CUTOFF_SIZE:
                print "INFO: %s is only %d bytes, not using it." % (
                    path, file_size)
                continue
            large_enough.append(i)
        return large_enough

    def process_edition(self, edition):
        data = GutenbergIllustratedDataProvider.data_for_edition(edition)

        identifier = edition.primary_identifier.identifier
        if identifier not in self.illustration_lists:
            # No illustrations for this edition. Nothing to do.
            return True

        data['identifier'] = identifier
        illustrations = self.illustration_lists[identifier]

        # The size filter is time-consuming, so we apply it here, when
        # we know we're going to generate covers for this particular
        # book, rather than ahead of time.
        illustrations = self.apply_size_filter(illustrations)

        if not illustrations:
            # All illustrations were filtered out. Nothing to do.
            return True

        data['illustrations'] = illustrations
        
        # Write the input to a temporary file.
        fh, input_path = tempfile.mkstemp()
        json.dump(data, open(input_path, "w"))

        # Make sure the output directory exists.
        if not os.path.exists(self.output_directory):
                         os.makedirs(self.output_directory)

        args = self.args_for(input_path)
        fh, output_capture_path = tempfile.mkstemp()
        output_capture = open(output_capture_path, "w")
        print output_capture_path
        print "Running"
        print " ".join(args)
        subprocess.call(args, stdout=output_capture)

        # We're done with the input file. Remove it.
        os.remove(input_path)
        return False

        # Associate 'cover' resources with the identifier

        # Upload the generated images to S3.
        # TODO: only upload images that changed.

    def args_for(self, input_path):
        return [self.binary_path, self.gutenberg_mirror, self.output_directory,
                input_path, self.font_path, self.font_path]
