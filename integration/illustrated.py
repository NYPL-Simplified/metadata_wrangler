from collections import defaultdict
import os
import re
from nose.tools import set_trace

from model import (
    Identifier,
    Edition,
    DataSource,
)

class GutenbergIllustratedDriver(object):
    """Manage the command-line Gutenberg Illustrated program.

    Given the path to a Project Gutenberg HTML mirror, this class can
    invoke Gutenberg Illustrated to generate covers and then upload
    the covers to S3.
    """

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
                if container is not None:
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

    @classmethod
    def data_from_file_list(cls, db, paths):
        data_source = DataSource.lookup(db, DataSource.GUTENBERG)
        seen_ids = set()
        for (gid, illustrations) in cls.illustrations_from_file_list(paths):
            edition = Edition.for_foreign_id(
                db, data_source, Identifier.GUTENBERG_ID, gid,
                create_if_not_exists=False)

            if not edition:
                # We don't know about this book.
                gid = container = working_directory = None
                continue

            data = cls.data_for_edition(edition)
            data['gid'] = gid
            data['illustrations'] = illustrations
            yield data
            seen_ids.add(gid)

