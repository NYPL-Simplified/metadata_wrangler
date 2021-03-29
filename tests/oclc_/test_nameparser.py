from oclc.classify import NameParser
from core.metadata_layer import ContributorData
from core.model import Contributor


class TestNameParser(object):

    def test_parse_multiple_default_roles(self):
        # Verify that default roles are handled properly.

        # The first person to be seen with no explicit role is
        # considered the primary author.
        primary = "Author, Primary"

        # After that, people with no explicit role are treated as
        # regular authors.
        secondary = "Author, Secondary"
        tertiary = "Author, Tertiary"

        # Once contributors start showing up with explicitly specified
        # roles, anyone without an explicitly specified role is given
        # the UNKNOWN_ROLE.
        illustrator = "Illustrator, Anne [Illustrator]"
        rando = "Hanger-On, Random"

        authors = " | ".join(
            [primary, secondary, tertiary, illustrator, rando]
        )

        # parse_multiple returns a list of ContributorData objects.
        results = NameParser.parse_multiple(authors)
        assert all(isinstance(r, ContributorData) for r in results)

        # The objects are returned in the order they appeared in
        # the original string, with the appropriate roles given.
        p, s, t, i, r = results
        assert primary == p.sort_name
        assert [Contributor.PRIMARY_AUTHOR_ROLE] == p.roles

        assert secondary == s.sort_name
        assert [Contributor.AUTHOR_ROLE] == s.roles

        assert tertiary == t.sort_name
        assert [Contributor.AUTHOR_ROLE] == t.roles

        assert "Illustrator, Anne" == i.sort_name
        assert [Contributor.ILLUSTRATOR_ROLE] == i.roles

        assert rando == r.sort_name
        assert [Contributor.UNKNOWN_ROLE] == r.roles

    def _test_default_role_transition(self):
        # Test the state machine that governs changes in the role
        # assigned to contributors who have no explicit role in the
        # data.
        m = NameParser._default_role_transition

        primary_author = [
            Contributor.PRIMARY_AUTHOR_ROLE, Contributor.ILLUSTRATOR_ROLE
        ]

        nonprimary_author = [
            Contributor.AUTHOR_ROLE, Contributor.ILLUSTRATOR_ROLE
        ]

        not_an_author = [Contributor.ILLUSTRATOR]

        # No matter what, the PRIMARY_AUTHOR role can only be used once
        # -- it transitions to AUTHOR.
        assert Contributor.AUTHOR_ROLE == m(primary_author, True)
        assert Contributor.AUTHOR_ROLE == m(primary_author, False)

        # If the current contributor was given AUTHOR because AUTHOR
        # is the current default, then AUTHOR remains the default.
        assert Contributor.AUTHOR_ROLE == m(primary_author, True)

        # If the current contributor was given AUTHOR and AUTHOR is
        # *not* the current default, then AUTHOR transitions to
        # UNKNOWN.
        assert Contributor.UNKNOWN_ROLE == m(primary_author, False)

        # Any other role transitions to UNKNOWN.
        assert Contributor.UNKNOWN_ROLE == m(not_an_author, True)
        assert Contributor.UNKNOWN_ROLE == m(not_an_author, False)

    def test_parse_multiple_real_case(self):
        # Test parse_multiple in a real, very complicated situation.
        x = "Barrie, J. M. (James Matthew), 1860-1937 | Unwin, Nora S. 1907-1982 [Illustrator] | Bedford, F. D. [Illustrator] | Zallinger, Jean [Illustrator] | Barrie, J. M. 1860-1937 [Author; Contributor; Creator; Bibliographic antecedent; Author of screenplay; Other] | McKowen, Scott [Illustrator]"
        data = NameParser.parse_multiple(x)

        # There are 6 ContributorData objects. Two of them represent the same
        # person, but the parser doesn't know that.
        barrie, unwin, bedford, zallinger, barrie2, mckowen = data

        assert "Barrie, J. M. (James Matthew)" == barrie.sort_name
        assert "1860" == barrie.extra[Contributor.BIRTH_DATE]
        assert "1937" == barrie.extra[Contributor.DEATH_DATE]
        assert [Contributor.PRIMARY_AUTHOR_ROLE] == barrie.roles

        assert "Unwin, Nora S." == unwin.sort_name
        assert "1907" == unwin.extra[Contributor.BIRTH_DATE]
        assert "1982" == unwin.extra[Contributor.DEATH_DATE]
        assert [Contributor.ILLUSTRATOR_ROLE] == unwin.roles

        assert "Bedford, F. D." == bedford.sort_name
        assert {} == bedford.extra
        assert [Contributor.ILLUSTRATOR_ROLE] == bedford.roles

        assert "Barrie, J. M." == barrie2.sort_name
        assert "1860" == barrie2.extra[Contributor.BIRTH_DATE]
        assert "1937" == barrie2.extra[Contributor.DEATH_DATE]

        # We're converting to a set to compare without respect to
        # order, but since 'Author', 'Creator' and 'Author of
        # Screenplay' all map to Contributor.AUTHOR_ROLE, we also
        # need to make sure AUTHOR_ROLE doesn't show up three times.
        assert (
            set([Contributor.AUTHOR_ROLE, Contributor.CONTRIBUTOR_ROLE,
                 Contributor.UNKNOWN_ROLE]) ==
            set(barrie2.roles)
        )
        assert 1 == barrie2.roles.count(Contributor.AUTHOR_ROLE)

        assert "McKowen, Scott" == mckowen.sort_name
        assert {} == mckowen.extra
        assert [Contributor.ILLUSTRATOR_ROLE] == mckowen.roles

    def test_parse(self):
        # Verify that NameParser.parse handles a single individual.

        default_role = object()

        # If no role is specified, the default role is used.
        contributor, default_role_used = NameParser.parse(
            "Got No Role", default_role
        )
        assert isinstance(contributor, ContributorData)
        assert "Got No Role" == contributor.sort_name
        assert [default_role] == contributor.roles
        assert True == default_role_used

        # If the author has an explicit role, it's used instead of
        # the default role.
        contributor, default_role_used = NameParser.parse(
            "Illustrator, Anne [Illustrator]", default_role
        )
        assert False == default_role_used
        assert [Contributor.ILLUSTRATOR_ROLE] == contributor.roles

        # If the author has an explicit role that can't be mapped
        # to our mapping, UNKNOWN_ROLE is used.
        contributor, default_role_used = NameParser.parse(
            "Urist Borushdumat [Fish Cleaner]", default_role
        )
        assert False == default_role_used
        assert [Contributor.UNKNOWN_ROLE] == contributor.roles
