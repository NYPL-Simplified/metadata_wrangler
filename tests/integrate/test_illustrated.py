# encoding: utf-8

import datetime
import pkgutil
import StringIO
from nose.tools import set_trace, eq_ 
from integration.illustrated import (
    GutenbergIllustratedDriver,
)

from tests.db import (
    DatabaseTest,
)

class TestShortDisplayTitle(object):

    def _shorten(self, original, shortened):
        eq_(
            shortened, GutenbergIllustratedDriver.short_display_title(original))

    def _not_shortened(self, original):
        eq_(
            None, GutenbergIllustratedDriver.short_display_title(original))

    def test_shortened(self):
        self._shorten("The Financier: A Novel", "The Financier")
        self._shorten("Bibliomania; or Book-Madness", "Bibliomania")
        self._shorten(
            "Punch, or the London Charivari. Volume 93. August 27, 1887",
            "Punch Volume 93. August 27, 1887",
        )
        self._shorten(
            "Punch, or the London Charivari, Vol. 108, April 27, 1895",
            "Punch, Vol. 108, April 27, 1895",
        )
        self._shorten(
            "The Son of Clemenceau, A Novel of Modern Love and Life",
            "The Son of Clemenceau"
        )
        self._shorten(
            "Oliver Wendell Holmes (from Literary Friends and Acquaintance)",
            "Oliver Wendell Holmes",
        )
        self._shorten(
            "Snow Bound, and other poems",
            "Snow Bound",
        )
        self._shorten(
            "Wonderwings and other Fairy Stories",
            "Wonderwings",
        )
        self._shorten(
            "Modern Painters, Volume 3 (of 5)",
            "Modern Painters, Volume 3",
        )
        self._shorten(
            "The Greville Memoirs (Third Part) Volume II (of II)",
            "The Greville Memoirs (Third Part) Volume II",
        )

    def test_not_shortened(self):
        self._not_shortened("Operation: Outer Space")
        #self._not_shortened("Sorry: Wrong Dimension")
        #self._not_shortened("Object: matrimony")
        self._not_shortened("The Life and Voyages of Christopher Columbus (Volume II)")
        self._not_shortened("Antonio Canova (1757-1822)")
        self._not_shortened(u"Mémoires de madame de Rémusat (2/3)")
