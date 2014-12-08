# encoding: utf-8

import datetime
import pkgutil
import StringIO
from nose.tools import set_trace, eq_ 
from integration.illustrated import (
    GutenbergIllustratedDataProvider,
)

from core.testing import (
    DatabaseTest,
)

class TestIsUsableImageName(object):

    def test_image_names(self):
        usable = GutenbergIllustratedDataProvider.is_usable_image_name
        eq_(True, usable("foo.jpg"))
        eq_(True, usable("foo.png"))
        eq_(True, usable("foo.gif"))
        eq_(True, usable("foo.jpeg"))

        eq_(False, usable("Thumbs.db"))
        eq_(False, usable("foo.txt"))
        eq_(False, usable("01th.jpg"))
        eq_(False, usable("01tn.jpg"))
        eq_(False, usable("01thumb.jpg"))
        eq_(False, usable("cover.jpg"))

class TestShortDisplayTitle(object):

    def _shorten(self, original, shortened):
        eq_(
            shortened, GutenbergIllustratedDataProvider.short_display_title(original))

    def _not_shortened(self, original):
        eq_(
            None, GutenbergIllustratedDataProvider.short_display_title(original))

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

class TestAuthorString(object):

    def _author(self, original, expect):
        eq_(
            expect, GutenbergIllustratedDataProvider.author_string(original))

    def test_author_string(self):

        self._author(["Various"], "")

        self._author(
            ["Mark Twain"], "Mark Twain"
        )

        self._author(
            ["George Blacker Morgan", "William Parker Monteagle"],
            "George Blacker Morgan & William Parker Monteagle"
        )

        self._author(
            ['C. Th. Scharten', 'Margot Vos', 'Wies Moens', 'Willem Kloos',
             'P. C. Boutens'],
            "C. Th. Scharten, Margot Vos, Wies Moens, Willem Kloos & P. C. Boutens"
        )

        self._author(
            ['Scharten', 'Vos', 'Moens', 'Kloos', 'Boutens'],
            'Scharten, Vos, Moens, Kloos & Boutens'
        )

    def test_images_gathered(self):
        data = """./2/8/8/6/28862/28862-h/images:
icover.jpg
ipublogo.jpg
"""
        data = data.split("\n")
        illustrations = list(
            GutenbergIllustratedDataProvider.illustrations_from_file_list(data))
        eq_(
            [('28862', ['./2/8/8/6/28862/28862-h/images/ipublogo.jpg'])],
            illustrations)

    def test_images_gathered_multiple(self):
        data = """./1/7/0/2/17022/17022-h/0501051h-images:
fda-01.jpg

./1/7/0/6/17068/17068-h:
17068-h.htm
images

./1/7/0/6/17068/17068-h/images:
01.png
02.png"""
        data = data.split("\n")
        illustrations = list(
            GutenbergIllustratedDataProvider.illustrations_from_file_list(data))
        eq_(
            [
                ('17022', ['./1/7/0/2/17022/17022-h/0501051h-images/fda-01.jpg']),
                ('17068', ['./1/7/0/6/17068/17068-h/images/01.png', 
                           './1/7/0/6/17068/17068-h/images/02.png'])],
            illustrations
        )

    def test_page_images_ignored(self):
        data = """./2/8/8/6/28863/28863-page-images:
f0001.png
f0002-blank.png
p0716.png
p0717.png
p0718.png
p0719.png
p0720.png
p0721.png
p0722.png
p0723.png
p0724.png
"""
        data = data.split("\n")
        illustrations = GutenbergIllustratedDataProvider.illustrations_from_file_list(
            data)
        eq_([], list(illustrations))
