from ..appeal import FeatureCounter
from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)

class TestFeatureCounter(object):

    def test_basic(self):
        counter = FeatureCounter(["a", "b", "c"])
        counter.add_counts("a b c d b a a a ab ac addd")
        eq_([4, 2, 1], counter.row())

    def test_multiword(self):
        counter = FeatureCounter(["world", "superb world", "awful world"])
        eq_(["world", ("superb", "world"), ("awful", "world")],
            counter.features)
        counter.add_counts("the world is a superb world, a superb world indeed.")
        counter.add_counts("An Awful World, But What A World!")
        eq_([5, 2, 1], counter.row())


    def test_multiword_limited_to_two_words(self):
        assert_raises_regexp(
            ValueError,
            "'has', 'three', 'words'] has more than two words",
            FeatureCounter, ["this one", "has three words"]
        )
