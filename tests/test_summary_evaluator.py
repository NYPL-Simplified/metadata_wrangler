"""Test the code that evaluates the quality of summaries."""

from util.summary import SummaryEvaluator
from nose.tools import eq_, set_trace

class TestSummaryEvaluator(object):

    def _best(self, *summaries):
        e = SummaryEvaluator()
        for s in summaries:
            e.add(s)
        e.ready()
        return e.best_choice()[0]

    def test_four_sentences_is_better_than_three(self):
        s1 = "Hey, this is Sentence one. And now, here is Sentence two."
        s2 = "Sentence one. Sentence two. Sentence three. Sentence four."
        eq_(s2, self._best(s1, s2))

    def test_four_sentences_is_better_than_five(self):
        s1 = "Sentence 1. Sentence 2. Sentence 3. Sentence 4. Sentence 5."
        s2 = "Sentence one. Sentence two. Sentence three.  Sentence four."
        eq_(s2, self._best(s1, s2))

    def test_shorter_is_better(self):
        s1 = "A very long sentence."
        s2 = "Tiny sentence."
        eq_(s2, self._best(s1, s2))

    def test_noun_phrase_coverage_is_important(self):

        s1 = "The story of Alice and the White Rabbit."
        s2 = "The story of Alice and the Mock Turtle."
        s3 = "Alice meets the Mock Turtle and the White Rabbit."
        # s3 is longer, and they're all one sentence, but s3 mentions
        # three noun phrases instead of two.
        eq_(s3, self._best(s1, s2, s3))
