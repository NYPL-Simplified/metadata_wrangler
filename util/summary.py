from textblob import TextBlob
from collections import Counter
from nose.tools import set_trace

# Strings indicative of descriptions we can't use.
"version of"
"Retelling of"
"Abridged"
"retelling"
"adaptation of"
"Look for"
"new edition"
"excerpts" "version", "edition" "complete texts" "in one volume"
"contains"
"Includes"
"Excerpts"
"Selections"
"This is"
"--Container"
"--Original container"
"Here"
"..."
"PLAYAWAY"
"complete novels"
"the .* Collection"


class SummaryEvaluator(object):

    """Evaluate summaries of a book to find a usable summary.

    A usable summary will have good coverage of the popular noun
    phrases found across all summaries of the book, will have an
    approximate length of four sentences (this is customizable), and
    will not mention words that indicate it's a summary of a specific
    edition of the book.

    All else being equal, a shorter summary is better.
    """
    def __init__(self, optimal_number_of_sentences=4,
                 noun_phrases_to_consider=10):
        self.optimal_number_of_sentences=optimal_number_of_sentences
        self.summaries = []
        self.noun_phrases = Counter()
        self.blobs = dict()
        self.scores = dict()
        self.noun_phrases_to_consider = float(noun_phrases_to_consider)

    def add(self, summary):
        if summary in self.blobs:
            # We already evaluated this summary. Don't count it more than once
            return
        blob = TextBlob(summary)
        self.blobs[summary] = blob
        self.summaries.append(summary)
        for phrase in blob.noun_phrases:
            self.noun_phrases[phrase] = self.noun_phrases[phrase] + 1

    def ready(self):
        """We are done adding to the corpus and ready to start evaluating."""
        self.top_noun_phrases = set([
            k for k, v in self.noun_phrases.most_common(
                int(self.noun_phrases_to_consider))])

    def best_choice(self):
        c = self.best_choices(1)
        if c:
            return c[0]
        else:
            return None, None

    def best_choices(self, n=3):
        """Choose the best `n` choices among the current summaries."""
        scores = Counter()
        for summary in self.summaries:
            scores[summary] = self.score(summary)
        return scores.most_common(n)

    def score(self, summary):
        """Score a summary relative to our current view of the dataset."""
        if summary in self.scores:
            return self.scores[summary]
        score = 1
        blob = self.blobs[summary]

        top_noun_phrases_used = len(
            [p for p in self.top_noun_phrases if p in blob.noun_phrases])
        score = 1 * (top_noun_phrases_used/self.noun_phrases_to_consider)

        try:
            sentences = len(blob.sentences)
        except Exception, e:
            # Can't parse into sentences for whatever reason.
            # Make a really bad guess.
            sentences = summary.count(". ") + 1
        off_from_optimal = abs(sentences-self.optimal_number_of_sentences)
        if off_from_optimal == 1:
            off_from_optimal = 1.5
        if off_from_optimal:
            # This summary is too long or too short.
            score /= (off_from_optimal ** 1.5)

        return score
