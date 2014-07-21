from textblob import TextBlob
from collections import Counter


class SummaryEvaluator(object):

    """Evaluate summaries of a book to find a usable summary.

    A usable summary will have good coverage of the popular noun
    phrases found across all summaries of the book, will have an
    approximate length of four sentences (this is customizable), and
    will not mention words that indicate it's a summary of a specific
    edition of the book.

    All else being equal, a shorter summary is better.
    """
    def __init__(self, optimal_number_of_sentences=4):
        self.optimal_number_of_sentences=optimal_number_of_sentences
        self.summaries = []
        self.noun_phrases = Counter()
        self.blobs = {}

    def add_summary(self, summary):
        blob = TextBlob(summary)
        self.blobs[summary] = blob
        for phrase in blob.noun_phrases:
            self.counts[phrase] = self.counts[phrase] + 1

    def choose(self, top_choices=3):
        """Choose the best `top_choices` of the current summaries."""
        scores = Counter()
        for summary in self.summaries:
            scores[summary] = self.score(summary)
        return scores.most_common(top_choices)

    def score(self, summary):
        """Score a summary relative to our current view of the dataset."""
        score = 0.0
        blob = blobs[summary]
        for phrase in blob.noun_phrases:
            # A summary gets points for using noun phrases common
            # to all summaries.
            if counts[phrase] > 1:
                score += counts[phrase]

        sentences = len(blob.sentences)
        off_from_optimal = abs(sentences-self.optimal_number_of_sentences)
        if off_from_optimal:
            # This summary is too long or too short.
            score /= (off_from_optimal * 0.8)

        # All else being equal, shorter summaries are better.
        score = score / len(summary)

        return score
