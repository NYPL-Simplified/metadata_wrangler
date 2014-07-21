from textblob import TextBlob
from collections import Counter

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

    def add(self, summary):
        blob = TextBlob(summary)
        self.blobs[summary] = blob
        self.summaries.append(summary)
        for phrase in blob.noun_phrases:
            self.noun_phrases[phrase] = self.noun_phrases[phrase] + 1

    def best_choices(self, n=3):
        """Choose the best `n` choices among the current summaries."""
        scores = Counter()
        for summary in self.summaries:
            scores[summary] = self.score(summary)
        return scores.most_common(n)

    def score(self, summary):
        """Score a summary relative to our current view of the dataset."""
        score = 0.0
        blob = self.blobs[summary]

        scaled_noun_phrases = Counter()
        total_occurances = float(sum(x for x in self.noun_phrases.values()
                                     if x > 1))
        if total_occurances > 0:
            for k, v in self.noun_phrases.items():
                scaled_noun_phrases[k] = v/total_occurances
        else:
            scaled_noun_phrases = self.noun_phrases

        unique_noun_phrases = 0
        for phrase in blob.noun_phrases:
            # A summary gets points for using noun phrases common
            # to all summaries. A summary loses points for unique noun
            # phrases.
            if phrase in scaled_noun_phrases:
                score += scaled_noun_phrases[phrase]
            else:
                unique_noun_phrases += 1
            
        try:
            sentences = len(blob.sentences)
        except Exception, e:
            # Can't parse into sentences for whatever reason.
            # Make a really bad guess.
            sentences = summary.count(". ") + 1
        off_from_optimal = abs(sentences-self.optimal_number_of_sentences)
        if off_from_optimal:
            # This summary is too long or too short.
            score /= (off_from_optimal ** 1.5)

        # All else being equal, shorter summaries are better.
        score = score / (len(summary) * 0.75)

        if unique_noun_phrases:
            score = score * (0.8 ** unique_noun_phrases)

        return score
