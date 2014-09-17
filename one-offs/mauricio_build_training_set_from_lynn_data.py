from pdb import set_trace
import sys
import os
import numpy
import csv
from textblob import TextBlob
from collections import defaultdict

class ReviewParser(object):

    def __init__(self, input_file):
        self.input_file = input_file
        self.reviews_by_product_id = defaultdict(list)

    def parse(self, product_id_filter):
        current_review = None
        c = 0
        for line in open(self.input_file):
            line = line.strip()
            if not line:
                if current_review:
                    yield current_review
                    c += 1
                    if not c % 1000:
                        print "%d reviews" % c
                current_review = None
                continue
            if line.startswith("product/productId"):
                key, value = line.split(": ", 1)
                if not product_id_filter or value not in product_id_filter:
                    continue
                current_review = { key : value }
            if current_review:
                key, value = line.split(": ", 1)
                current_review[key] = value
                
class ReviewAnnotator(object):

    STOPWORDS = set(["a's", "able", "about", "above", "according",
                 "accordingly", "across", "actually", "after", "afterwards",
                 "again", "against", "ain't", "all", "allow",
                 "allows", "almost", "alone", "along", "already",
                 "also", "although", "always", "am", "among",
                 "amongst", "an", "and", "another", "any",
                 "anybody", "anyhow", "anyone", "anything", "anyway",
                 "anyways", "anywhere", "apart", "appear", "appreciate",
                 "appropriate", "are", "aren't", "around", "as",
                 "aside", "ask", "asking", "associated", "at",
                 "available", "away", "awfully", "be", "became",
                 "because", "become", "becomes", "becoming", "been",
                 "before", "beforehand", "behind", "being", "believe",
                 "below", "beside", "besides", "best", "better",
                 "between", "beyond", "both", "brief", "but",
                 "by", "c'mon", "c's", "came", "can",
                 "can't", "cannot", "cant", "cause", "causes",
                 "certain", "certainly", "changes", "clearly", "co",
                 "com", "come", "comes", "concerning", "consequently",
                 "consider", "considering", "contain", "containing", "contains",
                 "corresponding", "could", "couldn't", "course", "currently",
                 "definitely", "described", "despite", "did", "didn't",
                 "different", "do", "does", "doesn't", "doing",
                 "don't", "done", "down", "downwards", "during",
                 "each", "edu", "eg", "eight", "either",
                 "else", "elsewhere", "enough", "entirely", "especially",
                 "et", "etc", "even", "ever", "every",
                 "everybody", "everyone", "everything", "everywhere", "ex",
                 "exactly", "example", "except", "far", "few",
                 "fifth", "first", "five", "followed", "following",
                 "follows", "for", "former", "formerly", "forth",
                 "four", "from", "further", "furthermore", "get",
                 "gets", "getting", "given", "gives", "go",
                 "goes", "going", "gone", "got", "gotten",
                 "greetings", "had", "hadn't", "happens", "hardly",
                 "has", "hasn't", "have", "haven't", "having",
                 "he", "he's", "hello", "help", "hence",
                 "her", "here", "here's", "hereafter", "hereby",
                 "herein", "hereupon", "hers", "herself", "hi",
                 "him", "himself", "his", "hither", "hopefully",
                 "how", "howbeit", "however", "i'd", "i'll",
                 "i'm", "i've", "ie", "if", "ignored",
                 "immediate", "in", "inasmuch", "inc", "indeed",
                 "indicate", "indicated", "indicates", "inner", "insofar",
                 "instead", "into", "inward", "is", "isn't",
                 "it", "it'd", "it'll", "it's", "its",
                 "itself", "just", "keep", "keeps", "kept",
                 "know", "known", "knows", "last", "lately",
                 "later", "latter", "latterly", "least", "less",
                 "lest", "let", "let's", "like", "liked",
                 "likely", "little", "look", "looking", "looks",
                 "ltd", "mainly", "many", "may", "maybe",
                 "me", "mean", "meanwhile", "merely", "might",
                 "more", "moreover", "most", "mostly", "much",
                 "must", "my", "myself", "name", "namely",
                 "nd", "near", "nearly", "necessary", "need",
                 "needs", "neither", "never", "nevertheless", "new",
                 "next", "nine", "no", "nobody", "non",
                 "none", "noone", "nor", "normally", "not",
                 "nothing", "novel", "now", "nowhere", "obviously",
                 "of", "off", "often", "oh", "ok",
                 "okay", "old", "on", "once", "one",
                 "ones", "only", "onto", "or", "other",
                 "others", "otherwise", "ought", "our", "ours",
                 "ourselves", "out", "outside", "over", "overall",
                 "own", "particular", "particularly", "per", "perhaps",
                 "placed", "please", "plus", "possible", "presumably",
                 "probably", "provides", "que", "quite", "qv",
                 "rather", "rd", "re", "really", "reasonably",
                 "regarding", "regardless", "regards", "relatively", "respectively",
                 "right", "said", "same", "saw", "say",
                 "saying", "says", "second", "secondly", "see",
                 "seeing", "seem", "seemed", "seeming", "seems",
                 "seen", "self", "selves", "sensible", "sent",
                 "serious", "seriously", "seven", "several", "shall",
                 "she", "should", "shouldn't", "since", "six",
                 "so", "some", "somebody", "somehow", "someone",
                 "something", "sometime", "sometimes", "somewhat", "somewhere",
                 "soon", "sorry", "specified", "specify", "specifying",
                 "still", "sub", "such", "sup", "sure",
                 "t's", "take", "taken", "tell", "tends",
                 "th", "than", "thank", "thanks", "thanx",
                 "that", "that's", "thats", "the", "their",
                 "theirs", "them", "themselves", "then", "thence",
                 "there", "there's", "thereafter", "thereby", "therefore",
                 "therein", "theres", "thereupon", "these", "they",
                 "they'd", "they'll", "they're", "they've", "think",
                 "third", "this", "thorough", "thoroughly", "those",
                 "though", "three", "through", "throughout", "thru",
                 "thus", "to", "together", "too", "took",
                 "toward", "towards", "tried", "tries", "truly",
                 "try", "trying", "twice", "two", "un",
                 "under", "unfortunately", "unless", "unlikely", "until",
                 "unto", "up", "upon", "us", "use",
                 "used", "useful", "uses", "using", "usually",
                 "value", "various", "very", "via", "viz",
                 "vs", "want", "wants", "was", "wasn't",
                 "way", "we", "we'd", "we'll", "we're",
                 "we've", "welcome", "well", "went", "were",
                 "weren't", "what", "what's", "whatever", "when",
                 "whence", "whenever", "where", "where's", "whereafter",
                 "whereas", "whereby", "wherein", "whereupon", "wherever",
                 "whether", "which", "while", "whither", "who",
                 "who's", "whoever", "whole", "whom", "whose",
                 "why", "will", "willing", "wish", "with",
                 "within", "without", "won't", "wonder", "would",
                 "wouldn't", "yes", "yet", "you", "you'd",
                 "you'll", "you're", "you've", "your", "yours",
                 "yourself", "yourselves", "zero"]

                    + ["a", "able", "about", "across", "after", "all", "almost", "also", "am", "among", "an", "and", "any", "are", "as", "at", "be", "because", "been", "but", "by", "can", "cannot", "could", "dear", "did", "do", "does", "either", "else", "ever", "every", "for", "from", "get", "got", "had", "has", "have", "he", "her", "hers", "him", "his", "how", "however", "i", "if", "in", "into", "is", "it", "its", "just", "least", "let", "like", "likely", "may", "me", "might", "most", "must", "my", "neither", "no", "nor", "not", "of", "off", "often", "on", "only", "or", "other", "our", "own", "rather", "said", "say", "says", "she", "should", "since", "so", "some", "than", "that", "the", "their", "them", "then", "there", "these", "they", "this", "tis", "to", "too", "twas", "us", "wants", "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "would", "yet", "you", "your", "ain't", "aren't", "can't", "could've", "couldn't", "didn't", "doesn't", "don't", "hasn't", "he'd", "he'll", "he's", "how'd", "how'll", "how's", "i'd", "i'll", "i'm", "i've", "isn't", "it's", "might've", "mightn't", "must've", "mustn't", "shan't", "she'd", "she'll", "she's", "should've", "shouldn't", "that'll", "that's", "there's", "they'd", "they'll", "they're", "they've", "wasn't", "we'd", "we'll", "we're", "weren't", "what'd", "what's", "when'd", "when'll", "when's", "where'd", "where'll", "where's", "who'd", "who'll", "who's", "why'd", "why'll", "why's", "won't", "would've", "wouldn't", "you'd", "you'll", "you're", "you've", "'s", "n't", "'m", "'d"]


                    + ["read", "reading", "kindle", "amazon", "book", "books"]
                )

    def __init__(self, reviews_path, input_path):
        self.isbn_to_title = dict()
        self.headers = None

        self.line_number_for_isbn = dict()
        self.rows_by_line_number = dict()
        self.rows_by_isbn = dict()
        self.reviews = ReviewParser(reviews_path)
        self.isbns_for_line_number = defaultdict(list)

        self.all_isbns = set()
        self.scores_by_isbn = defaultdict(list)
        self.review_words_by_isbn = defaultdict(list)

        with open(input_path) as csvfile:
            reader = csv.reader(csvfile)
            
            for row in reader:
                if not self.headers:
                    self.headers = row + ["Mean review score", "Median review score", "Review words"]
                    continue
                num_reviews, title, author, quadrant, lanes, fiction, audience, isbns = row
                if quadrant not in ("Story", "Character", "Language", "Setting"):
                    raise "%s has bad quadrant: %r" % (title, quadrant)
                isbns = isbns.split(", ")
                self.isbns_for_line_number[reader.line_num] = isbns
                self.rows_by_line_number[reader.line_num] = row
                for isbn in isbns:
                    self.rows_by_isbn[isbn] = row
                    self.line_number_for_isbn[isbn] = reader.line_num
                    self.all_isbns.add(isbn)

        self.missing_isbns = set(self.all_isbns)

        for review in self.reviews.parse(self.all_isbns):
            if isbn in self.missing_isbns:
                print isbn
                self.missing_isbns.remove(isbn)
            isbn = review['product/productId']
            line_number = self.line_number_for_isbn[isbn]
            self.scores_by_isbn[isbn].append(review['review/score'])
            review_text = review['review/text']
            for word in TextBlob(review_text).words:
                word = word.lower()
                if word not in self.STOPWORDS:
                    self.review_words_by_isbn[isbn].append(word)

    def annotate(self, output_path):
        writer = csv.writer(open(output_path, "w"), quoting=csv.QUOTE_NONNUMERIC)
        #writer.writerow(self.headers)
        for line_number, isbns in self.isbns_for_line_number.items():
            
            scores = []
            words = []
            row = self.rows_by_line_number[line_number]
            for isbn in isbns:
                for score in self.scores_by_isbn[isbn]:
                    scores.append(float(score))
                words.extend(self.review_words_by_isbn[isbn])
            if scores:
                mean_score = numpy.mean(scores)
                median_score = numpy.median(scores) 
            else:
                mean_score = median_score = 0
            row.append(mean_score)
            row.append(median_score)
            row.append(", ".join(words))
            writer.writerow(row)

if __name__ == '__main__':
    data_dir = sys.argv[1]
    reviews_path = os.path.join(data_dir, "SNAP", "Books.txt")
    input_path = os.path.join(data_dir, "Interest Vocabulary", "training set initial.csv")
    output_path = os.path.join(data_dir, "Interest Vocabulary", "training set with reviews.csv")
    annotator = ReviewAnnotator(reviews_path, input_path)
    annotator.annotate(output_path)
    print "Missing ISBNs:"
    print annotator.missing_isbns
