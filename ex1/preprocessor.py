#!/usr/bin/env python3
"""
Example: python preprocessor.py reviews_devset.json --stopwords stopwords.txt > preprocessor.out
"""

from mrjob.protocol import RawProtocol, JSONValueProtocol
from mrjob.job import MRJob
import re


def load_stopwords(path: str) -> set[str]:
    """
    Load stopwords from a file efficiently.
    """
    stopwords = set()
    with open(path, "r", encoding="utf-8") as f:
        stopwords = set(line.strip() for line in f if line.strip())
    return stopwords

# Regular expression defining what separates words in the text
# This regex matches spaces, tabs, digits, punctuation, and special characters
WORD_RE = re.compile(
    r"[\s\t\d\(\)\[\]\{\}\.\!\?\,\;\:\+\=\-\_\"\'`\~\#\@\&\*\%\€\$\§\\\/]+"
)


class PreprocessorJob(MRJob):

    # Define input protocol as JSON values
    INPUT_PROTOCOL = JSONValueProtocol
    # Define output protocol as raw key-value pairs
    OUTPUT_PROTOCOL = RawProtocol

    def configure_args(self):
        """
        Configure command-line arguments for the job.
        Adds a required --stopwords argument to specify the stopwords file.
        """
        super(PreprocessorJob, self).configure_args()
        self.add_file_arg("--stopwords", help="Path to the stopwords file")

    def mapper_init(self):
        """
        Initialize the mapper by loading the stopwords file.
        This is called once before processing begins.
        """
        self.stopwords = load_stopwords(self.options.stopwords)

    def mapper(self, _, data):
        """
        Process each review document and emit key-value pairs.
        
        For each review, this function:
        1. Extracts the review text and category
        2. Tokenizes the review text (splits into words)
        3. Filters out stopwords and single-character tokens
        4. Emits counts for:
           - Category occurrences
           - Token occurrences
           - Token-category co-occurrences
        
        Args:
            _: Unused key parameter (mrjob convention)
            data: JSON object containing review data
            
        Yields:
            Key-value pairs for counting categories, tokens, and token-category pairs
            - "C.*.category" → 1  (Category counter)
            - "T.token.*" → 1     (Token counter)
            - "TC.token.category" → 1  (Token-category co-occurrence counter)
        """
        # Extract the review text and convert to lowercase
        text = data.get("reviewText", "").lower()
        # Extract the category of the review
        category = data.get("category", "")

        # Split the text into tokens using the regular expression
        # Filter out single characters and stopwords
        tokens = set()
        for token in WORD_RE.split(text):
            if token and len(token) > 1 and token not in self.stopwords:
                tokens.add(token)

        # Emit a count for this category
        yield f"C.*.{category}", 1
        
        # For each token, emit counts for:
        # 1. The token itself (regardless of category)
        # 2. The token-category pair (co-occurrence)
        for token in tokens:
            yield f"T.{token}.*", 1
            yield f"TC.{token}.{category}", 1


    def combiner(self, key, values):
        """
        Combine values with the same key at the map stage.
        
        This is an optimization that reduces the amount of data
        transferred between the map and reduce stages by summing
        values locally.
        
        Args:
            key: The key being processed
            values: List of values for this key
            
        Yields:
            The key and the sum of its values
        """
        yield key, sum(values)

    def reducer(self, key, values):
        """
        Emit as raw values.
        Output:
          - "C.*.category" → count
          - "T.token.*" → count
          - "TC.token.category" → count
        """
        yield key, f"{sum(values)}"


if __name__ == "__main__":
    PreprocessorJob.run()
