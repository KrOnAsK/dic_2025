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


WORD_RE = re.compile(
    r"[\s\t\d\(\)\[\]\{\}\.\!\?\,\;\:\+\=\-\_\"\'`\~\#\@\&\*\%\€\$\§\\\/]+"
)


class PreprocessorJob(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = RawProtocol

    def configure_args(self):
        super(PreprocessorJob, self).configure_args()
        self.add_file_arg("--stopwords", help="Path to the stopwords file")

    def mapper_init(self):
        self.stopwords = load_stopwords(self.options.stopwords)

    def mapper(self, _, data):
        """
        Input: {"reviewID": ..., "category": ..., "reviewText": ...}
        Output:
          - "C.*.category" → 1
          - "T.token.*" → 1
          - "TC.token.category" → 1
        """
        text = data.get("reviewText", "").lower()
        category = data.get("category", "")

        # Split on delimiters
        # Filter out single chars and stopwords
        tokens = set()
        for token in WORD_RE.split(text):
            if token and len(token) > 1 and token not in self.stopwords:
                tokens.add(token)

        yield f"C.*.{category}", 1
        for token in tokens:
            yield f"T.{token}.*", 1
            yield f"TC.{token}.{category}", 1

    def combiner(self, key, values):
        """
        Simple sum.
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
