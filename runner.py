#!/usr/bin/env python3
"""
Example: python runner.py reviews_devset.json --stopwords stopwords.txt > output.txt
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import time
import heapq
import re
from collections import Counter, defaultdict


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


class ChiSquaredJob(MRJob):

    def configure_args(self):
        super(ChiSquaredJob, self).configure_args()
        self.add_file_arg("--stopwords", help="Path to the stopwords file")

    def mapper_init(self):
        self.stopwords = load_stopwords(self.options.stopwords)

    def mapper(self, _, line):
        """
        Input: {"reviewID": ..., "category": ..., "reviewText": ...}
        Output:
          - ("C_COUNT", category) → 1
          - (category, token) → count
        """
        data = json.loads(line)
        text = data.get("reviewText", "")
        category = data.get("category", "")

        if not text or not category:
            return

        # Convert to lowercase (case folding)
        text = text.lower()

        # Split on delimiters and filter
        tokens = [
            token
            for token in WORD_RE.split(text)
            if token and len(token) > 1 and token not in self.stopwords
        ]

        if len(tokens) == 0:
            return

        yield ("C_COUNT", category), 1
        for token, count in Counter(tokens).items():
            yield (category, token), count

    def combiner(self, key, values):
        """
        Combiner that summarizes local data.
        """
        yield key, sum(values)

    def reducer(self, key, values):
        """
        First reducer to aggregate counts. Send to same reducer in next step by setting key to None
        """
        yield None, (key, sum(values))

    def reducer2_init(self):
        """
        Initialize for streaming chi-squared calculation.
        """
        self.total = 0
        self.category = defaultdict(int)
        self.token = defaultdict(int)
        self.category_token = defaultdict(dict)

    def reducer2(self, _, lines):
        """
        Second reducer that calculates chi-squared in a streaming fashion.
        """
        for key, count in lines:
            if key[0] == "C_COUNT":
                _, category = key
                self.total += count
                self.category[category] = count
            else:
                category, token = key
                self.token[token] += count
                self.category_token[category][token] = count

    def reducer2_final(self):
        """
        Calculate and output chi-squared scores.
        """
        # Process each category
        for category, tokens in sorted(self.category_token.items()):
            # Calculate chi-squared for each token in this category
            scores = []
            for token, A in tokens.items():
                B = self.token[token] - A
                C = self.category[category] - A
                D = self.total - A - B - C

                numerator = self.total * (A * D - B * C) ** 2
                denominator = (A + B) * (A + C) * (B + D) * (C + D)

                if denominator != 0:
                    chi2 = numerator / denominator
                    scores.append((token, chi2))

            # Get top 75 terms
            top_terms = heapq.nlargest(75, scores, key=lambda x: x[1])
            out = " ".join(f"{t}:{v}" for t, v in top_terms)
            yield category, out

        # Yield all tokens
        all_tokens = sorted(self.token.keys())
        yield "MERGED_DICT", " ".join(all_tokens)

    def steps(self):
        """
        Define the steps of the MRJob.
        """
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer,
            ),
            MRStep(
                reducer_init=self.reducer2_init,
                reducer=self.reducer2,
                reducer_final=self.reducer2_final,
            ),
        ]


if __name__ == "__main__":
    start = time.time()
    ChiSquaredJob.run()
    end = time.time()
    print(f"Job execution time: {end - start:.2f} seconds")
