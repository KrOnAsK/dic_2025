#!/usr/bin/env python3
"""
Example: python runner.py reviews_devset.json --stopwords stopwords.txt > output.txt
"""

from mrjob.protocol import RawProtocol, JSONValueProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import heapq
import re
import sys


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

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = RawProtocol

    def configure_args(self):
        super(ChiSquaredJob, self).configure_args()
        self.add_file_arg("--stopwords", help="Path to the stopwords file")

    def mapper_init(self):
        self.stopwords = load_stopwords(self.options.stopwords)

    def mapper(self, _, data):
        """
        Input: {"reviewID": ..., "category": ..., "reviewText": ...}
        Output:
          - ("D", "*", "*") → 1
          - ("C", "*", category) → 1
          - ("T", token, "*") → int
          - ("TC", token, category) → int
        """
        text = data.get("reviewText", "").lower()
        category = data.get("category", "")

        # Split on delimiters
        # Filter out single chars and stopwords
        tokens = set()
        for token in WORD_RE.split(text):
            if token and len(token) > 1 and token not in self.stopwords:
                tokens.add(token)

        yield ("D", "*", "*"), 1
        yield ("C", "*", category), 1
        for token in tokens:
            yield ("T", token, "*"), 1
            yield ("TC", token, category), 1

    def combiner(self, key, values):
        """
        Simple sum.
        """
        yield key, sum(values)

    def reducer(self, key, values):
        """
        Emit extra tokens to reduce load in shuffling.
        """
        yield key, sum(values)

    def chi_mapper_init(self):
        self.num_partitions = 25
        self.N = 0
        self.map_C = {}
        self.map_T = {}
        self.hash_token = lambda token: hash(token) % self.num_partitions

    def chi_mapper(self, key, count):
        """
        Distribute randomly.
        Output:
          - (partition) → ("TC", token, category, int)
        """
        id, token, category = key

        if id == "D":
            self.N = count
        elif id == "C":
            self.map_C[category] = count
        elif id == "T":
            self.map_T[token] = count
        elif id == "TC":
            partition = self.hash_token(token)
            yield partition, ("TC", token, category, count)

    def chi_mapper_final(self):
        """
        Distribute metadata to all that need it.
        """
        if self.N:
            for partition in range(0, self.num_partitions):
                yield partition, ("N", self.N)

        for c, c_val in self.map_C.items():
            for partition in range(0, self.num_partitions):
                yield partition, ("C", c, c_val)

        for t, t_val in self.map_T.items():
            yield self.hash_token(t), ("T", t, t_val)

    def chi_reducer_init(self):
        self.N = 0
        self.map_C = {}
        self.map_T = {}
        self.map_TC = {}

    def chi_reducer(self, _, lines):
        """
        Build dictionaries.
        """
        for value in lines:
            if value[0] == "N":
                self.N = value[1]
            elif value[0] == "C":
                self.map_C[value[1]] = value[2]
            elif value[0] == "T":
                self.map_T[value[1]] = value[2]
            elif value[0] == "TC":
                self.map_TC[(value[1], value[2])] = value[3]
            else:
                raise Exception("unreachable")

    def chi_reducer_final(self):
        """
        Compute chi-squared values.
        """
        for (token, category), A in self.map_TC.items():
            B = self.map_T[token] - A
            C = self.map_C[category] - A
            D = self.N - A - B - C

            numerator = self.N * (A * D - B * C) ** 2
            denominator = (A + B) * (A + C) * (B + D) * (C + D)

            chi2 = numerator / denominator
            yield category, (chi2, token)

    def keep_top_75(self, category, term_chi_pairs):
        """
        Keep top 75 tokens per category.
        """
        top_75 = []
        for chi2, term in term_chi_pairs:
            if len(top_75) < 75:
                heapq.heappush(top_75, (chi2, term))
            else:
                heapq.heappushpop(top_75, (chi2, term))
        yield None, (category, sorted(top_75, reverse=True))

    def finializer(self, _, lines):
        all_tokens = set()
        for category, values in sorted(lines):
            all_tokens.update([t for _, t in values])
            formatted = [f"{term}:{chi2:.4f}" for chi2, term in values]
            yield category, " ".join(formatted)
        yield "MERGED_DICT", " ".join(sorted(all_tokens))

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
                mapper_init=self.chi_mapper_init,
                mapper=self.chi_mapper,
                mapper_final=self.chi_mapper_final,
                reducer_init=self.chi_reducer_init,
                reducer=self.chi_reducer,
                reducer_final=self.chi_reducer_final,
            ),
            MRStep(
                reducer=self.keep_top_75,
            ),
            MRStep(
                reducer=self.finializer,
            ),
        ]


if __name__ == "__main__":
    print("Starting script", file=sys.stderr)
    start = time.time()
    ChiSquaredJob.run()
    end = time.time()
    print(f"Job execution time: {end - start:.2f} seconds", file=sys.stderr)
