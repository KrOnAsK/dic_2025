#!/usr/bin/env python3
"""
Example: python chisquared.py preprocessor.out --n int --category_counts path --token_counts path > output.txt
"""

from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq


class ChiSquaredJob(MRJob):

    INPUT_PROTOCOL = RawProtocol
    OUTPUT_PROTOCOL = RawProtocol

    def configure_args(self):
        super(ChiSquaredJob, self).configure_args()
        """
        Configure allowed argument parameters.
        """
        self.add_passthru_arg("--n", type=int, help="Total number of documents")
        self.add_file_arg("--category_counts", help="Path to the category_counts file")
        self.add_file_arg("--token_counts", help="Path to the token_counts file")

    def mapper_init(self):
        """
        Initialization to access data from preprocessor.
        """
        self.N = self.options.n

        self.map_C = {}
        with open(self.options.category_counts, "r", encoding="utf-8") as f:
            for line in f.readlines():
                key, value = line.split("\t")
                self.map_C[key] = int(value)

        self.map_T = {}
        with open(self.options.token_counts, "r", encoding="utf-8") as f:
            for line in f.readlines():
                key, value = line.split("\t")
                self.map_T[key] = int(value)

    def mapper(self, key, value):
        """
        Compute chi-squared values.
        """
        id, token, category = key.split(".")
        A = int(value)
        B = self.map_T[token] - A
        C = self.map_C[category] - A
        D = self.N - A - B - C

        numerator = self.N * (A * D - B * C) ** 2
        denominator = (A + B) * (A + C) * (B + D) * (C + D)

        chi2 = numerator / denominator
        yield category, (chi2, token)

    def reducer(self, category, term_chi_pairs):
        """
        Keep top 75 tokens per category.
        """
        top_75 = heapq.nlargest(75, term_chi_pairs)
        yield category, " ".join(
            [f"{token}:{chi2:.4f}" for chi2, token in sorted(top_75, reverse=True)]
        )


if __name__ == "__main__":
    ChiSquaredJob.run()
