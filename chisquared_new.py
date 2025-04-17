

from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import timeit

class ChiSquaredJob1(MRJob):
    def mapper(self, _, line):
        """
        Input: {"reviewID": ..., "category": ..., "tokens": [...]}
        Output:
          - ("TOTAL_DOCS", None) → 1
          - ("CATEGORY_DOC_COUNT", category) → 1
          - ("TERM_IN_CAT", (term, category)) → 1
          - ("TERM_GLOBAL", term) → 1
        """
        data = json.loads(line)
        category = data["category"]
        tokens = set(data["tokens"])  #we want to meassure pressence not frequency

        #total documents
        yield ("TOTAL_DOCS", None), 1

        #documents per category 
        yield("CATEGORY_DOC_COUNT", category), 1

        #terms per category
        for token in tokens:
            yield ("TERM_IN_CAT", (token, category)), 1

        #terms across all categories 
        for token in tokens:
            yield ("TERM_GLOBAL", token), 1

    def combiner(self, key, values):
        """
        Combiner to sum up the values for each key.
        """
        yield key, sum(values)

    def reducer(self, key, values):
        """
        Reducer to sum up the values for each key.
        """

        yield key, sum(values)

    def combiner2(self, key, values):
        yield None, (key, sum(values))

    def reducer2(self, _, lines):
        #https://stackoverflow.com/questions/15051137/mrjob-can-a-reducer-perform-2-operations
        token_total = dict()
        cat_total = dict()
        docs_total = 0
        token_cat_total = dict()
        token_cat_chi2 = dict()
        
        for key, val in lines:
            kind, content = key
            if kind == "TERM_IN_CAT":
                token, category = content
                token_cat_total[(token, category)] = val
            elif kind == "CATEGORY_DOC_COUNT":
                cat_total[content] = val
            elif kind == "TOTAL_DOCS":
                docs_total = val
            elif kind == "TERM_GLOBAL":
                    token_total[content] = val

        for token, category in token_cat_total:
            A = token_cat_total[(token, category)]
            B = token_total[token] - A
            C = cat_total[category] - A
            D = docs_total - A - B - C
            numerator = (A * D - B * C) ** 2 * docs_total
            denominator = (A + B) * (C + D) * (A + C) * (B + D)
            chi2 = numerator / denominator if denominator != 0 else 0
            token_cat_chi2[(token, category)] = chi2
            yield (token, category), chi2
                
    def steps(self):
        """
        Define the steps of the MRJob.
        """
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            ),
            MRStep(
                combiner=self.combiner2,
                reducer=self.reducer2
            )
        ]
        
if __name__ == '__main__':
    start = timeit.timeit()
    ChiSquaredJob1.run()
    end = timeit.timeit()
    print(end - start)



