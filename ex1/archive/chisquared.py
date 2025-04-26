

from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re

class ChiSquaredJob1(MRJob):
    def mapper(self, _, line):
        """
        Input: {"reviewID": ..., "category": ..., "tokens": [...]}
        Output:
          - ("TOTAL_DOCS", None) → 1
          - ("CATEGORY_DOC_COUNT", category) → 1
          - ("TERM_IN_CATEGORY", (term, category)) → 1
          - ("TERM_GLOBAL", term) → 1
        """
        data = json.loads(line)
        category = data["category"]
        tokens = set(data["tokens"])  #we want to meassure pressence not frequency

        #total documents
        yield ("TOTAL_DOCS", None), 1

        #documents per category 
        yield("CATEGORY_DOC_COUNT", category), 1

        #documents with (term, category)
        for token in tokens:
            yield ("TerminCategory", (token, category)), 1

        #count term across all categories 
        for token in tokens:
            yield ("TERM_GLOBAL", token), 1

    def combiner(self,key,values):
                """
                Combiner to sum up the values for each key.
                """
                yield key, sum(values)

    def reducer(self, key, values):
                """
                Reducer to sum up the values for each key.
                """
                yield key, sum(values)  

    def steps(self):
        """
        Define the steps of the MRJob.
        """
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            )
        ]
        
if __name__ == '__main__':
    ChiSquaredJob1.run()




