

from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import time
import heapq


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
        Reducer to sum up the values for each key. In order to be able to process them in a 
        single reducer step in reducer 2, we merge the results into one key using None.
        #https://stackoverflow.com/questions/15051137/mrjob-can-a-reducer-perform-2-operations
        #https://mrjob.readthedocs.io/en/latest/guides/writing-mrjobs.html
        """
        

        yield None, (key, sum(values))

    def reducer2(self, _, lines):
        """
        Input: null [[<"TOTAL_DOCS"/"CATEGORY_DOC_COUNT"/"TERM_GLOBAL">, "bogeyman"], 2]
        Input: null [[<"TERM_IN_CAT">, ["checker", "Toys_and_Game"]], 1]
        Output:
          - ("TOTAL_DOCS", None) → 1
          - ("CATEGORY_DOC_COUNT", category) → 1
          - ("TERM_IN_CAT", (term, category)) → 1
          - ("TERM_GLOBAL", term) → 1
        """        
        token_total = dict()
        cat_total = dict()
        docs_total = 0
        token_cat_total = dict()
        token_cat_chi2 = dict()
        all_tokens = set()
        
        #We loop through the previously summed up values (total tokens, tokens per category, ...) and read them into respective dictionaries
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

        #loop through the dictionary containing the information of each token in each category and calculate chi-square value.
        for token, category in token_cat_total:
            all_tokens.add(token)
            A = token_cat_total[(token, category)]
            B = token_total[token] - A
            C = cat_total[category] - A
            D = docs_total - A - B - C
            numerator = (A * D - B * C) ** 2 * docs_total
            denominator = (A + B) * (C + D) * (A + C) * (B + D)
            chi2 = numerator / denominator if denominator != 0 else 0
            if category not in token_cat_chi2:
                token_cat_chi2[category] = dict()
            token_cat_chi2[category][token] = chi2
            #yield (token, category), chi2

        #loop through all categories, fetch top 75 tokens according to the chi-square value and create a printline, which is then yielded as a result.
        for category in token_cat_chi2:
            top_terms = heapq.nlargest(75, token_cat_chi2[category].items(), key=lambda x: x[1])
            printline = category + " " + " ".join(f"{token}:{chi2}" for token, chi2 in top_terms)
            yield None, printline

        #add all tokens sorted
        yield None, " ".join(f"{token}" for token in sorted(all_tokens))
                
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
            #MRStep(
            #    reducer=self.reducer2
            #)
        ]
        
if __name__ == '__main__':
    start = time.time()
    ChiSquaredJob1.run()
    end = time.time()
    print(end - start)



