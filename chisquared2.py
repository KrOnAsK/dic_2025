from mrjob.job import MRJob
from mrjob.step import MRStep
import json


class chisquared2(MRJob):
    def mapper(self, _, line):
        parts = line.strip().split("\t", 1)
        if len(parts) != 2:
            return  # skip bad line

        try:
            key = json.loads(parts[0])
            value = json.loads(parts[1])
        except json.JSONDecodeError as e:
            return


        if key[0] == "TOTAL_DOCS":
            yield (None, None), ("TOTAL_DOCS", value)
            
        elif key[0] == "CATEGORY_DOC_COUNT":
            category = key[1]
            yield (None, category), ("CATEGORY_TOTAL", value)   

        elif key[0] == "TERM_GLOBAL":
            term = key[1]
            yield (term, None), ("TERM_TOTAL", value)   

        elif key[0] == "TERM_IN_CATEGORY":
            term, category = key[1]
            yield (term, category), ("A", value)


    def reducer(self, key, values):
        """
        Input: key = (term, category)
        Collect: A, term total, category total, total_docs
        """ 
        A = None
        term_total = None
        cat_total = None
        total_docs = None
        for kind, val in values:
            if kind == "A":
                A = val
            elif kind == "TERM_TOTAL":
                term_total = val
            elif kind == "CATEGORY_TOTAL":
                cat_total = val
            elif kind == "TOTAL_DOCS":
                total_docs = val
        #now we compute the chi squared value but only if we have a valid key value pair 
        if key[0] is not None and key[1] is not None:
            term, category = key 
            if None not in (A,term_total, cat_total, total_docs):
                B = cat_total - A
                C = term_total - A
                D = total_docs - A - B - C
                numerator = (A * D - B * C) ** 2 * total_docs
                denominator = (A + B) * (C + D) * (A + C) * (B + D)
                chi2 = numerator / denominator if denominator != 0 else 0
                yield category, (term, round(chi2, 5))
            
    def steps(self):
        return [MRStep(mapper=self.mapper,
                    reducer = self.reducer)]
        
if __name__ == "__main__":
    chisquared2.run()    

        
        