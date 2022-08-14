import re
import mrjob.job as job

class MRWordFrequencyCount(job.MRJob):
    def mapper(self, _, line):
        for word in line.split():
            yield word, 1

    def reducer(self, keys, values): 
        yield keys, sum(values)

if __name__ == "__main__":
    MRWordFrequencyCount().run()
