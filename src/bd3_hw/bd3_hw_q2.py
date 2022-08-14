import mrjob.job as job
import re

class MRWordFrequencyCounter(job.MRJob):
    def mapper(self, keys, line):
        for word in re.compile(r"[\w']+").findall(line):
            yield word, 1

    def reducer(self, keys, values):
        yield keys, sum(values)

if __name__ == "__main__":
    MRWordFrequencyCounter().run()
