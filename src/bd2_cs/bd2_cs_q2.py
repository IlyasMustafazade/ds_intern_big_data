import mrjob.job as job
import mrjob.step as step

class MRWordFrequencyCount(job.MRJob):
    def mapper(self, keys, line):
        as_lst = line.split(",")
        yield as_lst[1], 1

    def reducer(self, keys, values): 
        yield keys, sum(values)

if __name__ == "__main__":
    MRWordFrequencyCount().run()

