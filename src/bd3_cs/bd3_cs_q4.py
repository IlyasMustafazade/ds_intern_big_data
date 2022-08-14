import mrjob.job as job
import numpy as np

class MRAverager(job.MRJob):

    def mapper(self, keys, line):
        comma_sep = line.split(",")
        tab_sep = line.split()
        city = comma_sep[1][:-3]
        n_people = int(tab_sep[1])
        yield city, (n_people, 1)

    def reducer(self, keys, values):
        sum_n_people = 0
        sum_count = 0
        for n_people, count in values:
            sum_n_people += n_people
            sum_count += count
        yield keys, sum_n_people / sum_count


if __name__ == "__main__":
    print("Question 4 -> \n")
    MRAverager().run()

