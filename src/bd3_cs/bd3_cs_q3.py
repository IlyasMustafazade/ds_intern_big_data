from mrjob.job import MRJob
import mrjob.step as step

class MRExtreme(MRJob):
    def mapper(self, keys_, line):
        comma_sep = line.split(",")
        gender = comma_sep[1]
        gender = gender[0]
        space_sep = line.split()
        n_project = space_sep[1]
        yield gender, int(n_project)

class MRMax(MRExtreme):
    def reducer(self, keys_, values):
        yield keys_, max(values)

class MRMin(MRExtreme):
    def reducer(self, keys_, values):
        yield keys_, min(values)

if __name__ == "__main__":
    print("Question 3 -> \n")
    MRMax().run()
    MRMin().run()
