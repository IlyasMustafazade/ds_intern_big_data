import mrjob.job as job
import mrjob.step as step

class MRMaxTemp(job.MRJob):
    dates = []
    temps = []
    print_count = []
    def mapper(self, keys_, text):
        tab_sep = text.split()
        comma_sep = tab_sep[0].split(",")
        self.dates.append(comma_sep[1])
        self.temps.append(float(tab_sep[1]))
        yield comma_sep[1], float(tab_sep[1])

    def reducer(self, keys_, values):
        max_idx = self.temps.index(max(self.temps))
        self.print_count.append(0)
        if len(self.print_count) <= 1:
            yield "Date with highest temperature -> ", f"{self.dates[max_idx]}, {self.temps[max_idx]} degrees"

    def steps(self):
        return [step.MRStep(mapper=self.mapper, reducer=self.reducer)]

if __name__ == "__main__":
    MRMaxTemp().run()
