import mrjob.job as job
import mrjob.step as step

class MRExtremeVoterCount(job.MRJob):
    content = []
    results = []

    def mapper(self, keys_, line):
        as_lst = line.split(",")
        first_part = ",".join(as_lst[:-1])
        yield first_part, (int)(as_lst[2])

    def reducer(self, keys_, values):
        yield keys_, max(values)

    def mapper_2(self, keys_, line):
        keys_lst = keys_.split(",")
        self.content.append((keys_lst[1], line))
        yield keys_lst[1], line

    def reducer_2(self, keys_, values):
        counter, idx = 1, 0
        vals, times = [], []
        for pair in self.content:
            times.append(pair[0])
            vals.append(pair[1])
            if counter % 6 == 0:
                min_idx = vals.index(min(vals))
                max_idx = vals.index(max(vals))
                city_name = ["Baki", "Gence", "Sumqayit"][idx]
                str_ = f"Min votes -> {vals[min_idx]}  at  {times[min_idx]}, \
                         Max votes -> {vals[max_idx]}  at {times[max_idx]}"
                self.results.append((city_name, str_))
                if len(self.results) == 3:
                    return self.results
                vals, times = [], []
                idx += 1
            counter += 1

    def steps(self):
        return [step.MRStep(mapper=self.mapper,
                   reducer=self.reducer),
                step.MRStep(mapper=self.mapper_2, reducer=self.reducer_2)]


if __name__ == "__main__":
    MRExtremeVoterCount().run()
