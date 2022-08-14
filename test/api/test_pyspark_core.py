import pyspark as ps
import re

def main():
    tester = SparkTester()

    conf = tester.test_conf()
    tester.test_context(conf=conf)

    tester.stop_sc()
    tester.close_file()


class SparkTester:
    def __init__(self):
        outf_name = "test_pyspark_core"
        self.outf = open(f"{outf_name}.txt", "w")
        self.sc = None

    def test_conf(self):
        print(f"test_conf output -->\n", file=self.outf)
        conf = ps.conf.SparkConf()
        master = "local"
        app_name = "Exploring pyspark"
        conf.setMaster(master).setAppName(app_name)
        conf_as_str = conf.toDebugString()
        print(f"Configuration string:\n{conf_as_str}\n", file=self.outf)
        return conf

    def test_context(self, conf=None):
        print(f"test_context output -->\n", file=self.outf)
        sc = ps.context.SparkContext(conf=conf)
        print(f"Type of SparkContext object: {type(sc)}\n", file=self.outf)
        self.sc = sc
    
    def test_session(self):
        print(f"test_session output -->\n", file=self.outf)
        ss = ps.sql.SparkSession(self.sc)
        print(f"Type of SparkSession object: {type(ss)}\n", file=self.outf)
    
    def test_textFile(self):
        print(f"test_textFile output -->\n", file=self.outf)
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\training.txt"
        rdd = self.sc.textFile(inf_path)
        print(f"Type of RDD object: {type(rdd)}\n", file=self.outf)
        return rdd

    def test_collect(self, rdd=None):     
        print(f"test_collect output -->\n", file=self.outf)
        lst = rdd.collect()
        print(f"RDD object as list:\n{lst}\n", file=self.outf)
        return lst

    def test_parallelize(self, lst=None):
        print(f"test_parallelize output -->\n", file=self.outf)
        rdd = self.sc.parallelize(lst)
        print(f"RDD object as list after parallelizing:\n{rdd.collect()}\n", file=self.outf)

    def test_map(self, rdd=None):
        print(f"test_map output -->\n", file=self.outf)
        rdd_sq = rdd.map(lambda s: [float(i) for i in s.split(",")])
        rdd_sq = rdd_sq.map(lambda lst: [i * i for i in lst])
        lst_sq = rdd_sq.collect()
        print(f"Type of RDD after squaring: {type(rdd_sq)}\n", file=self.outf)
        print(f"Training list after squaring:\n{lst_sq}\n", file=self.outf)
    
    def test_filter(self):
        print(f"test_filter output -->\n", file=self.outf)
        lst = [i for i in range(10)]
        rdd = self.sc.parallelize(lst)
        filtered_rdd = rdd.filter(lambda i: i > 2)
        filtered_lst = filtered_rdd.collect()
        print(f"Filtered list:\n{filtered_lst}\n", file=self.outf)

    def test_distinct_lines(self):
        print(f"test_distinct_lines output -->\n", file=self.outf)
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\ilyas.txt"
        rdd = self.sc.textFile(inf_path)
        distinct_rdd = rdd.distinct()
        filtered_rdd = distinct_rdd.filter(lambda s: (not s.isspace()) and len(s) > 1)
        distinct_lst = filtered_rdd.collect()
        print(f"Unique Lines:\n{distinct_lst}\n", file=self.outf)
    
    def test_union(self):
        print(f"test_union output -->\n", file=self.outf)
        rdd1 = self.sc.parallelize(["apple", "pear", "bread"])
        rdd2 = self.sc.parallelize(["cheese", "butter", "bread"])
        union_rdd = rdd1.union(rdd2)
        union_rdd = union_rdd.distinct()
        union_lst = union_rdd.collect()
        print(f"Union:\n{union_lst}\n", file=self.outf)
        return rdd1, rdd2
    
    def test_intersection(self, rdd1=None, rdd2=None):
        print(f"test_intersection output -->\n", file=self.outf)
        intersection_rdd = rdd1.intersection(rdd2)
        intersection_lst = intersection_rdd.collect()
        print(f"Intersection:\n{intersection_lst}\n", file=self.outf)

    def test_subtraction(self, rdd1=None, rdd2=None):
        print(f"test_subtraction output -->\n", file=self.outf)
        subtraction_rdd = rdd1.subtract(rdd2)
        subtraction_lst = subtraction_rdd.collect()
        print(f"Subtraction:\n{subtraction_lst}\n", file=self.outf)  
    
    def test_cartesian(self, rdd1=None, rdd2=None):
        print(f"test_cartesian output -->\n", file=self.outf)
        cartesian_rdd = rdd1.cartesian(rdd2)
        cartesian_lst = cartesian_rdd.collect()
        print(f"Cartesian Product:\n{cartesian_lst}\n", file=self.outf)
    
    def test_reduceByKey(self):
        print(f"test_reduceByKey output -->\n", file=self.outf)
        lst = [("a", 2), ("a", 3), ("b", 4), ("c", 9), ("b", 1)]
        rdd = self.sc.parallelize(lst)
        reduced_rdd = rdd.reduceByKey(lambda x, y: x + y)
        reduced_lst = reduced_rdd.collect()
        print(f"Reduced:\n{reduced_lst}\n", file=self.outf)
        return rdd

    def test_actions(self, rdd=None):
        print(f"test_actions output -->\n", file=self.outf)
        print(f"First:\n{rdd.first()}\n", file=self.outf)
        print(f"Top 3:\n{rdd.top(3)}\n", file=self.outf)
        print(f"Take 3:\n{rdd.take(3)}\n", file=self.outf)
    
    def test_reduce(self):
        print(f"test_reduce output -->\n", file=self.outf)
        lst = [1, 2, 3, 4, 5]
        rdd = self.sc.parallelize(lst)
        reduced_rdd = rdd.reduce(lambda x, y: x + y)
        print(f"Original list:\n{lst}\n", file=self.outf)
        print(f"Reduced by add: {reduced_rdd}\n", file=self.outf)

    def test_groupByKey(self, rdd=None):
        print(f"test_groupByKey output -->\n", file=self.outf)
        grouped_rdd = rdd.groupByKey()
        grouped_rdd = grouped_rdd.mapValues(list)
        grouped_lst = grouped_rdd.collect()
        print(f"Grouped list: {grouped_lst}\n", file=self.outf)

    def test_join(self, rdd=None):
        print(f"test_join output -->\n", file=self.outf)
        lst2 = [("a", 23), ("c", 12), ("w", 34), ("r", 21), ("a", 35), ("b", 3)]
        rdd2 = self.sc.parallelize(lst2)
        first_join_second = rdd.join(rdd2)
        second_join_first = rdd2.join(rdd)
        print(f"First join second:\n{first_join_second.collect()}\n", file=self.outf)
        print(f"Second join first:\n{second_join_first.collect()}\n", file=self.outf)

    def test_countByKey(self):
        print(f"test_countByKey output -->\n", file=self.outf)
        lst = [("a", 2), ("b", 5), ("a", 3), ("b", 4), ("c", 13)]
        rdd = self.sc.parallelize(lst)
        counted_dict = rdd.countByKey()
        print(f"Original list:\n{lst}\n", file=self.outf)
        print(f"Result after countByKey:\n{counted_dict}\n", file=self.outf)
        return rdd

    def test_collectAsMap(self, rdd=None):
        print(f"test_collectAsMap output -->\n", file=self.outf)
        dct = rdd.collectAsMap()
        print(f"Result after collectAsMap:\n{dct}\n", file=self.outf)
    
    def test_lookup(self):
        print(f"test_lookup output -->\n", file=self.outf)
        rdd = self.sc.parallelize([1, 1, 2, 3, 4, 4, 1, 5, 6, 3, 1, 2])
        rdd = rdd.map(lambda x: (x, x + 3))
        lst = rdd.collect()
        lookup_lst = rdd.lookup(1)
        print(f"As dictionary:\n{lst}\n", file=self.outf)
        print(f"Result after lookup:\n{lookup_lst}\n", file=self.outf)
    
    def test_find_longest_line(self):
        print(f"test_find_longest_line output -->\n", file=self.outf)
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w2\\Life_of_PI_good_review.txt"
        rdd = self.sc.textFile(inf_path)
        rdd = rdd.map(lambda s: (count_words(s), s))
        lst = rdd.collect()
        max_elem = rdd.max()
        print(f"Max:\n{max_elem}\n", file=self.outf)

    def stop_sc(self):
        self.sc.stop()

    def close_file(self):
        self.outf.close()


def count_words(line=None):
    pattern = re.compile(r"\b\w+\b")
    matched_lst = re.findall(pattern, line)
    count = len(matched_lst)
    return count


if __name__ == "__main__": main()
