import re
import pyspark as ps
import pyspark.sql as psql
import pyspark.sql.functions as sql_f
import pyspark.ml as pml
import pyspark.ml.linalg as linalg
import pyspark.ml.evaluation as peval

def main():
    tester = SparkSQLMLTester()
    conf = tester.test_conf()
    tester.test_context(conf=conf)
    tester.test_session()

    rdd_train = tester.test_load_train()
    tester.test_make_train(rdd=rdd_train)

    rdd_test = tester.test_load_test()
    tester.test_make_test(rdd=rdd_test)

    logistic_cl = tester.test_LogisticRegression()
    decision_tree_cl = tester.test_DecisionTreeClassifier()

    pipe = tester.test_Pipeline(cl=logistic_cl)
    
    transformer = tester.test_fit(pipe=pipe)
    transformed_df = tester.test_transform(transformer=transformer)

    evaluator = tester.test_Evaluator()
    tester.test_evaluate(evaluator=evaluator, transformed_df=transformed_df)

    tester.sc.stop()
    tester.outf.close()


class SparkSQLMLTester:
    def __init__(self):
        outf_name = "test_pyspark_sql_ml"
        self.outf = open(f"{outf_name}.txt", "w")
        self.sc = None
        self.train = None
        self.test = None
    
    def test_conf(self):
        print(f"test_conf output -->\n", file=self.outf)
        conf = ps.conf.SparkConf()
        master = "local[4]"
        app_name = "Exploring pyspark.sql.ml"
        conf.setMaster(master).setAppName(app_name)
        conf_as_str = conf.toDebugString()
        print(f"Configuration string:\n{conf_as_str}\n", file=self.outf)
        return conf

    def test_context(self, conf=None):
        print(f"test_context output -->\n", file=self.outf)
        sc = ps.context.SparkContext(conf=conf)
        self.sc = sc
        print(f"Type of SparkContext object: {type(self.sc)}\n", file=self.outf)
    
    def test_session(self):
        print(f"test_session output -->\n", file=self.outf)
        ss = ps.sql.SparkSession(self.sc)
        print(f"Type of SparkSession object: {type(ss)}\n", file=self.outf)
    
    def test_load_train(self):
        print(f"test_load_train output -->\n", file=self.outf)
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\training.txt"
        rdd = self.sc.textFile(inf_path)
        lst = rdd.collect()
        print(f"Training data: {lst}\n", file=self.outf)
        return rdd
    
    def test_make_train(self, rdd=None):
        print(f"test_make_train output -->\n", file=self.outf)
        train_rdd = rdd.map(lambda s: extract_num_lst(s))
        train_rdd = train_rdd.filter(lambda lst: len(lst) > 0)
        train_rdd = train_rdd.map(lambda lst: (float(lst[0]), linalg.Vectors.dense([float(s) for s in lst[1:]])))
        columns = ["label", "features"]
        self.train = train_rdd.toDF(columns)
        lst = train_rdd.collect()
        print(f"Modified training data: {lst}\n", file=self.outf)
    
    def test_load_test(self):
        print(f"test_load_test output -->\n", file=self.outf)
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\unlabeled.txt"
        rdd = self.sc.textFile(inf_path)
        lst = rdd.collect()
        print(f"Test data: {lst}\n", file=self.outf)
        return rdd

    def test_make_test(self, rdd=None):
        print(f"test_make_test output -->\n", file=self.outf)
        test_rdd = rdd.map(lambda s: extract_num_lst(s))
        test_rdd = test_rdd.filter(lambda lst: len(lst) > 0)
        test_rdd = test_rdd.map(lambda lst: (-1, linalg.Vectors.dense([float(s) for s in lst])))
        columns = ["label", "features"]
        self.test = test_rdd.toDF(columns)
        lst = test_rdd.collect()
        print(f"Modified test data: {lst}\n", file=self.outf)
    
    def test_LogisticRegression(self):
        print(f"test_LogisticRegression output -->\n", file=self.outf)
        regressor = pml.classification.LogisticRegression(featuresCol="features", labelCol="label")
        print(f"Regressor type: {type(regressor)}\n", file=self.outf)
        return regressor
    
    def test_DecisionTreeClassifier(self):
        print(f"test_DecisionTreeClassifier output -->\n", file=self.outf)
        regressor = pml.classification.DecisionTreeClassifier(featuresCol="features", labelCol="label")
        print(f"Regressor type: {type(regressor)}\n", file=self.outf)
        return regressor

    def test_Pipeline(self, cl=None):
        print(f"test_Pipeline output -->\n", file=self.outf)
        stage_lst = [cl]
        pipe = pml.Pipeline(stages=stage_lst)
        print(f"Pipe type: {type(pipe)}\n", file=self.outf)
        return pipe
    
    def test_fit(self, pipe=None):
        print(f"test_fit output -->\n", file=self.outf)
        transformer = pipe.fit(self.train)
        print(f"Transformer type: {type(transformer)}\n", file=self.outf)
        return transformer
    
    def test_transform(self, transformer=None):
        print(f"test_transform output -->\n", file=self.outf)
        transformed_df = transformer.transform(self.test)
        transformed_lst = transformed_df.collect()
        print(f"Transformed df type: {type(transformed_df)}\n", file=self.outf)
        print(f"Transformed list:\n{transformed_lst}\n", file=self.outf)
        transformed_df.show()
        return transformed_df

    def test_Evaluator(self):
        print(f"test_Evaluator output -->\n", file=self.outf)
        evaluator = peval.BinaryClassificationEvaluator()
        print(f"Evaluator type: {type(evaluator)}\n", file=self.outf)
        return evaluator

    def test_evaluate(self, evaluator=None, transformed_df=None, ):
        print(f"test_evaluate output -->\n", file=self.outf)
        evaluation = evaluator.evaluate(transformed_df)
        print(f"Evaluation: {evaluation}\n", file=self.outf)


def extract_num_lst(string=None):
    pattern = re.compile("\w+\.\w+")
    num_lst = re.findall(pattern, string)
    return num_lst


if __name__ == "__main__": main()
