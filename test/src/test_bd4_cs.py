import unittest
import sys
import os
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame
from pyspark.ml.linalg import Vectors

FILE_DEPTH = 3
sys.path.append("\\".join(os.path.abspath(__file__).split("\\")[:-FILE_DEPTH]))
from modules.s_session import make_ss
import src.bd4_cs as bd4_cs


class TestCaseStudy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        master = "local"
        app_name = "TestCaseStudy"
        cls.ss = make_ss(master=master, app_name=app_name)
        cls.df = cls.load_df()
    
    @classmethod
    def tearDownClass(cls):
        TestCaseStudy.ss.stop()
    
    @classmethod
    def load_df(cls):
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern_big_data\\resource\\w4\\churn.csv"
        return TestCaseStudy.ss.read.load(
            inf_path, format="csv", sep=",", inferSchema="true", header="true")

    def test_prepare_df(self):
        prepared_df = bd4_cs.prepare_df(df=TestCaseStudy.df)
        dtypes = prepared_df.dtypes
        dtype_lst = [type_ for name, type_ in dtypes]
        self.assertNotIn("string", dtype_lst)
        columns = prepared_df.columns
        deleted_cols = ("RowNumber", "CustomerId", "Surname")
        for col in deleted_cols:
            self.assertNotIn(col, columns)
    
    def test_make_cl(self):
        cl = bd4_cs.make_cl("forest")
        self.assertTrue(isinstance(cl, RandomForestClassifier))
        cl = bd4_cs.make_cl("logistic")
        self.assertTrue(isinstance(cl, LogisticRegression))
        with self.assertRaises(ValueError):
            cl = bd4_cs.make_cl("linear")
        
    def test_make_pipe(self):
        cl = bd4_cs.make_cl("logistic")
        pipe = bd4_cs.make_pipe(cl)
        self.assertTrue(isinstance(pipe, Pipeline))
        stages = pipe.getStages()
        self.assertTrue(isinstance(stages[0], LogisticRegression))
        cl = bd4_cs.make_cl("forest")
        pipe = bd4_cs.make_pipe(cl)
        self.assertTrue(isinstance(pipe, Pipeline))
        stages = pipe.getStages()
        self.assertTrue(isinstance(stages[0], RandomForestClassifier))
    
    def test_group_features(self):
        prepared_df = bd4_cs.prepare_df(df=TestCaseStudy.df)
        df = bd4_cs.group_features(df=prepared_df)
        self.assertTrue(isinstance(df, DataFrame))
        columns = df.columns
        required_columns = ["features", "label"]
        self.assertEqual(columns, required_columns)
    
    def test_ml2(self):
        pipe, grouped_df = bd4_cs.ml2(df=TestCaseStudy.df, cl_type="forest")
        dtypes = grouped_df.dtypes
        dtype_lst = [type_ for name, type_ in dtypes]
        self.assertNotIn("string", dtype_lst)
        columns = grouped_df.columns
        deleted_cols = ("RowNumber", "CustomerId", "Surname")
        for col in deleted_cols:
            self.assertNotIn(col, columns)
        self.assertTrue(isinstance(pipe, Pipeline))
        stages = pipe.getStages()
        self.assertTrue(isinstance(stages[0], RandomForestClassifier))
        self.assertTrue(isinstance(grouped_df, DataFrame))
        columns = grouped_df.columns
        required_columns = ["features", "label"]
        self.assertEqual(columns, required_columns)
    
    def test_ml3(self):
        pipe, grouped_df = bd4_cs.ml2(df=TestCaseStudy.df, cl_type="logistic")
        predicted_df = bd4_cs.ml3(pipe=pipe, df=grouped_df)
    
    def test_ml4(self):
        pipe, grouped_df = bd4_cs.ml2(df=TestCaseStudy.df, cl_type="forest")
        predicted_df = bd4_cs.ml3(pipe=pipe, df=grouped_df)
        auc = bd4_cs.ml4(df_transformed=predicted_df)
        print(f"AUC score: {auc}")
    

if __name__ == "__main__": unittest.main(verbosity=3)

