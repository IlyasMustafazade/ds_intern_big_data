import sys
import os
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler

FILE_DEPTH = 2
sys.path.append("\\".join(os.path.abspath(__file__).split("\\")[:-FILE_DEPTH]))
from modules.s_context import make_sc
from modules.s_session import make_ss


class CaseStudy:
    def __init__(self):
        self.sc = make_sc(master="local", app_name="bd4_hw")
        self.ss = make_ss(sc=self.sc)
    

def sql1(cs, df=None):
    male_df = df.filter(col("SEX") == "male")
    female_df = df.filter(col("SEX") == "female")
    return male_df.count(), female_df.count()


def sql2(cs, df=None):
    grouped_df = df.groupBy("SEX")
    min_df = grouped_df.agg({"AGE": "min"}).filter(col("SEX") == "female")
    max_df = grouped_df.agg({"AGE": "max"}).filter(col("SEX") == "female")
    return max_df.toPandas(), min_df.toPandas()


def ml1(cs):
    inf_path = input("Copy and paste absolute path of churn.csv: ")
    if len(inf_path) < 1:
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\churn.csv"
    churn_df = cs.ss.read.load(inf_path, format="csv", sep=",", inferSchema="true", header="true")
    return churn_df, churn_df.summary().toPandas()
    
    
def ml2(df=None, cl_type=None):
    df = prepare_df(df=df)
    cl = make_cl(cl_type=cl_type)
    pipe = make_pipe(cl=cl)
    grouped_df = group_features(df=df)
    return pipe, grouped_df


def make_cl(cl_type=None):
    cl_tpl = ("forest", "logistic")
    if not (cl_type in cl_tpl):
        raise ValueError("Invalid value for classifier type")
    if cl_type == "forest":
        cl = RandomForestClassifier()
    elif cl_type == "logistic":
        cl = LogisticRegression()
    return cl


def prepare_df(df=None):
    drop_tpl = ("RowNumber", "CustomerId", "Surname")
    dropped_df = df.drop(*drop_tpl)
    df_dtypes = dropped_df.dtypes
    num_col_lst = []
    for name, type_ in df_dtypes:
        if type_ != "string":
            num_col_lst.append(name)
    num_col_tpl = tuple(num_col_lst)
    numeric_df = dropped_df.select(*num_col_tpl)
    return numeric_df


def make_pipe(cl=None):
    stages = [cl]
    pipe = Pipeline(stages=stages)
    return pipe


def group_features(df=None):
    LABEL_COL = "Exited"
    rdd = df.rdd
    feature_lst = list(rdd.first().asDict().keys())
    feature_lst = [feature for feature in feature_lst if (feature != LABEL_COL)]
    rdd = rdd.map(lambda r: (Vectors.dense([r[feature] for feature in feature_lst]), r["Exited"]))
    col_names = ["features", "label"]
    result_df = rdd.toDF(col_names)
    return result_df


def ml3(pipe=None, df=None):
    test_size = 0.2
    train_df, test_df = df.randomSplit(weights=[1 - test_size, test_size], seed=2022)
    pipe_model = pipe.fit(train_df)
    df_transformed = pipe_model.transform(test_df)
    return df_transformed


def ml4(df_transformed=None):
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="prediction")
    auc = evaluator.evaluate(df_transformed)
    return auc
 