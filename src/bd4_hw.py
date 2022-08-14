import sys
import os
import numpy as np
import pandas as pd
import pyspark as ps
import pyspark.sql as psql
import pyspark.ml as pml
import pyspark.sql.functions as sql_f
import pyspark.ml.linalg as linalg
import pyspark.ml.evaluation as peval

FILE_DEPTH = 2
sys.path.append("\\".join(os.path.abspath(__file__).split("\\")[:-FILE_DEPTH]))
from modules.s_context import make_sc
from modules.s_session import make_ss


def main():
    outf_name = "bd4_hw"
    outf = open(f"{outf_name}.txt", "w")

    hw = Homework(outf=outf)

    inf_path = input("Copy and paste absolute path of surnames.txt: ")
    if len(inf_path) < 1:
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\surnames.txt"
    surnames_rdd = hw.sc.textFile(inf_path)

    inf_path = input("Copy and paste absolute path of hepatitis.txt: ")
    if len(inf_path) < 1:
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\hepatitis.csv"
    hepatitis_rdd = hw.sc.textFile(inf_path)
    first = hepatitis_rdd.first()
    drop_lst = [first]
    hepatitis_rdd = hepatitis_rdd.filter(lambda s: s not in drop_lst)

    transformations1(hw, rdd=surnames_rdd)
    transformations2(hw, rdd=surnames_rdd)
    rdd1, rdd2 = transformations3(hw)
    find_a(hw, rdd1, rdd2)
    find_b(hw, rdd1, rdd2)
    find_c(hw, rdd1, rdd2)
    find_d(hw, rdd1, rdd2)
    actions1(hw)
    actions2(hw, rdd=surnames_rdd)
    spark_sql1(hw, rdd=hepatitis_rdd)
    spark_sql2(hw)

    hw.ss.stop()
    outf.close()


class Homework:
    def __init__(self, outf=None):
        self.outf = outf
        self.sc = make_sc(master="local", app_name="bd4_hw")
        self.ss = make_ss(sc=self.sc)
    

def transformations1(hw=None, rdd=None):
    MAX_LEN = 7
    filtered_rdd = rdd.filter(lambda s: len(s) <= MAX_LEN)
    return filtered_rdd


def transformations2(hw=None, rdd=None):
    first_letter_rdd = rdd.map(lambda s: s[0])
    return first_letter_rdd


def transformations3(hw=None):
    lst = [i for i in range(10)]
    rdd1 = hw.sc.parallelize(lst)
    rdd2 = rdd1.map(lambda i: i * i)
    return rdd1, rdd2


def find_a(hw=None, rdd1=None, rdd2=None):
    rdd3 = rdd1.union(rdd2)
    rdd3 = rdd3.distinct()
    return rdd3


def find_b(hw=None, rdd1=None, rdd2=None):
    rdd3 = rdd1.intersection(rdd2)
    return rdd3


def find_c(hw=None, rdd1=None, rdd2=None):
    rdd3 = rdd1.subtract(rdd2)
    return rdd3


def find_d(hw=None, rdd1=None, rdd2=None):
    rdd3 = rdd1.cartesian(rdd2)
    return rdd3


def actions1(hw=None):
    inf_path1 = input("Copy and paste absolute path of doc1-2.txt: ")
    if len(inf_path1) < 1:
        inf_path1 = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\ex_4\\doc1-2.txt"
    
    inf_path2 = input("Copy and paste absolute path of doc2.txt: ")
    if len(inf_path2) < 1:
        inf_path2 = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\ex_4\\doc2.txt"

    rdd1 = hw.sc.textFile(inf_path1)
    rdd2 = hw.sc.textFile(inf_path2)
    if rdd1.count() > rdd2.count():
        rdd3 = rdd1
    else: rdd3 = rdd2
    return rdd3


def actions2(hw=None, rdd=None):
    mapped_rdd = rdd.map(lambda s: (len(s), s))
    max_pair = mapped_rdd.max()
    max_len = max_pair[0]
    return max_len


def spark_sql1(hw=None, rdd=None):
    mapped_rdd = rdd.map(lambda s: (s.split(",")[1], float(s.split(",")[0])))
    male_rdd = mapped_rdd.filter(lambda t: t[0] == "male")
    male_age_rdd = male_rdd.map(lambda t: t[1])
    mean = male_age_rdd.mean()
    return mean


def spark_sql2(hw=None):
    inf_path = input("Copy and paste absolute path of hepatitis.csv: ")
    if len(inf_path) < 1:
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\hepatitis.csv"
    df = hw.ss.read.load(inf_path, format="csv", sep=",", inferSchema="true", header="true")
    df.createOrReplaceTempView("df")
    distinct_df = hw.ss.sql("SELECT DISTINCT AGE from df")
    distinct_df = distinct_df.orderBy("AGE", ascending=True)
    return distinct_df


if __name__ == "__main__": main()

