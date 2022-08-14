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

FILE_DEPTH = 4
sys.path.append("\\".join(os.path.abspath(__file__).split("\\")[:-FILE_DEPTH]))
from src.bd4_hw import spark_sql1, Homework

def main():
    outf_name = "spark_sql1"
    outf = open(f"{outf_name}.txt", "w", encoding="utf-8")

    hw = Homework(outf=outf)
    inf_path = input("Copy and paste absolute path of hepatitis.txt: ")
    if len(inf_path) < 1:
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\hepatitis.csv"
    hepatitis_rdd = hw.sc.textFile(inf_path)
    first = hepatitis_rdd.first()
    drop_lst = [first]
    hepatitis_rdd = hepatitis_rdd.filter(lambda s: s not in drop_lst)
    print(f"CSV RDD type -->\n{type(hepatitis_rdd)}\n", file=outf)

    mean = spark_sql1(hw, rdd=hepatitis_rdd)
    print(f"Mean age -->\n{mean}\n", file=outf)


if __name__ == "__main__": main()

