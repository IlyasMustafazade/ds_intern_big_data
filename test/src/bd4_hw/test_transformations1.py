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
from src.bd4_hw import transformations1, Homework

def main():
    outf_name = "test_transformations1"
    outf = open(f"{outf_name}.txt", "w", encoding="utf-8")

    hw = Homework(outf=outf)
    inf_path = input("Copy and paste absolute path of surnames.txt: ")
    if len(inf_path) < 1:
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\surnames.txt"
    surnames_rdd = hw.sc.textFile(inf_path)

    filtered_rdd = transformations1(hw, rdd=surnames_rdd)
    lst = filtered_rdd.collect()
    print(f"Filtered list -->\n{lst}\n", file=outf)


if __name__ == "__main__": main()

