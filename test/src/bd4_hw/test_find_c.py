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
from src.bd4_hw import transformations3, find_c, Homework

def main():
    outf_name = "find_c"
    outf = open(f"{outf_name}.txt", "w", encoding="utf-8")

    hw = Homework(outf=outf)
    rdd1, rdd2 = transformations3(hw)

    result_rdd = find_c(hw, rdd1=rdd1, rdd2=rdd2)
    lst = result_rdd.collect()
    print(f"Subtraction list -->\n{lst}\n", file=outf)


if __name__ == "__main__": main()

