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
from src.bd4_hw import actions1, Homework

def main():
    outf_name = "actions1"
    outf = open(f"{outf_name}.txt", "w", encoding="utf-8")

    hw = Homework(outf=outf)

    original_rdd = actions1(hw)
    lst = original_rdd.collect()
    print(f"Original list -->\n{lst}\n", file=outf)


if __name__ == "__main__": main()








