import os
import sys
import numpy as np
import pandas as pd
import pyspark as ps
import pyspark.sql as psql
import pyspark.ml as pml
import pyspark.sql.functions as sql_f
import pyspark.ml.linalg as linalg
import pyspark.ml.evaluation as peval

FILE_DEPTH = 3
sys.path.append("\\".join(os.path.abspath(__file__).split("\\")[:-FILE_DEPTH]))
from modules.s_session import make_ss

def main():
    outf_name = "test_make_ss"
    outf = open(f"{outf_name}.txt", "w")

    master = "local"
    app_name = "Testing make_ss"

    ss = make_ss(sc=ps.context.SparkContext(
            conf=ps.conf.SparkConf().setMaster("local").setAppName("app_name")))
    print(ss, file=outf)


if __name__ == "__main__": main()
