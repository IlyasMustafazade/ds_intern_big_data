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
from src.bd4_hw import spark_sql2, Homework

def main():
    outf_name = "spark_sql2"
    outf = open(f"{outf_name}.txt", "w", encoding="utf-8")

    hw = Homework(outf=outf)

    distinct_df = spark_sql2(hw)
    distinct_df_pd = distinct_df.toPandas()
    print(f"Distinct dataframe -->\n{distinct_df_pd}\n", file=outf)


if __name__ == "__main__": main()

