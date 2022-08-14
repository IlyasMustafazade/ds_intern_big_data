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
from modules.s_context import make_sc

def main():
    outf_name = "test_make_sc"
    outf = open(f"{outf_name}.txt", "w")

    master = "local"
    app_name = "Testing make_sc"

    sc = make_sc(master=master, app_name=app_name)
    print(sc, file=outf)
    

if __name__ == "__main__": main()
