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
from modules.s_conf import make_conf


def main():
    outf_name = "test_make_conf"
    outf = open(f"{outf_name}.txt", "w")
    
    master = "local"
    app_name = "Testing make_conf"

    conf = make_conf(master=master, app_name=app_name)

    conf_as_str = conf.toDebugString()
    print(f"Configuration string:\n{conf_as_str}\n", file=outf)

    outf.close()


if __name__ == "__main__": main()
