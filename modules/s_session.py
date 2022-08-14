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

def make_ss(master=None, app_name=None, sc=None):
    if sc is None:
        sc = make_sc(master=master, app_name=app_name)
    return psql.SparkSession(sc)
