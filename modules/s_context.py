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
from modules.s_conf import make_conf

def make_sc(master=None, app_name=None):
    conf = make_conf(master=master, app_name=app_name)
    return ps.context.SparkContext(conf=conf)
    
