import numpy as np
import pandas as pd
import pyspark as ps
import pyspark.sql as psql
import pyspark.ml as pml
import pyspark.sql.functions as sql_f
import pyspark.ml.linalg as linalg
import pyspark.ml.evaluation as peval

def make_conf(master=None, app_name=None):
    conf = ps.conf.SparkConf()
    conf.setMaster(master).setAppName(app_name)
    return conf
