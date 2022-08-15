import sys
import os
from pyspark.sql import SparkSession

FILE_DEPTH = 3
sys.path.append("\\".join(os.path.abspath(__file__).split("\\")[:-FILE_DEPTH]))
from modules.s_context import make_sc

def make_ss(master=None, app_name=None, sc=None):
    if sc is None:
        sc = make_sc(master=master, app_name=app_name)
    return SparkSession(sc)
