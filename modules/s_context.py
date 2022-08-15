import sys
import os
from pyspark import SparkContext

FILE_DEPTH = 3
sys.path.append("\\".join(os.path.abspath(__file__).split("\\")[:-FILE_DEPTH]))
from modules.s_conf import make_conf

def make_sc(master=None, app_name=None):
    conf = make_conf(master=master, app_name=app_name)
    return SparkContext(conf=conf)
    