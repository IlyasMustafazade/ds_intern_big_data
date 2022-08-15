import unittest
from pyspark import SparkConf, SparkContext
import sys
import os

FILE_DEPTH = 2
sys.path.append("\\".join(os.path.abspath(__file__).split("\\")[:-FILE_DEPTH]))
import modules.s_context as s_context

class TestContext(unittest.TestCase):
    def test_make_sc(self):
        master = "local"
        app_name = "test_make_sc"
        sc = s_context.make_sc(master=master, app_name=app_name)
        self.assertTrue(isinstance(sc, SparkContext))
        conf = sc.getConf()
        self.assertTrue(isinstance(conf, SparkConf))

if __name__ == "__main__": unittest.main()

