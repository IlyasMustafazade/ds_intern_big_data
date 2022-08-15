import unittest
import sys
import os
from pyspark.sql import SparkSession


FILE_DEPTH = 2
sys.path.append("\\".join(os.path.abspath(__file__).split("\\")[:-FILE_DEPTH]))
import modules.s_session as s_session
from modules.s_context import make_sc


class TestSession(unittest.TestCase):
    def test_make_ss(self):
        master = "local"
        app_name = "test_make_ss"
        ss = s_session.make_ss(master=master, app_name=app_name)
        self.assertTrue(isinstance(ss, SparkSession))
        sc = ss.sparkContext
        conf = sc.getConf()
        self.assertEqual(conf.get("spark.master"), master)
        self.assertEqual(conf.get("spark.app.name"), app_name)
        master = "local[4]"
        app_name = "test_make_ss2"
        ss.stop()
        sc = make_sc(master=master, app_name=app_name)
        ss = s_session.make_ss(sc=sc)
        conf = sc.getConf()
        self.assertTrue(isinstance(ss, SparkSession))
        self.assertEqual(conf.get("spark.master"), "local[4]")
        self.assertEqual(conf.get("spark.app.name"), "test_make_ss2")
    
    
if __name__ == "__main__": unittest.main()

