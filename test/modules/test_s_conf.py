import unittest
from pyspark import SparkConf
import sys
import os

FILE_DEPTH = 2
sys.path.append("\\".join(os.path.abspath(__file__).split("\\")[:-FILE_DEPTH]))
import modules.s_conf as s_conf

class TestConf(unittest.TestCase):
    def test_make_conf(self):
        master = "local"
        app_name = "test_make_conf"
        conf = s_conf.make_conf(
                master=master, app_name=app_name)
        self.assertTrue(isinstance(conf, SparkConf))
        self.assertEqual(conf.get("spark.master"), master)
        self.assertEqual(conf.get("spark.app.name"), app_name)
    
if __name__ == "__main__": unittest.main()
        
