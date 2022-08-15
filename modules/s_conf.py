from pyspark.conf import SparkConf

def make_conf(master=None, app_name=None):
    conf = SparkConf()
    conf.setMaster(master).setAppName(app_name)
    return conf
