import mrjob.job as job
import re

class MRDirectoryMaker(job.MRJob): pass

def mkdir(dir_name=None):
    folder_path_hdfs = f"/{dir_name}"
    file_system = MRDirectoryMaker().make_runner().fs
    if file_system.exists(folder_path_hdfs) is False:
        file_system.mkdir(folder_path_hdfs)

if __name__ == "__main__":
    print("Question 1 -> \n")
    mkdir(dir_name="myfiles")
