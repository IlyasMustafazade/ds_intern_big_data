from mrjob.job import MRJob
import re

class MRFileMaker(MRJob): pass

def mkfile(file_name=None, dir_name=None, file_system=None):
    folder_path_hdfs = f"/{dir_name}"
    if file_system.exists(f"{folder_path_hdfs}/{file_name}") is False:
        file_system.touchz(f"{folder_path_hdfs}/{file_name}")

def rmfile(file_name=None, dir_name=None, file_system=None):
    file_system.rm(f"/{dir_name}/{file_name}")


if __name__ == "__main__":
    print("Question 2 -> \n")
    file_system = MRFileMaker().make_runner().fs
    dir_name = "myfiles"
    # a)
    mkfile(file_name="any_file.py", dir_name=dir_name, file_system=file_system)
    print("After making file: ")
    print(list(file_system.ls(f"/{dir_name}")))
    # b)
    print("After removing file: ")
    rmfile(file_name="any_file.py", dir_name=dir_name, file_system=file_system)
    print(list(file_system.ls(f"/{dir_name}")))
