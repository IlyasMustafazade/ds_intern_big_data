import mrjob.job as job
import re

class MRFileUploader(job.MRJob): pass

def upload_file(file_path=None):
    file_system = MRFileUploader().make_runner().fs
    if file_system.exists("/ilyas") is False:
        file_system.mkdir("/ilyas")
    file_name = file_path.split("\\")[-1]
    if file_system.exists(f"/ilyas/{file_name}") is False:
        file_system.put(f"{file_path}", f"/ilyas/{file_name}")


if __name__ == "__main__":
    file_path = input("Copy and paste absolute path of bd3_hw_q2.txt: ")
    if len(file_path) < 1:
        file_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd3_hw\\bd3_hw_q2.txt"
    upload_file(file_path=file_path)
