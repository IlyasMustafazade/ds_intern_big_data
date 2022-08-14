import mrjob
import subprocess
import sys

inp = input("Copy and paste absolute path of bd3_hw_q1.py: ")
q1_script_path = \
    "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd3_hw\\bd3_hw_q1.py" if len(inp) < 1 else inp

inp = input("Copy and paste absolute path of bd3_hw_q2.py: ")
q2_script_path = \
    "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd3_hw\\bd3_hw_q2.py" if len(inp) < 1 else inp

sys.path.append(q1_script_path)
from bd3_hw_q1 import upload_file

inp = input("Copy and paste absolute path of bd3_hw_q2.txt: ")
text_inp = \
    "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd3_hw\\bd3_hw_q2.txt" if len(inp) < 1 else inp

script_path_lst = [q1_script_path, q2_script_path]

def run_q1(script_path=None, text_inp=None):
    print("Question 1 -> \n")
    subprocess.run(f"python {script_path}", shell=True)

def run_q2(script_path=None, text_inp=None):
    print("Question 2 -> \n")
    outfile_path = input("Enter path you want to store bd3_hw_q2_out.txt: ")
    if len(outfile_path) < 1:
        outfile_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd3_hw\\bd3_hw_q2_out.txt"
    subprocess.run(f"python {script_path} < {text_inp} > {outfile_path}", shell=True)
    upload_file(file_path=outfile_path)

runner_lst = [run_q1, run_q2]
for i in range(len(runner_lst)):
    runner_lst[i](script_path=script_path_lst[i], text_inp=text_inp)
