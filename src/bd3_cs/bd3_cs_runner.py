import mrjob
import subprocess
import sys

inp = input("Copy and paste absolute path of bd3_cs_q1.py: ")
q1_script_path = \
    "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd3_cs\\bd3_cs_q1.py" if len(inp) < 1 else inp

inp = input("Copy and paste absolute path of bd3_cs_q2.py: ")
q2_script_path = \
    "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd3_cs\\bd3_cs_q2.py" if len(inp) < 1 else inp

inp = input("Copy and paste absolute path of bd3_cs_q3.py: ")
q3_script_path = \
    "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd3_cs\\bd3_cs_q3.py" if len(inp) < 1 else inp

inp = input("Copy and paste absolute path of bd3_cs_q4.py: ")
q4_script_path = \
    "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd3_cs\\bd3_cs_q4.py" if len(inp) < 1 else inp

inp = input("Copy and paste absolute path of bd3_cs_q5.py: ")
q5_script_path = \
    "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd3_cs\\bd3_cs_q5.py" if len(inp) < 1 else inp

sys.path.append(q1_script_path)
from bd3_cs_q1 import mkdir

script_path_lst = [q1_script_path, q2_script_path, q3_script_path, q4_script_path, q5_script_path]

def run_q1(script_path=None):
    subprocess.run(f"python {script_path}", shell=True)

def run_q2(script_path=None):
    subprocess.run(f"python {script_path}", shell=True)

def run_q3(script_path=None):
    inp = input("Copy and paste absolute path of projects.txt: ")
    file_path = \
        "C:\\Users\\ilyas\\source\\repos\ds_intern\\big_data\\resource\\w3\\projects.txt" if len(inp) < 1 else inp
    subprocess.run(f"python {script_path} {file_path}", shell=True)

def run_q4(script_path=None):
    inp = input("Copy and paste absolute path of demography.txt: ")
    file_path = \
        "C:\\Users\\ilyas\\source\\repos\ds_intern\\big_data\\resource\\w3\\demography.txt" if len(inp) < 1 else inp
    subprocess.run(f"python {script_path} {file_path}", shell=True)

def run_q5(script_path=None):
    subprocess.run(f"python {script_path}", shell=True)

runner_lst = [run_q1, run_q2, run_q3, run_q4, run_q5]
for i in range(len(runner_lst)):
    runner_lst[i](script_path=script_path_lst[i])
