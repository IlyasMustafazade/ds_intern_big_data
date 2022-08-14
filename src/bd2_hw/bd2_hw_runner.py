import os

def main():
    print("Question 1 -> \n")
    run_q1()
    print("Question 2 -> \n")
    run_q2()

def run_q1():
    script_path = input("Copy and paste absolute path of bd2_hw_q1.py: ")
    if len(script_path) < 1:
        script_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd2_hw\\bd2_hw_q1.py"
    folder_path = input("Copy and paste absolute path of input data directory: ")
    if len(folder_path) < 1:
        folder_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w2\\input_data"
    command1 = "python " + script_path + " " +  folder_path
    os.system(f'cmd /c {command1}')

def run_q2():
    script_path = input("Copy and paste absolute path of bd2_hw_q2.py: ")
    if len(script_path) < 1:
        script_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd2_hw\\bd2_hw_q2.py"
    file_path = input("Copy and paste absolute path of voters.csv: ")
    if len(file_path) < 1:
        file_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w2\\voters.csv"
    command2 = "python " + script_path + " " +  file_path
    os.system(f'cmd /c {command2}')


if __name__ == "__main__":
    main()


