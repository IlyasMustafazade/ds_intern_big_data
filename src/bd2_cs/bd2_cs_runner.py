import os

def main():
    print("Question 1 -> \n")
    run_q1()

    print("Question 2 -> \n")
    run_q2()

    print("Question 3 -> \n")
    run_q3()

def run_q1():
    script_path = input("Copy and paste absolute path of bd2_cs_q1.py: ")
    if len(script_path) < 1:
        script_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd2_cs\\bd2_cs_q1.py"
    file_path = input("Copy and paste absolute path of airportsensor.txt: ")
    if len(file_path) < 1:
        file_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w2\\airportsensor.txt"
    command = "python " + script_path + " " +  file_path
    os.system(f'cmd /c {command}')


def run_q2():
    script_path = input("Copy and paste absolute path of bd2_cs_q2.py: ")
    if len(script_path) < 1:
        script_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd2_cs\\bd2_cs_q2.py"
    file_path = input("Copy and paste absolute path of cards.csv: ")
    if len(file_path) < 1:
        file_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w2\\cards.csv"
    command = "python " + script_path + " " +  file_path
    os.system(f'cmd /c {command}')

def run_q3():
    script_path = input("Copy and paste absolute path of bd2_cs_q3.py: ")
    if len(script_path) < 1:
        script_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd2_cs\\bd2_cs_q3.py"
    file_path = input("Copy and paste absolute path of Life_of_PI_good_review.txt: ")
    if len(file_path) < 1:
        file_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w2\\Life_of_PI_good_review.txt"
    command = "python " + script_path + " " +  file_path
    os.system(f'cmd /c {command}')

if __name__ == "__main__":
    main()
