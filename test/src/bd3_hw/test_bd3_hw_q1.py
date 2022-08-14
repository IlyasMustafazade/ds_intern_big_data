import mrjob.job as job

def main():
    runner = job.MRJob().make_runner()
    file_system = runner.fs
    exists = file_system.exists("/ilyas")
    if exists is False:
        print("/ilyas does not exist")
    else: 
        print("/ilyas exists")
    file_path = input("Copy and paste absolute path of bd3_hw_q2.txt: ")
    if len(file_path) < 1:
        file_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\src\\bd3_hw\\bd3_hw_q2.txt"
    file_name = file_path.split("\\")[-1]
    print("File name -> ", file_name)
    if file_system.exists(f"/ilyas/{file_name}") is False:
        print(f"/ilyas/{file_name} does not exist")
    else:
        print(f"/ilyas/{file_name} exists")
    print(list(file_system.ls("/ilyas")))
    file_system.rm("/ilyas")


if __name__ == "__main__":
    main()
