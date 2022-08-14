import mrjob.job as job

def main():
    folder_name = "myfiles"
    file_system = job.MRJob().make_runner().fs
    print(list(file_system.ls(f"/{folder_name}")))
    file_system.rm(f"/{folder_name}")


if __name__ == "__main__":
    main()
