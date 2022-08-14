import mrjob.job as job

def main():
    file_system = job.MRJob().make_runner().fs
    print(list(file_system.ls("/ilyas")))  
    file_system.rm("/ilyas")


if __name__ == "__main__":
    main()
