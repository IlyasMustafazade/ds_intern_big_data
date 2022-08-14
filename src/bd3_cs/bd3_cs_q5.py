from pyhdfs import HdfsClient

def main():
    print("Question 5 -> \n")

    dir_name = "my_files"
    file_name = "any_file.r"
    file_content = "Any R file"

    client = HdfsClient()
    client.mkdirs(f"/{dir_name}")
    client.create(f"/{dir_name}/{file_name}")
    dest = input("Enter destination path: ")
    client.copy_to_local(f"/{dir_name}/{file_name}", dest)


if __name__ == "__main__":
    main()
