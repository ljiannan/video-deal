import os


def rename_files_recursively(folder_path):
    counter = 1
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_extension = os.path.splitext(file)[1]
            new_name = f"{counter:08d}{file_extension}"
            old_file_path = os.path.join(root, file)
            new_file_path = os.path.join(root, new_name)
            try:
                os.rename(old_file_path, new_file_path)
                print(f"Renamed {old_file_path} to {new_file_path}")
                counter += 1
            except Exception as e:
                print(f"Error renaming {old_file_path}: {e}")


if __name__ == "__main__":
    folder_path = "E:\图片"  # 请将 '.' 替换为你要处理的文件夹路径
    rename_files_recursively(folder_path)
