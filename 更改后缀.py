import os


def change_extension(folder_path):
    try:
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                new_filename = os.path.splitext(filename)[0] + '.png'
                new_file_path = os.path.join(folder_path, new_filename)
                os.rename(file_path, new_file_path)
                print(f"已将 {filename} 重命名为 {new_filename}")
    except FileNotFoundError:
        print(f"错误: 未找到文件夹 {folder_path}")
    except Exception as e:
        print(f"错误: 发生未知错误: {e}")


if __name__ == "__main__":
    folder_path = r'/Users/zg/.mounty/A07/新图片数据/动物4'
    change_extension(folder_path)
