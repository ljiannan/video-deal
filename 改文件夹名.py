import os
import random
import string

# 定义输入文件夹
input_folder = r'G:\片头片尾待质检\output81_mjtt1\dianshiju'


# 生成随机的6位数字名称
def generate_random_6_digit_name():
    return ''.join(random.choices(string.digits, k=6))


# 递归重命名文件夹
def rename_folders_recursively(folder_path):
    # 遍历当前文件夹中的所有子文件夹
    for root, dirs, files in os.walk(folder_path):
        for dir_name in dirs:
            # 构建子文件夹的完整路径
            dir_path = os.path.join(root, dir_name)

            # 生成新的随机名称并检查是否存在
            new_dir_name = generate_random_6_digit_name()
            new_dir_path = os.path.join(root, new_dir_name)
            while os.path.exists(new_dir_path):
                new_dir_name = generate_random_6_digit_name()
                new_dir_path = os.path.join(root, new_dir_name)

            # 重命名文件夹
            os.rename(dir_path, new_dir_path)
            print(f"Renamed {dir_path} to {new_dir_path}")

            # 递归重命名子文件夹中的子文件夹
            rename_folders_recursively(new_dir_path)


# 开始重命名
rename_folders_recursively(input_folder)
print("Done renaming all subdirectories.")