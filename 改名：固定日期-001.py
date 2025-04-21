# -*- coding: utf-8 -*-
"""
Time:     2025/2/6 9:31
Author:   ZhaoQi Cao(czq)
Version:  V 0.1
File:     rename_file_all.py
Describe: Write during the python at zgxmt, Github link: https://github.com/caozhaoqi
"""
import os
import logging
import datetime

# 配置日志记录
def setup_logging(log_directory):
    """配置日志记录到文件."""
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)  # 创建日志目录

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_directory, f"rename_log_{timestamp}.log")
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    print(f"日志记录到: {log_file}")


def rename_files_and_folders_recursive(directory, counter=None):
    """
    递归地重命名指定目录及其子目录下的所有文件，按顺序排列，并记录日志。

    Args:
        directory: 要重命名的目录的路径。
        counter:  （可选）用于保持重命名计数的外部计数器。如果没有提供，函数将创建一个。
    """

    if counter is None:
        counter = {'value': 1}  # 使用字典模拟可变整数，以便在递归调用中正确更新

    items = os.listdir(directory)
    items.sort()

    folders = [item for item in items if os.path.isdir(os.path.join(directory, item))]
    files = [item for item in items if os.path.isfile(os.path.join(directory, item))]

    items_to_rename = files  # 只对文件进行重命名

    for old_name in items_to_rename:
        old_path = os.path.join(directory, old_name)
        # 生成新文件名，格式为 "3.6-001"
        new_name = f"3.24-{str(counter['value']).zfill(3)}"
        # 添加新文件名的扩展名如果它是文件
        if os.path.isfile(old_path):
            new_name = new_name + os.path.splitext(old_name)[1]

        new_path = os.path.join(directory, new_name)

        # 防止冲突
        temp_counter = counter['value']
        while os.path.exists(new_path):
            temp_counter += 1
            new_name = f"3.24-{str(temp_counter).zfill(3)}"  # 修改临时计数器，生成新的名称
            if os.path.isfile(old_path):
                new_name = new_name + os.path.splitext(old_name)[1]
            new_path = os.path.join(directory, new_name)  # 更新new_path

        try:
            os.rename(old_path, new_path)
            logging.info(f"重命名: {old_name} -> {new_name}")
            print(f"重命名: {old_name} -> {new_name}")  # 同时打印到控制台
            counter['value'] = temp_counter + 1  # 更新外部计数器
        except OSError as e:
            logging.error(f"重命名 {old_name} 出错: {e}")
            print(f"重命名 {old_name} 出错: {e}")  # 同时打印到控制台

    # 递归调用，只重命名文件，不影响文件夹
    for folder in folders:
        folder_path = os.path.join(directory, folder)
        rename_files_and_folders_recursive(folder_path, counter)  # 递归调用文件夹


# 使用示例:
directory_path = r"E:\3.24派发数据\00_共享池" # 替换为你的目录路径
log_directory = r"./logs"  # 替换为你的日志目录路径， 不存在会自动创建

setup_logging(log_directory)
logging.info("开始递归重命名")  # 记录开始信息
rename_files_and_folders_recursive(directory_path)
logging.info("递归重命名完成")  # 记录完成信息