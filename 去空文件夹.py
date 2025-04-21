import os

def remove_empty_dirs(filepath):
    # 遍历路径下的所有文件和文件夹
    for root, dirs, files in os.walk(filepath, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            try:
                # 如果文件夹为空，则删除
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
                    print(f"Removed empty directory: {dir_path}")
            except Exception as e:
                print(f"Error removing directory {dir_path}: {e}")

# 示例使用
directory_path = r'G:\片头片尾待质检\output81_mjtt2\dianying'  # 替换为你的目标路径
remove_empty_dirs(directory_path)