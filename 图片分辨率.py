"""可用"""
import os
import shutil
import time
from PIL import Image


# 定义输入和输出文件夹
input_folder = r"E:\图片"  # 替换为你的输入文件夹路径
output_folder = r'E:\不合格\小于1024'  # 替换为你的输出文件夹路径

# 如果输出文件夹不存在，创建它
if not os.path.exists(output_folder):
    os.makedirs(output_folder)


# 定义重试移动文件的函数（使用指数退避策略）
def move_file_with_retry(src, dst, max_retries=10, initial_delay=2, backoff_factor=2):
    """
    尝试移动文件，如果文件被占用则重试。
    :param src: 源文件路径
    :param dst: 目标文件路径
    :param max_retries: 最大重试次数
    :param initial_delay: 初始延迟时间（秒）
    :param backoff_factor: 延迟时间增长系数
    :return: 是否成功移动
    """
    retries = 0
    current_delay = initial_delay
    while retries < max_retries:
        try:
            shutil.move(src, dst)
            return True
        except (PermissionError, OSError) as e:
            print(f"文件 {src} 被占用，{current_delay}秒后重试... (第 {retries + 1}/{max_retries} 次尝试)")
            time.sleep(current_delay)
            retries += 1
            current_delay *= backoff_factor  # 每次重试延迟时间翻倍
    print(f"经过 {max_retries} 次重试仍无法移动 {src}，请手动处理。")
    return False


# 遍历输入文件夹中的所有文件
for root, dirs, files in os.walk(input_folder):
    for file in files:
        # 检查文件是否为图片文件
        if file.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif', '.tiff')):
            file_path = os.path.join(root, file)
            print(f"当前处理图片:{file_path}")

            try:
                # 阶段一：获取图片尺寸并立即关闭文件
                width, height = 0, 0
                with Image.open(file_path) as img:  # 使用上下文管理器确保文件关闭
                    width, height = img.size

                # 阶段二：判断是否需要移动
                if min(width, height) < 1024:
                    # 构建目标路径，保持原有的文件夹结构
                    relative_path = os.path.relpath(root, input_folder)  # 获取相对路径
                    target_dir = os.path.join(output_folder, relative_path)  # 目标文件夹路径

                    # 如果目标文件夹不存在，创建它
                    if not os.path.exists(target_dir):
                        os.makedirs(target_dir)

                    # 构建目标文件路径
                    target_path = os.path.join(target_dir, file)

                    # 检查目标文件是否已存在
                    if os.path.exists(target_path):
                        print(f"文件 {target_path} 已存在，跳过。")
                    else:
                        # 移动文件到目标文件夹（带重试机制）
                        if move_file_with_retry(file_path, target_path):
                            print(f"\033[31m成功移动 {file_path} 到 {target_path}\033[0m")
                        else:
                            # 记录失败文件到日志
                            with open("failed_files.log", "a") as log:
                                log.write(f"{file_path}\n")

            except Exception as e:
                print(f"处理 {file_path} 时出错: {str(e)}")
                # 记录错误到日志
                with open("error_log.log", "a") as log:
                    log.write(f"{file_path} - {str(e)}\n")

print("所有图片处理完成。")