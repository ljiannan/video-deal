import os
import shutil
import subprocess
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


def setup_logger():
    """配置日志记录器，使其同时输出到控制台和文件"""
    logger = logging.getLogger('video_mover')
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler('video_mover.log', mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def get_unique_destination_path(destination_path):
    """
    (Used only when the destination file is valid)
    在文件名后附加一个计数器，例如 ' (1)', ' (2)'，直到找到一个唯一的文件名。
    """
    directory, full_filename = os.path.split(destination_path)
    base_name, extension = os.path.splitext(full_filename)
    counter = 1
    while True:
        new_filename = f"{base_name} ({counter}){extension}"
        new_destination_path = os.path.join(directory, new_filename)
        if not os.path.exists(new_destination_path):
            return new_destination_path
        counter += 1


def check_video_integrity(file_path, ffprobe_executable_path, logger):
    """使用指定路径的 ffprobe 检查视频文件的完整性"""
    try:
        command = [ffprobe_executable_path, '-v', 'error', '-i', file_path]
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError:
        # This is not an error, just a failed check. The calling function will log it.
        return False
    except Exception as e:
        logger.error(f"用 ffprobe 检查 '{os.path.basename(file_path)}' 时发生未知错误: {e}")
        return False


def copy_with_progress(source_path, destination_path, file_description):
    """使用 tqdm 显示进度条来复制文件"""
    file_size = os.path.getsize(source_path)
    chunk_size = 1024 * 1024  # 1MB

    with open(source_path, 'rb') as src_file, open(destination_path, 'wb') as dst_file:
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=file_description, leave=False) as pbar:
            while True:
                chunk = src_file.read(chunk_size)
                if not chunk:
                    break
                dst_file.write(chunk)
                pbar.update(len(chunk))


def process_single_video(source_path, destination_folder, ffprobe_path, logger):
    """
    处理单个视频的完整流程：
    1. 检查源文件。
    2. 检查目标文件是否存在，如果存在，则根据其完整性决定是替换还是重命名。
    3. 带进度复制。
    4. 删除源文件。
    """
    file_name = os.path.basename(source_path)

    try:
        logger.info(f"开始处理: {file_name}")

        # 步骤 1: 检查源文件完整性。如果源文件是坏的，直接跳过。
        if not check_video_integrity(source_path, ffprobe_path, logger):
            logger.warning(f"源文件 '{file_name}' 检查未通过，跳过。")
            return f"跳过 (源文件检查未通过): {file_name}"

        # 步骤 2: 决定最终的目标路径
        destination_path = os.path.join(destination_folder, file_name)

        if os.path.exists(destination_path):
            logger.info(f"目标文件 '{file_name}' 已存在。正在检查其完整性...")
            is_destination_ok = check_video_integrity(destination_path, ffprobe_path, logger)

            if not is_destination_ok:
                logger.warning(f"目标文件 '{file_name}' 已损坏或不完整。将被新文件替换。")
                # 路径保持不变，直接覆盖。先尝试删除旧的损坏文件。
                try:
                    os.remove(destination_path)
                except OSError as e:
                    logger.error(f"无法删除损坏的目标文件 '{destination_path}': {e}。跳过此文件。")
                    return f"失败 (无法删除损坏的目标): {file_name}"
            else:
                logger.info(f"目标文件 '{file_name}' 完好。将为新文件添加后缀。")
                destination_path = get_unique_destination_path(destination_path)
                new_file_name = os.path.basename(destination_path)
                logger.info(f"新文件将被保存为 '{new_file_name}'")

        # 步骤 3: 带进度条地复制文件到最终确定的路径
        final_file_name = os.path.basename(destination_path)
        copy_with_progress(source_path, destination_path, final_file_name)

        if os.path.getsize(source_path) != os.path.getsize(destination_path):
            raise IOError("复制失败：源文件和目标文件大小不匹配。")

        # 步骤 4: 复制成功后，删除源文件
        os.remove(source_path)
        logger.info(f"成功移动: {final_file_name}")
        return f"成功: {final_file_name}"

    except Exception as e:
        logger.error(f"处理 '{file_name}' 时发生严重错误: {e}")
        # 清理可能已创建的部分文件
        if 'destination_path' in locals() and os.path.exists(destination_path):
            os.remove(destination_path)
        return f"失败: {file_name}"


def move_videos_concurrently(source_folder, destination_folder, ffprobe_path, max_workers, logger):
    """
    使用线程池并发地移动视频。
    处理完一批后，会持续监控源文件夹，每60秒检查一次。
    """
    video_extensions = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.mpeg', '.mpg']

    while True:
        tasks_to_process = []
        for root, dirs, files in os.walk(source_folder):
            for file in files:
                if any(file.lower().endswith(ext) for ext in video_extensions):
                    tasks_to_process.append(os.path.join(root, file))

        if not tasks_to_process:
            logger.info("--- 源文件夹中无视频文件。将在 60 秒后重新扫描... ---")
            time.sleep(60)
            continue  # 继续下一次循环，重新扫描

        logger.info(f"--- 新一轮扫描发现 {len(tasks_to_process)} 个视频文件，开始处理... ---")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_video, task, destination_folder, ffprobe_path, logger): task for
                       task in tasks_to_process}

            for future in as_completed(futures):
                result = future.result()
                pass  # 日志记录已在 process_single_video 中完成

        logger.info("--- 本轮所有任务已处理完毕。即将开始新一轮扫描... ---")
        # 循环将立即开始，以检查在处理过程中可能已添加的新文件


if __name__ == "__main__":
    logger = setup_logger()

    # --- 请在这里修改为您自己的路径和配置 ---
    ffprobe_path = r"C:\Other\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffprobe.exe"
    source_directory = "C:/Users/YourUser/Downloads/VideosToProcess"
    destination_directory = "D:/MyVideoLibrary/FinishedVideos"
    MAX_CONCURRENT_MOVES = 3
    # --- 配置结束 ---

    if not os.path.isfile(ffprobe_path):
        logger.critical(f"致命错误：在指定路径找不到 ffprobe.exe 文件: {ffprobe_path}")
        exit()

    logger.info("脚本启动，开始持续监控...")
    logger.info(f"使用 ffprobe 路径: {ffprobe_path}")
    logger.info(f"源文件夹: {source_directory}")
    logger.info(f"目标文件夹: {destination_directory}")
    logger.info(f"最大并发数: {MAX_CONCURRENT_MOVES}")

    if not os.path.exists(destination_directory):
        logger.info(f"目标文件夹不存在，正在创建: {destination_directory}")
        os.makedirs(destination_directory)

    try:
        move_videos_concurrently(source_directory, destination_directory, ffprobe_path, MAX_CONCURRENT_MOVES, logger)
    except KeyboardInterrupt:
        logger.info("脚本被用户手动中断。正在退出...")
        exit()