import os
from logging.handlers import RotatingFileHandler

import cv2  # OpenCV库用于读取视频文件
import shutil  # 用于复制文件
from tqdm import tqdm  # 用于显示进度条
from concurrent.futures import ThreadPoolExecutor, as_completed  # 用于并行处理
from typing import List, Optional, Dict
import logging
import time  # 用于添加延迟

# 设置日志
log_dir = "./logs"
log_file = os.path.join(log_dir, "10-120-duration-time.log")
max_log_size = 10 * 1024 * 1024  # 10MB
backup_count = 5  # 最多保留5个日志文件

# 创建logs目录，如果不存在
os.makedirs(log_dir, exist_ok=True)
# 创建 RotatingFileHandler 实例
log_handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count, encoding='utf-8')
# 设置日志格式
log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_format)
# 获取 logger 实例
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# 添加 handler
logger.addHandler(log_handler)

# 添加控制台输出
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_format)
logger.addHandler(stream_handler)

# 可选的全局延迟变量
GLOBAL_DELAY = 0  # 秒，可以根据需要调整


def get_video_duration(video_path: str, cache: Dict[str, Optional[float]]) -> Optional[float]:
    """
    获取视频文件的时长（秒）。使用缓存避免重复读取视频时长。

    :param video_path: 视频文件的路径
    :param cache: 用于缓存视频时长的字典
    :return: 视频时长，单位为秒；如果无法读取时长，返回None
    """
    if video_path in cache:
        logger.debug(f"从缓存中获取视频时长: {video_path}")
        return cache[video_path]  # 如果视频时长已缓存，直接返回

    try:
        cap = cv2.VideoCapture(video_path)
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
        duration = frame_count / fps if fps > 0 else 0
        cap.release()
        cache[video_path] = duration  # 缓存视频时长
        logger.debug(f"计算视频时长: {video_path}, 时长: {duration}")
        return duration
    except Exception as e:
        logger.error(f"无法读取视频 {video_path}: {e}")
        cache[video_path] = None
        return None


def find_videos_within_duration(directory: str, min_duration: float, max_duration: float) -> List[str]:
    """
    递归查找目录下时长在指定范围内的视频文件

    :param directory: 要查找的视频文件所在目录
    :param min_duration: 视频的最小时长，单位秒
    :param max_duration: 视频的最大时长，单位秒
    :return: 符合条件的视频文件路径列表
    """
    videos = []
    video_cache: Dict[str, Optional[float]] = {}  # Cache video durations
    logger.info(f"开始在目录 {directory} 中查找时长在 {min_duration}-{max_duration} 秒之间的视频")  # Log start

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.mp4', '.avi', '.mov', '.mkv', '.flv')):
                video_path = os.path.join(root, file)
                logger.debug(f"正在检查文件: {video_path}")
                duration = get_video_duration(video_path, video_cache)
                if duration is not None and min_duration <= duration <= max_duration:
                    videos.append(video_path)
                    logger.info(f"找到符合条件的视频: {video_path}, 时长: {duration}")
                else:
                    logger.debug(
                        f"视频 {video_path} 时长不符合条件 (时长: {duration if duration is not None else '未知'})")
    logger.info(f"在目录 {directory} 中找到 {len(videos)} 个符合条件的视频")  # Log end
    return videos


def move_video(video_path: str, target_folder: str, delay: float = 0) -> bool:
    """
    移动单个视频文件到目标文件夹，并添加可选的延迟。

    :param video_path: 视频文件的路径
    :param target_folder: 目标文件夹路径
    :param delay: 移动操作后的延迟时间，单位为秒，默认为0
    :return: 如果移动成功返回True，否则返回False
    """
    try:
        video_name = os.path.basename(video_path)
        target_path = os.path.join(target_folder, video_name)

        logger.info(f"正在移动视频: {video_path} 到 {target_path}")
        # 移动文件
        shutil.move(video_path, target_path)
        logger.info(f"成功移动 {video_path} 到 {target_path}")

        # 添加延迟
        if delay > 0:
            logger.info(f"添加延迟 {delay} 秒")
            time.sleep(delay)
            logger.info(f"延迟结束")

        return True
    except Exception as e:
        logger.error(f"移动视频文件 {video_path} 时出错: {e}")
        return False


def process_videos(videos: List[str], target_folder: str, delay: float = 0) -> None:  # 添加延迟参数
    """
    处理视频文件，检查时长并移动符合条件的视频到目标文件夹

    :param videos: 视频文件路径列表
    :param target_folder: 目标文件夹路径
    :param delay: 移动操作后的延迟时间，单位为秒，默认为0
    """
    logger.info(f"开始处理 {len(videos)} 个视频，目标目录: {target_folder}, 延迟: {delay} 秒")

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(move_video, video, target_folder, delay): video for video in videos}  # Map future to video

        # 使用 tqdm 显示进度条
        for future in tqdm(as_completed(futures), total=len(videos), desc="移动视频", unit="个"):
            video = futures[future]  # Get the video associated with the completed future
            try:
                future.result()  # 确保捕获异常
            except Exception as e:
                logger.error(f"处理视频 {video} 时出错: {e}")
    logger.info("所有视频处理完成。")


def main(source_directory: str, target_directory: str, min_duration: float = 5, max_duration: float = 120,
         delay: float = 0) -> None:  # 添加延迟参数
    """
    主函数：查找指定时长范围内的视频文件并移动到目标文件夹

    :param source_directory: 源目录，包含要筛选的视频文件
    :param target_directory: 目标目录，用于保存符合条件的视频文件
    :param min_duration: 视频文件的最小时长，单位秒，默认10秒
    :param max_duration: 视频文件的最大时长，单位秒，默认120秒
    :param delay: 移动操作后的延迟时间，单位为秒，默认为0
    """
    logger.info(f"开始处理，源目录: {source_directory}, 目标目录: {target_directory}, "
                f"时长范围: {min_duration}-{max_duration} 秒, 延迟: {delay} 秒")

    # 查找符合条件的视频文件
    videos_to_move = find_videos_within_duration(source_directory, min_duration, max_duration)

    if not videos_to_move:
        logger.info("没有找到符合条件的视频文件。")
        return

    # 将符合条件的视频移动到目标文件夹
    process_videos(videos_to_move, target_directory, delay)

    logger.info("处理完成。")


if __name__ == "__main__":
    source_directory = r"E:\pexels\分辨率合格"  # 请替换为你的源目录路径
    target_directory = r"E:\pexels\合格"# 请替换为目标文件夹路径
    move_delay = GLOBAL_DELAY  # 使用全局延迟或根据需要修改

    os.makedirs(target_directory, exist_ok=True)
    logger.info(f"目标目录已创建或已存在: {target_directory}")

    # 调用主函数
    main(source_directory, target_directory, delay=move_delay)  # 传递延迟参数