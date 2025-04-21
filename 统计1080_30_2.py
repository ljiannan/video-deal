import logging
import os
import subprocess
import json
from dataclasses import fields
from logging.handlers import RotatingFileHandler
from datetime import datetime

# 日志配置
log_dir = "./logs"
log_file = os.path.join(log_dir, "count_1080_30_2.log")
max_log_size = 10 * 1024 * 1024  # 10MB
backup_count = 5  # 最多保留5个日志文件

# 创建logs目录，如果不存在
os.makedirs(log_dir, exist_ok=True)

# 创建 RotatingFileHandler 实例
log_handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count, encoding='utf-8')
log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_format)

# 获取 logger 实例
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# 添加控制台输出
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_format)
logger.addHandler(stream_handler)


def get_video_info(video_path, ffprobe_path):
    """使用 ffprobe 获取视频的分辨率和时长"""
    try:
        command = [
            ffprobe_path, '-v', 'error',
            '-show_entries', 'format=duration',
            '-show_entries', 'stream=width,height',
            '-of', 'json', video_path
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        info = json.loads(result.stdout)

        # 提取分辨率
        width, height = 0, 0
        if 'streams' in info:
            for stream in info['streams']:
                if 'width' in stream and 'height' in stream:
                    width = stream.get('width', 0)
                    height = stream.get('height', 0)
                    break  # 只取第一个有效的流

        # 提取时长
        duration = 0
        if 'format' in info and 'duration' in info['format']:
            duration = float(info['format']['duration'])

        return width, height, duration
    except FileNotFoundError:
        logger.error(f"错误: ffprobe 可执行文件未找到在: {ffprobe_path}")
        return None, None, None
    except Exception as e:
        logger.error(f"获取视频信息时出错: {e}")
        return None, None, None


def get_file_size(video_path):
    """获取视频文件的大小"""
    try:
        return os.path.getsize(video_path)
    except Exception as e:
        logger.error(f"获取文件大小时出错: {e}")
        return 0


def process_videos_in_directory(directory, ffprobe_path):
    """处理目录中的视频文件"""
    total_count = 0
    total_size = 0
    total_duration = 0

    for root, _, files in os.walk(directory):
        for file in files:
            if file.startswith('._') or file == '.DS_Store':
                continue
            elif file.lower().endswith(('.mp4', '.avi', '.mkv', '.mov', '.flv', '.wmv', '.webm')):
                video_path = os.path.join(root, file)
                width, height, duration = get_video_info(video_path, ffprobe_path)
                if width is not None and height is not None and duration is not None:
                    short_edge = min(width, height)
                    if short_edge >= 720 and duration >= 0:
                        size = get_file_size(video_path)
                        total_count += 1
                        total_size += size
                        total_duration += duration
                        logger.info(
                            f"视频: {video_path}，分辨率: {width}x{height}，时长: {duration:.2f}秒，大小: {size / (1024 * 1024):.2f} MB")

    logger.info(f"\n符合条件的视频总数: {total_count}")
    logger.info(f"符合条件的视频总大小: {total_size / (1024 * 1024 * 1024):.2f} GB")
    logger.info(f"符合条件的视频总时长: {total_duration / 3600:.2f} 小时")


# 主程序
if __name__ == "__main__":
    # 文件夹路径
    directory = r'F:\ELSE\片头片尾待质检\待质检'
    ffprobe_path = r"D:\ffmpeg-7.0.2-essentials_build\bin\ffprobe.exe"

    # 记录程序开始时间
    start_time = datetime.now()
    logger.info(f"程序开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 处理视频文件
    process_videos_in_directory(directory, ffprobe_path)

    # 记录程序结束时间
    end_time = datetime.now()
    logger.info(f"程序结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 计算并打印运行时长
    duration = end_time - start_time
    logger.info(f"程序运行时长: {duration}")