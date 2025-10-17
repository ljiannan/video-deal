

import logging
import os
import subprocess
import json
from datetime import datetime
import concurrent.futures
from queue import Queue
from tqdm import tqdm

# 日志配置
log_dir = "./logs"
log_file = os.path.join(log_dir, "count_1080_30_2.log")
max_log_size = 10 * 1024 * 1024  # 10MB
backup_count = 5

os.makedirs(log_dir, exist_ok=True)
from logging.handlers import RotatingFileHandler

# 日志去重
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    log_handler = RotatingFileHandler(
        log_file, maxBytes=max_log_size, backupCount=backup_count, encoding='utf-8'
    )
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_format)
    logger.addHandler(log_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_format)
    logger.addHandler(stream_handler)

def get_video_info(video_path, ffprobe_path, retry=2):
    """使用 ffprobe 获取视频的分辨率和时长，失败自动重试"""
    for attempt in range(retry):
        try:
            command = [
                ffprobe_path, '-v', 'error',
                '-show_entries', 'format=duration',
                '-show_entries', 'stream=width,height',
                '-of', 'json', video_path
            ]
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
            info = json.loads(result.stdout)
            width, height = 0, 0
            if 'streams' in info:
                for stream in info['streams']:
                    if 'width' in stream and 'height' in stream:
                        width = stream.get('width', 0)
                        height = stream.get('height', 0)
                        break
            duration = 0
            if 'format' in info and 'duration' in info['format']:
                duration = float(info['format']['duration'])
            return width, height, duration
        except Exception as e:
            if attempt == retry - 1:
                logger.error(f"获取视频信息时出错: {e} ({video_path})")
    return None, None, None

def get_file_size(video_path):
    """获取视频文件的大小"""
    try:
        return os.path.getsize(video_path)
    except Exception as e:
        logger.error(f"获取文件大小时出错: {e} ({video_path})")
        return 0

def process_video(video_path, ffprobe_path, result_queue):
    """处理单个视频（线程安全）"""
    try:
        width, height, duration = get_video_info(video_path, ffprobe_path)
        if width is not None and height is not None and duration is not None:
            short_edge = min(width, height)
            if short_edge >= 1080 and duration >= 5:
                size = get_file_size(video_path)
                result_queue.put((size, duration))
                logger.info(
                    f"视频: {video_path}，分辨率: {width}x{height}，时长: {duration:.2f}秒，大小: {size / (1024 * 1024):.2f} MB"
                )
    except Exception as e:
        logger.error(f"处理视频时出错: {e} ({video_path})")

def process_videos_in_directory(directory, ffprobe_path, max_workers=8):
    video_paths = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith((
                '.mp4', '.avi', '.mkv', '.mov', '.flv', '.wmv', '.webm', '.mpg', '.mpeg', '.3gp', '.ts', '.m2ts',
                '.mts', '.vob', '.rm', '.rmvb', '.f4v', '.m4v', '.asf', '.m2v', '.divx', '.xvid', '.ogv', '.amv',
                '.yuv', '.h264', '.264', '.dat', '.mxf', '.mpv', '.nsv', '.drc', '.mng', '.roq', '.svi', '.viv',
                '.qt', '.trp', '.bik', '.bik2', '.evo', '.wtv', '.pva', '.mpe', '.mp1', '.mp2', '.mp4v', '.mpg4',
                '.lrv', '.fli', '.flc', '.f4p', '.f4a', '.f4b', '.ismv', '.m2p', '.m2t', '.m1v', '.m1a', '.m2a',
                '.m4e', '.mjp', '.mjpeg', '.mj2', '.nut', '.tsv', '.vdat', '.vp3', '.vp6', '.vp7', '.webm', '.y4m'
            )):
                video_paths.append(os.path.join(root, file))
    video_paths.sort()  # 保证顺序一致

    total_count = 0
    total_size = 0
    total_duration = 0
    failed_files = []

    result_queue = Queue()
    logger.info(f"共发现 {len(video_paths)} 个视频文件，开始处理...")

    def safe_process(video_path, ffprobe_path, result_queue):
        try:
            width, height, duration = get_video_info(video_path, ffprobe_path)
            if width is not None and height is not None and duration is not None:
                short_edge = min(width, height)
                if short_edge >= 1080 and duration >= 5:
                    size = get_file_size(video_path)
                    result_queue.put((size, duration))
                    logger.info(
                        f"视频: {video_path}，分辨率: {width}x{height}，时长: {duration:.2f}秒，大小: {size / (1024 * 1024):.2f} MB"
                    )
            else:
                failed_files.append(video_path)
        except Exception as e:
            logger.error(f"处理视频时出错: {e} ({video_path})")
            failed_files.append(video_path)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(safe_process, video_path, ffprobe_path, result_queue)
            for video_path in video_paths
        ]
        for _ in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc='处理进度'):
            pass

    while not result_queue.empty():
        size, duration = result_queue.get()
        total_count += 1
        total_size += size
        total_duration += duration

    logger.info(f"符合条件的视频总数: {total_count}")
    logger.info(f"符合条件的视频总大小: {total_size / (1024 * 1024 * 1024):.2f} GB")
    logger.info(f"符合条件的视频总时长: {total_duration / 3600:.2f} 小时")
    if total_count > 0:
        logger.info(f"平均单个视频大小: {total_size / total_count / (1024 * 1024):.2f} MB")
        logger.info(f"平均单个视频时长: {total_duration / total_count:.2f} 秒")
    logger.info(f"扫描到的视频总数: {len(video_paths)}，成功统计: {total_count}，失败: {len(failed_files)}")
    if failed_files:
        with open('failed_videos.txt', 'w', encoding='utf-8') as f:
            for file in failed_files:
                f.write(file + '\n')
        logger.info(f"失败文件已保存到 failed_videos.txt")

def main():
    directory = r"Z:\项目\航拍特写\李建楠\8.06"
    ffprobe_path = r"D:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe"
    max_workers = 24  # 线程数，可根据CPU核心数调整

    logger.info(f"检测目录是否存在: {directory} -> {os.path.exists(directory)}")
    if not os.path.exists(directory):
        logger.error(f"指定目录不存在: {directory}")
        return

    if not os.path.exists(ffprobe_path):
        logger.error(f"ffprobe 路径不存在: {ffprobe_path}")
        return

    start_time = datetime.now()
    logger.info(f"程序开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"使用 {max_workers} 个线程并行处理")

    process_videos_in_directory(directory, ffprobe_path, max_workers)

    end_time = datetime.now()
    logger.info(f"程序结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"程序运行时长: {end_time - start_time}")

if __name__ == "__main__":
    main()