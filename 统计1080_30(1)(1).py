import os
import subprocess
import json



def get_video_info(video_path):
    """使用 ffprobe 获取视频的分辨率和时长"""
    ffprobe_path=r"D:\ffmpeg-7.0.2-essentials_build\bin\ffprobe.exe"
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

        # 提取时长
        duration = 0
        if 'format' in info:
            duration = float(info['format'].get('duration', 0))

        return width, height, duration
    except Exception as e:
        print(f"获取视频信息时出错: {e}")
        return None, None, None

def get_file_size(video_path):
    """获取视频文件的大小"""
    try:
        return os.path.getsize(video_path)
    except Exception as e:
        print(f"获取文件大小时出错: {e}")
        return 0


def process_videos_in_directory(directory):
    total_count = 0
    total_size = 0
    total_duration = 0

    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.mp4', '.avi', '.mkv', '.mov', '.flv', '.wmv', '.webm')):
                video_path = os.path.join(root, file)
                width, height, duration = get_video_info(video_path)
                if width is not None and height is not None and duration is not None:
                    short_edge = min(width, height)
                    if short_edge >= 1080 and duration >= 5:
                        size = get_file_size(video_path)
                        total_count += 1
                        total_size += size
                        total_duration += duration
                        print(
                            f"视频: {video_path}，分辨率: {width}x{height}，时长: {duration:.2f}秒，大小: {size / (1024 * 1024):.2f} MB")


    print(f"\n符合条件的视频总数: {total_count}")
    print(f"符合条件的视频总大小: {total_size / (1024 * 1024 * 1024):.2f} GB")
    print(f"符合条件的视频总时长: {total_duration / 3600:.2f} 小时")


# 文件夹路径
directory = r"H:\0421音效乐器采集\youtube\20250421"
from datetime import datetime

# 记录程序开始时间
start_time = datetime.now()
process_videos_in_directory(directory)
end_time = datetime.now()
print(f"程序开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"程序结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

# 计算并打印运行时长
duration = end_time - start_time
print(f"程序运行时长: {duration}")

