import os
import shutil
import subprocess
import re
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

        return width, height,duration
    except Exception as e:
        print(f"获取视频信息时出错: {e}")
        return None, None

def copy_high_res_videos(filepath, outputfile):
    try:
        """复制高分辨率视频到指定目录的同名子文件夹中"""
        for root, dirs, files in os.walk(filepath):
            # 获取当前子文件夹相对于filepath的相对路径
            rel_path = os.path.relpath(root, filepath)
            # 构建outputfile中对应的子文件夹路径
            output_subfolder = os.path.join(outputfile, rel_path)

            # 如果该子文件夹在outputfile中不存在，则创建它
            if not os.path.exists(output_subfolder):
                os.makedirs(output_subfolder)

            for file in files:
                if file.lower().endswith(('.mp4', '.avi', '.mkv', '.mov', '.flv', '.wmv', '.webm','.ts','.m2ts')):
                    video_path = os.path.join(root, file)
                    print(f"当前处理视频:{video_path}")
                    width, height,duration = get_video_info(video_path)
                    if min(width,height)< 1080 or duration < 10 :
                        # 构建outputfile中对应文件的完整路径
                        output_video_path = os.path.join(output_subfolder, file)
                        shutil.move(video_path, output_video_path)
                        print(f"\033[31mMoved {video_path} to {output_video_path}\033[0m")
    except Exception as e:
        print(str(e))


filepath = r"F:\ELSE\片头片尾待质检"  # 修改为你的视频文件夹路径
outputfile = r"F:\ELSE\片头片尾待质检1080下"  # 修改为你的输出文件夹路径
if not os.path.exists(outputfile):
    os.makedirs(outputfile)

from datetime import datetime

# 记录程序开始时间
start_time = datetime.now()
copy_high_res_videos(filepath, outputfile)
end_time = datetime.now()
print(f"程序开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"程序结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

# 计算并打印运行时长
duration = end_time - start_time
print(f"程序运行时长: {duration}")
