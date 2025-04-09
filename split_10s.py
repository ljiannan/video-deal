"""视频长度大于等于20秒，则切成十秒十秒一个的视频"""
import os
import subprocess
import logging
from pathlib import Path
from datetime import timedelta

# 配置
FFMPEG_PATH = r"D:\ffmpeg-7.0.2-essentials_build\bin\ffmpeg.exe"
FFPROBE_PATH = r"D:\ffmpeg-7.0.2-essentials_build\bin\ffprobe.exe"
INPUT_DIRECTORY = r"E:\乐器演奏\youtube\20250409\合格\钢琴"
OUTPUT_DIRECTORY = r"E:\乐器演奏\youtube\20250409\合格\钢琴out"
LOG_FILE = r"C:\Users\DELL\Desktop\cam-prcess-data-1\src\spider\logs\split.log"
panduan_time = 180
qiege_time = 90

def setup_logging(log_file):
    """配置日志(UTF-8编码)"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


def get_video_duration(ffprobe_path, input_video):
    """获取视频时长(秒)"""
    cmd = [
        ffprobe_path, '-v', 'error', '-show_entries',
        'format=duration', '-of',
        'default=noprint_wrappers=1:nokey=1', input_video
    ]
    try:
        duration = float(subprocess.check_output(cmd).decode('utf-8').strip())
        return duration
    except subprocess.CalledProcessError as e:
        logging.error(f"获取视频时长失败: {input_video}, 错误: {e}")
        return 0
    except Exception as e:
        logging.error(f"处理视频时长时发生意外错误: {input_video}, 错误: {e}")
        return 0


def split_single_segment(ffmpeg_path, input_video, output_path, start_time, duration):
    """切割单个视频片段"""
    cmd = [
        ffmpeg_path, '-ss', str(start_time), '-i', input_video,
        '-c', 'copy', '-t', str(duration), output_path
    ]
    try:
        subprocess.run(cmd, check=True, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"切割片段失败: {input_video} (从{start_time}开始), 错误: {e.stderr.decode('utf-8')}")
        return False


def process_single_video(ffmpeg_path, ffprobe_path, input_video, output_dir):
    """处理单个视频文件"""
    video_name = Path(input_video).stem
    segment_num = 1
    processed_duration = 0
    total_duration = get_video_duration(ffprobe_path, input_video)

    if total_duration < 10:  # 如果视频总长度小于10秒，直接跳过
        logging.info(f"视频 {input_video} 时长不足10秒，跳过处理")
        return 0

    while processed_duration < total_duration:
        remaining_duration = total_duration - processed_duration

        # 确定当前要切割的时长
        if remaining_duration >= panduan_time: #判断视频长度是否大于指定秒数
            current_duration = qiege_time  # 每次切成多少秒
        else:
            current_duration = remaining_duration  # 剩余部分全部输出

        # 生成输出文件名
        output_path = os.path.join(output_dir, f"{video_name}_{segment_num:03d}.mp4")

        # 切割片段
        if split_single_segment(ffmpeg_path, input_video, output_path,
                              processed_duration, current_duration):
            processed_duration += current_duration
            segment_num += 1
            logging.info(f"已切割片段: {output_path} (时长: {current_duration:.2f}秒)")
        else:
            break

    return processed_duration


def process_videos(ffmpeg_path, ffprobe_path, input_dir, output_base_dir):
    """处理目录中的所有视频"""
    os.makedirs(output_base_dir, exist_ok=True)

    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.lower().endswith(('.mp4', '.avi', '.mov', '.mkv', '.flv')):
                input_path = os.path.join(root, file)
                total_duration = get_video_duration(ffprobe_path, input_path)

                if total_duration >= panduan_time:  # 只要视频长度≥10秒就处理
                    # 为每个视频创建单独的输出文件夹
                    video_name = Path(file).stem
                    output_dir = os.path.join(output_base_dir, video_name)
                    os.makedirs(output_dir, exist_ok=True)

                    logging.info(f"开始处理视频: {input_path} (总时长: {total_duration:.2f}秒)")

                    processed = process_single_video(ffmpeg_path, ffprobe_path, input_path, output_dir)

                    logging.info(
                        f"完成处理视频: {input_path}, 已处理时长: {processed:.2f}秒, 剩余时长: {total_duration - processed:.2f}秒")


if __name__ == "__main__":


    # 初始化日志
    setup_logging(LOG_FILE)

    try:
        logging.info("====== 开始视频分割任务 ======")
        process_videos(FFMPEG_PATH, FFPROBE_PATH, INPUT_DIRECTORY, OUTPUT_DIRECTORY)
        logging.info("====== 视频分割任务完成 ======")
    except Exception as e:
        logging.error(f"任务执行失败: {str(e)}", exc_info=True)