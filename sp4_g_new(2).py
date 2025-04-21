# _*_ coding: utf-8 _*_
"""
Time:     2025/1/22 17:36
Author:   ZhaoQi Cao(czq)
Version:  V 0.1
File:     sp4_g_new.py
Describe: Write during the python at zgxmt, Github link: https://github.com/caozhaoqi

整体流程

0.扫描已处理log(scan_logs_csv) 生成csv 移动csv中文件至指定路径(move_csv_video)
1.移动文件区分中英文(move_file_ch_en)
2.运行sp4_g_new切割片头片尾
3.获取切割完成数据

"""

import os
import subprocess
import logging
import json
import time
import csv
import shutil
import re
import concurrent.futures
import cv2

# 日志配置
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, 'skip_head_tail.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# 路径配置
root_path = r"G:\航拍特写原始数据分辨率筛选1600#85#2.11\已处理\1080\分辨率合格\Drone China\上方"
output_root = r"H:\0415（106盘处理）"
# RTX 4060专用配置
ffmpeg_path = r"D:\ffmpeg-7.0.2-essentials_build\bin\ffmpeg.exe"
ffprobe_path = r"D:\ffmpeg-7.0.2-essentials_build\bin\ffprobe.exe"
# 配置参数b
batch_size = 15  # 每批处理的视频文件数量
head_cut_time = 60 * 4  # 片头时间（单位：秒）
tail_cut_time = 60 * 2  # 片尾时间（单位：秒）

# CSV 文件路径
csv_file_path = os.path.join(log_dir, 'processed_videos.csv')


def sanitize_filename(filename):
    """清理文件名中的不可见字符"""
    return ''.join(c for c in filename if 32 <= ord(c) < 127)


def is_valid_file(file_path):
    """判断文件是否为有效的视频文件"""
    valid_extensions = ('.mp4', '.avi', '.mov', '.mkv', '.m4v', '.wmv', '.rmvb', '.dat', '.vob')
    exclude_dirs = {'$RECYCLE.BIN', 'System Volume Information', 'Windows', 'Program Files', 'Program Files (x86)'}

    file_name = os.path.basename(file_path)
    file_ext = os.path.splitext(file_name)[1].lower()
    return (
            os.path.isfile(file_path) and
            file_ext in valid_extensions and
            not any(excluded in file_path for excluded in exclude_dirs)
    )


def has_chinese_characters(text):
    """判断字符串是否包含中文"""
    return bool(re.search('[\u4e00-\u9fff]', text))


def get_video_info_ffmpeg(video_file, max_retries=3):
    """使用 ffprobe 获取视频文件信息：帧率、帧数和时长，增加重试机制"""
    for attempt in range(max_retries):
        try:
            # 检查文件是否存在且可读
            if not os.path.exists(video_file):
                logging.error(f"文件不存在: {repr(video_file)}")
                return None

            if not os.access(video_file, os.R_OK):
                logging.error(f"文件不可读: {repr(video_file)}")
                return None

            # 尝试使用 utf-8 编码处理路径
            try:
                encoded_path = video_file.encode('utf-8')
                decoded_path = encoded_path.decode('utf-8')
            except UnicodeEncodeError:
                # 如果失败，使用系统默认编码
                encoded_path = os.fsencode(video_file)
                decoded_path = os.fsdecode(encoded_path)

            # 添加文件大小检查
            file_size = os.path.getsize(video_file)
            if file_size == 0:
                logging.error(f"文件大小为0: {repr(video_file)}")
                return None

            command = [
                str(ffprobe_path),
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_streams',
                '-show_format',  # 添加format信息获取
                decoded_path
            ]
            result = subprocess.run(command, check=False, capture_output=True)
            if result.returncode != 0:
                logging.error(
                    f"ffprobe 执行错误，文件：{repr(video_file)}，尝试次数: {attempt + 1}，错误码：{result.returncode}，stderr：{repr(result.stderr.decode('utf-8', 'ignore'))}, stdout: {repr(result.stdout.decode('utf-8', 'ignore'))}")
                print(
                    f"ffprobe 执行错误，文件：{repr(video_file)}，尝试次数: {attempt + 1}，错误码：{result.returncode}，stderr：{repr(result.stderr.decode('utf-8', 'ignore'))}, stdout: {repr(result.stdout.decode('utf-8', 'ignore'))}")
                continue

            try:
                output = json.loads(result.stdout.decode('utf-8', 'ignore'))
            except json.JSONDecodeError as e:
                logging.error(
                    f"JSON 解析错误，文件：{repr(video_file)}，尝试次数: {attempt + 1}，错误：{e}，ffprobe 输出：{repr(result.stdout.decode('utf-8', 'ignore'))}")
                print(
                    f"JSON 解析错误，文件：{repr(video_file)}，尝试次数: {attempt + 1}，错误：{e}，ffprobe 输出：{repr(result.stdout.decode('utf-8', 'ignore'))}")
                continue

            for stream in output.get('streams', []):
                if stream['codec_type'] == 'video':
                    fps = eval(stream.get('avg_frame_rate', '0'))  # 使用eval处理分数格式
                    duration = float(stream.get('duration', 0))

                    if 'nb_frames' in stream:
                        frames = int(stream['nb_frames'])
                        return fps, frames, duration
                    elif duration > 0 and fps > 0:
                        frames = int(duration * fps)
                        logging.warning(
                            f"文件: {repr(video_file)} 缺少 nb_frames, 使用 duration 和 avg_frame_rate 计算的帧数：{frames}")
                        print(
                            f"文件: {repr(video_file)} 缺少 nb_frames, 使用 duration 和 avg_frame_rate 计算的帧数：{frames}")
                        return fps, frames, duration
                    else:
                        logging.error(f"文件: {repr(video_file)} 缺少 nb_frames 或 duration 或 avg_frame_rate。")
                        print(f"文件: {repr(video_file)} 缺少 nb_frames 或 duration 或 avg_frame_rate。")
                        break

            # 在返回前添加额外验证
            if fps <= 0 or frames <= 0 or duration <= 0:
                logging.error(f"无效的视频参数: fps={fps}, frames={frames}, duration={duration}")
                return None

            return fps, frames, duration

        except Exception as e:
            logging.error(f"获取视频信息时出错，文件：{repr(video_file)}，尝试次数: {attempt + 1}，错误：{str(e)}")
            time.sleep(1)  # 添加延迟后再重试

    return None


# 新增：使用 OpenCV 获取视频信息的函数
def get_video_info_opencv(video_file):
    """使用 OpenCV 获取视频文件信息：帧率、帧数和时长"""
    try:
        cap = cv2.VideoCapture(video_file)
        if not cap.isOpened():
            logging.error(f"无法打开视频文件：{repr(video_file)}")
            return None

        fps = cap.get(cv2.CAP_PROP_FPS)
        frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        duration = frames / fps if fps > 0 else 0

        cap.release()
        return fps, frames, duration
    except Exception as e:
        logging.error(f"使用 OpenCV 获取视频信息时出错，文件：{repr(video_file)}，错误：{e}")
        return None


def skip_head_tail(video_file):
    """跳过视频的片头和片尾"""
    logging.info(f"开始处理：{repr(video_file)}")
    print(f"开始处理：{repr(video_file)}")
    start_time = time.time()

    # # 检查是否已处理
    # if video_file in processed_videos:
    #     logging.info(f"跳过已处理的视频 {repr(video_file)}。")
    #     print(f"跳过已处理的视频 {repr(video_file)}。")
    #     return

    # 检查文件是否存在
    if not os.path.exists(video_file):
        logging.error(f"文件 {repr(video_file)} 不存在。")
        return

    # 根据路径是否包含中文选择处理方式
    video_info = get_video_info_ffmpeg(video_file)
    if video_info is None:
        # 如果 ffprobe 失败，尝试使用 OpenCV
        video_info = get_video_info_opencv(video_file)
    if video_info is None:
        logging.warning(f"跳过视频 {repr(video_file)}，无法获取视频信息。")
        print(f"跳过视频 {repr(video_file)}，无法获取视频信息。")
        return

    fps, frames, duration = video_info
    logging.info(f"视频信息：帧率={fps}, 帧数={frames}, 时长={duration}")
    print(f"视频信息：帧率={fps}, 帧数={frames}, 时长={duration}")

    # 计算有效时长
    effective_duration = duration - head_cut_time - tail_cut_time
    logging.info(f"有效时长：{effective_duration}")
    print(f"有效时长：{effective_duration}")

    if effective_duration <= 0:
        logging.warning(f"视频 {repr(video_file)} 有效时长不足，无法跳过片头片尾。")
        return

    # 构造输出路径
    base_filename = os.path.splitext(os.path.basename(video_file))[0]
    relative_path = os.path.relpath(os.path.dirname(video_file), root_path)
    output_dir = os.path.join(output_root, relative_path, base_filename)
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{base_filename}_no_head_tail.mp4")
    if os.path.exists(output_file):
        logging.info("输出路径已存在")
    else:
        logging.info(f"输出路径: {repr(output_dir)}, 输出文件路径：{repr(output_file)}")
        print(f"输出路径: {repr(output_dir)}, 输出文件路径：{repr(output_file)}")

        # 构建 FFmpeg 命令
        encoded_input = os.fsencode(video_file)
        encoded_output = os.fsencode(output_file)
        command = [
            str(ffmpeg_path),
            '-y',
            '-ss', str(head_cut_time),
            '-i', os.fsdecode(encoded_input),
            '-t', str(effective_duration),
            '-c:v', 'copy',
            '-c:a', 'copy',
            os.fsdecode(encoded_output)
        ]
        logging.info(f"ffmpeg 命令: {repr(' '.join(command))}")
        print(f"ffmpeg 命令: {repr(' '.join(command))}")

        # 执行 FFmpeg 命令
        try:
            subprocess.run(command, check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            logging.info(f"成功处理 {repr(video_file)}，跳过片头 {head_cut_time} 秒，片尾 {tail_cut_time} 秒。")
            print(f"成功处理 {repr(video_file)}，跳过片头 {head_cut_time} 秒，片尾 {tail_cut_time} 秒。")
        except subprocess.CalledProcessError as e:
            logging.error(f"处理 {repr(video_file)} 时出错: {e.stderr.decode('utf-8', 'ignore') or '未知错误'}")
            print(f"处理 {repr(video_file)} 时出错: {e.stderr.decode('utf-8', 'ignore') or '未知错误'}")
        except Exception as e:
            logging.error(f"处理 {repr(video_file)} 时出现未知错误: {e}")
            print(f"处理 {repr(video_file)} 时出现未知错误: {e}")

    # 记录处理完成
    end_time = time.time()
    logging.info(f"{repr(video_file)} 的处理完成，耗时 {end_time - start_time:.2f} 秒")
    print(f"{repr(video_file)} 的处理完成，耗时 {end_time - start_time:.2f} 秒")

    # 将处理完成的视频路径添加到集合
    # processed_videos.add(video_file)


def load_processed_videos():
    """加载已处理的视频路径"""
    processed_videos = set()
    if os.path.exists(csv_file_path):
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row:
                    processed_videos.add(row[0])
    return processed_videos


def save_processed_videos(processed_videos):
    """保存已处理的视频路径"""
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for video_path in processed_videos:
            writer.writerow([video_path])


def main():
    """主程序"""
    # 加载已处理的视频
    # processed_videos = load_processed_videos()
    # 获取视频文件列表
    video_files = []
    for root, _, files in os.walk(root_path):
        for file in files:
            file_path = os.path.join(root, file)
            if is_valid_file(file_path):
                video_files.append(file_path)

    if not video_files:
        logging.warning(f"在目录 {root_path} 中未找到任何视频文件。")
        print(f"在目录 {root_path} 中未找到任何视频文件。")
        return

    # 处理视频文件
    start_main_time = time.time()
    # num_workers = min(os.cpu_count() * 4, len(video_files)) #禁用多线程
    # with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    #   futures = [executor.submit(skip_head_tail, video_file, processed_videos) for video_file in video_files]
    #  for future in concurrent.futures.as_completed(futures):
    #     future.result()
    for video_file in video_files:
        skip_head_tail(video_file)

    # 保存处理过的视频
    # save_processed_videos(processed_videos)

    # 总耗时
    end_main_time = time.time()
    logging.info(f"所有视频文件处理完成，总耗时 {end_main_time - start_main_time:.2f} 秒")
    print(f"所有视频文件处理完成，总耗时 {end_main_time - start_main_time:.2f} 秒")


if __name__ == "__main__":
    main()