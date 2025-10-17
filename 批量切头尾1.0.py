# _*_ coding: utf-8 _*_
"""
Time:     2025/8/27
Author:   L
Version:  V 0.1
File:     sp4_g_new.py

优化版本：支持断点续传和自动进度记录

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
import threading
import signal
import sys
from tqdm import tqdm
from datetime import datetime

PROGRESS_FOLDER = r"Z:\personal_folder\L\去片头片尾处理完数据"
# 日志配置
log_dir = PROGRESS_FOLDER  # 使用集中化进度记录文件夹
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, 'skip_head_tail.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# 路径配置
root_path = r"Z:\a项目\航拍特写\李建楠\8.10"
output_root = r"Z:\a项目\航拍特写\李建楠\成品"
# RTX 4060专用配置
ffmpeg_path = r"D:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe"
ffprobe_path = r"D:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffprobe.exe"
# 配置参数
batch_size = 15  # 每批处理的视频文件数量
head_cut_time = 60 * 1  # 片头时间（单位：秒）
tail_cut_time = 60 * 1  # 片尾时间（单位：秒）
MAX_WORKERS = 4  # 最大并行线程数，可根据机器调整（建议CPU核心数或磁盘带宽决定）


# ========== 并发自适应辅助 ==========
def is_network_path(path: str) -> bool:
    """粗略判断是否为网络路径/映射盘。UNC 路径或常见网络盘符(Z/Y/X/W)视为网络路径。"""
    try:
        if not path:
            return False
        if path.startswith('\\\\'):
            return True
        drive, _ = os.path.splitdrive(path)
        return drive.upper().startswith(('Z:', 'Y:', 'X:', 'W:'))
    except Exception:
        return False


def estimate_avg_input_size_gb(files):
    """抽样估算输入平均大小(GB)，最多采样20个文件。"""
    if not files:
        return 0.0
    sample = files[:min(20, len(files))]
    total = 0
    counted = 0
    for p in sample:
        try:
            if os.path.exists(p):
                total += os.path.getsize(p)
                counted += 1
        except Exception:
            pass
    if counted == 0:
        return 0.0
    return total / counted / (1024 ** 3)


def compute_adaptive_workers(video_files, output_dir_root: str) -> int:
    """根据输出位置与输入规模自适应选择并发。网络盘与超大文件降低并发。"""
    cpu = os.cpu_count() or 4
    net = is_network_path(output_dir_root)
    avg_gb = estimate_avg_input_size_gb(video_files)

    if net:
        base = 1 if avg_gb >= 2 else 2
    else:
        base = max(1, min(MAX_WORKERS, max(2, cpu // 2)))

    if avg_gb >= 8:
        base = max(1, base - 2)
    elif avg_gb >= 4:
        base = max(1, base - 1)

    return max(1, min(base, len(video_files)))


# CSV 文件路径
csv_file_path = os.path.join(PROGRESS_FOLDER, 'processed_videos.csv')

# ========= 断点续传与进度管理 =========
# 集中化进度记录配置
# PROGRESS_FOLDER = r"Z:\personal_folder\L\去片头片尾处理完数据"  # 进度记录文件夹
PROGRESS_FILE = os.path.join(PROGRESS_FOLDER, "sp4_progress.json")  # 进度记录文件
TEMP_PROGRESS_FILE = os.path.join(PROGRESS_FOLDER, "sp4_progress.tmp")  # 临时进度文件
progress_save_lock = threading.Lock()

# 全局进度条位置计数器
global progress_bar_counter
progress_bar_counter = 0
progress_bar_lock = threading.Lock()


class ProgressManager:
    def __init__(self, progress_file: str = PROGRESS_FILE, temp_file: str = TEMP_PROGRESS_FILE):
        self.progress_file = progress_file
        self.temp_file = temp_file

        # 自动创建进度记录文件夹
        self.ensure_progress_folder()

        self.progress_data = self.load_progress()

    def ensure_progress_folder(self):
        """确保进度记录文件夹存在"""
        progress_folder = os.path.dirname(self.progress_file)
        if not os.path.exists(progress_folder):
            try:
                os.makedirs(progress_folder, exist_ok=True)
                logging.info(f"已创建进度记录文件夹: {progress_folder}")
                print(f"✅ 已创建进度记录文件夹: {progress_folder}")
            except Exception as e:
                logging.error(f"创建进度记录文件夹失败: {e}")
                print(f"❌ 创建进度记录文件夹失败: {e}")
                # 如果无法创建指定文件夹，回退到当前目录
                fallback_folder = os.path.dirname(os.path.abspath(__file__))
                self.progress_file = os.path.join(fallback_folder, "sp4_progress.json")
                self.temp_file = os.path.join(fallback_folder, "sp4_progress.tmp")
                logging.warning(f"回退到当前目录: {fallback_folder}")
                print(f"⚠️  回退到当前目录: {fallback_folder}")
        else:
            logging.info(f"进度记录文件夹已存在: {progress_folder}")
            print(f"✅ 进度记录文件夹已存在: {progress_folder}")

    def load_progress(self):
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logging.info(
                        f"加载进度: 已完成 {len(data.get('completed', []))}, 处理中 {len(data.get('processing', []))}, 失败 {len(data.get('failed', []))}")
                    return data
        except Exception as e:
            logging.warning(f"加载进度失败: {e}")
        return {'completed': [], 'processing': [], 'failed': [], 'start_time': None}

    def save_progress(self):
        # 线程锁 + 重试，避免 Windows 下文件占用导致的保存失败
        with progress_save_lock:
            max_retries = 3
            retry_delay = 0.5
            for attempt in range(max_retries):
                try:
                    with open(self.temp_file, 'w', encoding='utf-8') as f:
                        json.dump(self.progress_data, f, ensure_ascii=False, indent=2)
                    if os.path.exists(self.progress_file):
                        os.remove(self.progress_file)
                    os.rename(self.temp_file, self.progress_file)
                    return
                except (OSError, PermissionError) as e:
                    if attempt < max_retries - 1:
                        logging.warning(f"保存进度重试 {attempt + 1}/{max_retries}: {e}")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        logging.error(f"保存进度失败(已重试 {max_retries} 次): {e}")
                        return

    def mark_processing(self, video_path: str):
        name = os.path.basename(video_path)
        if name not in self.progress_data['processing']:
            self.progress_data['processing'].append(name)
        self.save_progress()

    def mark_completed(self, video_path: str):
        name = os.path.basename(video_path)
        if name not in self.progress_data['completed']:
            self.progress_data['completed'].append(name)
        if name in self.progress_data['processing']:
            self.progress_data['processing'].remove(name)
        # 从失败中移除同名
        self.progress_data['failed'] = [f for f in self.progress_data['failed'] if f.get('name') != name]
        self.save_progress()

    def mark_failed(self, video_path: str, error_msg: str = ""):
        name = os.path.basename(video_path)
        if name not in [f.get('name') for f in self.progress_data['failed']]:
            self.progress_data['failed'].append({'name': name, 'error': error_msg, 'time': datetime.now().isoformat()})
        if name in self.progress_data['processing']:
            self.progress_data['processing'].remove(name)
        self.save_progress()

    def is_completed(self, video_path: str) -> bool:
        name = os.path.basename(video_path)
        return name in self.progress_data['completed']

    def is_processing(self, video_path: str) -> bool:
        name = os.path.basename(video_path)
        return name in self.progress_data['processing']

    def get_completed_count(self):
        return len(self.progress_data.get('completed', []))

    def get_processing_count(self):
        return len(self.progress_data.get('processing', []))

    def get_failed_count(self):
        return len(self.progress_data.get('failed', []))

    def set_start_time(self):
        if not self.progress_data.get('start_time'):
            self.progress_data['start_time'] = datetime.now().isoformat()
            self.save_progress()

    def print_summary(self):
        c = self.get_completed_count()
        p = self.get_processing_count()
        f = self.get_failed_count()
        logging.info(f"进度摘要: 已完成 {c}, 处理中 {p}, 失败 {f}")

        if f > 0:
            logging.info("失败的文件:")
            for fail_info in self.progress_data.get('failed', []):
                logging.info(f"  - {fail_info['name']}: {fail_info['error']}")


progress_manager = ProgressManager()


def concat_mp4_files(file_list, output_path):
    """使用 concat demuxer 无损拼接多个 mp4 片段。file_list 为绝对路径列表。"""
    concat_dir = os.path.dirname(output_path)
    os.makedirs(concat_dir, exist_ok=True)
    list_file = os.path.join(concat_dir, 'concat_list.txt')
    with open(list_file, 'w', encoding='utf-8') as f:
        for p in file_list:
            safe_path = p.replace('\\', '/').replace("'", "'\\''")
            f.write(f"file '{safe_path}'\n")
    cmd = [str(ffmpeg_path), '-y', '-f', 'concat', '-safe', '0', '-i', list_file, '-c', 'copy', output_path]
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
    if proc.returncode != 0:
        raise RuntimeError(f"拼接失败: {proc.stderr}")
    try:
        os.remove(list_file)
    except Exception:
        pass


def _signal_handler(signum, frame):
    logging.info("收到退出信号，保存进度...")
    progress_manager.save_progress()
    logging.info("进度已保存，安全退出。")
    try:
        sys.exit(0)
    except SystemExit:
        pass


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def get_media_duration_seconds(media_path: str) -> float:
    """使用 ffprobe 获取媒体时长(秒)，失败返回 0.0"""
    try:
        if not os.path.exists(media_path) or os.path.getsize(media_path) < 1024:
            return 0.0
        # 使用最轻量输出格式，避免JSON解析与多余信息
        cmd = [
            str(ffprobe_path),
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            media_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=20)
        if result.returncode != 0:
            return 0.0
        val = result.stdout.strip()
        return float(val) if val else 0.0
    except Exception:
        return 0.0


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


def get_input_effective_duration_seconds(video_file: str) -> float:
    """快速获取输入视频的有效时长(秒)：总时长-片头-片尾。失败返回0。"""
    total = get_media_duration_seconds(video_file)
    if total <= 0:
        return 0.0
    return max(0.0, total - head_cut_time - tail_cut_time)


# 移除 OpenCV 路径，减少依赖与潜在卡顿


def skip_head_tail(video_file, video_idx=0, total_videos=1):
    """跳过视频的片头和片尾，支持断点续传"""
    # 使用全局计数器分配进度条位置
    global progress_bar_counter, progress_bar_lock
    with progress_bar_lock:
        current_position = progress_bar_counter
        progress_bar_counter += 1

    filename = os.path.basename(video_file)
    pbar = tqdm(total=100, desc=f"视频 {video_idx + 1}/{total_videos}: {filename[:25]:<25}",
                position=current_position + 1,
                leave=True,
                bar_format='{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]')

    logging.info(f"开始处理：{repr(video_file)}")
    print(f"开始处理：{repr(video_file)}")
    start_time = time.time()

    # 检查是否已处理
    if progress_manager.is_completed(video_file):
        logging.info(f"跳过已处理的视频 {repr(video_file)}。")
        print(f"跳过已处理的视频 {repr(video_file)}。")
        pbar.update(100)
        pbar.set_postfix_str("已跳过✓")
        pbar.close()
        return True

    # 检查文件是否存在
    if not os.path.exists(video_file):
        logging.error(f"文件 {repr(video_file)} 不存在。")
        pbar.set_postfix_str("文件不存在✗")
        pbar.close()
        return False

    # 快速获取时长
    duration = get_media_duration_seconds(video_file)
    if duration <= 0:
        logging.warning(f"跳过视频 {repr(video_file)}，无法获取视频时长。")
        print(f"跳过视频 {repr(video_file)}，无法获取视频时长。")
        progress_manager.mark_failed(video_file, "无法获取视频时长")
        pbar.set_postfix_str("无法获取时长✗")
        pbar.close()
        return False
    logging.info(f"视频信息：时长={duration}")
    print(f"视频信息：时长={duration}")

    # 计算有效时长
    effective_duration = duration - head_cut_time - tail_cut_time
    logging.info(f"有效时长：{effective_duration}")
    print(f"有效时长：{effective_duration}")

    if effective_duration <= 0:
        logging.warning(f"视频 {repr(video_file)} 有效时长不足，无法跳过片头片尾。")
        progress_manager.mark_failed(video_file, "有效时长不足")
        pbar.set_postfix_str("有效时长不足✗")
        pbar.close()
        return False

    # 构造输出路径
    base_filename = os.path.splitext(os.path.basename(video_file))[0]
    relative_path = os.path.relpath(os.path.dirname(video_file), root_path)
    output_dir = os.path.join(output_root, relative_path, base_filename)
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{base_filename}_no_head_tail.mp4")
    logging.info(f"输出路径: {repr(output_dir)}, 输出文件路径：{repr(output_file)}")
    print(f"输出路径: {repr(output_dir)}, 输出文件路径：{repr(output_file)}")

    # 标记处理中
    progress_manager.mark_processing(video_file)

    # 断点续传逻辑：检查已存在的输出片段
    try:
        existing_dur = get_media_duration_seconds(output_file) if os.path.exists(output_file) else 0.0
    except Exception:
        existing_dur = 0.0

    try:
        if existing_dur >= effective_duration * 0.99:
            # 已经完成
            progress_manager.mark_completed(video_file)
            logging.info(f"检测到已完成的输出，跳过: {output_file}")
            print(f"检测到已完成的输出，跳过: {output_file}")
            pbar.update(100)
            pbar.set_postfix_str("已完成✓")
            pbar.close()
            return True

        if 0 < existing_dur < effective_duration * 0.99:
            # 部分完成 -> 续传
            logging.info(f"检测到未完成的输出，已完成 {existing_dur:.1f}s / {effective_duration:.1f}s，尝试续传...")
            pbar.set_postfix_str("续传中...")

            part1_path = output_file + ".part1.mp4"
            try:
                shutil.move(output_file, part1_path)
            except Exception:
                part1_path = None

            remaining = max(0.0, effective_duration - existing_dur)
            start_offset = head_cut_time + int(existing_dur)
            part2_path = output_file + ".part2.mp4"
            command = [
                str(ffmpeg_path), '-y', '-nostdin', '-hide_banner', '-loglevel', 'error',
                '-ss', str(start_offset),
                '-i', video_file,
                '-t', str(remaining),
                '-c:v', 'copy', '-c:a', 'copy',
                '-avoid_negative_ts', 'make_zero',
                part2_path
            ]
            logging.info(f"ffmpeg 续传命令: {' '.join(map(str, command))}")
            print(f"ffmpeg 续传命令: {' '.join(map(str, command))}")
            subprocess.run(command, check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

            # 拼接 part1 + part2
            concat_inputs = []
            if part1_path and os.path.exists(part1_path):
                concat_inputs.append(part1_path)
            if os.path.exists(part2_path):
                concat_inputs.append(part2_path)
            if concat_inputs:
                tmp_out = output_file + ".tmp.mp4"
                concat_mp4_files(concat_inputs, tmp_out)
                shutil.move(tmp_out, output_file)
                for p in concat_inputs:
                    try:
                        os.remove(p)
                    except Exception:
                        pass
        else:
            # 全量处理
            pbar.set_postfix_str("处理中...")
            command = [
                str(ffmpeg_path), '-y', '-nostdin', '-hide_banner', '-loglevel', 'error',
                '-ss', str(head_cut_time),
                '-i', video_file,
                '-t', str(effective_duration),
                '-c:v', 'copy', '-c:a', 'copy',
                '-avoid_negative_ts', 'make_zero',
                output_file
            ]
            logging.info(f"ffmpeg 命令: {' '.join(map(str, command))}")
            print(f"ffmpeg 命令: {' '.join(map(str, command))}")
            subprocess.run(command, check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        # 校验输出
        out_size = os.path.getsize(output_file) if os.path.exists(output_file) else 0
        out_dur = get_media_duration_seconds(output_file)
        if out_size < 1024 or out_dur <= 0:
            raise RuntimeError(f"输出文件无效或时长为0: {output_file}")

        progress_manager.mark_completed(video_file)
        logging.info(f"成功处理 {repr(video_file)}，跳过片头 {head_cut_time} 秒，片尾 {tail_cut_time} 秒。")
        print(f"成功处理 {repr(video_file)}，跳过片头 {head_cut_time} 秒，片尾 {tail_cut_time} 秒。")
        end_time = time.time()
        logging.info(f"{repr(video_file)} 的处理完成，耗时 {end_time - start_time:.2f} 秒")
        print(f"{repr(video_file)} 的处理完成，耗时 {end_time - start_time:.2f} 秒")

        pbar.update(100)
        pbar.set_postfix_str("完成✓")
        pbar.close()
        return True

    except subprocess.CalledProcessError as e:
        msg = e.stderr.decode('utf-8', 'ignore') if hasattr(e, 'stderr') else str(e)
        logging.error(f"处理 {repr(video_file)} 时出错: {msg}")
        print(f"处理 {repr(video_file)} 时出错: {msg}")
        progress_manager.mark_failed(video_file, msg)
        pbar.set_postfix_str("失败✗")
        pbar.close()
        return False
    except Exception as e:
        logging.error(f"处理 {repr(video_file)} 时出现未知错误: {e}")
        print(f"处理 {repr(video_file)} 时出现未知错误: {e}")
        progress_manager.mark_failed(video_file, str(e))
        pbar.set_postfix_str("失败✗")
        pbar.close()
        return False


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


def process_videos_in_parallel(video_files):
    """并行处理视频文件"""
    if not video_files:
        return 0, 0

    # 重置进度条计数器
    global progress_bar_counter
    progress_bar_counter = 0

    # 创建总进度条，显示文件处理进度
    total_pbar = tqdm(total=len(video_files), desc="📁 总文件进度", position=0, leave=True,
                      bar_format='{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')

    # 创建成功/失败计数器
    success_count = 0
    failed_count = 0

    def task_done_callback(future):
        nonlocal success_count, failed_count
        try:
            result = future.result()
            if result:
                success_count += 1
            else:
                failed_count += 1
            total_pbar.update(1)
            # 更新总进度条的后缀信息
            total_pbar.set_postfix({
                '成功': success_count,
                '失败': failed_count,
                '成功率': f"{success_count / (success_count + failed_count) * 100:.1f}%" if (
                                                                                                        success_count + failed_count) > 0 else "0%"
            })
        except Exception as e:
            failed_count += 1
            total_pbar.update(1)
            logging.error(f"任务回调异常: {e}")
            total_pbar.set_postfix({
                '成功': success_count,
                '失败': failed_count,
                '成功率': f"{success_count / (success_count + failed_count) * 100:.1f}%" if (
                                                                                                        success_count + failed_count) > 0 else "0%"
            })

    # 使用线程池处理，避免FFmpeg进程冲突（自适应并发）
    max_workers = compute_adaptive_workers(video_files, output_root)
    logging.info(f"使用 {max_workers} 个线程并行处理（自适应）")
    print(f"🔧 并发: {max_workers} (自适应)")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(skip_head_tail, video_file, i, len(video_files))
                   for i, video_file in enumerate(video_files)]
        for future in futures:
            future.add_done_callback(task_done_callback)
        concurrent.futures.wait(futures)

    total_pbar.close()
    logging.info(f"📊 处理完成统计: 成功 {success_count} 个, 失败 {failed_count} 个, 总计 {len(video_files)} 个视频")
    return success_count, failed_count


def main():
    """主程序"""
    # 显示进度记录文件夹信息
    print(f"📁 进度记录文件夹: {PROGRESS_FOLDER}")
    if os.path.exists(PROGRESS_FOLDER):
        print(f"✅ 进度记录文件夹已存在")
        # 检查文件夹权限
        try:
            test_file = os.path.join(PROGRESS_FOLDER, "test_write.tmp")
            with open(test_file, 'w') as f:
                f.write("test")
            os.remove(test_file)
            print(f"✅ 文件夹写入权限正常")
        except Exception as e:
            print(f"❌ 文件夹写入权限异常: {e}")
            print(f"⚠️  将使用当前目录作为备选")
    else:
        print(f"⚠️  进度记录文件夹不存在，将自动创建")

    # 显示进度摘要
    progress_manager.print_summary()

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

    # 断点续传：过滤已完成或已存在有效输出的文件
    progress_manager.set_start_time()
    pending_files = []
    auto_synced = 0

    logging.info("开始扫描输出目录，同步已存在的文件...")

    for video_file in video_files:
        # 跳过已标记完成的
        if progress_manager.is_completed(video_file):
            continue

        # 自动同步：若输出已经存在且有效，则直接标记完成
        base_filename = os.path.splitext(os.path.basename(video_file))[0]
        relative_path = os.path.relpath(os.path.dirname(video_file), root_path)
        output_dir = os.path.join(output_root, relative_path, base_filename)
        output_file = os.path.join(output_dir, f"{base_filename}_no_head_tail.mp4")

        # 仅当输出已存在时，才对输入做一次快速时长探测，减少无谓开销
        effective_duration = 0.0
        if os.path.exists(output_file):
            effective_duration = get_input_effective_duration_seconds(video_file)

        if os.path.exists(output_file) and effective_duration > 0:
            out_dur = get_media_duration_seconds(output_file)
            if out_dur >= effective_duration * 0.99:
                progress_manager.mark_completed(video_file)
                auto_synced += 1
                logging.info(f"自动同步: {os.path.basename(video_file)}")
                continue

        pending_files.append(video_file)

    start_main_time = time.time()
    success_count = 0
    failed_count = 0

    if not pending_files:
        logging.info("没有待处理文件，可能已全部完成。")
        print("没有待处理文件，可能已全部完成。")
    else:
        logging.info(f"开始处理 {len(pending_files)} 个待处理/可续传的视频文件...")
        try:
            success_count, failed_count = process_videos_in_parallel(pending_files)
            logging.info(f"🎯 本次处理结果: 成功 {success_count} 个, 失败 {failed_count} 个")
        except Exception as e:
            logging.error(f"主程序异常: {e}", exc_info=True)

    # 总耗时
    end_main_time = time.time()
    logging.info(
        f"所有视频文件处理完成，总耗时 {end_main_time - start_main_time:.2f} 秒。成功 {success_count}，失败 {failed_count}，自动同步 {auto_synced}")
    print(
        f"所有视频文件处理完成，总耗时 {end_main_time - start_main_time:.2f} 秒。成功 {success_count}，失败 {failed_count}，自动同步 {auto_synced}")

    # 最终进度摘要
    progress_manager.print_summary()


if __name__ == "__main__":
    main()