# _*_ coding: utf-8 _*_
"""
批处理裁剪1080.py - 视频批量裁剪和分辨率调整工具
支持断点续传、硬件加速、ROI选择等功能

作者: L
版本: 1.0
功能: 批量裁剪视频到指定分辨率，支持断点续传和硬件加速
"""

# ==================== START: 用户配置区域 ====================
# !!! 请根据你的实际情况修改以下配置 !!!

# --- FFmpeg 路径配置 ---
# 请将此路径修改为你电脑上 ffmpeg.exe 和 ffprobe.exe 的实际路径
FFMPEG_PATH = r"D:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe"
FFPROBE_PATH = r"D:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffprobe.exe"

# --- 输入输出路径配置 ---
# 输入目录：包含待处理视频文件的文件夹
input_dir = r"Z:\a项目\航拍特写\李建楠\8.10"
# 输出目录：处理后的视频文件保存位置
output_dir = r"Z:\a项目\航拍特写\李建楠\8.10\切完"

# --- 进度记录配置 ---
# 进度记录文件夹：用于存储处理进度，支持跨电脑同步
PROGRESS_FOLDER = r"Z:\personal_folder\L\处理完数据记录"

# --- 视频处理配置 ---
# 目标分辨率 (必须是16:9比例)
# 1080p: (1920, 1080)
# 4K:    (3840, 2160)
TARGET_RESOLUTION = (1920, 1080)

# --- 硬件配置 ---
# 并行处理数量：根据你的CPU核心数调整
# 建议设置为 CPU核心数的一半，最大不超过4
MAX_PARALLEL_WORKERS = 4

# --- 编码质量配置 ---
# 视频码率：影响文件大小和质量
VIDEO_BITRATE = "10M"
# 最大码率：峰值码率
MAX_BITRATE = "20M"
# 缓冲区大小：与码率相关
BUFFER_SIZE = "20M"

# ===================== END: 用户配置区域 =====================

# 导入必要的模块
import time
import cv2
import os
import subprocess
import glob
import concurrent.futures
import logging
import re
import locale
from pathlib import Path
from tqdm import tqdm
import threading
import shutil
import json
import platform
import pickle
from datetime import datetime
import signal
import sys

# 以下为系统配置，通常不需要修改
PROGRESS_FILE = os.path.join(PROGRESS_FOLDER, "video_processing_progress.json")
TEMP_PROGRESS_FILE = os.path.join(PROGRESS_FOLDER, "video_processing_progress.tmp")

# 配置验证函数
def validate_config():
    """验证配置参数的有效性"""
    errors = []
    warnings = []
    
    # 检查FFmpeg路径
    if not os.path.exists(FFMPEG_PATH):
        errors.append(f"FFmpeg路径不存在: {FFMPEG_PATH}")
    if not os.path.exists(FFPROBE_PATH):
        errors.append(f"FFprobe路径不存在: {FFPROBE_PATH}")
    
    # 检查输入输出路径
    if not os.path.exists(input_dir):
        errors.append(f"输入目录不存在: {input_dir}")
    if not os.path.exists(os.path.dirname(output_dir)):
        warnings.append(f"输出目录的父目录不存在，将自动创建: {os.path.dirname(output_dir)}")
    
    # 检查进度记录文件夹
    if not os.path.exists(PROGRESS_FOLDER):
        warnings.append(f"进度记录文件夹不存在，将自动创建: {PROGRESS_FOLDER}")
    
    # 检查分辨率配置
    if TARGET_RESOLUTION[0] % 16 != 0 or TARGET_RESOLUTION[1] % 9 != 0:
        warnings.append(f"目标分辨率 {TARGET_RESOLUTION} 不是标准的16:9比例")
    
    # 检查并行数量配置
    if MAX_PARALLEL_WORKERS < 1 or MAX_PARALLEL_WORKERS > 8:
        warnings.append(f"并行处理数量 {MAX_PARALLEL_WORKERS} 可能不是最优值，建议在1-8之间")
    
    # 显示错误和警告
    if errors:
        print("❌ 配置错误:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    if warnings:
        print("⚠️  配置警告:")
        for warning in warnings:
            print(f"  - {warning}")
        print()
    
    return True

# 进度管理类
class ProgressManager:
    def __init__(self, progress_file=PROGRESS_FILE, temp_file=TEMP_PROGRESS_FILE):
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
                self.progress_file = os.path.join(fallback_folder, "video_processing_progress.json")
                self.temp_file = os.path.join(fallback_folder, "video_processing_progress.tmp")
                logging.warning(f"回退到当前目录: {fallback_folder}")
                print(f"⚠️  回退到当前目录: {fallback_folder}")
        else:
            logging.info(f"进度记录文件夹已存在: {progress_folder}")
            print(f"✅ 进度记录文件夹已存在: {progress_folder}")
    
    def load_progress(self):
        """加载进度数据"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logging.info(f"加载进度记录: {len(data.get('completed', []))} 个已完成, {len(data.get('processing', []))} 个处理中")
                    logging.info(f"进度文件路径: {self.progress_file}")
                    # 显示前几个已完成文件作为示例
                    completed_files = data.get('completed', [])
                    if completed_files:
                        logging.info(f"已完成文件示例: {completed_files[:3]}")
                    return data
        except Exception as e:
            logging.warning(f"加载进度文件失败: {e}")
        return {'completed': [], 'processing': [], 'failed': [], 'start_time': None, 'roi_settings': None}
    
    def save_progress(self):
        """保存进度数据"""
        # 使用线程锁防止并发写入
        with progress_save_lock:
            import time
            max_retries = 3
            retry_delay = 0.5
            
            for attempt in range(max_retries):
                try:
                    # 先保存到临时文件
                    with open(self.temp_file, 'w', encoding='utf-8') as f:
                        json.dump(self.progress_data, f, ensure_ascii=False, indent=2)
                    
                    # 然后移动到正式文件
                    if os.path.exists(self.progress_file):
                        os.remove(self.progress_file)
                    os.rename(self.temp_file, self.progress_file)
                    return  # 成功保存，退出重试循环
                except (OSError, PermissionError) as e:
                    if attempt < max_retries - 1:
                        logging.warning(f"保存进度文件重试 {attempt + 1}/{max_retries}: {e}")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # 指数退避
                    else:
                        logging.error(f"保存进度文件失败 (已重试 {max_retries} 次): {e}")
                except Exception as e:
                    logging.error(f"保存进度文件失败: {e}")
                    break  # 非文件系统错误，不重试
    
    def get_file_signature(self, file_path):
        """获取文件的唯一标识（基于文件大小、修改时间和文件名）"""
        try:
            if not os.path.exists(file_path):
                return None
            stat = os.stat(file_path)
            # 使用文件名、大小和修改时间作为签名
            signature = {
                'name': os.path.basename(file_path),
                'size': stat.st_size,
                'mtime': stat.st_mtime,
                'ctime': stat.st_ctime
            }
            return signature
        except Exception as e:
            logging.warning(f"获取文件签名失败 {file_path}: {e}")
            return None
    
    def get_file_hash(self, file_path, chunk_size=8192, max_chunks=10):
        """获取文件的部分哈希值（用于更精确的识别）"""
        try:
            if not os.path.exists(file_path):
                return None
            import hashlib
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                chunk_count = 0
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    hash_md5.update(chunk)
                    chunk_count += 1
                    if chunk_count >= max_chunks:  # 只读取前几个块以提高速度
                        break
            return hash_md5.hexdigest()
        except Exception as e:
            logging.warning(f"获取文件哈希失败 {file_path}: {e}")
            return None
    
    def is_file_completed_by_content(self, video_path, output_dir):
        """基于文件内容检查是否已完成（支持跨目录）"""
        video_signature = self.get_file_signature(video_path)
        if not video_signature:
            return False, None
        
        # 检查进度记录中是否有相同签名的文件
        for completed_record in self.progress_data.get('completed', []):
            if isinstance(completed_record, dict):
                # 新格式：包含文件签名的记录
                if (completed_record.get('name') == video_signature['name'] and
                    completed_record.get('size') == video_signature['size']):
                    # 检查输出目录中是否存在对应的输出文件
                    output_name = completed_record.get('output_name', video_signature['name'])
                    output_path = os.path.join(output_dir, output_name)
                    if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
                        return True, completed_record
            else:
                # 旧格式：只有文件名的记录
                if completed_record == video_signature['name']:
                    # 检查输出目录中是否存在该文件
                    output_path = os.path.join(output_dir, completed_record)
                    if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
                        return True, completed_record
        
        return False, None
    
    def is_file_completed_by_name(self, video_path):
        """基于文件名检查是否已完成（向后兼容）"""
        video_name = os.path.basename(video_path)
        return video_name in self.progress_data.get('completed', [])
    
    def is_completed(self, video_path, output_dir=None):
        """检查视频是否已完成（优先使用内容检查）"""
        if output_dir:
            # 使用内容检查（推荐）
            completed, record = self.is_file_completed_by_content(video_path, output_dir)
            if completed:
                return True
        
        # 回退到文件名检查
        return self.is_file_completed_by_name(video_path)
    
    def mark_completed(self, video_path, output_path):
        """标记视频为已完成（记录文件签名）"""
        video_signature = self.get_file_signature(video_path)
        if not video_signature:
            logging.warning(f"无法获取文件签名，使用文件名记录: {video_path}")
            video_name = os.path.basename(video_path)
            if video_name not in self.progress_data['completed']:
                self.progress_data['completed'].append(video_name)
        else:
            # 记录完整的文件信息
            completed_record = {
                'name': video_signature['name'],
                'size': video_signature['size'],
                'mtime': video_signature['mtime'],
                'ctime': video_signature['ctime'],
                'output_name': os.path.basename(output_path),
                'output_size': os.path.getsize(output_path) if os.path.exists(output_path) else 0,
                'completed_time': datetime.now().isoformat()
            }
            
            # 移除旧的记录（如果存在）
            self.progress_data['completed'] = [
                record for record in self.progress_data['completed'] 
                if not (isinstance(record, dict) and record.get('name') == video_signature['name'])
            ]
            self.progress_data['completed'] = [
                record for record in self.progress_data['completed'] 
                if record != video_signature['name']
            ]
            
            # 添加新记录
            self.progress_data['completed'].append(completed_record)
        
        # 从处理中移除
        video_name = os.path.basename(video_path)
        if video_name in self.progress_data['processing']:
            self.progress_data['processing'].remove(video_name)
        # 从失败列表中移除
        if video_name in self.progress_data['failed']:
            self.progress_data['failed'].remove(video_name)
        
        self.save_progress()
    
    def mark_processing(self, video_path):
        """标记视频为处理中"""
        video_name = os.path.basename(video_path)
        if video_name not in self.progress_data['processing']:
            self.progress_data['processing'].append(video_name)
        self.save_progress()
    
    def mark_failed(self, video_path, error_msg=""):
        """标记视频为失败"""
        video_name = os.path.basename(video_path)
        if video_name not in self.progress_data['failed']:
            self.progress_data['failed'].append({
                'name': video_name,
                'error': error_msg,
                'time': datetime.now().isoformat()
            })
        # 从处理中移除
        if video_name in self.progress_data['processing']:
            self.progress_data['processing'].remove(video_name)
        self.save_progress()
    
    def is_processing(self, video_path):
        """检查视频是否正在处理中"""
        video_name = os.path.basename(video_path)
        return video_name in self.progress_data['processing']
    
    def get_completed_count(self):
        """获取已完成数量"""
        return len(self.progress_data['completed'])
    
    def get_processing_count(self):
        """获取处理中数量"""
        return len(self.progress_data['processing'])
    
    def get_failed_count(self):
        """获取失败数量"""
        return len(self.progress_data['failed'])
    
    def set_roi_settings(self, roi_settings):
        """保存ROI设置"""
        self.progress_data['roi_settings'] = roi_settings
        self.save_progress()
    
    def get_roi_settings(self):
        """获取ROI设置"""
        return self.progress_data.get('roi_settings')
    
    def set_start_time(self):
        """设置开始时间"""
        if not self.progress_data.get('start_time'):
            self.progress_data['start_time'] = datetime.now().isoformat()
            self.save_progress()
    
    def print_summary(self):
        """打印进度摘要"""
        completed = self.get_completed_count()
        processing = self.get_processing_count()
        failed = self.get_failed_count()
        logging.info(f"进度摘要: 已完成 {completed} 个, 处理中 {processing} 个, 失败 {failed} 个")
        
        if failed > 0:
            logging.info("失败的文件:")
            for fail_info in self.progress_data['failed']:
                logging.info(f"  - {fail_info['name']}: {fail_info['error']}")
    
    def cleanup_invalid_records(self, output_dir):
        """清理无效的记录（输出文件不存在的记录）"""
        cleaned_count = 0
        valid_completed = []
        
        for record in self.progress_data.get('completed', []):
            if isinstance(record, dict):
                # 新格式记录
                output_name = record.get('output_name', record.get('name'))
                output_path = os.path.join(output_dir, output_name)
                if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
                    valid_completed.append(record)
                else:
                    cleaned_count += 1
                    logging.info(f"清理无效记录: {record.get('name')} (输出文件不存在)")
            else:
                # 旧格式记录
                output_path = os.path.join(output_dir, record)
                if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
                    valid_completed.append(record)
                else:
                    cleaned_count += 1
                    logging.info(f"清理无效记录: {record} (输出文件不存在)")
        
        self.progress_data['completed'] = valid_completed
        
        # 清理失败记录
        valid_failed = []
        for fail_info in self.progress_data.get('failed', []):
            output_path = os.path.join(output_dir, fail_info['name'])
            if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
                valid_failed.append(fail_info)
            else:
                cleaned_count += 1
                logging.info(f"清理无效失败记录: {fail_info['name']} (输出文件不存在)")
        
        self.progress_data['failed'] = valid_failed
        
        if cleaned_count > 0:
            self.save_progress()
            logging.info(f"清理完成，移除了 {cleaned_count} 个无效记录")
        
        return cleaned_count

# 全局进度管理器
progress_manager = ProgressManager()

# 全局进度条位置计数器
global progress_bar_counter
progress_bar_counter = 0
progress_bar_lock = threading.Lock()

# 进度保存锁
progress_save_lock = threading.Lock()

# 信号处理器，确保程序退出时保存进度
def signal_handler(signum, frame):
    logging.info("收到退出信号，正在保存进度...")
    progress_manager.save_progress()
    logging.info("进度已保存，程序退出")
    sys.exit(0)

# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# 创建日志目录
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

# 设置日志处理器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "video_process.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 检查FFmpeg路径
if not os.path.exists(FFMPEG_PATH) or not os.path.exists(FFPROBE_PATH):
    logging.error(f"错误: FFmpeg/FFprobe 未在指定路径找到。请检查脚本顶部配置。路径: {FFMPEG_PATH}")
    exit(1)

# 全局配置
temp_dir = Path("./temp");
temp_dir.mkdir(exist_ok=True)
start_time = time.time()

# 硬件配置 - 长视频优化版
HW_CONFIGS = {
    "nvidia": {"encoders": ["h264_nvenc", "hevc_nvenc"],
               "options": {"preset": "p2", "b:v": VIDEO_BITRATE, "maxrate": MAX_BITRATE, "bufsize": BUFFER_SIZE,
                           "spatial_aq": "1", "temporal_aq": "1", "rc": "vbr", "cq": "25", "multipass": "fullres"}},
    "amd": {"encoders": ["h264_amf", "hevc_amf"],
            "options": {"quality": "balanced", "b:v": VIDEO_BITRATE, "rc": "vbr_peak_constrained", "usage": "transcoding"}},
    "intel": {"encoders": ["h264_qsv", "hevc_qsv"],
              "options": {"preset": "fast", "b:v": VIDEO_BITRATE, "global_quality": "25", "look_ahead": "1"}},
    "software": {"encoders": ["libx264", "libx265"],
                 "options": {"preset": "fast", "crf": "25", "threads": "0", "aq-mode": "3", "x264opts": "keyint=60:min-keyint=30"}}
}

# 添加ROI选择回退函数
def prompt_for_roi_fallback(original_frame, display_frame, scale_factor, target_resolution):
    """当GUI不可用时，基于预览图交互输入ROI，返回原始分辨率下的(x,y,w,h)"""
    preview_path = temp_dir / "roi_preview_720p.jpg"
    try:
        cv2.imwrite(str(preview_path), display_frame)
        logging.info(f"无法使用图形界面选择ROI。已生成预览图: {preview_path}")
        # 尝试自动打开预览图
        try:
            if platform.system() == "Windows":
                os.startfile(str(preview_path))
            elif platform.system() == "Darwin":
                subprocess.run(['open', str(preview_path)], check=False)
            else:
                subprocess.run(['xdg-open', str(preview_path)], check=False)
        except Exception:
            pass

        h_disp, w_disp = display_frame.shape[:2]
        logging.info(f"预览分辨率: {w_disp}x{h_disp}")
        logging.info("请输入基于预览图的ROI坐标 x y w h（以空格分隔）:")
        while True:
            user_input = input().strip()
            match = re.match(r'^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*$', user_input)
            if not match:
                logging.warning("格式无效，请重新输入: x y w h")
                continue
            x_disp, y_disp, w_disp_in, h_disp_in = map(int, match.groups())
            # 转换为原始分辨率
            x = int(x_disp / scale_factor)
            y = int(y_disp / scale_factor)
            w = int(w_disp_in / scale_factor)
            h = int(h_disp_in / scale_factor)

            orig_h, orig_w = original_frame.shape[:2]
            x = max(0, min(x, orig_w - 1))
            y = max(0, min(y, orig_h - 1))
            w = max(1, min(w, orig_w - x))
            h = max(1, min(h, orig_h - y))
            return (x, y, w, h)
    except Exception as e:
        raise e


def detect_hardware():
    cpu_count = os.cpu_count() or 4
    hw_info = {"cpu_cores": cpu_count, "encoder_type": "software", "encoder": "libx264", "options": {},
               "max_parallel": min(cpu_count // 2, 4)}
    try:
        result = subprocess.run([FFMPEG_PATH, '-hide_banner', '-encoders'], capture_output=True, text=True,
                                encoding='utf-8')
        if any(e in result.stdout for e in HW_CONFIGS["nvidia"]["encoders"]):
            hw_info.update({"encoder_type": "nvidia",
                            "encoder": next(e for e in HW_CONFIGS["nvidia"]["encoders"] if e in result.stdout),
                            "options": HW_CONFIGS["nvidia"]["options"].copy(), "max_parallel": 4})
        elif any(e in result.stdout for e in HW_CONFIGS["amd"]["encoders"]):
            hw_info.update(
                {"encoder_type": "amd", "encoder": next(e for e in HW_CONFIGS["amd"]["encoders"] if e in result.stdout),
                 "options": HW_CONFIGS["amd"]["options"].copy(), "max_parallel": 3})
        elif any(e in result.stdout for e in HW_CONFIGS["intel"]["encoders"]):
            hw_info.update({"encoder_type": "intel",
                            "encoder": next(e for e in HW_CONFIGS["intel"]["encoders"] if e in result.stdout),
                            "options": HW_CONFIGS["intel"]["options"].copy(), "max_parallel": 3})
        else:
            hw_info.update({"encoder_type": "software", "encoder": HW_CONFIGS["software"]["encoders"][0],
                            "options": HW_CONFIGS["software"]["options"].copy(),
                            "max_parallel": max(1, min(cpu_count // 2, 4))})
        logging.info(f"检测到的硬件: {hw_info}")
        return hw_info
    except Exception as e:
        logging.error(f"硬件检测失败: {e}");
        return hw_info


def parse_progress(line):
    info = {}
    patterns = {'frame': r'frame=\s*(\d+)', 'fps': r'fps=\s*([\d\.]+)', 'time': r'time=\s*(\d+):(\d+):([\d\.]+)',
                'speed': r'speed=\s*([\d\.]+)x', 'size': r'size=\s*(\d+)kB'}
    for key, pattern in patterns.items():
        match = re.search(pattern, line)
        if match:
            if key == 'time':
                info[key] = float(match.group(1)) * 3600 + float(match.group(2)) * 60 + float(match.group(3))
            elif key in ['fps', 'speed']:
                info[key] = float(match.group(1))
            else:
                info[key] = int(match.group(1))
    return info


def get_media_duration_seconds(media_path):
    """使用 ffprobe 获取媒体时长（秒）。失败返回 0.0"""
    try:
        if not os.path.exists(media_path) or os.path.getsize(media_path) < 1024:
            return 0.0
        cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format=duration',
               '-of', 'default=noprint_wrappers=1:nokey=1', media_path]
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
        if result.returncode != 0:
            return 0.0
        val = result.stdout.strip()
        return float(val) if val else 0.0
    except Exception:
        return 0.0


def concat_mp4_files(file_list, output_path):
    """使用 concat demuxer 无损拼接多个 mp4 片段。file_list 为绝对路径列表。"""
    concat_dir = os.path.dirname(output_path)
    os.makedirs(concat_dir, exist_ok=True)
    list_file = os.path.join(concat_dir, 'concat_list.txt')
    with open(list_file, 'w', encoding='utf-8') as f:
        for p in file_list:
            safe_path = p.replace('\\', '/').replace("'", "'\\''")
            f.write(f"file '{safe_path}'\n")
    cmd = [FFMPEG_PATH, '-y', '-f', 'concat', '-safe', '0', '-i', list_file, '-c', 'copy', output_path]
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
    if proc.returncode != 0:
        raise Exception(f"拼接失败: {proc.stderr}")
    try:
        os.remove(list_file)
    except Exception:
        pass

def build_ffmpeg_command(input_file, output_file, filter_complex, hw_info, seek_seconds=0):
    cmd = [FFMPEG_PATH, '-y', '-nostdin']
    if seek_seconds > 0: cmd.extend(['-ss', str(seek_seconds)])
    cmd.extend(['-i', input_file, '-vf', filter_complex, '-c:v', hw_info['encoder']])
    
    # 添加分辨率强制设置，确保输出为1920x1080
    cmd.extend(['-s', '1920x1080'])
    
    # 长视频优化参数
    if hw_info['encoder_type'] == 'nvidia':
        gop_settings = ['-g', '120', '-keyint_min', '60', '-rc', 'vbr', '-cq', '25']
    elif hw_info['encoder_type'] == 'amd':
        gop_settings = ['-g', '120', '-keyint_min', '60', '-rc', 'vbr_peak_constrained']
    elif hw_info['encoder_type'] == 'intel':
        gop_settings = ['-g', '120', '-keyint_min', '60', '-global_quality', '25']
    else:
        gop_settings = ['-g', '120', '-keyint_min', '60', '-sc_threshold', '40', '-bf', '2']
    
    cmd.extend(gop_settings)
    
    # 长视频优化的编码参数
    essential_options = {}
    if hw_info['encoder_type'] == 'nvidia':
        essential_options = {'preset': 'p2', 'b:v': VIDEO_BITRATE, 'maxrate': MAX_BITRATE, 'bufsize': BUFFER_SIZE}
    elif hw_info['encoder_type'] == 'amd':
        essential_options = {'quality': 'balanced', 'b:v': VIDEO_BITRATE}
    elif hw_info['encoder_type'] == 'intel':
        essential_options = {'preset': 'fast', 'b:v': VIDEO_BITRATE}
    else:
        essential_options = {'preset': 'fast', 'crf': '25', 'threads': '0'}
    
    for key, value in essential_options.items():
        cmd.extend([f'-{key}', str(value)])
    
    # 长视频稳定性参数
    cmd.extend([
        '-c:a', 'aac', '-b:a', '192k', 
        '-movflags', '+faststart', 
        '-map_metadata', '-1', 
        '-vsync', 'cfr',
        '-avoid_negative_ts', 'make_zero',
        '-fflags', '+genpts',
        '-max_muxing_queue_size', '2048',  # 增加队列大小
        '-probesize', '50M',  # 增加探测大小
        '-analyzeduration', '50M',  # 增加分析时长
        output_file
    ])
    return cmd


def run_ffmpeg_process(cmd, duration, pbar, initial_time_offset: float = 0.0):
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                               encoding='utf-8', errors='ignore', bufsize=1)
    last_percentage, last_update_time, stalled_time = 10, time.time(), 0
    no_progress_count = 0
    last_progress_time = 0
    
    # 对于长视频，调整超时参数
    is_long_video = duration > 3600  # 超过1小时算长视频
    max_stall_time = 600 if is_long_video else 300  # 长视频10分钟，短视频5分钟
    max_no_progress_time = 1800 if is_long_video else 600  # 长视频30分钟，短视频10分钟
    
    logging.info(f"视频时长: {duration:.1f}秒, 长视频模式: {is_long_video}, 最大卡死时间: {max_stall_time}秒, 最大无进度时间: {max_no_progress_time}秒, 续传偏移: {initial_time_offset:.1f}s")
    
    while process.poll() is None:
        line = process.stderr.readline()
        if line:
            progress_info = parse_progress(line)
            if 'time' in progress_info:
                last_update_time = time.time()
                no_progress_count = 0  # 重置无进度计数
                
                # 修复进度计算，避免超过100%
                current_time = initial_time_offset + progress_info['time']
                if current_time > duration:
                    current_time = duration
                
                # 改进进度计算，避免卡在85%
                if current_time >= duration * 0.95:  # 如果已经处理了95%以上
                    percentage = 95
                else:
                    percentage = min(95, 10 + current_time * 85 / duration)
                
                if percentage > last_percentage:
                    pbar.update(percentage - last_percentage)
                    last_percentage = percentage
                    last_progress_time = current_time
                    
                    postfix = {'FPS': f"{progress_info.get('fps', 0):.1f}",
                               '速度': f"{progress_info.get('speed', 0):.1f}x",
                               '大小': f"{progress_info.get('size', 0) / 1024:.1f}MB",
                               '时间': f"{current_time:.1f}s/{duration:.1f}s",
                               '进度': f"{current_time/duration*100:.1f}%"}
                    pbar.set_postfix(postfix)
                
                # 改进卡死检测，对长视频更宽容
                speed = progress_info.get('speed', 1.0)
                if speed < 0.01:  # 速度极慢
                    stalled_time += 1
                elif speed < 0.1 and is_long_video:  # 长视频允许更慢的速度
                    stalled_time += 0.5
                else:
                    stalled_time = 0
                
                if stalled_time > max_stall_time:
                    process.terminate()
                    raise Exception(f"处理速度过慢，可能已卡死 (速度: {speed}x, 卡死时间: {stalled_time}s)")
        else:
            no_progress_count += 1
            # 对于长视频，大幅增加超时时间
            if time.time() - last_update_time > max_no_progress_time:
                process.terminate()
                raise Exception(f"处理超时，{max_no_progress_time}秒内无任何进度更新")
        
        time.sleep(1 if is_long_video else 0.5)  # 长视频减少检查频率
    
    # 检查返回码
    if process.returncode != 0:
        stderr_output = process.stderr.read()
        raise Exception(f"ffmpeg处理失败 (代码 {process.returncode}): {stderr_output}")
    
    # 确保进度条到达100%
    if last_percentage < 100:
        pbar.update(100 - last_percentage)


def process_video(video_path, output_video_path, roi, hardware_info, video_idx=0, total_videos=1,
                  target_resolution=(1920, 1080)):
    filename = os.path.basename(video_path)
    # 使用全局计数器分配进度条位置
    global progress_bar_counter, progress_bar_lock
    with progress_bar_lock:
        current_position = progress_bar_counter
        progress_bar_counter += 1
    
    pbar = tqdm(total=100, desc=f"视频 {video_idx + 1}/{total_videos}: {filename[:25]:<25}", position=current_position + 1,
                leave=True,
                bar_format='{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]')
    
    # 标记为处理中
    progress_manager.mark_processing(video_path)
    
    try:
        probe_cmd = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=duration', '-of',
                     'json', video_path]
        result = subprocess.run(probe_cmd, capture_output=True, text=True, encoding='utf-8')
        duration = float(json.loads(result.stdout)['streams'][0].get('duration', '0'))
        if duration <= 0: raise Exception("视频时长为0或无效")

        filter_complex = f"crop={roi[2]}:{roi[3]}:{roi[0]}:{roi[1]},scale={target_resolution[0]}:{target_resolution[1]}"

        # 断点续传：如输出已存在且未完成，则从已完成时长继续
        existing_duration = get_media_duration_seconds(output_video_path)
        if existing_duration > 0 and existing_duration < duration * 0.99:
            logging.info(f"检测到未完成的输出，已完成 {existing_duration:.1f}s / {duration:.1f}s，尝试续传...")
            # 将已有部分暂存为 part1
            part1_path = output_video_path + ".part1.mp4"
            try:
                shutil.move(output_video_path, part1_path)
            except Exception:
                # 若移动失败，直接覆盖生成 part2，之后用更稳妥的 concat 拼接
                part1_path = None
            # 从 existing_duration 续传生成 part2
            part2_path = output_video_path + ".part2.mp4"
            cmd = build_ffmpeg_command(video_path, part2_path, filter_complex, hardware_info, seek_seconds=int(existing_duration))
            logging.info(f"执行命令(续传): {' '.join(cmd)}");
            run_ffmpeg_process(cmd, duration, pbar, initial_time_offset=existing_duration)
            # 拼接 part1 + part2 为最终文件
            try:
                concat_inputs = []
                if part1_path and os.path.exists(part1_path):
                    concat_inputs.append(part1_path)
                if os.path.exists(part2_path):
                    concat_inputs.append(part2_path)
                if concat_inputs:
                    tmp_output = output_video_path + ".tmp.mp4"
                    concat_mp4_files(concat_inputs, tmp_output)
                    shutil.move(tmp_output, output_video_path)
                    for p in concat_inputs:
                        try:
                            os.remove(p)
                        except Exception:
                            pass
            except Exception as ce:
                logging.error(f"拼接续传片段失败: {ce}")
        else:
            pbar.set_postfix_str("尝试硬件加速...")
            cmd = build_ffmpeg_command(video_path, output_video_path, filter_complex, hardware_info)
            logging.info(f"执行命令: {' '.join(cmd)}")
            run_ffmpeg_process(cmd, duration, pbar)
        
        # 验证输出文件
        if not os.path.exists(output_video_path) or os.path.getsize(output_video_path) < 1024:
            raise Exception(f"输出文件无效或太小: {output_video_path}")
        
        # 标记为已完成
        progress_manager.mark_completed(video_path, output_video_path)
        
        pbar.set_postfix_str("完成✓");
        logging.info(f"视频处理完成: {video_path}");
        pbar.close();
        return True
    except Exception as e:
        logging.warning(f"主策略失败: {e}. 切换至备用方案...")
        try:
            pbar.set_postfix_str("尝试快速CPU编码...");
            cpu_hw_info = {"encoder_type": "software", "encoder": "libx264",
                           "options": {"preset": "veryfast", "crf": "23", "threads": "0"}}
            # CPU 方案同样支持续传
            existing_duration = get_media_duration_seconds(output_video_path)
            if existing_duration > 0 and existing_duration < duration * 0.99:
                part1_path = output_video_path + ".part1.mp4"
                try:
                    shutil.move(output_video_path, part1_path)
                except Exception:
                    part1_path = None
                part2_path = output_video_path + ".part2.mp4"
                cmd = build_ffmpeg_command(video_path, part2_path, filter_complex, cpu_hw_info, seek_seconds=int(existing_duration))
                logging.info(f"执行命令 (CPU续传): {' '.join(cmd)}")
                run_ffmpeg_process(cmd, duration, pbar, initial_time_offset=existing_duration)
                try:
                    concat_inputs = []
                    if part1_path and os.path.exists(part1_path):
                        concat_inputs.append(part1_path)
                    if os.path.exists(part2_path):
                        concat_inputs.append(part2_path)
                    if concat_inputs:
                        tmp_output = output_video_path + ".tmp.mp4"
                        concat_mp4_files(concat_inputs, tmp_output)
                        shutil.move(tmp_output, output_video_path)
                        for p in concat_inputs:
                            try:
                                os.remove(p)
                            except Exception:
                                pass
                except Exception as ce:
                    logging.error(f"CPU拼接续传片段失败: {ce}")
            else:
                cmd = build_ffmpeg_command(video_path, output_video_path, filter_complex, cpu_hw_info)
                logging.info(f"执行命令 (CPU): {' '.join(cmd)}")
                run_ffmpeg_process(cmd, duration, pbar)
            
            # 验证输出文件
            if not os.path.exists(output_video_path) or os.path.getsize(output_video_path) < 1024:
                raise Exception(f"CPU编码输出文件无效或太小: {output_video_path}")
            
            # 标记为已完成
            progress_manager.mark_completed(video_path, output_video_path)
            
            pbar.set_postfix_str("完成(CPU)✓");
            logging.info(f"视频处理完成 (CPU): {video_path}");
            pbar.close();
            return True
        except Exception as e2:
            logging.error(f"所有策略均失败: {e2}");
            # 标记为失败
            progress_manager.mark_failed(video_path, str(e2))
            pbar.set_postfix_str("失败✗");
            pbar.close();
            return False


def process_videos_in_parallel(video_paths, output_paths, roi, hardware_info, target_resolution):
    if output_paths: os.makedirs(os.path.dirname(output_paths[0]), exist_ok=True)
    
    # 创建总进度条，显示文件处理进度
    total_pbar = tqdm(total=len(video_paths), desc="📁 总文件进度", position=0, leave=True,
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
                '成功率': f"{success_count/(success_count+failed_count)*100:.1f}%" if (success_count+failed_count) > 0 else "0%"
            })
        except Exception as e:
            failed_count += 1
            total_pbar.update(1)
            logging.error(f"任务回调异常: {e}")
            total_pbar.set_postfix({
                '成功': success_count,
                '失败': failed_count,
                '成功率': f"{success_count/(success_count+failed_count)*100:.1f}%" if (success_count+failed_count) > 0 else "0%"
            })

    max_workers = min(MAX_PARALLEL_WORKERS, 2 if hardware_info["encoder_type"] != "software" else 3)
    logging.info(f"为提高稳定性，并行数调整为: {max_workers}")
    executor_class = concurrent.futures.ThreadPoolExecutor if hardware_info[
                                                                  "encoder_type"] != "software" else concurrent.futures.ProcessPoolExecutor
    with executor_class(max_workers=max_workers) as executor:
        print("\033[2J\033[H", end="")
        futures = [executor.submit(process_video, vp, op, roi, hardware_info, i, len(video_paths),
                                   target_resolution) for i, (vp, op) in enumerate(zip(video_paths, output_paths))]
        for future in futures: future.add_done_callback(task_done_callback)
        concurrent.futures.wait(futures)
    
    total_pbar.close()
    logging.info(f"📊 处理完成统计: 成功 {success_count} 个, 失败 {failed_count} 个, 总计 {len(video_paths)} 个视频")
    return success_count, failed_count


if __name__ == '__main__':
    # 配置验证
    print("🔍 正在验证配置...")
    if not validate_config():
        print("❌ 配置验证失败，请检查上述错误并修改配置后重新运行")
        exit(1)
    print("✅ 配置验证通过")
    print()
    
    # 显示当前配置信息
    print("📋 当前配置:")
    print(f"  FFmpeg路径: {FFMPEG_PATH}")
    print(f"  FFprobe路径: {FFPROBE_PATH}")
    print(f"  输入目录: {input_dir}")
    print(f"  输出目录: {output_dir}")
    print(f"  进度记录文件夹: {PROGRESS_FOLDER}")
    print(f"  目标分辨率: {TARGET_RESOLUTION[0]}x{TARGET_RESOLUTION[1]}")
    print(f"  并行处理数量: {MAX_PARALLEL_WORKERS}")
    print(f"  视频码率: {VIDEO_BITRATE}")
    print()
    
    # 验证路径是否存在
    if not os.path.exists(input_dir):
        logging.error(f"输入目录不存在: {input_dir}")
        print(f"错误: 输入目录不存在: {input_dir}")
        print("请检查路径是否正确，或修改脚本中的 input_dir 变量")
        exit(1)
    
    # 检查输入目录中的MP4文件数量
    input_files = glob.glob(os.path.join(input_dir, '*.mp4'))
    if not input_files:
        logging.error(f"输入目录中没有找到MP4文件: {input_dir}")
        print(f"错误: 输入目录中没有找到MP4文件: {input_dir}")
        print("请检查目录路径是否正确，或者目录中是否包含MP4文件")
        exit(1)
    
    logging.info(f"输入目录: {input_dir}")
    logging.info(f"输出目录: {output_dir}")
    logging.info(f"进度文件: {PROGRESS_FILE}")
    logging.info(f"进度记录文件夹: {PROGRESS_FOLDER}")
    logging.info(f"找到 {len(input_files)} 个MP4文件")
    
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
    
    # 显示前几个文件作为确认
    print(f"输入目录中的文件示例:")
    for i, file_path in enumerate(input_files[:5]):
        print(f"  {i+1}. {os.path.basename(file_path)}")
    if len(input_files) > 5:
        print(f"  ... 还有 {len(input_files) - 5} 个文件")
    
    os.makedirs(output_dir, exist_ok=True)
    if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
    temp_dir.mkdir(exist_ok=True)
    

    # 显示进度摘要
    progress_manager.print_summary()
    
    # 清理进度文件中的无效记录（文件不存在或路径不匹配）
    logging.info("清理进度文件中的无效记录...")
    cleaned_count = progress_manager.cleanup_invalid_records(output_dir)
    
    # 检查是否有保存的ROI设置
    saved_roi = progress_manager.get_roi_settings()
    if saved_roi:
        logging.info(f"发现保存的ROI设置: {saved_roi}")
        print(f"发现保存的ROI设置: {saved_roi}")
        print("按回车键使用保存的设置，或输入 'r' 重新选择裁剪区域: ", end="")
        user_input = input().strip().lower()
        if user_input == 'r':
            logging.info("用户选择重新选择ROI区域")
            saved_roi = None
        else:
            logging.info("自动使用保存的ROI设置，无需重新选择")
            final_roi = saved_roi
            logging.info(f"使用保存的ROI设置: {final_roi}")
    else:
        saved_roi = None
    
    video_paths = glob.glob(os.path.join(input_dir, '*.mp4'))

    if video_paths:
        # 如果没有保存的ROI设置，需要重新选择
        if not saved_roi:
            frame_for_preview, video_for_preview = None, None
            for video_path in video_paths:
                try:
                    probe_cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format=duration', '-of',
                                 'default=noprint_wrappers=1:nokey=1', video_path]
                    result = subprocess.run(probe_cmd, capture_output=True, text=True, encoding='utf-8')
                    if result.returncode != 0 or not result.stdout.strip(): continue
                    duration = float(result.stdout.strip())
                    temp_preview_path = temp_dir / "preview_frame.jpg"
                    extract_cmd = [FFMPEG_PATH, '-ss', str(duration / 2), '-i', video_path, '-vframes', '1', '-q:v', '2',
                                   str(temp_preview_path), '-y']
                    subprocess.run(extract_cmd, capture_output=True, check=False)
                    frame = cv2.imread(str(temp_preview_path))
                    if frame is not None:
                        frame_for_preview, video_for_preview = frame, video_path
                        logging.info(f"成功为预览加载视频: {os.path.basename(video_for_preview)}");
                        break
                except Exception as e:
                    logging.warning(f"尝试为 {os.path.basename(video_path)} 创建预览失败: {e}")

            if frame_for_preview is None:
                logging.error("错误: 无法在目录中找到任何可以成功创建预览的视频文件。");
                exit(1)

            print(f"使用第一个可读视频进行预览: {os.path.basename(video_for_preview)}")
            video_height, video_width, _ = frame_for_preview.shape

            display_height = 800
            scale_factor = display_height / video_height if video_height > 0 else 1
            display_width = int(video_width * scale_factor)
            display_frame = cv2.resize(frame_for_preview, (display_width, display_height))

            cv2.putText(display_frame, "请用鼠标选择一个区域，然后按'空格'或'回车'确认", (20, 40), cv2.FONT_HERSHEY_SIMPLEX,
                        1, (255, 255, 255), 2)

            # 让用户选择ROI（若GUI不可用或取消，则回退到命令行输入）
            try:
                r = cv2.selectROI("交互式裁剪区域选择", display_frame, fromCenter=False)
                cv2.destroyAllWindows()
                if r[2] == 0 or r[3] == 0:
                    raise cv2.error("selectROI canceled", None, None)
                r_original = (int(r[0] / scale_factor), int(r[1] / scale_factor), int(r[2] / scale_factor),
                              int(r[3] / scale_factor))
            except cv2.error:
                logging.warning("cv2.selectROI 出错，使用命令行输入模式")
                r_original = prompt_for_roi_fallback(frame_for_preview, display_frame, scale_factor, TARGET_RESOLUTION)
            x, y, w, h = r_original
            print(f'您选择的裁剪框 (原始尺寸): {r_original}')

            target_width_calc, target_height_calc = w, int(w * 9 / 16)
            if target_height_calc > h: target_height_calc, target_width_calc = h, int(h * 16 / 9)

            center_x, center_y = x + w // 2, y + h // 2
            new_x, new_y = center_x - target_width_calc // 2, center_y - target_height_calc // 2

            new_x, new_y = max(0, new_x), max(0, new_y)
            if new_x + target_width_calc > video_width: new_x = video_width - target_width_calc
            if new_y + target_height_calc > video_height: new_y = video_height - target_height_calc

            final_roi = (new_x, new_y, target_width_calc, target_height_calc)

            print(f'脚本计算出的最终16:9裁剪参数: {final_roi}')
            print(f'所有视频将被裁剪为此尺寸，然后拉伸到: {TARGET_RESOLUTION[0]}x{TARGET_RESOLUTION[1]}')

            # ===== START: 新增的最终裁剪框预览功能 =====
            preview_image = frame_for_preview.copy()
            # 画出您选择的框 (红色)
            cv2.rectangle(preview_image, (r_original[0], r_original[1]),
                          (r_original[0] + r_original[2], r_original[1] + r_original[3]), (0, 0, 255), 2)
            cv2.putText(preview_image, 'Your Selection', (r_original[0], r_original[1] - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.9,
                        (0, 0, 255), 2)

            # 画出脚本计算出的16:9框 (绿色)
            cv2.rectangle(preview_image, (final_roi[0], final_roi[1]),
                          (final_roi[0] + final_roi[2], final_roi[1] + final_roi[3]), (0, 255, 0), 2)
            cv2.putText(preview_image, 'Final 16:9 Crop', (final_roi[0], final_roi[1] - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.9,
                        (0, 255, 0), 2)

            # 调整尺寸以便在屏幕上显示
            final_preview_display = cv2.resize(preview_image, (display_width, display_height))
            cv2.putText(final_preview_display, "按任意键开始处理...", (20, display_height - 20), cv2.FONT_HERSHEY_SIMPLEX,
                        1, (255, 255, 0), 2)

            # 显示预览（若GUI不可用则跳过）
            try:
                cv2.imshow('最终裁剪区域预览 (按任意键开始)', final_preview_display)
                cv2.waitKey(0)
                cv2.destroyAllWindows()
            except cv2.error:
                logging.warning("无法显示预览窗口，跳过预览步骤")
            # ===== END: 新增功能 =====

            # 保存ROI设置
            progress_manager.set_roi_settings(final_roi)

        # 断点续传：过滤已完成的视频，保留未完成和失败的视频
        filtered_video_paths = []
        filtered_output_paths = []
        completed_count = 0
        auto_synced_count = 0
        
        logging.info("开始扫描输出目录，同步已存在的文件...")
        
        for video_path in video_paths:
            video_name = os.path.basename(video_path)
            output_path = os.path.join(output_dir, video_name)
            
            # 使用新的基于内容的检查方法
            if progress_manager.is_completed(video_path, output_dir):
                completed_count += 1
                logging.info(f"跳过已完成: {video_name}")
                continue
            
            # 检查输出文件是否存在且完整（更精确的匹配）
            video_name_without_ext = os.path.splitext(video_name)[0]
            possible_output_files = []
            
            # 1. 精确匹配原文件名（主要检查方式）
            if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
                possible_output_files.append(output_path)
                logging.info(f"找到精确匹配: {video_name}")
            
            # 2. 查找以原文件名开头的文件（支持多个输出，但更严格）
            for output_file in os.listdir(output_dir):
                if (output_file.startswith(video_name_without_ext + "_") and 
                    output_file.endswith('.mp4')):
                    file_path = os.path.join(output_dir, output_file)
                    if file_path not in possible_output_files and os.path.getsize(file_path) > 1024:
                        possible_output_files.append(file_path)
                        logging.info(f"找到前缀匹配: {output_file}")
            
            # 3. 查找包含原文件名的文件（最严格的匹配，只在特定情况下使用）
            # 只有当文件名很长且包含关键标识时才使用
            if len(video_name_without_ext) > 20:  # 只对很长的文件名使用宽松匹配
                for output_file in os.listdir(output_dir):
                    if (video_name_without_ext in output_file and 
                        output_file.endswith('.mp4') and
                        output_file not in [os.path.basename(f) for f in possible_output_files]):
                        file_path = os.path.join(output_dir, output_file)
                        if os.path.getsize(file_path) > 1024:
                            # 额外验证：确保文件名相似度足够高
                            # 检查是否包含原文件名的主要部分（去掉通用词汇）
                            main_parts = [part for part in video_name_without_ext.split() 
                                         if len(part) > 3 and part.lower() not in 
                                         ['the', 'and', 'for', 'with', 'from', 'music', 'video', '4k', '8k']]
                            if len(main_parts) >= 2:  # 至少包含2个主要词汇
                                if any(part in output_file for part in main_parts):
                                    possible_output_files.append(file_path)
                                    logging.info(f"找到严格宽松匹配: {output_file}")
            
            if possible_output_files:
                # 验证所有找到的输出文件
                valid_files = []
                total_duration = 0
                
                for file_path in possible_output_files:
                    try:
                        probe_cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format=duration', '-of',
                                     'default=noprint_wrappers=1:nokey=1', file_path]
                        result = subprocess.run(probe_cmd, capture_output=True, text=True, encoding='utf-8')
                        if result.returncode == 0 and result.stdout.strip():
                            duration = float(result.stdout.strip())
                            valid_files.append(file_path)
                            total_duration += duration
                    except Exception as e:
                        logging.warning(f"验证文件失败 {os.path.basename(file_path)}: {e}")
                
                if valid_files:
                    # 文件存在且可读，标记为已完成
                    progress_manager.mark_completed(video_path, output_path)
                    auto_synced_count += 1
                    logging.info(f"自动同步: {video_name} (共 {len(valid_files)} 个输出文件, 总时长: {total_duration:.1f}s)")
                    continue
            
            # 添加到待处理列表
            filtered_video_paths.append(video_path)
            filtered_output_paths.append(output_path)
        
        if auto_synced_count > 0:
            logging.info(f"自动同步了 {auto_synced_count} 个已存在的输出文件到进度记录")

        total_completed = completed_count + auto_synced_count
        logging.info(f"视频统计: 总计 {len(video_paths)} 个, 已完成 {total_completed} 个 (其中自动同步 {auto_synced_count} 个), 待处理 {len(filtered_video_paths)} 个")
        
        if filtered_video_paths:
            # 设置开始时间
            progress_manager.set_start_time()
            
            # 重置进度条计数器
            progress_bar_counter = 0
            
            logging.info(f"开始处理 {len(filtered_video_paths)} 个待处理/可续传的视频文件...")
            try:
                hardware_info = detect_hardware()
                success_count, failed_count = process_videos_in_parallel(filtered_video_paths, filtered_output_paths, final_roi, hardware_info, TARGET_RESOLUTION)
                logging.info(f"🎯 本次处理结果: 成功 {success_count} 个, 失败 {failed_count} 个")
            except Exception as e:
                logging.error(f"主程序异常: {e}", exc_info=True)
        else:
            logging.info("所有视频都已处理完成！")
    else:
        logging.warning(f"在 {input_dir} 中没有找到MP4视频文件")

    # 最终进度摘要
    progress_manager.print_summary()
    
    # 清理临时文件
    try:
        if os.path.exists(temp_dir): 
            shutil.rmtree(temp_dir)
            logging.info("临时文件已清理")
    except Exception as e:
        logging.warning(f"清理临时文件失败: {e}")
    
    elapsed_time = time.time() - start_time
    logging.info(f'处理完成！总耗时: {elapsed_time:.2f}秒')