# _*_ coding: utf-8 _*_
"""
支持断点续传、硬件加速、ROI选择等功能

作者: L
版本: 2.0
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
input_dir = r"Z:\personal_folder\L\测试"
output_dir = r"Z:\personal_folder\L\测试完"

# --- 进度记录配置 ---
# 进度记录文件夹：用于存储处理进度，支持跨电脑同步
# 注意：进度文件现在会在输出目录中创建电脑独有的子文件夹
PROGRESS_FOLDER = r"Z:\personal_folder\L\处理完数据记录"

# --- 视频处理配置 ---
# 目标分辨率 (必须是16:9比例)
# 1080p: (1920, 1080)
# 4K:    (3840, 2160)
TARGET_RESOLUTION = (1920, 1080)

# --- 低分辨率视频跳过配置 ---
# 是否跳过处理1080p以下的视频
SKIP_LOW_RESOLUTION_VIDEOS = True
# 最小处理分辨率阈值 (宽度像素)
MIN_RESOLUTION_WIDTH = 1920
# 跳过的视频移动到的目录 (设为空字符串则不移动，只跳过)
SKIP_VIDEOS_MOVE_DIR = r"Z:\a项目\航拍特写\李建楠\测试\1\跳过的低分辨率视频"

# --- 支持的视频格式 ---
# 支持的视频文件扩展名
SUPPORTED_VIDEO_FORMATS = ['.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm', '.ts', '.m4v', '.3gp', '.f4v']

# --- 硬件配置 (自动检测) ---
# 系统将自动检测并优化i9处理器性能
# 手动override并行数量（设为0则自动检测）
MAX_PARALLEL_WORKERS_OVERRIDE = 0
# 向后兼容
MAX_PARALLEL_WORKERS = 6  # 提升默认并行数

# --- 质量控制配置 ---
# 质量模式：'highest' | 'high' | 'balanced' | 'fast'
QUALITY_MODE = 'highest'
# 自动码率：True=根据源视频自动调整，False=使用固定码率
AUTO_BITRATE = True
# 固定码率设置（仅在AUTO_BITRATE=False时使用）
VIDEO_BITRATE = "10M"
MAX_BITRATE = "20M"
BUFFER_SIZE = "20M"

# ===================== END: 用户配置区域 =====================

# 导入必要的模块
import time
import cv2
import os

# ==================== OpenCV版本检查和警告 ====================
def check_opencv_version():
    """检查OpenCV版本并给出警告"""
    current_version = cv2.__version__
    print(f"🔍 当前OpenCV版本: {current_version}")
    
    # 如果检测到4.12.0版本，给出警告和解决方案
    if current_version.startswith('4.12'):
        print("⚠️ 警告：检测到OpenCV 4.12.0版本")
        print("   该版本存在selectROI功能bug，可能导致裁剪区域选择失败")
        print("   建议解决方案：")
        print("   1. 卸载当前版本：pip uninstall opencv-python")
        print("   2. 安装稳定版本：pip install opencv-python==4.10.0.84")
        print("   3. 或者使用conda：conda install opencv=4.10.0")
        print("   正在尝试继续运行，但可能遇到GUI问题...")
        return False
    else:
        print("✅ OpenCV版本正常")
        return True

# 检查版本
version_ok = check_opencv_version()
print(f"📦 使用OpenCV版本: {cv2.__version__}")
# ==================== 版本检查完成 ====================
import subprocess
import glob
import concurrent.futures
import logging
import re
import locale
from pathlib import Path
from tqdm import tqdm

# 初始化OpenCV GUI后端
def init_opencv_gui():
    """初始化OpenCV GUI后端，确保selectROI可以正常工作"""
    try:
        # 再次验证OpenCV版本
        current_version = cv2.__version__
        print(f"🔧 GUI初始化时OpenCV版本: {current_version}")
        
        # 尝试创建一个测试窗口
        test_img = np.zeros((100, 100, 3), dtype=np.uint8)
        cv2.namedWindow("test_window", cv2.WINDOW_NORMAL)
        cv2.imshow("test_window", test_img)
        cv2.waitKey(1)  # 处理窗口事件
        cv2.destroyWindow("test_window")
        print("✅ OpenCV GUI后端初始化成功")
        return True
    except Exception as e:
        print(f"❌ OpenCV GUI后端初始化失败: {e}")
        logging.warning(f"OpenCV GUI后端初始化失败: {e}")
        return False
import threading
import shutil
import json
import platform
import pickle
import numpy as np
from datetime import datetime
import signal
import sys
import psutil
import hashlib
import uuid
import socket
import multiprocessing
from typing import List, Tuple, Dict, Optional
import math

# 以下为系统配置，通常不需要修改
# 注意：进度文件路径现在会动态生成，基于电脑唯一标识
PROGRESS_FILE = None  # 动态设置
TEMP_PROGRESS_FILE = None  # 动态设置

# ==================== START: 新增功能函数 ====================

def get_computer_unique_id() -> str:
    """获取电脑的唯一标识符，用于创建独有的进度文件"""
    try:
        # 方法1: 尝试获取MAC地址
        mac = uuid.getnode()
        mac_str = f"{mac:012x}"
        
        # 方法2: 获取计算机名
        hostname = socket.gethostname()
        
        # 方法3: 尝试获取硬盘序列号（Windows）
        disk_serial = "unknown"
        if platform.system() == "Windows":
            try:
                result = subprocess.run(['wmic', 'diskdrive', 'get', 'serialnumber'], 
                                      capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    lines = result.stdout.strip().split('\n')
                    for line in lines[1:]:  # Skip header
                        serial = line.strip()
                        if serial and serial != "SerialNumber":
                            disk_serial = serial[:16]  # Take first 16 chars
                            break
            except Exception:
                pass
        
        # 组合信息创建唯一标识
        combined = f"{hostname}_{mac_str}_{disk_serial}"
        unique_hash = hashlib.md5(combined.encode()).hexdigest()[:12]
        
        # 格式: hostname_hash (便于识别)
        return f"{hostname}_{unique_hash}"
    
    except Exception as e:
        logging.warning(f"获取电脑唯一标识失败，使用随机标识: {e}")
        # 回退方案
        return f"{platform.node()}_{uuid.uuid4().hex[:8]}"

def find_video_files(directory: str) -> List[str]:
    """查找目录中所有支持的视频文件"""
    video_files = []
    
    try:
        for format_ext in SUPPORTED_VIDEO_FORMATS:
            pattern = os.path.join(directory, f'*{format_ext}')
            files = glob.glob(pattern, recursive=False)
            video_files.extend(files)
            
            # 也搜索大写扩展名
            pattern_upper = os.path.join(directory, f'*{format_ext.upper()}')
            files_upper = glob.glob(pattern_upper, recursive=False)
            video_files.extend(files_upper)
        
        # 去重并排序
        video_files = list(set(video_files))
        video_files.sort()
        
        logging.info(f"在目录 {directory} 中找到 {len(video_files)} 个支持的视频文件")
        
        # 按格式分组统计
        format_count = {}
        for file in video_files:
            ext = os.path.splitext(file)[1].lower()
            format_count[ext] = format_count.get(ext, 0) + 1
        
        logging.info(f"视频格式分布: {format_count}")
        
    except Exception as e:
        logging.error(f"搜索视频文件时出错: {e}")
    
    return video_files

def detect_advanced_hardware() -> Dict:
    """高级硬件检测和性能优化"""
    try:
        # 基础CPU信息
        cpu_count = multiprocessing.cpu_count()
        cpu_freq = psutil.cpu_freq()
        memory = psutil.virtual_memory()
        
        # 检测CPU型号
        cpu_info = platform.processor()
        is_i9 = 'i9' in cpu_info.lower()
        is_high_end = any(x in cpu_info.lower() for x in ['i9', 'i7', 'ryzen 9', 'ryzen 7'])
        
        # GPU检测
        gpu_info = detect_gpu_capabilities()
        
        # 根据硬件配置优化参数
        if is_i9:
            # i9处理器优化配置 - 更激进的并行策略
            max_parallel = min(cpu_count - 2, 20)  # 保留2个核心给系统，提升上限
            buffer_size = "100M"  # 增大缓冲区
            probe_size = "200M"   # 增大探测大小
        elif is_high_end:
            # 高端处理器配置
            max_parallel = min(cpu_count - 1, 12)  # 提升并行数
            buffer_size = "50M" 
            probe_size = "100M"
        else:
            # 普通处理器配置
            max_parallel = min(cpu_count // 2, 6)  # 轻微提升
            buffer_size = "30M"
            probe_size = "50M"
        
        # 内存优化 - 更好地利用32GB内存
        memory_gb = memory.total / (1024**3)
        if memory_gb >= 32:
            # 32GB以上内存，可以支持更多并行
            max_parallel = min(max_parallel, int(memory_gb // 1.5))  # 更激进的内存利用
        elif memory_gb >= 16:
            max_parallel = min(max_parallel, 10)  # 提升16GB内存的并行数
        else:
            max_parallel = min(max_parallel, 6)
        
        # 应用用户覆盖设置
        if MAX_PARALLEL_WORKERS_OVERRIDE > 0:
            max_parallel = MAX_PARALLEL_WORKERS_OVERRIDE
        
        hw_info = {
            'cpu_cores': cpu_count,
            'cpu_freq_max': cpu_freq.max if cpu_freq else 0,
            'memory_gb': memory_gb,
            'is_i9': is_i9,
            'is_high_end': is_high_end,
            'max_parallel': max_parallel,
            'buffer_size': buffer_size,
            'probe_size': probe_size,
            'gpu_info': gpu_info
        }
        
        # 更新编码器配置
        hw_info.update(gpu_info)
        
        logging.info(f"硬件检测完成: CPU={cpu_count}核心, 内存={memory_gb:.1f}GB, "
                    f"i9={is_i9}, 并行数={max_parallel}, GPU={gpu_info.get('encoder_type', 'unknown')}")
        
        return hw_info
        
    except Exception as e:
        logging.error(f"高级硬件检测失败: {e}")
        return detect_hardware()  # 回退到原始检测

def detect_gpu_capabilities() -> Dict:
    """检测GPU能力和优化编码器选择"""
    gpu_info = {"encoder_type": "software", "encoder": "libx264", "options": {}}
    
    try:
        # 检测可用的硬件编码器
        result = subprocess.run([FFMPEG_PATH, '-hide_banner', '-encoders'], 
                              capture_output=True, text=True, encoding='utf-8', timeout=10)
        
        if result.returncode != 0:
            return gpu_info
        
        encoders_output = result.stdout
        
        # NVIDIA检测 (优先级最高)
        nvidia_encoders = ['h264_nvenc', 'hevc_nvenc', 'av1_nvenc']
        for encoder in nvidia_encoders:
            if encoder in encoders_output:
                gpu_info.update({
                    "encoder_type": "nvidia",
                    "encoder": encoder,
                    "options": get_nvidia_optimized_options(),
                    "max_parallel": 6 if encoder == 'h264_nvenc' else 4
                })
                logging.info(f"检测到NVIDIA编码器: {encoder}")
                return gpu_info
        
        # AMD检测
        amd_encoders = ['h264_amf', 'hevc_amf', 'av1_amf']
        for encoder in amd_encoders:
            if encoder in encoders_output:
                gpu_info.update({
                    "encoder_type": "amd",
                    "encoder": encoder,
                    "options": get_amd_optimized_options(),
                    "max_parallel": 4
                })
                logging.info(f"检测到AMD编码器: {encoder}")
                return gpu_info
        
        # Intel检测
        intel_encoders = ['h264_qsv', 'hevc_qsv', 'av1_qsv']
        for encoder in intel_encoders:
            if encoder in encoders_output:
                gpu_info.update({
                    "encoder_type": "intel",
                    "encoder": encoder,
                    "options": get_intel_optimized_options(),
                    "max_parallel": 4
                })
                logging.info(f"检测到Intel编码器: {encoder}")
                return gpu_info
        
        # 软件编码器优化
        gpu_info.update({
            "encoder_type": "software",
            "encoder": "libx264",
            "options": get_software_optimized_options(),
            "max_parallel": min(multiprocessing.cpu_count() // 2, 8)
        })
        logging.info("使用优化的软件编码器")
        
    except Exception as e:
        logging.warning(f"GPU检测失败: {e}")
    
    return gpu_info

def get_nvidia_optimized_options() -> Dict:
    """NVIDIA编码器优化参数 - 采用1.0版本的稳定策略"""
    # 使用1.0版本的稳定参数组合，避免兼容性问题
    if QUALITY_MODE == 'highest':
        return {
            'preset': 'p2',  # 使用稳定的p2预设
            'rc': 'vbr',
            'cq': '20'  # 适中的质量值
        }
    elif QUALITY_MODE == 'high':
        return {
            'preset': 'p2',
            'rc': 'vbr',
            'cq': '23'
        }
    elif QUALITY_MODE == 'balanced':
        return {
            'preset': 'p4',
            'rc': 'vbr',
            'cq': '25'
        }
    else:  # fast
        return {
            'preset': 'p6',
            'rc': 'cbr'
        }

def get_nvidia_fallback_options() -> Dict:
    """NVIDIA编码器备用参数（兼容性优先）- 采用1.0版本策略"""
    # 使用最保守的参数，确保最大兼容性
    return {
        'preset': 'p4',        # 中等预设，兼容性好
        'rc': 'vbr',           # VBR模式
        'cq': '25'             # 中等质量，移除所有可能有问题的高级参数
    }

def get_amd_optimized_options() -> Dict:
    """AMD编码器优化参数 - 采用1.0版本的稳定策略"""
    # 使用1.0版本的简化参数，避免复杂的QP设置
    if QUALITY_MODE in ['highest', 'high']:
        return {
            'quality': 'balanced',  # 使用稳定的balanced模式
            'rc': 'vbr_peak_constrained'
        }
    elif QUALITY_MODE == 'balanced':
        return {
            'quality': 'balanced',
            'rc': 'vbr_peak_constrained'
        }
    else:  # fast
        return {
            'quality': 'speed',
            'rc': 'cbr'
        }

def get_intel_optimized_options() -> Dict:
    """Intel编码器优化参数 - 采用1.0版本的稳定策略"""
    # 使用1.0版本的简化参数，避免复杂的look_ahead设置
    if QUALITY_MODE in ['highest', 'high']:
        return {
            'preset': 'fast',  # 使用稳定的fast预设
            'global_quality': '25'
        }
    elif QUALITY_MODE == 'balanced':
        return {
            'preset': 'fast',
            'global_quality': '25'
        }
    else:  # fast
        return {
            'preset': 'fast',
            'global_quality': '28'
        }

def get_software_optimized_options() -> Dict:
    """软件编码器优化参数 - 采用1.0版本的稳定策略"""
    # 使用1.0版本的简化参数，移除所有可能有问题的高级参数
    if QUALITY_MODE == 'highest':
        return {
            'preset': 'slow',  # 使用稳定的slow预设
            'crf': '20',
            'threads': '0'
        }
    elif QUALITY_MODE == 'high':
        return {
            'preset': 'medium',
            'crf': '23',
            'threads': '0'
        }
    elif QUALITY_MODE == 'balanced':
        return {
            'preset': 'fast',
            'crf': '25',
            'threads': '0'
        }
    else:  # fast
        return {
            'preset': 'veryfast',
            'crf': '28',
            'threads': '0'
        }

def get_media_duration_seconds(video_path: str) -> float:
    """获取媒体文件的时长（秒）"""
    if not os.path.exists(video_path):
        return 0.0
    
    try:
        # 使用ffprobe获取时长
        cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format=duration', 
               '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=30)
        
        if result.returncode == 0 and result.stdout.strip():
            duration = float(result.stdout.strip())
            return duration if duration > 0 else 0.0
        else:
            # 如果format获取失败，尝试从视频流获取
            cmd = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                   '-show_entries', 'stream=duration', 
                   '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=30)
            
            if result.returncode == 0 and result.stdout.strip():
                duration = float(result.stdout.strip())
                return duration if duration > 0 else 0.0
            
            return 0.0
    except Exception as e:
        logging.warning(f"获取视频时长失败 {video_path}: {e}")
        return 0.0

def get_video_resolution(video_path: str) -> Tuple[int, int]:
    """获取视频分辨率 (宽度, 高度) - 增强版本，支持多种重试机制"""
    max_retries = 3
    retry_delay = 1.0
    
    for attempt in range(max_retries):
        try:
            # 方法1: 使用 CSV 格式输出 (原方法，但增强解析)
            cmd = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                   '-show_entries', 'stream=width,height', 
                   '-of', 'csv=s=x:p=0', video_path]
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=15)
            
            if result.returncode == 0 and result.stdout.strip():
                output = result.stdout.strip()
                logging.debug(f"ffprobe CSV输出 (尝试 {attempt + 1}): '{output}'")
                
                # 清理输出并尝试解析
                if 'x' in output:
                    parts = output.split('x')
                    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                        try:
                            width = int(parts[0].strip())
                            height = int(parts[1].strip())
                            if width > 0 and height > 0:
                                logging.info(f"方法1成功获取分辨率: {width}x{height} - {os.path.basename(video_path)}")
                                return width, height
                        except ValueError as e:
                            logging.debug(f"CSV格式解析失败: {e}, 输出: '{output}'")
            
            # 方法2: 使用 JSON 格式输出 (更可靠)
            cmd_json = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                        '-show_entries', 'stream=width,height', 
                        '-of', 'json', video_path]
            result_json = subprocess.run(cmd_json, capture_output=True, text=True, encoding='utf-8', timeout=15)
            
            if result_json.returncode == 0 and result_json.stdout.strip():
                try:
                    import json
                    data = json.loads(result_json.stdout)
                    if 'streams' in data and len(data['streams']) > 0:
                        stream = data['streams'][0]
                        width = stream.get('width')
                        height = stream.get('height')
                        if width and height and width > 0 and height > 0:
                            logging.info(f"方法2成功获取分辨率: {width}x{height} - {os.path.basename(video_path)}")
                            return width, height
                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    logging.debug(f"JSON格式解析失败: {e}")
            
            # 方法3: 使用 default 格式输出
            cmd_default = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                          '-show_entries', 'stream=width,height', 
                          '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
            result_default = subprocess.run(cmd_default, capture_output=True, text=True, encoding='utf-8', timeout=15)
            
            if result_default.returncode == 0 and result_default.stdout.strip():
                lines = result_default.stdout.strip().split('\n')
                if len(lines) >= 2:
                    try:
                        width = int(lines[0].strip())
                        height = int(lines[1].strip())
                        if width > 0 and height > 0:
                            logging.info(f"方法3成功获取分辨率: {width}x{height} - {os.path.basename(video_path)}")
                            return width, height
                    except (ValueError, IndexError) as e:
                        logging.debug(f"Default格式解析失败: {e}, 输出: '{result_default.stdout.strip()}'")
            
            # 方法4: 使用流信息查询 (最保守的方法)
            cmd_stream = [FFPROBE_PATH, '-v', 'quiet', '-print_format', 'json', 
                         '-show_streams', video_path]
            result_stream = subprocess.run(cmd_stream, capture_output=True, text=True, encoding='utf-8', timeout=20)
            
            if result_stream.returncode == 0 and result_stream.stdout.strip():
                try:
                    import json
                    data = json.loads(result_stream.stdout)
                    for stream in data.get('streams', []):
                        if stream.get('codec_type') == 'video':
                            width = stream.get('width')
                            height = stream.get('height')
                            if width and height and width > 0 and height > 0:
                                logging.info(f"方法4成功获取分辨率: {width}x{height} - {os.path.basename(video_path)}")
                                return width, height
                            break
                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    logging.debug(f"流信息解析失败: {e}")
            
            # 如果所有方法都失败，记录详细信息并重试
            if attempt < max_retries - 1:
                logging.warning(f"尝试 {attempt + 1}/{max_retries} 获取分辨率失败，{retry_delay}秒后重试: {os.path.basename(video_path)}")
                time.sleep(retry_delay)
                retry_delay *= 1.5  # 指数退避
            else:
                # 记录详细的错误信息
                logging.error(f"所有方法都无法获取视频分辨率: {os.path.basename(video_path)}")
                logging.error(f"  方法1 CSV 返回码: {result.returncode}, 输出: '{result.stdout.strip()}', 错误: '{result.stderr.strip()}'")
                logging.error(f"  方法2 JSON 返回码: {result_json.returncode}, 输出长度: {len(result_json.stdout)}")
                logging.error(f"  方法3 Default 返回码: {result_default.returncode}, 输出: '{result_default.stdout.strip()}'")
                logging.error(f"  方法4 Stream 返回码: {result_stream.returncode}, 输出长度: {len(result_stream.stdout)}")
                
        except subprocess.TimeoutExpired:
            logging.warning(f"获取分辨率超时 (尝试 {attempt + 1}/{max_retries}): {os.path.basename(video_path)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 1.5
        except Exception as e:
            logging.warning(f"获取分辨率异常 (尝试 {attempt + 1}/{max_retries}): {os.path.basename(video_path)} - {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 1.5
    
    # 所有尝试都失败
    logging.error(f"获取视频分辨率最终失败: {os.path.basename(video_path)}")
    return 0, 0

def should_skip_low_resolution_video(video_path: str) -> Tuple[bool, Tuple[int, int]]:
    """检查是否应该跳过低分辨率视频 - 增强版本
    
    Returns:
        Tuple[bool, Tuple[int, int]]: (是否跳过, (宽度, 高度))
    """
    if not SKIP_LOW_RESOLUTION_VIDEOS:
        return False, (0, 0)
    
    # 先检查文件是否存在和基本属性
    if not os.path.exists(video_path):
        logging.error(f"视频文件不存在: {video_path}")
        return False, (0, 0)
    
    file_size = os.path.getsize(video_path)
    if file_size < 1024:  # 小于1KB的文件肯定有问题
        logging.warning(f"视频文件太小 ({file_size} bytes)，跳过分辨率检查: {os.path.basename(video_path)}")
        return False, (0, 0)
    
    # 获取视频分辨率
    width, height = get_video_resolution(video_path)
    
    if width == 0 or height == 0:
        # 无法获取分辨率的视频，生成详细诊断报告
        logging.warning(f"无法获取分辨率，不跳过: {os.path.basename(video_path)}")
        logging.info(f"  建议手动检查此文件: {video_path}")
        logging.info(f"  文件大小: {file_size / (1024*1024):.2f} MB")
        
        # 生成诊断报告
        try:
            diagnosis = diagnose_video_file(video_path)
            logging.error(f"🔍 视频文件诊断报告 - {os.path.basename(video_path)}:")
            logging.error(f"  文件存在: {diagnosis['file_exists']}")
            logging.error(f"  文件大小: {diagnosis['file_size_mb']} MB")
            logging.error(f"  FFprobe可访问: {diagnosis['ffprobe_accessible']}")
            
            if 'raw_outputs' in diagnosis:
                for cmd_name, output in diagnosis['raw_outputs'].items():
                    if isinstance(output, dict):
                        if 'error' in output:
                            logging.error(f"  {cmd_name}: 错误 - {output['error']}")
                        else:
                            logging.error(f"  {cmd_name}: 返回码={output['returncode']}, 输出长度={output['stdout_length']}")
                            if output['returncode'] != 0 and output['stderr']:
                                logging.error(f"    错误信息: {output['stderr'][:100]}")
                            elif output['stdout']:
                                logging.error(f"    输出内容: {output['stdout'][:100]}")
            
            if diagnosis.get('duration', 0) > 0:
                logging.info(f"  文件时长: {diagnosis['duration']:.1f}秒 (文件可能有效但分辨率信息缺失)")
            elif 'duration_error' in diagnosis:
                logging.warning(f"  时长获取错误: {diagnosis['duration_error']}")
            else:
                logging.warning(f"  无法获取时长信息，文件可能损坏")
                
        except Exception as diag_error:
            logging.error(f"  诊断过程出错: {diag_error}")
        
        return False, (width, height)
    
    # 验证分辨率的合理性
    if width < 64 or height < 64 or width > 10000 or height > 10000:
        logging.warning(f"分辨率异常 ({width}x{height})，不跳过: {os.path.basename(video_path)}")
        return False, (width, height)
    
    # 检查是否低于最小分辨率阈值
    should_skip = width < MIN_RESOLUTION_WIDTH
    
    if should_skip:
        logging.info(f"跳过低分辨率视频: {os.path.basename(video_path)} ({width}x{height})")
        logging.info(f"  原因: 宽度 {width}px < 最小要求 {MIN_RESOLUTION_WIDTH}px")
    else:
        logging.debug(f"分辨率符合要求: {os.path.basename(video_path)} ({width}x{height})")
    
    return should_skip, (width, height)

def diagnose_video_file(video_path: str) -> Dict:
    """诊断视频文件，返回详细信息用于问题排查"""
    diagnosis = {
        'file_path': video_path,
        'file_name': os.path.basename(video_path),
        'file_exists': False,
        'file_size': 0,
        'file_size_mb': 0,
        'ffprobe_accessible': False,
        'raw_outputs': {},
        'resolution': (0, 0),
        'duration': 0,
        'diagnosis_time': time.time()
    }
    
    try:
        # 基本文件检查
        if os.path.exists(video_path):
            diagnosis['file_exists'] = True
            file_size = os.path.getsize(video_path)
            diagnosis['file_size'] = file_size
            diagnosis['file_size_mb'] = round(file_size / (1024*1024), 2)
        else:
            return diagnosis
        
        # 检查ffprobe是否可访问
        try:
            test_cmd = [FFPROBE_PATH, '-version']
            test_result = subprocess.run(test_cmd, capture_output=True, text=True, timeout=5)
            diagnosis['ffprobe_accessible'] = test_result.returncode == 0
        except Exception:
            diagnosis['ffprobe_accessible'] = False
        
        if not diagnosis['ffprobe_accessible']:
            return diagnosis
        
        # 测试各种ffprobe命令
        commands_to_test = [
            ('csv_format', [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                           '-show_entries', 'stream=width,height', '-of', 'csv=s=x:p=0', video_path]),
            ('json_format', [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                            '-show_entries', 'stream=width,height', '-of', 'json', video_path]),
            ('default_format', [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                               '-show_entries', 'stream=width,height', '-of', 'default=noprint_wrappers=1:nokey=1', video_path]),
            ('full_stream_info', [FFPROBE_PATH, '-v', 'quiet', '-print_format', 'json', '-show_streams', video_path])
        ]
        
        for cmd_name, cmd in commands_to_test:
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=30)
                diagnosis['raw_outputs'][cmd_name] = {
                    'returncode': result.returncode,
                    'stdout': result.stdout.strip()[:500],  # 限制输出长度
                    'stderr': result.stderr.strip()[:500],
                    'stdout_length': len(result.stdout),
                    'stderr_length': len(result.stderr)
                }
            except subprocess.TimeoutExpired:
                diagnosis['raw_outputs'][cmd_name] = {'error': 'timeout'}
            except Exception as e:
                diagnosis['raw_outputs'][cmd_name] = {'error': str(e)}
        
        # 尝试获取时长
        try:
            duration = get_media_duration_seconds(video_path)
            diagnosis['duration'] = duration
        except Exception as e:
            diagnosis['duration_error'] = str(e)
            
    except Exception as e:
        diagnosis['general_error'] = str(e)
    
    return diagnosis

def move_skipped_video(video_path: str, reason: str = "低分辨率") -> bool:
    """移动跳过的视频到指定目录
    
    Args:
        video_path: 源视频路径
        reason: 跳过原因
    
    Returns:
        bool: 是否成功移动
    """
    if not SKIP_VIDEOS_MOVE_DIR or not SKIP_VIDEOS_MOVE_DIR.strip():
        # 如果没有设置移动目录，则不移动
        return True
    
    try:
        # 确保移动目录存在
        os.makedirs(SKIP_VIDEOS_MOVE_DIR, exist_ok=True)
        
        video_name = os.path.basename(video_path)
        dest_path = os.path.join(SKIP_VIDEOS_MOVE_DIR, video_name)
        
        # 如果目标文件已存在，添加序号
        if os.path.exists(dest_path):
            name_without_ext = os.path.splitext(video_name)[0]
            ext = os.path.splitext(video_name)[1]
            counter = 1
            while os.path.exists(dest_path):
                new_name = f"{name_without_ext}_{counter}{ext}"
                dest_path = os.path.join(SKIP_VIDEOS_MOVE_DIR, new_name)
                counter += 1
        
        # 移动文件
        shutil.move(video_path, dest_path)
        logging.info(f"已移动{reason}视频: {video_name} -> {dest_path}")
        return True
        
    except Exception as e:
        logging.error(f"移动跳过视频失败 {video_path}: {e}")
        return False

def analyze_video_quality(video_path: str) -> Dict:
    """分析视频质量参数，用于优化输出设置"""
    try:
        # 获取详细的视频信息
        cmd = [FFPROBE_PATH, '-v', 'quiet', '-print_format', 'json', 
               '-show_format', '-show_streams', video_path]
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
        
        if result.returncode != 0 or not result.stdout.strip():
            return {}
        
        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            logging.warning(f"JSON解析失败 {video_path}: {e}")
            return {}
        video_stream = None
        
        # 找到视频流
        for stream in data.get('streams', []):
            if stream.get('codec_type') == 'video':
                video_stream = stream
                break
        
        if not video_stream:
            return {}
        
        # 提取关键信息
        width = int(video_stream.get('width', 0))
        height = int(video_stream.get('height', 0))
        fps = eval(video_stream.get('r_frame_rate', '25/1'))  # 帧率
        duration = float(video_stream.get('duration', 0))
        
        # 计算原始码率
        format_info = data.get('format', {})
        bitrate = int(format_info.get('bit_rate', 0))
        
        # 色彩空间信息
        color_space = video_stream.get('color_space', 'unknown')
        color_primaries = video_stream.get('color_primaries', 'unknown')
        
        quality_info = {
            'width': width,
            'height': height,
            'fps': fps,
            'duration': duration,
            'bitrate': bitrate,
            'bitrate_mbps': bitrate / 1000000 if bitrate > 0 else 0,
            'color_space': color_space,
            'color_primaries': color_primaries,
            'codec': video_stream.get('codec_name', 'unknown'),
            'pixel_format': video_stream.get('pix_fmt', 'unknown')
        }
        
        logging.debug(f"视频质量分析 {os.path.basename(video_path)}: {quality_info}")
        return quality_info
        
    except Exception as e:
        logging.warning(f"分析视频质量失败 {video_path}: {e}")
        return {}

def calculate_optimal_bitrate(source_info: Dict, target_resolution: Tuple[int, int]) -> Tuple[str, str, str]:
    """根据源视频信息计算最优码率"""
    if not AUTO_BITRATE or not source_info:
        return VIDEO_BITRATE, MAX_BITRATE, BUFFER_SIZE
    
    try:
        source_width = source_info.get('width', 1920)
        source_height = source_info.get('height', 1080)
        source_bitrate = source_info.get('bitrate_mbps', 10)
        source_fps = source_info.get('fps', 25)
        
        # 计算分辨率比例
        source_pixels = source_width * source_height
        target_pixels = target_resolution[0] * target_resolution[1]
        resolution_ratio = target_pixels / source_pixels if source_pixels > 0 else 1
        
        # 计算帧率比例（假设目标帧率保持原始帧率）
        fps_ratio = min(source_fps / 25, 1.5)  # 限制帧率影响
        
        # 基础码率计算
        base_bitrate = source_bitrate * resolution_ratio * fps_ratio
        
        # 质量模式调整
        quality_multipliers = {
            'highest': 1.3,
            'high': 1.1,
            'balanced': 1.0,
            'fast': 0.8
        }
        base_bitrate *= quality_multipliers.get(QUALITY_MODE, 1.0)
        
        # 确保码率在合理范围内
        if target_resolution == (1920, 1080):
            min_bitrate, max_suggested = 3, 25
        elif target_resolution == (3840, 2160):  # 4K
            min_bitrate, max_suggested = 10, 50
        else:
            min_bitrate, max_suggested = 2, 30
        
        base_bitrate = max(min_bitrate, min(base_bitrate, max_suggested))
        
        # 生成码率设置
        video_bitrate = f"{base_bitrate:.0f}M"
        max_bitrate = f"{base_bitrate * 1.5:.0f}M"
        buffer_size = f"{base_bitrate * 2:.0f}M"
        
        logging.info(f"自动码率计算: {video_bitrate} (最大: {max_bitrate}, 缓冲: {buffer_size})")
        return video_bitrate, max_bitrate, buffer_size
        
    except Exception as e:
        logging.warning(f"码率计算失败，使用默认值: {e}")
        return VIDEO_BITRATE, MAX_BITRATE, BUFFER_SIZE

def create_enhanced_progress_checkpoint(video_path: str, current_time: float, total_duration: float, segment_info: Dict = None) -> Dict:
    """创建增强的进度检查点，支持段级别续传"""
    checkpoint = {
        'video_path': video_path,
        'current_time': current_time,
        'total_duration': total_duration,
        'progress_percent': (current_time / total_duration * 100) if total_duration > 0 else 0,
        'timestamp': datetime.now().isoformat(),
        'computer_id': get_computer_unique_id(),  # 添加电脑唯一标识
        'segment_info': segment_info or {}
    }
    return checkpoint

def save_progress_checkpoint(checkpoint: Dict, checkpoint_file: str):
    """保存进度检查点到文件"""
    try:
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
            # 减少频繁的检查点保存日志
            pass
    except Exception as e:
        logging.warning(f"保存检查点失败: {e}")

def load_progress_checkpoint(checkpoint_file: str) -> Optional[Dict]:
    """从文件加载进度检查点"""
    try:
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
            # 减少频繁的检查点加载日志
            return checkpoint
    except Exception as e:
        logging.warning(f"加载检查点失败: {e}")
    return None

# ===================== END: 新增功能函数 =====================

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
    
    # 检查低分辨率跳过配置
    if SKIP_LOW_RESOLUTION_VIDEOS:
        if MIN_RESOLUTION_WIDTH <= 0:
            warnings.append(f"最小分辨率宽度设置无效: {MIN_RESOLUTION_WIDTH}")
        if SKIP_VIDEOS_MOVE_DIR and SKIP_VIDEOS_MOVE_DIR.strip():
            # 检查移动目录的父目录是否存在
            parent_dir = os.path.dirname(SKIP_VIDEOS_MOVE_DIR)
            if parent_dir and not os.path.exists(parent_dir):
                warnings.append(f"跳过视频移动目录的父目录不存在: {parent_dir}")
        print(f"✅ 低分辨率跳过功能已启用 (最小宽度: {MIN_RESOLUTION_WIDTH}px)")
    else:
        print(f"ℹ️  低分辨率跳过功能已禁用")
    
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
    def __init__(self, progress_file=PROGRESS_FILE, temp_file=TEMP_PROGRESS_FILE, output_dir=None):
        # 动态创建电脑独有的进度文件路径
        if output_dir and os.path.exists(output_dir):
            # 在输出目录创建电脑独有的进度文件夹
            computer_id = get_computer_unique_id()
            progress_folder = os.path.join(output_dir, '.progress', computer_id)
            self.progress_file = os.path.join(progress_folder, "video_processing_progress.json")
            self.temp_file = os.path.join(progress_folder, "video_processing_progress.tmp")
            logging.info(f"使用电脑独有进度文件夹: {progress_folder}")
        else:
            # 回退到原有逻辑
            if progress_file and temp_file:
                self.progress_file = progress_file
                self.temp_file = temp_file
            else:
                # 使用默认PROGRESS_FOLDER
                computer_id = get_computer_unique_id()
                progress_folder = os.path.join(PROGRESS_FOLDER, computer_id)
                self.progress_file = os.path.join(progress_folder, "video_processing_progress.json")
                self.temp_file = os.path.join(progress_folder, "video_processing_progress.tmp")
                logging.info(f"使用默认进度文件夹: {progress_folder}")
        
        self.computer_id = get_computer_unique_id()
        self.progress_folder = os.path.dirname(self.progress_file)
        
        # 自动创建进度记录文件夹
        self.ensure_progress_folder()
        
        self.progress_data = self.load_progress()
        
        # 为单个视频进度文件创建子文件夹
        self.individual_progress_folder = os.path.join(self.progress_folder, 'individual')
        if not os.path.exists(self.individual_progress_folder):
            os.makedirs(self.individual_progress_folder, exist_ok=True)
    
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
        with progress_save_lock:  # 添加锁保护，防止并发读写冲突
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
        with progress_save_lock:  # 添加锁保护，防止并发冲突
            video_name = os.path.basename(video_path)
            if video_name not in self.progress_data['processing']:
                self.progress_data['processing'].append(video_name)
            self.save_progress()
    
    def atomic_check_and_mark_processing(self, video_path, output_dir):
        """原子性检查是否已完成并标记为处理中，防止重复处理"""
        with progress_save_lock:
            # 在锁内重新检查是否已完成（双重检查锁定模式）
            if self.is_completed(video_path, output_dir):
                return False, "已完成"
            
            # 检查是否正在处理中
            if self.is_processing(video_path):
                return False, "正在处理中"
            
            # 最终检查输出文件是否存在（防止文件系统延迟）
            video_name = os.path.basename(video_path)
            output_path = os.path.join(output_dir, video_name)
            if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
                # 快速验证文件完整性
                try:
                    duration = get_media_duration_seconds(output_path)
                    if duration > 0:
                        # 文件存在且有效，标记为已完成
                        self.mark_completed(video_path, output_path)
                        return False, f"文件已存在且有效（时长: {duration:.1f}s）"
                except Exception:
                    pass  # 验证失败，继续处理
            
            # 原子性标记为处理中
            self.mark_processing(video_path)
            return True, "可以处理"
    
    def clean_error_message(self, error_msg):
        """清理错误消息，移除FFmpeg进度信息"""
        if not error_msg:
            return "处理失败"
        
        # 如果错误消息包含FFmpeg输出，进行过滤
        if 'frame=' in error_msg or 'fps=' in error_msg:
            lines = error_msg.split('\n')
            clean_lines = []
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # 跳过进度信息行和统计信息
                if (line.startswith('frame=') or 
                    'fps=' in line or 
                    'time=' in line or 
                    'bitrate=' in line or 
                    'speed=' in line or
                    'Last message repeated' in line or
                    line.startswith('[libx264') or
                    line.startswith('[aac') or
                    'Avg QP:' in line or
                    'mb I' in line or
                    'mb P' in line or
                    'mb B' in line or
                    'transform intra:' in line or
                    'coded y,uvDC' in line or
                    'Weighted P-Frames:' in line or
                    'kb/s:' in line or
                    'Qavg:' in line):
                    continue
                
                # 保留错误信息
                if ('Error' in line or 'error' in line or 
                    'failed' in line or 'Failed' in line or
                    'Terminating' in line or 'exit' in line):
                    clean_lines.append(line)
            
            # 只保留前3行关键错误信息
            if clean_lines:
                return '\n'.join(clean_lines[:3])
            else:
                return "FFmpeg处理失败"
        
        return error_msg
    
    def mark_failed(self, video_path, error_msg=""):
        """标记视频为失败"""
        with progress_save_lock:  # 原子性操作，防止并发冲突
            video_name = os.path.basename(video_path)
            # 清理错误消息
            clean_error = self.clean_error_message(error_msg)
            
            # 检查是否已经在失败列表中
            failed_names = [f.get('name') if isinstance(f, dict) else f for f in self.progress_data['failed']]
            if video_name not in failed_names:
                self.progress_data['failed'].append({
                    'name': video_name,
                    'error': clean_error,
                    'time': datetime.now().isoformat()
                })
            # 从处理中移除
            if video_name in self.progress_data['processing']:
                self.progress_data['processing'].remove(video_name)
            self.save_progress()
    
    def is_processing(self, video_path):
        """检查视频是否正在处理中"""
        with progress_save_lock:  # 添加锁保护，防止并发读写冲突
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
    
    def get_individual_progress_file(self, video_path):
        """获取单个视频的进度文件路径"""
        video_name = os.path.splitext(os.path.basename(video_path))[0]
        # 使用安全的文件名
        safe_name = "".join(c for c in video_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_name = safe_name[:100]  # 限制长度
        return os.path.join(self.individual_progress_folder, f"{safe_name}.json")
    
    def save_individual_progress(self, video_path, progress_data):
        """保存单个视频的进度"""
        try:
            progress_file = self.get_individual_progress_file(video_path)
            with open(progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            # 减少单视频进度保存失败的日志
            pass
    
    def load_individual_progress(self, video_path):
        """加载单个视频的进度"""
        try:
            progress_file = self.get_individual_progress_file(video_path)
            if os.path.exists(progress_file):
                with open(progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            # 减少单视频进度加载失败的日志
            pass
        return None
    
    def update_individual_progress(self, video_path, status, progress_percent=0, message=""):
        """更新单个视频的进度"""
        progress_data = {
            'video_path': video_path,
            'status': status,  # 'processing', 'completed', 'failed'
            'progress_percent': progress_percent,
            'message': message,
            'timestamp': time.time(),
            'computer_id': self.computer_id
        }
        self.save_individual_progress(video_path, progress_data)
    
    def cleanup_individual_progress(self, video_path):
        """清理单个视频的进度文件"""
        try:
            progress_file = self.get_individual_progress_file(video_path)
            if os.path.exists(progress_file):
                os.remove(progress_file)
        except Exception as e:
            # 减少清理进度文件失败的日志
            pass
    
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

# 全局进度管理器 - 将在主程序中初始化
progress_manager = None

# 全局进度条位置管理
global progress_bar_counter
progress_bar_counter = 0
progress_bar_lock = threading.Lock()
# 可用的进度条位置池 - 用于重用已完成视频的位置
available_positions = []
position_lock = threading.Lock()

def get_progress_bar_position():
    """获取一个可用的进度条位置"""
    global progress_bar_counter
    with position_lock:
        if available_positions:
            # 重用已释放的位置
            return available_positions.pop(0)
        else:
            # 分配新位置
            with progress_bar_lock:
                current_position = progress_bar_counter
                progress_bar_counter += 1
                return current_position

def release_progress_bar_position(position):
    """释放进度条位置供其他视频重用"""
    with position_lock:
        if position not in available_positions:
            available_positions.append(position)
            available_positions.sort()  # 保持位置有序，优先重用较小的位置

# 进度保存锁
progress_save_lock = threading.Lock()

# 文件处理锁 - 防止重复处理
file_processing_locks = {}
file_locks_lock = threading.Lock()  # 保护文件锁字典的锁

def acquire_file_processing_lock(file_path):
    """获取文件处理锁，防止重复处理同一文件"""
    file_key = os.path.abspath(file_path)
    with file_locks_lock:
        if file_key not in file_processing_locks:
            file_processing_locks[file_key] = threading.Lock()
        file_lock = file_processing_locks[file_key]
    
    # 尝试获取锁，非阻塞
    acquired = file_lock.acquire(blocking=False)
    if acquired:
        logging.debug(f"🔒 获取文件处理锁成功: {os.path.basename(file_path)}")
    else:
        logging.info(f"⏭️ 文件正在被其他线程处理，跳过: {os.path.basename(file_path)}")
    return acquired, file_lock

def release_file_processing_lock(file_path, file_lock):
    """释放文件处理锁"""
    try:
        if file_lock:  # 确保锁对象存在
            file_lock.release()
            logging.debug(f"🔓 释放文件处理锁: {os.path.basename(file_path)}")
    except threading.ThreadError:
        logging.warning(f"⚠️ 尝试释放未持有的锁: {os.path.basename(file_path)}")
    except Exception as e:
        logging.error(f"❌ 释放文件锁时发生异常: {os.path.basename(file_path)}, 错误: {e}")

def cleanup_file_processing_locks():
    """清理不再使用的文件锁"""
    try:
        with file_locks_lock:
            keys_to_remove = []
            for file_key, lock in list(file_processing_locks.items()):  # 使用list()防止字典在迭代时被修改
                try:
                    # 尝试快速获取和释放锁来检查是否空闲
                    if lock.acquire(blocking=False):
                        lock.release()
                        keys_to_remove.append(file_key)
                except Exception as e:
                    logging.warning(f"检查锁状态时发生异常: {file_key}, 错误: {e}")
                    # 如果锁有异常，也将其标记为待清理
                    keys_to_remove.append(file_key)
            
            for key in keys_to_remove:
                try:
                    del file_processing_locks[key]
                except KeyError:
                    pass  # 键可能已经被其他线程删除
            
            if keys_to_remove:
                logging.info(f"🧹 清理了 {len(keys_to_remove)} 个空闲的文件锁")
    except Exception as e:
        logging.error(f"❌ 清理文件锁时发生异常: {e}")

def get_file_processing_locks_status():
    """获取当前文件处理锁的状态信息"""
    try:
        with file_locks_lock:
            total_locks = len(file_processing_locks)
            active_locks = 0
            locked_files = []
            
            for file_key, lock in file_processing_locks.items():
                try:
                    # 非阻塞尝试获取锁来检查状态
                    if not lock.acquire(blocking=False):
                        active_locks += 1
                        locked_files.append(os.path.basename(file_key))
                    else:
                        lock.release()  # 立即释放测试锁
                except:
                    active_locks += 1  # 异常的锁也算作活跃状态
                    locked_files.append(os.path.basename(file_key))
            
            return {
                'total_locks': total_locks,
                'active_locks': active_locks,
                'locked_files': locked_files
            }
    except Exception as e:
        logging.error(f"获取文件锁状态时发生异常: {e}")
        return {'total_locks': 0, 'active_locks': 0, 'locked_files': []}

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

# 设置一些模块的日志级别为WARNING以减少输出
logging.getLogger('PIL').setLevel(logging.WARNING)
logging.getLogger('matplotlib').setLevel(logging.WARNING)

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
                           "spatial_aq": "1", "temporal_aq": "1", "rc": "vbr", "cq": "25"}},
    "amd": {"encoders": ["h264_amf", "hevc_amf"],
            "options": {"quality": "balanced", "b:v": VIDEO_BITRATE, "rc": "vbr_peak_constrained", "usage": "transcoding"}},
    "intel": {"encoders": ["h264_qsv", "hevc_qsv"],
              "options": {"preset": "fast", "b:v": VIDEO_BITRATE, "global_quality": "25", "look_ahead": "1"}},
    "software": {"encoders": ["libx264", "libx265"],
                 "options": {"preset": "fast", "crf": "25", "threads": "0", "aq-mode": "3", "x264opts": "keyint=60:min-keyint=30"}}
}

# 添加ROI选择回退函数
def adjust_roi_to_video_size(roi, video_width, video_height):
    """
    智能调整ROI：在用户选择的ROI区域内找到最大的16:9矩形，然后按比例缩小
    
    流程：
    1. 用户选择ROI区域
    2. 在该区域内找到最大的16:9比例矩形
    3. 按比例缩小这个16:9矩形，使其与正常视频画面保持相同比例关系
    
    Args:
        roi: (x, y, w, h) 用户选择的ROI区域
        video_width: 视频宽度
        video_height: 视频高度
    
    Returns:
        adjusted_roi: (x, y, w, h) 调整后的16:9 ROI
        was_adjusted: bool 是否进行了调整
    """
    x, y, w, h = roi
    original_roi = roi
    was_adjusted = False
    
    # 首先处理ROI超出边界的情况
    # 1. 处理负坐标和边界溢出
    if x < 0:
        w = w + x  # 减少宽度
        x = 0
    if y < 0:
        h = h + y  # 减少高度
        y = 0
    
    # 确保ROI不超出视频边界
    if x + w > video_width:
        w = video_width - x
    if y + h > video_height:
        h = video_height - y
    
    # 确保ROI有效
    if w <= 0 or h <= 0:
        # ROI无效，重置为视频中心的合理区域
        w = min(video_width * 0.8, video_width)
        h = min(video_height * 0.8, video_height)
        x = (video_width - w) // 2
        y = (video_height - h) // 2
        was_adjusted = True
        logging.warning(f"ROI无效，重置为中心区域: ({x}, {y}, {w}, {h})")
    
    # 2. 在调整后的ROI区域内找到最大的16:9矩形
    target_aspect = 16 / 9
    roi_aspect = w / h if h > 0 else target_aspect
    
    # 计算在当前ROI区域内能容纳的最大16:9矩形
    if roi_aspect > target_aspect:
        # ROI比16:9更宽，以高度为准
        new_h = h
        new_w = int(h * target_aspect)
        # 在ROI内水平居中
        new_x = x + (w - new_w) // 2
        new_y = y
    else:
        # ROI比16:9更高（或相等），以宽度为准
        new_w = w
        new_h = int(w / target_aspect)
        # 在ROI内垂直居中
        new_x = x
        new_y = y + (h - new_h) // 2
    
    # 确保新的16:9矩形在视频边界内
    new_x = max(0, min(new_x, video_width - new_w))
    new_y = max(0, min(new_y, video_height - new_h))
    
    # 如果调整后的矩形仍然超出边界，需要缩小
    if new_x + new_w > video_width or new_y + new_h > video_height:
        # 重新计算能容纳的最大16:9尺寸
        max_w = video_width - new_x
        max_h = video_height - new_y
        
        if max_w / max_h > target_aspect:
            # 高度是限制因素
            new_h = max_h
            new_w = int(max_h * target_aspect)
        else:
            # 宽度是限制因素
            new_w = max_w
            new_h = int(max_w / target_aspect)
        
        was_adjusted = True
    
    # 3. 检查是否需要按比例缩小
    # 计算16:9矩形相对于视频的比例
    video_aspect = video_width / video_height
    roi_to_video_ratio_w = new_w / video_width
    roi_to_video_ratio_h = new_h / video_height
    
    # 如果ROI占视频的比例太大（比如超过90%），按比例缩小
    max_ratio = 0.9  # 最大占比90%
    if roi_to_video_ratio_w > max_ratio or roi_to_video_ratio_h > max_ratio:
        scale_factor = min(max_ratio / roi_to_video_ratio_w, max_ratio / roi_to_video_ratio_h)
        
        scaled_w = int(new_w * scale_factor)
        scaled_h = int(scaled_w / target_aspect)  # 保持16:9比例
        
        # 重新居中
        center_x = new_x + new_w // 2
        center_y = new_y + new_h // 2
        
        new_x = center_x - scaled_w // 2
        new_y = center_y - scaled_h // 2
        
        # 确保不超出边界
        new_x = max(0, min(new_x, video_width - scaled_w))
        new_y = max(0, min(new_y, video_height - scaled_h))
        
        new_w, new_h = scaled_w, scaled_h
        was_adjusted = True
        
        logging.info(f"ROI占比过大，按比例缩小: 缩放因子={scale_factor:.3f}")
    
    # 确保最小尺寸（保持16:9比例）
    min_w = 64  # 最小宽度
    min_h = int(min_w / target_aspect)  # 对应的16:9高度
    
    if new_w < min_w or new_h < min_h:
        new_w = max(min_w, new_w)
        new_h = int(new_w / target_aspect)
        
        # 重新检查边界
        if new_x + new_w > video_width:
            new_x = video_width - new_w
        if new_y + new_h > video_height:
            new_y = video_height - new_h
        
        was_adjusted = True
    
    final_roi = (new_x, new_y, new_w, new_h)
    
    # 如果有调整，记录信息
    if was_adjusted or final_roi != original_roi:
        was_adjusted = True
        logging.info(f"ROI智能调整为16:9: {original_roi} -> {final_roi}")
        
        # 计算调整信息
        original_area = original_roi[2] * original_roi[3]
        final_area = new_w * new_h
        area_change_percent = (final_area - original_area) / original_area * 100 if original_area > 0 else 0
        
        final_aspect = new_w / new_h if new_h > 0 else target_aspect
        
        print(f"🎯 ROI智能调整为16:9:")
        print(f"   用户选择ROI: {original_roi}")
        print(f"   最终16:9 ROI: {final_roi}")
        print(f"   宽高比: {final_aspect:.3f} (目标: {target_aspect:.3f})")
        print(f"   面积变化: {area_change_percent:+.1f}%")
        print(f"   占视频比例: {new_w/video_width*100:.1f}% × {new_h/video_height*100:.1f}%")
        
        if abs(final_aspect - target_aspect) < 0.01:
            print(f"   ✅ 成功调整为标准16:9比例")
        else:
            print(f"   ⚠️  比例略有偏差，受边界限制")
    
    return final_roi, was_adjusted

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
    """向后兼容的硬件检测函数，调用新的高级检测"""
    try:
        # 使用新的高级硬件检测
        return detect_advanced_hardware()
    except Exception as e:
        logging.error(f"高级硬件检测失败，使用基础检测: {e}")
        # 回退到基础检测
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
    except Exception as e2:
        logging.error(f"硬件检测失败: {e2}")
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
        if not os.path.exists(media_path):
            return 0.0
        
        # 移除文件大小限制，因为正在处理的文件可能很大但损坏
        file_size = os.path.getsize(media_path)
        if file_size == 0:
            return 0.0
            
        cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format=duration',
               '-of', 'default=noprint_wrappers=1:nokey=1', media_path]
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=10)
        
        if result.returncode != 0:
            # 如果format读取失败，尝试从视频流读取
            cmd_stream = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0',
                         '-show_entries', 'stream=duration',
                         '-of', 'default=noprint_wrappers=1:nokey=1', media_path]
            result_stream = subprocess.run(cmd_stream, capture_output=True, text=True, encoding='utf-8', timeout=10)
            if result_stream.returncode == 0 and result_stream.stdout.strip():
                val = result_stream.stdout.strip()
                return float(val) if val and val != 'N/A' else 0.0
            return 0.0
            
        val = result.stdout.strip()
        return float(val) if val and val != 'N/A' else 0.0
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

def build_ffmpeg_command(input_file, output_file, filter_complex, hw_info, seek_seconds=0, source_quality_info=None, target_resolution=(1920, 1080)):
    """构建FFmpeg命令 - 采用1.0版本的稳定策略"""
    cmd = [FFMPEG_PATH, '-y', '-nostdin']
    
    if seek_seconds > 0: 
        cmd.extend(['-ss', str(seek_seconds)])
    
    cmd.extend(['-i', input_file, '-vf', filter_complex, '-c:v', hw_info['encoder']])
    
    # 添加分辨率强制设置，确保输出分辨率（采用1.0版本策略）
    cmd.extend(['-s', f'{target_resolution[0]}x{target_resolution[1]}'])
    
    # 获取动态码率设置
    if source_quality_info and AUTO_BITRATE:
        video_bitrate, max_bitrate, buffer_size = calculate_optimal_bitrate(source_quality_info, target_resolution)
    else:
        video_bitrate, max_bitrate, buffer_size = VIDEO_BITRATE, MAX_BITRATE, BUFFER_SIZE
    
    # GOP设置（采用1.0版本的稳定配置）
    if hw_info['encoder_type'] == 'nvidia':
        gop_settings = ['-g', '120', '-keyint_min', '60', '-rc', 'vbr', '-cq', '25']
    elif hw_info['encoder_type'] == 'amd':
        gop_settings = ['-g', '120', '-keyint_min', '60', '-rc', 'vbr_peak_constrained']
    elif hw_info['encoder_type'] == 'intel':
        gop_settings = ['-g', '120', '-keyint_min', '60', '-global_quality', '25']
    else:
        gop_settings = ['-g', '120', '-keyint_min', '60', '-sc_threshold', '40', '-bf', '2']
    
    cmd.extend(gop_settings)
    
    # 核心编码参数（采用1.0版本的essential_options策略）
    essential_options = {}
    if hw_info['encoder_type'] == 'nvidia':
        essential_options = {'preset': 'p2', 'b:v': video_bitrate, 'maxrate': max_bitrate, 'bufsize': buffer_size}
    elif hw_info['encoder_type'] == 'amd':
        essential_options = {'quality': 'balanced', 'b:v': video_bitrate}
    elif hw_info['encoder_type'] == 'intel':
        essential_options = {'preset': 'fast', 'b:v': video_bitrate}
    else:
        essential_options = {'preset': 'fast', 'crf': '25', 'threads': '0'}
    
    for key, value in essential_options.items():
        cmd.extend([f'-{key}', str(value)])
    
    # 长视频稳定性参数（采用1.0版本的稳定设置）
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


def run_ffmpeg_process(cmd, duration, pbar, initial_time_offset: float = 0.0, video_path: str = None):
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                               encoding='utf-8', errors='ignore', bufsize=1)
    last_percentage, last_update_time, stalled_time = 0, time.time(), 0
    no_progress_count = 0
    last_progress_time = 0
    
    # 对于长视频，调整超时参数
    is_long_video = duration > 3600  # 超过1小时算长视频
    max_stall_time = 300 if is_long_video else 120  # 长视频5分钟，短视频2分钟
    max_no_progress_time = 600 if is_long_video else 300  # 长视频10分钟，短视频5分钟
    
    # 添加进度监控变量
    last_progress_percentage = 0
    progress_stuck_time = 0
    progress_check_interval = 30  # 每30秒检查一次进度是否卡住
    last_progress_check = time.time()
    
    logging.info(f"视频时长: {duration:.1f}秒, 长视频模式: {is_long_video}, 最大卡死时间: {max_stall_time}秒, 最大无进度时间: {max_no_progress_time}秒, 续传偏移: {initial_time_offset:.1f}s")
    
    while process.poll() is None:
        line = process.stderr.readline()
        if line:
            progress_info = parse_progress(line)
            if 'time' in progress_info:
                last_update_time = time.time()
                no_progress_count = 0  # 重置无进度计数
                
                # 采用1.0版本的稳定进度计算策略
                current_time = initial_time_offset + progress_info['time']
                if current_time > duration:
                    current_time = duration
                
                # 采用1.0版本的进度计算：限制到95%，然后由update_final_progress处理
                if current_time >= duration * 0.95:  # 如果已经处理了95%以上
                    percentage = 95
                else:
                    percentage = min(95, 10 + current_time * 85 / duration)  # 10%-95%范围
                
                # 简化进度检查（采用1.0版本策略）
                # 移除复杂的进度卡住检查，使用更简单的逻辑
                
                if percentage > last_percentage:
                    pbar.update(percentage - last_percentage)
                    last_percentage = percentage
                    last_progress_time = current_time
                    
                    # 更新单视频进度
                    if video_path:
                        progress_manager.update_individual_progress(
                            video_path, 'processing', percentage, 
                            f"处理中 {percentage:.1f}% ({current_time:.1f}s/{duration:.1f}s)"
                        )
                    
                    # 采用1.0版本的简化显示
                    postfix = {'FPS': f"{progress_info.get('fps', 0):.1f}",
                               '速度': f"{progress_info.get('speed', 0):.1f}x",
                               '大小': f"{progress_info.get('size', 0) / 1024:.1f}MB",
                               '时间': f"{current_time:.1f}s/{duration:.1f}s",
                               '进度': f"{current_time/duration*100:.1f}%"}
                    pbar.set_postfix(postfix)
                    
                    # 简化日志输出
                    if int(percentage) % 20 == 0 and int(percentage) != int(last_percentage):
                        logging.info(f"处理进度: {percentage:.1f}% - FPS: {progress_info.get('fps', 0):.1f}, 速度: {progress_info.get('speed', 0):.1f}x, 时间: {current_time:.1f}s/{duration:.1f}s")
                
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
        # 只读取剩余的stderr内容，并过滤掉进度信息
        remaining_stderr = process.stderr.read()
        error_lines = []
        for line in remaining_stderr.split('\n'):
            # 过滤掉进度行和一些常见的非错误信息
            if (line.strip() and 
                not line.startswith('frame=') and 
                not line.startswith('size=') and
                not line.startswith('time=') and
                not line.startswith('bitrate=') and
                not line.startswith('speed=') and
                'fps=' not in line and
                not line.strip().startswith('Last message repeated')):
                error_lines.append(line.strip())
        
        # 只保留最后10行真正的错误信息
        filtered_errors = error_lines[-10:] if error_lines else ["无具体错误信息"]
        error_msg = '\n'.join(filtered_errors)
        raise Exception(f"ffmpeg处理失败 (代码 {process.returncode}): {error_msg}")
    
    # 采用1.0版本的策略：确保进度条到达95%（由update_final_progress处理95-100%）
    if last_percentage < 95:
        pbar.update(95 - last_percentage)


def update_final_progress(pbar, video_path, stage_name="最终处理"):
    """更新最终进度 - 采用1.0版本的简化策略"""
    # 采用1.0版本的简单做法：直接跳到100%
    if pbar.n < 100:
        pbar.update(100 - pbar.n)
        pbar.set_postfix_str(f"{stage_name}完成✓")
        if video_path:
            progress_manager.update_individual_progress(video_path, 'completed', 100, f"{stage_name}完成")


def process_video(video_path, output_video_path, roi, hardware_info, video_idx=0, total_videos=1,
                  target_resolution=(1920, 1080)):
    filename = os.path.basename(video_path)
    output_dir = os.path.dirname(output_video_path)
    
    # 获取文件处理锁，防止重复处理
    lock_acquired, file_lock = acquire_file_processing_lock(video_path)
    if not lock_acquired:
        logging.info(f"⏭️ 文件 {filename} 已被其他线程处理，跳过")
        # 获取当前锁状态信息
        lock_status = get_file_processing_locks_status()
        logging.debug(f"当前锁状态: 总锁数={lock_status['total_locks']}, 活跃锁数={lock_status['active_locks']}")
        return False
    
    try:
        # 获取一个可用的进度条位置
        current_position = get_progress_bar_position()
        
        pbar = tqdm(total=100, desc=f"视频 {video_idx + 1}/{total_videos}: {filename[:25]:<25}", position=current_position + 1,
                    leave=False,  # 完成后自动清除进度条
                    bar_format='{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]')
        
        # 原子性检查并标记为处理中，防止重复处理
        can_process, reason = progress_manager.atomic_check_and_mark_processing(video_path, output_dir)
        if not can_process:
            pbar.set_postfix_str(f"跳过: {reason}")
            logging.info(f"跳过视频处理: {filename} - {reason}")
            pbar.update(100)
            time.sleep(0.1)
            release_progress_bar_position(current_position)
            pbar.close()
            return True  # 返回True因为文件已经完成或正在被处理
        
        # 从这里开始是实际的处理逻辑
        pbar.set_postfix_str("开始处理...")
        
    except Exception as lock_e:
        # 锁获取阶段的异常处理
        logging.error(f"获取处理锁或初始化时发生异常: {lock_e}")
        release_file_processing_lock(video_path, file_lock)
        return False
    
    # 主处理逻辑，确保在所有情况下都能释放锁
    try:
        # 初始化单视频进度
        progress_manager.update_individual_progress(video_path, 'processing', 0, "开始处理视频")
        
        # 获取视频尺寸信息用于ROI调整
        try:
            pbar.set_postfix_str("检查视频尺寸...")
            probe_cmd = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                        '-show_entries', 'stream=width,height', '-of', 'csv=s=x:p=0', video_path]
            result = subprocess.run(probe_cmd, capture_output=True, text=True, encoding='utf-8')
            if result.returncode == 0 and 'x' in result.stdout:
                video_width, video_height = map(int, result.stdout.strip().split('x'))
                logging.info(f"视频尺寸: {video_width}x{video_height}")
                
                # 检查并调整ROI
                adjusted_roi, was_adjusted = adjust_roi_to_video_size(roi, video_width, video_height)
                if was_adjusted:
                    pbar.set_postfix_str("ROI已自动调整")
                    roi = adjusted_roi  # 使用调整后的ROI
                    logging.info(f"为视频 {filename} 调整ROI: {roi}")
            else:
                logging.warning(f"无法获取视频 {filename} 的尺寸信息，使用原始ROI")
        except Exception as e:
            logging.warning(f"获取视频尺寸失败: {e}，使用原始ROI")
        
        # 创建视频专用的检查点文件
        checkpoint_dir = os.path.join(os.path.dirname(progress_manager.progress_file), 'checkpoints')
        os.makedirs(checkpoint_dir, exist_ok=True)
        checkpoint_file = os.path.join(checkpoint_dir, f"{hashlib.md5(video_path.encode()).hexdigest()}.json")
        
        try:
            # 分析视频质量
            pbar.set_postfix_str("分析视频质量...")
            source_quality_info = analyze_video_quality(video_path)
            
            # 尝试从视频流获取时长信息
            probe_cmd = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=duration', '-of',
                         'json', video_path]
            result = subprocess.run(probe_cmd, capture_output=True, text=True, encoding='utf-8')
            
            duration = 0.0
            if result.returncode == 0 and result.stdout.strip():
                try:
                    probe_data = json.loads(result.stdout)
                    if 'streams' in probe_data and len(probe_data['streams']) > 0:
                        stream_duration = probe_data['streams'][0].get('duration')
                        if stream_duration and stream_duration != 'N/A':
                            duration = float(stream_duration)
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    logging.warning(f"解析视频流时长失败: {e}")
            
            # 如果从视频流获取失败，尝试从format获取
            if duration <= 0:
                logging.info("从视频流获取时长失败，尝试从format获取...")
                probe_cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format=duration', '-of',
                             'json', video_path]
                result = subprocess.run(probe_cmd, capture_output=True, text=True, encoding='utf-8')
                
                if result.returncode == 0 and result.stdout.strip():
                    try:
                        probe_data = json.loads(result.stdout)
                        if 'format' in probe_data:
                            format_duration = probe_data['format'].get('duration')
                            if format_duration and format_duration != 'N/A':
                                duration = float(format_duration)
                    except (json.JSONDecodeError, ValueError, KeyError) as e:
                        logging.warning(f"解析format时长失败: {e}")
            
            # 最后的备用方案：使用现有的get_media_duration_seconds函数
            if duration <= 0:
                logging.info("使用备用方案获取视频时长...")
                duration = get_media_duration_seconds(video_path)
            
            if duration <= 0: 
                raise Exception("视频时长为0或无效. 切换至备用方案...")

            # 使用force_original_aspect_ratio=disable强制缩放到目标分辨率
            filter_complex = f"crop={roi[2]}:{roi[3]}:{roi[0]}:{roi[1]},scale={target_resolution[0]}:{target_resolution[1]}:force_original_aspect_ratio=disable"

            # 记录源视频信息到进度条
            if source_quality_info:
                format_info = f"{source_quality_info.get('codec', 'unknown')} {source_quality_info.get('bitrate_mbps', 0):.1f}Mbps"
                pbar.set_description(f"视频 {video_idx + 1}/{total_videos}: {filename[:20]:<20} ({format_info})")
            
            logging.info(f"开始处理视频: {filename}, 时长: {duration:.1f}s, 质量: {source_quality_info.get('bitrate_mbps', 0):.1f}Mbps")

            # 增强断点续传：检查检查点和输出文件
            checkpoint = load_progress_checkpoint(checkpoint_file)
            existing_duration = 0
            output_file_exists = os.path.exists(output_video_path)
            
            if output_file_exists:
                existing_duration = get_media_duration_seconds(output_video_path)
                # 如果输出文件存在但时长为0，说明文件损坏，删除重新开始
                if existing_duration == 0.0 and os.path.getsize(output_video_path) > 0:
                    logging.warning(f"检测到损坏的输出文件 (大小:{os.path.getsize(output_video_path)} bytes, 时长:0s)，删除重新开始")
                    try:
                        os.remove(output_video_path)
                        output_file_exists = False
                        existing_duration = 0
                        logging.info("已删除损坏的输出文件")
                    except Exception as e:
                        logging.error(f"删除损坏文件失败: {e}")
            
            logging.info(f"断点续传检查: 检查点文件={os.path.exists(checkpoint_file)}, "
                        f"输出文件存在={output_file_exists}, "
                        f"输出文件时长={existing_duration:.1f}s, 源文件时长={duration:.1f}s")
            
            # 额外的调试信息
            if checkpoint:
                logging.info(f"检查点详情: 时间={checkpoint.get('current_time', 0):.1f}s, "
                            f"电脑ID匹配={checkpoint.get('computer_id') == get_computer_unique_id()}")
            
            resume_from = 0
            
            # 优先使用检查点信息
            if checkpoint and checkpoint.get('computer_id') == get_computer_unique_id():
                checkpoint_time = checkpoint.get('current_time', 0)
                logging.info(f"发现检查点: 时间={checkpoint_time:.1f}s")
                
                if checkpoint_time > 10:  # 检查点时间合理
                    resume_from = checkpoint_time
                    logging.info(f"✅ 使用检查点续传: 从 {resume_from:.1f}s 继续")
                
            # 如果没有有效检查点，检查输出文件
            elif output_file_exists and existing_duration > 10 and existing_duration < duration * 0.99:
                resume_from = existing_duration
                logging.info(f"✅ 使用输出文件续传: 从 {resume_from:.1f}s 继续")
                
            # 判断是否需要续传
            if resume_from > 10:
                logging.info(f"🔄 断点续传模式: 从 {resume_from:.1f}s 继续处理")
            elif output_file_exists and existing_duration >= duration * 0.99:
                logging.info(f"✅ 输出文件已完整 ({existing_duration:.1f}s >= {duration * 0.99:.1f}s)，跳过处理")
                # 标记为已完成并跳过
                progress_manager.mark_completed(video_path, output_video_path)
                pbar.set_postfix_str("已完成✓")
                pbar.update(100)
                # 短暂延迟，让用户看到完成状态
                time.sleep(0.1)
                # 释放进度条位置供其他视频重用
                release_progress_bar_position(current_position)
                pbar.close()
                return True
            else:
                logging.info("🆕 全新处理: 没有发现可续传的文件")
        
            if resume_from > 10:  # 只有超过10秒才值得续传
                progress_percent = (resume_from / duration) * 100
                logging.info(f"🔄 断点续传: {filename} - 已完成 {resume_from:.1f}s / {duration:.1f}s ({progress_percent:.1f}%)")
                pbar.set_postfix_str(f"续传中 ({progress_percent:.1f}%)")
                
                # 保存续传检查点
                resume_checkpoint = create_enhanced_progress_checkpoint(video_path, resume_from, duration)
                save_progress_checkpoint(resume_checkpoint, checkpoint_file)
                
                # 更新进度条显示续传进度（但不超过90%，为后续处理留空间）
                initial_progress = min(progress_percent, 90)
                pbar.update(initial_progress)
                
                # 将已有部分暂存为 part1
                part1_path = output_video_path + ".part1.mp4"
                try:
                    if os.path.exists(output_video_path):
                        shutil.move(output_video_path, part1_path)
                except Exception as e:
                    logging.warning(f"移动输出文件失败: {e}")
                    part1_path = None
                
                # 从 resume_from 续传生成 part2
                part2_path = output_video_path + ".part2.mp4"
                cmd = build_ffmpeg_command(video_path, part2_path, filter_complex, hardware_info, 
                                         seek_seconds=int(resume_from), source_quality_info=source_quality_info, 
                                         target_resolution=target_resolution)
                logging.info(f"执行命令(增强续传): {' '.join(cmd)}")
                remaining_duration = duration - resume_from
                logging.info(f"续传剩余时长: {remaining_duration:.1f}s")
                run_ffmpeg_process(cmd, remaining_duration, pbar, initial_time_offset=resume_from, video_path=video_path)
                
                # 更新最终阶段进度 (95-100%)
                update_final_progress(pbar, video_path, "续传处理")
                
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
                # 完整处理
                pbar.set_postfix_str("尝试优化编码...")
                cmd = build_ffmpeg_command(video_path, output_video_path, filter_complex, hardware_info, 
                                         source_quality_info=source_quality_info, target_resolution=target_resolution)
                logging.info(f"执行命令(全新处理): {' '.join(cmd)}")
                
                # 定期保存检查点
                def save_checkpoint_during_processing(current_time):
                    checkpoint = create_enhanced_progress_checkpoint(video_path, current_time, duration)
                    save_progress_checkpoint(checkpoint, checkpoint_file)
                
                run_ffmpeg_process(cmd, duration, pbar, video_path=video_path)
            
            # 更新最终阶段进度 (95-100%)
            update_final_progress(pbar, video_path, "主要处理")
            
            # 验证输出文件
            if not os.path.exists(output_video_path) or os.path.getsize(output_video_path) < 1024:
                raise Exception(f"输出文件无效或太小: {output_video_path}")
            
            # 清理检查点文件
            try:
                if os.path.exists(checkpoint_file):
                    os.remove(checkpoint_file)
            except Exception:
                pass
            
            # 标记为已完成
            progress_manager.mark_completed(video_path, output_video_path)
            
            # 更新单视频进度为完成
            progress_manager.update_individual_progress(video_path, 'completed', 100, "处理完成")
        
        # 清理单视频进度文件（可选，保留一段时间用于查看）
        # progress_manager.cleanup_individual_progress(video_path)
        
            pbar.set_postfix_str("完成✓")
            logging.info(f"视频处理完成: {video_path} -> {output_video_path}")
            # 短暂延迟，让用户看到完成状态
            time.sleep(0.1)
            # 释放进度条位置供其他视频重用
            release_progress_bar_position(current_position)
            pbar.close()
            return True
            
        except Exception as e:
            logging.info("主策略失败，切换至备用方案...")
            # 只在调试模式下记录详细错误
            logging.debug(f"主策略错误详情: {e}")
        
            # 如果是NVENC失败，先尝试兼容性更好的NVENC参数
            if hardware_info['encoder_type'] == 'nvidia' and ('InitializeEncoder failed' in str(e) or 'Invalid Level' in str(e) or 'Invalid argument' in str(e)):
                try:
                    pbar.set_postfix_str("尝试兼容NVENC编码...")
                    logging.info("NVENC参数错误，尝试兼容性参数")
                    
                    # 重新计算filter_complex，使用强制缩放
                    filter_complex = f"crop={roi[2]}:{roi[3]}:{roi[0]}:{roi[1]},scale={target_resolution[0]}:{target_resolution[1]}:force_original_aspect_ratio=disable"
                    
                    # 使用兼容性更好的NVENC配置
                    fallback_hw_info = {
                        "encoder_type": "nvidia",
                        "encoder": hardware_info['encoder'],
                        "options": get_nvidia_fallback_options(),
                        "probe_size": "25M",
                        "buffer_size": "1024"
                    }
                    
                    cmd = build_ffmpeg_command(video_path, output_video_path, filter_complex, fallback_hw_info,
                                             source_quality_info=source_quality_info, target_resolution=target_resolution)
                    logging.info(f"执行命令 (NVENC兼容模式): {' '.join(cmd)}")
                    run_ffmpeg_process(cmd, duration, pbar, video_path=video_path)
                    
                    # 更新最终阶段进度 (95-100%)
                    update_final_progress(pbar, video_path, "NVENC兼容处理")
                    
                    # 验证输出文件
                    if not os.path.exists(output_video_path) or os.path.getsize(output_video_path) < 1024:
                        raise Exception(f"NVENC兼容模式输出文件无效或太小: {output_video_path}")
                    
                    # 清理检查点文件
                    try:
                        if os.path.exists(checkpoint_file):
                            os.remove(checkpoint_file)
                    except Exception:
                        pass
                    
                    # 标记为已完成
                    progress_manager.mark_completed(video_path, output_video_path)
                    
                    # 更新单视频进度为完成
                    progress_manager.update_individual_progress(video_path, 'completed', 100, "处理完成(NVENC兼容)")
                    
                    pbar.set_postfix_str("完成(NVENC兼容)✓")
                    logging.info(f"视频处理完成 (NVENC兼容模式): {video_path} -> {output_video_path}")
                    # 短暂延迟，让用户看到完成状态
                    time.sleep(0.5)
                    # 释放进度条位置供其他视频重用
                    release_progress_bar_position(current_position)
                    pbar.close()
                    return True
                    
                except Exception as nvenc_fallback_error:
                    logging.info("NVENC兼容模式失败，切换至CPU编码...")
                    logging.debug(f"NVENC兼容模式错误详情: {nvenc_fallback_error}")
        
            try:
                pbar.set_postfix_str("尝试快速CPU编码...")
                # 重新计算filter_complex，确保在备用方案中也可用，使用强制缩放
                filter_complex = f"crop={roi[2]}:{roi[3]}:{roi[0]}:{roi[1]},scale={target_resolution[0]}:{target_resolution[1]}:force_original_aspect_ratio=disable"
                
                # 使用快速CPU编码配置
                cpu_hw_info = {
                    "encoder_type": "software", 
                    "encoder": "libx264",
                    "options": {"preset": "veryfast", "crf": "23", "threads": "0"},
                    "probe_size": "25M",
                    "buffer_size": "1024"
                }
            
                # CPU 方案同样支持增强续传
                existing_duration = get_media_duration_seconds(output_video_path)
                if existing_duration > 10 and existing_duration < duration * 0.99:
                    part1_path = output_video_path + ".part1.mp4"
                    try:
                        if os.path.exists(output_video_path):
                            shutil.move(output_video_path, part1_path)
                    except Exception as e:
                        logging.warning(f"CPU续传移动文件失败: {e}")
                        part1_path = None
                    
                    part2_path = output_video_path + ".part2.mp4"
                    cmd = build_ffmpeg_command(video_path, part2_path, filter_complex, cpu_hw_info, 
                                             seek_seconds=int(existing_duration), source_quality_info=source_quality_info,
                                             target_resolution=target_resolution)
                    logging.info(f"执行命令 (CPU增强续传): {' '.join(cmd)}")
                    run_ffmpeg_process(cmd, duration, pbar, initial_time_offset=existing_duration, video_path=video_path)
                    
                    # 更新最终阶段进度 (95-100%)
                    update_final_progress(pbar, video_path, "CPU续传处理")
                    
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
                    cmd = build_ffmpeg_command(video_path, output_video_path, filter_complex, cpu_hw_info,
                                             source_quality_info=source_quality_info, target_resolution=target_resolution)
                    logging.info(f"执行命令 (CPU完整处理): {' '.join(cmd)}")
                    run_ffmpeg_process(cmd, duration, pbar, video_path=video_path)
            
                # 更新最终阶段进度 (95-100%)
                update_final_progress(pbar, video_path, "CPU完整处理")
                
                # 验证输出文件
                if not os.path.exists(output_video_path) or os.path.getsize(output_video_path) < 1024:
                    raise Exception(f"CPU编码输出文件无效或太小: {output_video_path}")
                
                # 清理检查点文件
                try:
                    if os.path.exists(checkpoint_file):
                        os.remove(checkpoint_file)
                except Exception:
                    pass
                
                # 标记为已完成
                progress_manager.mark_completed(video_path, output_video_path)
                
                # 更新单视频进度为完成
                progress_manager.update_individual_progress(video_path, 'completed', 100, "处理完成(CPU)")
                
                pbar.set_postfix_str("完成(CPU)✓")
                logging.info(f"视频处理完成 (CPU回退): {video_path} -> {output_video_path}")
                # 释放进度条位置供其他视频重用
                release_progress_bar_position(current_position)
                pbar.close()
                return True
            except Exception as e2:
                logging.error(f"所有策略均失败: {e2}")
                # 标记为失败
                progress_manager.mark_failed(video_path, str(e2))
                
                # 更新单视频进度为失败
                progress_manager.update_individual_progress(video_path, 'failed', 0, f"处理失败: {str(e2)}")
                
                pbar.set_postfix_str("失败✗")
                # 释放进度条位置供其他视频重用
                release_progress_bar_position(current_position)
                pbar.close()
                return False
    
    except Exception as outer_e:
        # 最外层异常处理：确保清理"正在处理"状态
        logging.error(f"视频处理过程中发生未捕获异常: {outer_e}")
        
        # 从处理中移除（无论是否已经在失败列表中）
        video_name = os.path.basename(video_path)
        if video_name in progress_manager.progress_data['processing']:
            progress_manager.progress_data['processing'].remove(video_name)
            progress_manager.save_progress()
            logging.info(f"已从处理中列表移除: {video_name}")
        
        # 标记为失败
        progress_manager.mark_failed(video_path, str(outer_e))
        
        # 更新单视频进度为失败
        progress_manager.update_individual_progress(video_path, 'failed', 0, f"处理异常中断: {str(outer_e)}")
        
        pbar.set_postfix_str("异常中断✗")
        # 释放进度条位置供其他视频重用
        release_progress_bar_position(current_position)
        pbar.close()
        return False
    
    except KeyboardInterrupt:
        # 用户中断处理：确保清理"正在处理"状态
        logging.info(f"用户中断视频处理: {video_path}")
        
        # 从处理中移除
        video_name = os.path.basename(video_path)
        if video_name in progress_manager.progress_data['processing']:
            progress_manager.progress_data['processing'].remove(video_name)
            progress_manager.save_progress()
            logging.info(f"已从处理中列表移除: {video_name}")
        
        # 更新单视频进度为中断
        progress_manager.update_individual_progress(video_path, 'interrupted', 0, "用户中断处理")
        
        pbar.set_postfix_str("用户中断⏸")
        # 释放进度条位置供其他视频重用
        release_progress_bar_position(current_position)
        pbar.close()
        raise  # 重新抛出KeyboardInterrupt以便上层处理
    
    finally:
        # 确保在所有情况下都能释放文件处理锁
        release_file_processing_lock(video_path, file_lock)


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

    # 使用新的硬件检测结果确定并行数
    max_workers = hardware_info.get("max_parallel", 4)
    if hardware_info["encoder_type"] != "software":
        # 硬件编码器：减少并行数以避免GPU资源争抢
        max_workers = min(max_workers, 6)
    else:
        # 软件编码器：可以使用更多并行，但要考虑I9性能
        cpu_cores = hardware_info.get("cpu_cores", 8)
        if hardware_info.get('is_i9', False):
            # i9处理器优化：使用更多并行数
            max_workers = min(max_workers, min(cpu_cores - 2, 16))
        else:
            max_workers = min(max_workers, cpu_cores // 2)
    
    logging.info(f"硬件类型: {hardware_info['encoder_type']}, 并行数: {max_workers}")
    if hardware_info.get('is_i9', False):
        logging.info("检测到i9处理器，已启用性能优化模式")
    
    # 根据硬件类型选择执行器
    if hardware_info["encoder_type"] != "software":
        # 硬件编码器使用线程池（GPU共享）
        executor_class = concurrent.futures.ThreadPoolExecutor
    else:
        # 软件编码器使用进程池（CPU密集型）
        executor_class = concurrent.futures.ProcessPoolExecutor
    with executor_class(max_workers=max_workers) as executor:
        print("\033[2J\033[H", end="")
        logging.info(f"创建 {len(video_paths)} 个并行任务...")
        futures = [executor.submit(process_video, vp, op, roi, hardware_info, i, len(video_paths),
                                   target_resolution) for i, (vp, op) in enumerate(zip(video_paths, output_paths))]
        for future in futures: future.add_done_callback(task_done_callback)
        logging.info(f"所有任务已提交，等待完成...")
        # 等待所有任务完成，添加超时处理
        try:
            concurrent.futures.wait(futures, timeout=None)
            logging.info("所有并行任务已完成")
        except Exception as e:
            logging.error(f"并行处理异常: {e}")
            # 取消未完成的任务
            for future in futures:
                if not future.done():
                    future.cancel()
    
    total_pbar.close()
    
    # 清理不再使用的文件处理锁
    cleanup_file_processing_locks()
    
    # 显示最终锁状态
    final_lock_status = get_file_processing_locks_status()
    if final_lock_status['active_locks'] > 0:
        logging.warning(f"⚠️ 处理完成后仍有 {final_lock_status['active_locks']} 个活跃锁")
        logging.debug(f"活跃锁文件: {final_lock_status['locked_files']}")
    
    logging.info(f"📊 处理完成统计: 成功 {success_count} 个, 失败 {failed_count} 个, 总计 {len(video_paths)} 个视频")
    return success_count, failed_count


def test_resolution_detection(video_path: str = None):
    """测试视频分辨率检测功能"""
    if not video_path:
        # 如果没有指定文件，扫描输入目录中的第一个视频
        test_files = find_video_files(input_dir)
        if not test_files:
            print("❌ 测试失败：没有找到测试视频文件")
            return False
        video_path = test_files[0]
    
    print(f"🧪 测试视频分辨率检测功能")
    print(f"📁 测试文件: {os.path.basename(video_path)}")
    print(f"📍 完整路径: {video_path}")
    
    # 测试基本的分辨率获取
    print(f"\n🔍 开始分辨率检测...")
    start_time = time.time()
    width, height = get_video_resolution(video_path)
    detection_time = time.time() - start_time
    
    print(f"✅ 检测结果: {width}x{height}")
    print(f"⏱️  检测耗时: {detection_time:.2f}秒")
    
    # 测试跳过检查功能
    print(f"\n🎯 测试跳过检查功能...")
    should_skip, (w, h) = should_skip_low_resolution_video(video_path)
    print(f"📊 分辨率: {w}x{h}")
    print(f"🚫 是否跳过: {'是' if should_skip else '否'}")
    
    if should_skip:
        print(f"📏 跳过原因: 宽度 {w}px < 最小要求 {MIN_RESOLUTION_WIDTH}px")
    else:
        print(f"✅ 分辨率符合处理要求")
    
    # 如果检测失败，生成详细诊断
    if width == 0 or height == 0:
        print(f"\n⚠️  分辨率检测失败，生成诊断报告...")
        diagnosis = diagnose_video_file(video_path)
        
        print(f"📋 诊断报告:")
        print(f"  文件存在: {diagnosis['file_exists']}")
        print(f"  文件大小: {diagnosis['file_size_mb']} MB")
        print(f"  FFprobe可用: {diagnosis['ffprobe_accessible']}")
        
        if 'raw_outputs' in diagnosis:
            print(f"  FFprobe命令测试结果:")
            for cmd_name, output in diagnosis['raw_outputs'].items():
                if isinstance(output, dict):
                    if 'error' in output:
                        print(f"    {cmd_name}: ❌ 错误 - {output['error']}")
                    else:
                        status = "✅ 成功" if output['returncode'] == 0 else f"❌ 失败({output['returncode']})"
                        print(f"    {cmd_name}: {status}")
                        if output['stdout']:
                            print(f"      输出: {output['stdout'][:100]}")
        
        return False
    else:
        print(f"\n✅ 分辨率检测功能正常工作")
        return True

if __name__ == '__main__':
    # 检查是否运行测试模式
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--test-resolution':
        print("🧪 运行分辨率检测测试模式")
        test_video = sys.argv[2] if len(sys.argv) > 2 else None
        test_result = test_resolution_detection(test_video)
        exit(0 if test_result else 1)
    
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
    if SKIP_LOW_RESOLUTION_VIDEOS:
        print(f"  跳过低分辨率: 启用 (最小宽度: {MIN_RESOLUTION_WIDTH}px)")
        if SKIP_VIDEOS_MOVE_DIR and SKIP_VIDEOS_MOVE_DIR.strip():
            print(f"  跳过视频移动目录: {SKIP_VIDEOS_MOVE_DIR}")
        else:
            print(f"  跳过视频处理: 仅跳过，不移动")
    else:
        print(f"  跳过低分辨率: 禁用")
    print()
    
    # 验证路径是否存在
    if not os.path.exists(input_dir):
        logging.error(f"输入目录不存在: {input_dir}")
        print(f"错误: 输入目录不存在: {input_dir}")
        print("请检查路径是否正确，或修改脚本中的 input_dir 变量")
        exit(1)
    
    # 检查输入目录中的视频文件数量
    input_files = find_video_files(input_dir)
    if not input_files:
        logging.error(f"输入目录中没有找到支持的视频文件: {input_dir}")
        print(f"错误: 输入目录中没有找到支持的视频文件: {input_dir}")
        print(f"支持的格式: {', '.join(SUPPORTED_VIDEO_FORMATS)}")
        print("请检查目录路径是否正确，或者目录中是否包含支持的视频文件")
        exit(1)
    
    logging.info(f"输入目录: {input_dir}")
    logging.info(f"输出目录: {output_dir}")
    logging.info(f"进度记录文件夹: {PROGRESS_FOLDER}")
    logging.info(f"找到 {len(input_files)} 个支持的视频文件")
    
    # 初始化全局进度管理器（使用电脑独有的进度文件）
    os.makedirs(output_dir, exist_ok=True)  # 确保输出目录存在
    progress_manager = ProgressManager(output_dir=output_dir)
    logging.info(f"进度文件路径: {progress_manager.progress_file}")
    logging.info(f"电脑标识: {progress_manager.computer_id}")
    
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
    
    # 首先扫描视频文件
    video_paths = find_video_files(input_dir)
    
    if not video_paths:
        logging.warning(f"在 {input_dir} 中没有找到视频文件")
        exit(1)
    
    # ========================
    # 优化：将ROI选择移到分辨率检测前
    # ========================
    
    # 先进行ROI区域选择（使用第一个可用的视频作为预览）
    print(f"📹 发现 {len(video_paths)} 个视频文件，开始ROI区域选择...")
    
    # 检查是否有保存的ROI设置
    saved_roi = progress_manager.get_roi_settings()
    final_roi = None
    
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
    
    # 如果没有保存的ROI设置，需要重新选择
    if final_roi is None:
        # 初始化OpenCV GUI后端
        gui_available = init_opencv_gui()
        if not gui_available:
            print("⚠️ OpenCV图形界面不可用，将使用命令行输入模式")
            logging.warning("OpenCV GUI后端不可用，将使用fallback模式")
        
        frame_for_preview, video_for_preview = None, None
        # 找到第一个可以成功创建预览的视频文件（不需要预先检测分辨率）
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
                    logging.info(f"成功为预览加载视频: {os.path.basename(video_for_preview)}")
                    break
            except Exception as e:
                logging.warning(f"尝试为 {os.path.basename(video_path)} 创建预览失败: {e}")

        if frame_for_preview is None:
            logging.error("错误: 无法在目录中找到任何可以成功创建预览的视频文件。")
            exit(1)

        print(f"使用第一个可读视频进行预览: {os.path.basename(video_for_preview)}")
        video_height, video_width, _ = frame_for_preview.shape

        display_height = 800
        scale_factor = display_height / video_height if video_height > 0 else 1
        display_width = int(video_width * scale_factor)
        display_frame = cv2.resize(frame_for_preview, (display_width, display_height))

        cv2.putText(display_frame, "请用鼠标选择一个区域，然后按'空格'或'回车'确认", (20, 40), cv2.FONT_HERSHEY_SIMPLEX,
                    1, (255, 255, 255), 2)

        # 让用户选择ROI - 优先使用图形界面
        if gui_available:
            try:
                # 确保OpenCV GUI后端可用
                cv2.namedWindow("交互式裁剪区域选择", cv2.WINDOW_NORMAL)
                cv2.resizeWindow("交互式裁剪区域选择", display_width, display_height)
                
                # 设置窗口属性以确保可见性
                cv2.setWindowProperty("交互式裁剪区域选择", cv2.WND_PROP_TOPMOST, 1)
                
                print("\n🎯 ROI选择窗口已打开，请按以下步骤操作：")
                print("1. 在弹出的窗口中用鼠标拖拽选择裁剪区域")
                print("2. 选择完成后按'空格键'或'回车键'确认")
                print("3. 如需取消选择，按'ESC'键或'c'键")
                print("4. 如果窗口没有出现，请检查任务栏或最小化的窗口\n")
                
                # 对于4.12.0版本，使用增强的selectROI调用
                if cv2.__version__.startswith('4.12'):
                    print("⚠️ 使用OpenCV 4.12.0版本的增强selectROI模式")
                    # 多次尝试selectROI，有时第一次会失败
                    for attempt in range(3):
                        try:
                            # 强制刷新窗口
                            cv2.imshow("交互式裁剪区域选择", display_frame)
                            cv2.waitKey(100)  # 给窗口更多时间渲染
                            
                            r = cv2.selectROI("交互式裁剪区域选择", display_frame, fromCenter=False, showCrosshair=True)
                            break  # 如果成功就退出循环
                        except Exception as e:
                            print(f"selectROI尝试 {attempt + 1}/3 失败: {e}")
                            if attempt == 2:  # 最后一次尝试
                                raise e
                            cv2.waitKey(500)  # 等待一下再重试
                else:
                    r = cv2.selectROI("交互式裁剪区域选择", display_frame, fromCenter=False, showCrosshair=True)
                
                cv2.destroyAllWindows()
                
                if r[2] == 0 or r[3] == 0:
                    print("❌ 未选择有效区域或已取消选择")
                    raise cv2.error("selectROI canceled", None, None)
                    
                r_original = (int(r[0] / scale_factor), int(r[1] / scale_factor), int(r[2] / scale_factor),
                              int(r[3] / scale_factor))
                print(f"✅ ROI选择成功: {r_original}")
                
            except (cv2.error, Exception) as e:
                logging.warning(f"cv2.selectROI 出错: {e}，使用命令行输入模式")
                print("⚠️ 图形界面选择失败，切换到命令行输入模式")
                r_original = prompt_for_roi_fallback(frame_for_preview, display_frame, scale_factor, TARGET_RESOLUTION)
        else:
            # GUI不可用，直接使用命令行输入模式
            print("🔧 使用命令行输入模式选择ROI区域")
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
        if gui_available:
            try:
                cv2.namedWindow('最终裁剪区域预览 (按任意键开始)', cv2.WINDOW_NORMAL)
                cv2.imshow('最终裁剪区域预览 (按任意键开始)', final_preview_display)
                cv2.waitKey(0)
                cv2.destroyAllWindows()
                print("✅ 预览完成，开始处理视频...")
            except cv2.error:
                logging.warning("无法显示预览窗口，跳过预览步骤")
        else:
            print("ℹ️ GUI不可用，跳过预览步骤，直接开始处理...")
        # ===== END: 新增功能 =====

        # 保存ROI设置
        progress_manager.set_roi_settings(final_roi)
    
    print(f"✅ ROI选择完成，最终ROI参数: {final_roi}")
    print(f"🔍 现在开始检测视频分辨率并筛选待处理文件...")
    
    # 预检查视频完成状态，现在进行分辨率检测和筛选
    logging.info("预检查视频处理状态...")
    filtered_video_paths = []
    completed_count = 0
    auto_synced_count = 0
    skipped_count = 0
    skipped_videos = []  # 存储跳过的视频信息
    
    logging.info("开始扫描输出目录，同步已存在的文件...")
    
    for video_path in video_paths:
        video_name = os.path.basename(video_path)
        output_path = os.path.join(output_dir, video_name)
        
        # 首先检查是否应该跳过低分辨率视频
        should_skip, (width, height) = should_skip_low_resolution_video(video_path)
        if should_skip:
            skipped_count += 1
            skipped_info = {
                'path': video_path,
                'name': video_name,
                'resolution': f"{width}x{height}",
                'width': width,
                'height': height,
                'reason': f"分辨率({width}x{height})低于最小要求({MIN_RESOLUTION_WIDTH}px宽度)"
            }
            skipped_videos.append(skipped_info)
            logging.info(f"跳过低分辨率视频: {video_name} ({width}x{height})")
            continue
        
        # 使用新的基于内容的检查方法
        if progress_manager.is_completed(video_path, output_dir):
            completed_count += 1
            logging.info(f"跳过已完成: {video_name}")
            continue
        
        # 检查是否为正在处理中的文件（支持断点续传）
        if progress_manager.is_processing(video_path):
            logging.info(f"检测到处理中文件，支持断点续传: {video_name}")
            # 不跳过，而是添加到待处理列表中进行断点续传
            filtered_video_paths.append(video_path)
            continue
        
        # 检查输出文件是否存在且完整（精确匹配优先，防止重复处理）
        video_name_without_ext = os.path.splitext(video_name)[0]
        possible_output_files = []
        
        # 1. 精确匹配原文件名（主要检查方式，最高优先级）
        if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
            possible_output_files.append(output_path)
            logging.info(f"找到精确匹配（最高优先级）: {video_name}")
        else:
            # 只有在精确匹配失败时才进行其他匹配，防止重复处理
            try:
                # 2. 严格的前缀匹配（仅限于合理的变体）
                for output_file in os.listdir(output_dir):
                    # 严格匹配：必须是 "原文件名_数字.mp4" 或 "原文件名_标识.mp4" 的格式
                    if (output_file.startswith(video_name_without_ext) and 
                        output_file.endswith('.mp4') and
                        output_file != video_name):  # 排除自身
                        
                        # 检查是否是合法的变体（避免匹配到相似但不相关的文件）
                        suffix = output_file[len(video_name_without_ext):]
                        # 合法的后缀格式: _数字, _processed, _cropped, _resized 等
                        valid_suffixes = ['_processed', '_cropped', '_resized', '_output', '_final']
                        is_valid_variant = False
                        
                        # 检查是否以合法后缀开头
                        if any(suffix.startswith(vs) for vs in valid_suffixes):
                            is_valid_variant = True
                        # 或者是数字后缀 _1, _2, _001 等
                        elif suffix.startswith('_') and len(suffix) > 1:
                            number_part = suffix[1:].replace('.mp4', '')
                            if number_part.isdigit() or (number_part.startswith('0') and number_part[1:].isdigit()):
                                is_valid_variant = True
                        
                        if is_valid_variant:
                            file_path = os.path.join(output_dir, output_file)
                            if file_path not in possible_output_files and os.path.getsize(file_path) > 1024:
                                possible_output_files.append(file_path)
                                logging.info(f"找到严格前缀匹配: {output_file}")
            except OSError as e:
                logging.warning(f"扫描输出目录时出错: {e}")
            
            # 3. 移除宽松匹配逻辑，防止误匹配导致的重复处理问题
            # 原来的宽松匹配可能导致将不相关的文件误认为是已处理的输出
        
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
    
    if auto_synced_count > 0:
        logging.info(f"自动同步了 {auto_synced_count} 个已存在的输出文件到进度记录")

    # 统计断点续传文件数量
    resume_count = sum(1 for video_path in filtered_video_paths if progress_manager.is_processing(video_path))
    new_process_count = len(filtered_video_paths) - resume_count

    total_completed = completed_count + auto_synced_count
    logging.info(f"视频统计: 总计 {len(video_paths)} 个, 已完成 {total_completed} 个 (其中自动同步 {auto_synced_count} 个), 跳过 {skipped_count} 个, 待处理 {len(filtered_video_paths)} 个")
    
    # 显示跳过视频的统计信息
    if skipped_count > 0:
        print(f"\n📊 跳过低分辨率视频统计:")
        print(f"   总计跳过: {skipped_count} 个视频")
        print(f"   跳过原因: 分辨率低于 {MIN_RESOLUTION_WIDTH}px 宽度")
        
        if SKIP_VIDEOS_MOVE_DIR and SKIP_VIDEOS_MOVE_DIR.strip():
            print(f"   移动目录: {SKIP_VIDEOS_MOVE_DIR}")
        else:
            print(f"   处理方式: 仅跳过，不移动文件")
        
        # 显示前5个跳过的视频作为示例
        print(f"   跳过视频示例:")
        for i, skipped in enumerate(skipped_videos[:5]):
            print(f"     {i+1}. {skipped['name']} ({skipped['resolution']})")
        if len(skipped_videos) > 5:
            print(f"     ... 还有 {len(skipped_videos) - 5} 个")
        print()
    
    if resume_count > 0:
        print(f"🔄 检测到 {resume_count} 个文件支持断点续传")
        logging.info(f"断点续传文件: {resume_count} 个, 新处理文件: {new_process_count} 个")
    
    if new_process_count > 0:
        print(f"🆕 新处理文件: {new_process_count} 个")
    
    # 如果没有待处理的视频，直接结束
    if not filtered_video_paths:
        logging.info("所有视频都已处理完成！")
        # 最终进度摘要
        progress_manager.print_summary()
        
        # 处理跳过的低分辨率视频
        if skipped_count > 0:
            print(f"\n📦 处理跳过的低分辨率视频...")
            logging.info(f"开始处理 {skipped_count} 个跳过的低分辨率视频")
            
            if SKIP_VIDEOS_MOVE_DIR and SKIP_VIDEOS_MOVE_DIR.strip():
                moved_count = 0
                failed_move_count = 0
                
                for skipped_info in skipped_videos:
                    if move_skipped_video(skipped_info['path'], "低分辨率"):
                        moved_count += 1
                    else:
                        failed_move_count += 1
                
                print(f"✅ 跳过视频移动完成:")
                print(f"   成功移动: {moved_count} 个")
                if failed_move_count > 0:
                    print(f"   移动失败: {failed_move_count} 个")
                print(f"   移动到: {SKIP_VIDEOS_MOVE_DIR}")
                
                logging.info(f"跳过视频移动完成: 成功 {moved_count} 个, 失败 {failed_move_count} 个")
            else:
                print(f"ℹ️  跳过的 {skipped_count} 个低分辨率视频保留在原位置")
                logging.info(f"跳过的 {skipped_count} 个低分辨率视频未移动（配置为不移动）")
        
        # 清理临时文件
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                logging.info("临时文件已清理")
        except Exception as e:
            logging.warning(f"清理临时文件失败: {e}")
        
        elapsed_time = time.time() - start_time
        logging.info(f'处理完成！总耗时: {elapsed_time:.2f}秒')
        
        # 最终统计汇总
        if skipped_count > 0:
            print(f"\n📈 最终统计汇总:")
            print(f"   总视频数量: {len(video_paths)}")
            print(f"   已完成处理: {total_completed}")
            print(f"   跳过低分辨率: {skipped_count}")
            print(f"   待处理视频: 0 (全部完成)")
            print(f"   总耗时: {elapsed_time:.2f}秒")
        exit(0)
    
    # ROI选择已在前面完成，直接使用已选择的ROI区域
    print(f"📹 发现 {len(filtered_video_paths)} 个待处理视频，使用已选择的ROI区域进行处理...")
    print(f"🔲 使用ROI参数: {final_roi}")
    logging.info(f"使用之前选择的ROI设置: {final_roi}")

    # 创建输出路径列表
    filtered_output_paths = []
    for video_path in filtered_video_paths:
        video_name = os.path.basename(video_path)
        output_path = os.path.join(output_dir, video_name)
        filtered_output_paths.append(output_path)
    
    # 设置开始时间
    progress_manager.set_start_time()
    
    # 重置进度条计数器
    progress_bar_counter = 0
    
    logging.info(f"开始处理 {len(filtered_video_paths)} 个待处理/可续传的视频文件...")
    try:
        # 使用增强的硬件检测
        hardware_info = detect_advanced_hardware()
        logging.info(f"硬件配置摘要: {hardware_info['encoder_type']} 编码器, {hardware_info.get('cpu_cores', 'N/A')} 核心, "
                   f"{hardware_info.get('memory_gb', 'N/A'):.1f}GB 内存, 并行数: {hardware_info.get('max_parallel', 'N/A')}")
        
        success_count, failed_count = process_videos_in_parallel(filtered_video_paths, filtered_output_paths, final_roi, hardware_info, TARGET_RESOLUTION)
        logging.info(f"🎯 本次处理结果: 成功 {success_count} 个, 失败 {failed_count} 个")
    except Exception as e:
        logging.error(f"主程序异常: {e}", exc_info=True)

    # 最终进度摘要
    progress_manager.print_summary()
    
    # 处理跳过的低分辨率视频
    if skipped_count > 0:
        print(f"\n📦 处理跳过的低分辨率视频...")
        logging.info(f"开始处理 {skipped_count} 个跳过的低分辨率视频")
        
        if SKIP_VIDEOS_MOVE_DIR and SKIP_VIDEOS_MOVE_DIR.strip():
            moved_count = 0
            failed_move_count = 0
            
            for skipped_info in skipped_videos:
                if move_skipped_video(skipped_info['path'], "低分辨率"):
                    moved_count += 1
                else:
                    failed_move_count += 1
            
            print(f"✅ 跳过视频移动完成:")
            print(f"   成功移动: {moved_count} 个")
            if failed_move_count > 0:
                print(f"   移动失败: {failed_move_count} 个")
            print(f"   移动到: {SKIP_VIDEOS_MOVE_DIR}")
            
            logging.info(f"跳过视频移动完成: 成功 {moved_count} 个, 失败 {failed_move_count} 个")
        else:
            print(f"ℹ️  跳过的 {skipped_count} 个低分辨率视频保留在原位置")
            logging.info(f"跳过的 {skipped_count} 个低分辨率视频未移动（配置为不移动）")
    
    # 清理临时文件
    try:
        if os.path.exists(temp_dir): 
            shutil.rmtree(temp_dir)
            logging.info("临时文件已清理")
    except Exception as e:
        logging.warning(f"清理临时文件失败: {e}")
    
    elapsed_time = time.time() - start_time
    logging.info(f'处理完成！总耗时: {elapsed_time:.2f}秒')
    
    # 最终统计汇总
    if skipped_count > 0:
        print(f"\n📈 最终统计汇总:")
        print(f"   总视频数量: {len(video_paths)}")
        print(f"   已完成处理: {total_completed}")
        print(f"   跳过低分辨率: {skipped_count}")
        print(f"   本次处理: {len(filtered_video_paths) if 'filtered_video_paths' in locals() else 0}")
        print(f"   处理耗时: {elapsed_time:.2f}秒")