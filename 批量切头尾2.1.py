# _*_ coding: utf-8 _*_
"""
批量切头尾2.0 - 极致性能版 (基于批量裁剪2.0思维重构)
Time:     2025/9/17
Author:   L 
Version:  V 2.0 Ultimate
File:     批量切头尾2.0_.py


"""

import time
import os
import sys

# 导入必要的模块
import subprocess
import logging
import json
import csv
import shutil
import re
import concurrent.futures
import threading
import signal
import psutil
import platform
import hashlib
import uuid
import socket
import numpy as np
from pathlib import Path
from queue import Queue, Empty
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any
from tqdm import tqdm
from datetime import datetime
import multiprocessing
import math
import gc
import weakref

# ==================== 编码配置 ====================
# 设置环境变量确保UTF-8编码
os.environ['PYTHONIOENCODING'] = 'utf-8'
if platform.system() == "Windows":
    os.environ['PYTHONUTF8'] = '1'

# ==================== START: 用户配置区域 ====================
# !!! 请根据你的实际情况修改以下配置 !!!

# --- FFmpeg 路径配置 ---
# 请将此路径修改为你电脑上 ffmpeg.exe 和 ffprobe.exe 的实际路径
FFMPEG_PATH = r"D:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe"
FFPROBE_PATH = r"D:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffprobe.exe"

# --- 输入输出路径配置 ---
# 输入目录：包含待处理视频文件的文件夹
root_path = r"Z:\personal_folder\L\测试\跳过的低分辨率视频"
output_root = r"Z:\personal_folder\L\Ljn\测试\损坏的视频"

# --- 进度记录配置 ---
# 进度记录文件夹：用于存储处理进度，支持跨电脑同步
# 注意：进度文件现在会在输出目录中创建电脑独有的子文件夹
PROGRESS_FOLDER = r"Z:\personal_folder\L\去片头片尾处理完数据"

# --- 切头尾时间配置 ---
head_cut_time = 90  # 片头时间（秒）
tail_cut_time = 90  # 片尾时间（秒）

# --- 支持的视频格式 ---
SUPPORTED_VIDEO_FORMATS = ['.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm', '.ts', '.m4v', '.3gp', '.f4v']

# --- 硬件配置 (自动检测) ---
# 系统将自动检测并优化i9处理器性能
# 手动override并行数量（设为0则自动检测）
MAX_PARALLEL_WORKERS_OVERRIDE = 0
# 向后兼容
MAX_PARALLEL_WORKERS = 8  # 提升默认并行数

# --- 质量控制配置 ---
# 质量模式：'highest' | 'high' | 'balanced' | 'fast'
QUALITY_MODE = 'highest'
# 自动码率：True=根据源视频自动调整，False=使用固定码率
AUTO_BITRATE = True
# 固定码率设置（仅在AUTO_BITRATE=False时使用）
VIDEO_BITRATE = "10M"
MAX_BITRATE = "20M"
BUFFER_SIZE = "20M"

# --- 低分辨率视频跳过配置 ---
# 是否跳过处理1080p以下的视频
SKIP_LOW_RESOLUTION_VIDEOS = True
# 最小处理分辨率阈值 (宽度像素)
MIN_RESOLUTION_WIDTH = 1920
# 跳过的视频移动到的目录 (设为空字符串则不移动，只跳过)
SKIP_VIDEOS_MOVE_DIR = r"Z:\personal_folder\L\测试\跳过的低分辨率视频"

# ===================== END: 用户配置区域 =====================

# ==================== START: 新增功能函数 (基于批量裁剪2.0) ====================

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
        for root, dirs, files in os.walk(directory):
            # 跳过系统文件夹
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in {'$RECYCLE.BIN', 'System Volume Information'}]
            
            for file in files:
                file_path = os.path.join(root, file)
                ext = os.path.splitext(file)[1].lower()
                if ext in SUPPORTED_VIDEO_FORMATS:
                    # 文件大小检查 (至少1MB)
                    try:
                        if os.path.getsize(file_path) >= 1024 * 1024:
                            video_files.append(file_path)
                    except OSError:
                        continue
        
        logging.info(f"在目录 {directory} 中找到 {len(video_files)} 个支持的视频文件")
        
    except Exception as e:
        logging.error(f"搜索视频文件时出错: {e}")
    
    return video_files

def detect_advanced_hardware() -> Dict:
    """高级硬件检测和性能优化 (基于批量裁剪2.0)"""
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
        return detect_hardware_capabilities()  # 回退到原始检测

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
        nvidia_encoders = ['h264_nvenc', 'hevc_nvenc']
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
        amd_encoders = ['h264_amf', 'hevc_amf']
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
        intel_encoders = ['h264_qsv', 'hevc_qsv']
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
    """NVIDIA编码器优化参数"""
    if QUALITY_MODE == 'highest':
        return {
            'preset': 'p1',  # 最高质量预设
            'rc': 'vbr',
            'cq': '18',  # 高质量CQ值
            'spatial_aq': '1',
            'temporal_aq': '1',
            'bf': '2',
            'refs': '4'
        }
    elif QUALITY_MODE == 'high':
        return {
            'preset': 'p2',
            'rc': 'vbr',
            'cq': '22',
            'spatial_aq': '1',
            'temporal_aq': '1',
        }
    elif QUALITY_MODE == 'balanced':
        return {
            'preset': 'p4',
            'rc': 'vbr',
            'cq': '25',
            'spatial_aq': '1'
        }
    else:  # fast
        return {
            'preset': 'p6',
            'rc': 'cbr',
            'b:v': '8M'
        }

def get_amd_optimized_options() -> Dict:
    """AMD编码器优化参数"""
    if QUALITY_MODE == 'highest':
        return {
            'quality': 'quality',
            'rc': 'vbr_latency',
            'qp_i': '18',
            'qp_p': '20',
            'qp_b': '22',
            'bf': '2',
            'refs': '4'
        }
    elif QUALITY_MODE == 'high':
        return {
            'quality': 'quality',
            'rc': 'vbr_latency',
            'qp_i': '22',
            'qp_p': '24',
            'qp_b': '26'
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
    """Intel编码器优化参数"""
    if QUALITY_MODE == 'highest':
        return {
            'preset': 'veryslow',
            'global_quality': '18',
            'look_ahead': '1',
            'look_ahead_depth': '60',
            'bf': '2',
            'refs': '4'
        }
    elif QUALITY_MODE == 'high':
        return {
            'preset': 'slow',
            'global_quality': '22',
            'look_ahead': '1',
            'look_ahead_depth': '40'
        }
    elif QUALITY_MODE == 'balanced':
        return {
            'preset': 'medium',
            'global_quality': '25',
            'look_ahead': '1'
        }
    else:  # fast
        return {
            'preset': 'fast',
            'global_quality': '28'
        }

def get_software_optimized_options() -> Dict:
    """软件编码器优化参数"""
    if QUALITY_MODE == 'highest':
        return {
            'preset': 'veryslow',
            'crf': '18',
            'threads': '0',
            'aq-mode': '3',
            'me': 'umh',
            'subme': '10',
            'ref': '5',
            'bframes': '8',
            'b-adapt': '2',
            'trellis': '2'
        }
    elif QUALITY_MODE == 'high':
        return {
            'preset': 'slow',
            'crf': '20',
            'threads': '0',
            'aq-mode': '3',
            'me': 'umh',
            'subme': '8',
            'ref': '4'
        }
    elif QUALITY_MODE == 'balanced':
        return {
            'preset': 'medium',
            'crf': '23',
            'threads': '0',
            'aq-mode': '2'
        }
    else:  # fast
        return {
            'preset': 'fast',
            'crf': '25',
            'threads': '0'
        }

def get_media_duration_seconds(video_path: str) -> float:
    """
    获取媒体文件的时长（秒）- 增强版本，多重备用方案和详细日志
    """
    video_name = os.path.basename(video_path)
    
    # 基本检查
    if not os.path.exists(video_path):
        logging.warning(f"文件不存在: {video_name}")
        return 0.0
    
    # 文件大小检查
    try:
        file_size = os.path.getsize(video_path)
        if file_size == 0:
            logging.warning(f"文件为空: {video_name}")
            return 0.0
        elif file_size < 1024:  # 小于1KB
            logging.warning(f"文件过小: {video_name} ({file_size} bytes)")
            return 0.0
    except OSError as e:
        logging.error(f"无法获取文件大小: {video_name} -> {e}")
        return 0.0
    
    max_retries = 3
    retry_delay = 1.0
    
    logging.debug(f"开始获取视频时长: {video_name} ({file_size/1024/1024:.1f}MB)")
    
    for attempt in range(max_retries):
        try:
            # 方法1: 从format获取时长 (最常用)
            logging.debug(f"尝试方法1 - format时长 (第{attempt+1}次): {video_name}")
            cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format=duration', 
                   '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=30)
            
            if result.returncode == 0 and result.stdout.strip():
                try:
                    duration = float(result.stdout.strip())
                    if duration > 0:
                        logging.info(f"✅ 方法1成功获取时长: {duration:.1f}s - {video_name}")
                        return duration
                except ValueError as e:
                    logging.debug(f"方法1时长解析失败: {video_name} -> {e}, 输出: '{result.stdout.strip()}'")
            else:
                logging.debug(f"方法1命令失败: {video_name} -> 返回码: {result.returncode}, 错误: {result.stderr}")
            
            # 方法2: 从视频流获取时长
            logging.debug(f"尝试方法2 - 视频流时长 (第{attempt+1}次): {video_name}")
            cmd = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                   '-show_entries', 'stream=duration', 
                   '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=30)
            
            if result.returncode == 0 and result.stdout.strip():
                try:
                    duration = float(result.stdout.strip())
                    if duration > 0:
                        logging.info(f"✅ 方法2成功获取时长: {duration:.1f}s - {video_name}")
                        return duration
                except ValueError as e:
                    logging.debug(f"方法2时长解析失败: {video_name} -> {e}, 输出: '{result.stdout.strip()}'")
            else:
                logging.debug(f"方法2命令失败: {video_name} -> 返回码: {result.returncode}")
            
            # 方法3: 使用JSON格式获取详细信息
            logging.debug(f"尝试方法3 - JSON格式 (第{attempt+1}次): {video_name}")
            cmd = [FFPROBE_PATH, '-v', 'error', '-print_format', 'json', 
                   '-show_format', '-show_streams', video_path]
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=30)
            
            if result.returncode == 0 and result.stdout.strip():
                try:
                    data = json.loads(result.stdout)
                    
                    # 优先从format获取时长
                    format_duration = data.get('format', {}).get('duration')
                    if format_duration and format_duration != 'N/A':
                        duration = float(format_duration)
                        if duration > 0:
                            logging.info(f"✅ 方法3-format成功获取时长: {duration:.1f}s - {video_name}")
                            return duration
                    
                    # 然后从视频流获取时长
                    for stream in data.get('streams', []):
                        if stream.get('codec_type') == 'video':
                            stream_duration = stream.get('duration')
                            if stream_duration and stream_duration != 'N/A':
                                duration = float(stream_duration)
                                if duration > 0:
                                    logging.info(f"✅ 方法3-stream成功获取时长: {duration:.1f}s - {video_name}")
                                    return duration
                            break
                    
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    logging.debug(f"方法3 JSON解析失败: {video_name} -> {e}")
            else:
                logging.debug(f"方法3命令失败: {video_name} -> 返回码: {result.returncode}")
            
            # 如果不是最后一次尝试，等待后重试
            if attempt < max_retries - 1:
                logging.debug(f"第{attempt+1}次尝试失败，{retry_delay}秒后重试: {video_name}")
                time.sleep(retry_delay)
                retry_delay *= 1.5  # 指数退避
                
        except subprocess.TimeoutExpired:
            logging.warning(f"获取时长超时 (第{attempt+1}次): {video_name}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 1.5
        except Exception as e:
            logging.warning(f"获取时长异常 (第{attempt+1}次): {video_name} -> {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 1.5
    
    # 所有方法都失败了，生成诊断报告
    logging.error(f"❌ 所有方法都无法获取时长: {video_name}")
    logging.error(f"  建议手动检查此文件: {video_path}")
    logging.error(f"  文件大小: {file_size / (1024*1024):.2f} MB")
    
    # 生成诊断报告
    try:
        diagnosis = diagnose_video_file(video_path)
        logging.error(f"🔍 视频文件诊断报告 - {video_name}:")
        logging.error(f"  文件存在: {diagnosis['file_exists']}")
        logging.error(f"  文件大小: {diagnosis['file_size_mb']} MB")
        logging.error(f"  FFprobe可访问: {diagnosis['ffprobe_accessible']}")
        
        if 'raw_outputs' in diagnosis:
            for cmd_name, output in diagnosis['raw_outputs'].items():
                if isinstance(output, dict):
                    if 'error' in output:
                        logging.error(f"  {cmd_name}: 错误 - {output['error']}")
                    elif 'output' in output:
                        logging.error(f"  {cmd_name}: 输出 - {output['output'][:100]}...")
    except Exception as diag_e:
        logging.error(f"生成诊断报告失败: {diag_e}")
    
    return 0.0

def diagnose_video_file(video_path: str) -> Dict:
    """
    诊断视频文件，返回详细信息用于问题排查
    参考批量裁剪2.0.py的实现
    """
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
            ('full_stream_info', [FFPROBE_PATH, '-v', 'quiet', '-print_format', 'json', '-show_streams', video_path]),
            ('duration_format', [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format=duration', 
                                 '-of', 'default=noprint_wrappers=1:nokey=1', video_path]),
            ('duration_stream', [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                                 '-show_entries', 'stream=duration', '-of', 'default=noprint_wrappers=1:nokey=1', video_path])
        ]
        
        for cmd_name, cmd in commands_to_test:
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=15)
                diagnosis['raw_outputs'][cmd_name] = {
                    'returncode': result.returncode,
                    'output': result.stdout,
                    'error': result.stderr
                }
            except Exception as e:
                diagnosis['raw_outputs'][cmd_name] = {'error': str(e)}
        
        # 尝试获取时长 - 使用简化版本避免递归调用
        try:
            cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format=duration', 
                   '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=15)
            if result.returncode == 0 and result.stdout.strip():
                duration = float(result.stdout.strip())
                diagnosis['duration'] = duration if duration > 0 else 0
        except Exception as e:
            diagnosis['duration_error'] = str(e)
            
        # 尝试获取分辨率 - 使用简化版本
        try:
            cmd = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                   '-show_entries', 'stream=width,height', '-of', 'csv=s=x:p=0', video_path]
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=15)
            if result.returncode == 0 and result.stdout.strip():
                output = result.stdout.strip()
                if 'x' in output:
                    parts = output.split('x')
                    if len(parts) == 2:
                        width = int(parts[0].strip())
                        height = int(parts[1].strip())
                        diagnosis['resolution'] = (width, height)
        except Exception as e:
            diagnosis['resolution_error'] = str(e)
            
    except Exception as e:
        diagnosis['general_error'] = str(e)
    
    return diagnosis

def get_video_resolution(video_path: str) -> Optional[Tuple[int, int]]:
    """
    获取视频文件的分辨率 (宽度, 高度) - 增强版本，参考批量裁剪2.0.py的多重重试机制
    
    Args:
        video_path: 视频文件路径
        
    Returns:
        tuple: (宽度, 高度) 或 None (如果获取失败)
    """
    video_name = os.path.basename(video_path)
    max_retries = 3
    retry_delay = 1.0
    
    # 基本检查
    if not os.path.exists(video_path):
        logging.warning(f"视频文件不存在: {video_name}")
        return None
    
    logging.debug(f"开始获取视频分辨率: {video_name}")
    
    for attempt in range(max_retries):
        try:
            # 方法1: 使用 CSV 格式输出 (最快最可靠)
            logging.debug(f"尝试方法1 - CSV格式 (第{attempt+1}次): {video_name}")
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
                                logging.info(f"✅ 方法1成功获取分辨率: {width}x{height} - {video_name}")
                                return (width, height)
                        except ValueError as e:
                            logging.debug(f"CSV格式解析失败: {e}, 输出: '{output}'")
            else:
                logging.debug(f"方法1命令失败: {video_name} -> 返回码: {result.returncode}")
            
            # 方法2: 使用 JSON 格式输出 (更可靠)
            logging.debug(f"尝试方法2 - JSON格式 (第{attempt+1}次): {video_name}")
            cmd_json = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                        '-show_entries', 'stream=width,height', 
                        '-of', 'json', video_path]
            result_json = subprocess.run(cmd_json, capture_output=True, text=True, encoding='utf-8', timeout=15)
            
            if result_json.returncode == 0 and result_json.stdout.strip():
                try:
                    data = json.loads(result_json.stdout)
                    streams = data.get('streams', [])
                    if streams:
                        stream = streams[0]
                        width = int(stream.get('width', 0))
                        height = int(stream.get('height', 0))
                        if width > 0 and height > 0:
                            logging.info(f"✅ 方法2成功获取分辨率: {width}x{height} - {video_name}")
                            return (width, height)
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    logging.debug(f"方法2 JSON解析失败: {video_name} -> {e}")
            else:
                logging.debug(f"方法2命令失败: {video_name} -> 返回码: {result_json.returncode}")
            
            # 方法3: 使用默认格式输出
            logging.debug(f"尝试方法3 - 默认格式 (第{attempt+1}次): {video_name}")
            cmd_default = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                           '-show_entries', 'stream=width,height', 
                           '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
            result_default = subprocess.run(cmd_default, capture_output=True, text=True, encoding='utf-8', timeout=15)
            
            if result_default.returncode == 0 and result_default.stdout.strip():
                try:
                    lines = result_default.stdout.strip().split('\n')
                    if len(lines) >= 2:
                        width = int(lines[0].strip())
                        height = int(lines[1].strip())
                        if width > 0 and height > 0:
                            logging.info(f"✅ 方法3成功获取分辨率: {width}x{height} - {video_name}")
                            return (width, height)
                except (ValueError, IndexError) as e:
                    logging.debug(f"方法3解析失败: {video_name} -> {e}")
            else:
                logging.debug(f"方法3命令失败: {video_name} -> 返回码: {result_default.returncode}")
            
            # 方法4: 使用完整流信息 (备用)
            logging.debug(f"尝试方法4 - 完整流信息 (第{attempt+1}次): {video_name}")
            cmd_full = [FFPROBE_PATH, '-v', 'quiet', '-print_format', 'json', '-show_streams', video_path]
            result_full = subprocess.run(cmd_full, capture_output=True, text=True, encoding='utf-8', timeout=15)
            
            if result_full.returncode == 0 and result_full.stdout.strip():
                try:
                    data = json.loads(result_full.stdout)
                    for stream in data.get('streams', []):
                        if stream.get('codec_type') == 'video':
                            width = int(stream.get('width', 0))
                            height = int(stream.get('height', 0))
                            if width > 0 and height > 0:
                                logging.info(f"✅ 方法4成功获取分辨率: {width}x{height} - {video_name}")
                                return (width, height)
                            break
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    logging.debug(f"方法4 JSON解析失败: {video_name} -> {e}")
            else:
                logging.debug(f"方法4命令失败: {video_name} -> 返回码: {result_full.returncode}")
            
            # 如果不是最后一次尝试，等待后重试
            if attempt < max_retries - 1:
                logging.debug(f"第{attempt+1}次尝试失败，{retry_delay}秒后重试: {video_name}")
                time.sleep(retry_delay)
                retry_delay *= 1.5  # 指数退避
                
        except subprocess.TimeoutExpired:
            logging.warning(f"获取分辨率超时 (第{attempt+1}次): {video_name}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 1.5
        except Exception as e:
            logging.warning(f"获取分辨率异常 (第{attempt+1}次): {video_name} -> {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 1.5
    
    # 所有方法都失败了，记录详细错误
    logging.error(f"❌ 所有方法都无法获取分辨率: {video_name}")
    
    # 生成诊断报告（如果需要的话）
    try:
        file_size = os.path.getsize(video_path)
        logging.error(f"  文件大小: {file_size / (1024*1024):.2f} MB")
        
        # 简化的诊断 - 避免重复的诊断报告
        logging.error(f"  建议手动检查文件: {video_path}")
        
    except Exception as e:
        logging.error(f"获取文件信息失败: {e}")
    
    return None

def should_skip_low_resolution_video(video_path: str) -> Tuple[bool, Optional[Tuple[int, int]], str]:
    """
    检查是否应该跳过低分辨率视频
    
    Args:
        video_path: 视频文件路径
        
    Returns:
        tuple: (是否跳过, 分辨率(宽度,高度), 跳过原因)
    """
    if not SKIP_LOW_RESOLUTION_VIDEOS:
        return False, None, ""
    
    try:
        resolution = get_video_resolution(video_path)
        
        if resolution is None:
            # 无法获取分辨率时不跳过，让后续处理来决定
            logging.info(f"无法获取分辨率，不跳过: {os.path.basename(video_path)}")
            return False, None, "无法获取分辨率"
        
        width, height = resolution
        
        if width < MIN_RESOLUTION_WIDTH:
            reason = f"分辨率 {width}x{height} 低于最小宽度 {MIN_RESOLUTION_WIDTH}px"
            logging.info(f"跳过低分辨率视频: {os.path.basename(video_path)} -> {reason}")
            return True, resolution, reason
        
        # 不跳过
        return False, resolution, ""
        
    except Exception as e:
        logging.warning(f"检查视频分辨率时出错: {video_path} -> {e}")
        # 出错时不跳过，让后续处理来决定
        return False, None, f"检查出错: {str(e)}"

def move_skipped_video(video_path: str, skip_reason: str) -> bool:
    """
    移动跳过的视频到指定目录
    
    Args:
        video_path: 原视频路径
        skip_reason: 跳过原因
        
    Returns:
        bool: 是否移动成功
    """
    if not SKIP_VIDEOS_MOVE_DIR or not SKIP_VIDEOS_MOVE_DIR.strip():
        return True  # 配置为不移动时返回成功
    
    try:
        # 确保移动目录存在
        if not os.path.exists(SKIP_VIDEOS_MOVE_DIR):
            os.makedirs(SKIP_VIDEOS_MOVE_DIR, exist_ok=True)
            logging.info(f"创建跳过视频目录: {SKIP_VIDEOS_MOVE_DIR}")
        
        # 构建目标路径
        filename = os.path.basename(video_path)
        target_path = os.path.join(SKIP_VIDEOS_MOVE_DIR, filename)
        
        # 处理重名文件
        counter = 1
        base_name, ext = os.path.splitext(filename)
        while os.path.exists(target_path):
            new_filename = f"{base_name}_{counter}{ext}"
            target_path = os.path.join(SKIP_VIDEOS_MOVE_DIR, new_filename)
            counter += 1
        
        # 移动文件
        shutil.move(video_path, target_path)
        logging.info(f"跳过视频已移动: {filename} -> {os.path.basename(target_path)}")
        logging.info(f"移动原因: {skip_reason}")
        
        return True
        
    except Exception as e:
        logging.error(f"移动跳过视频失败: {video_path} -> {e}")
        return False

def analyze_video_quality(video_path: str) -> Dict:
    """
    分析视频质量参数，用于优化输出设置 - 增强版本，更详细的日志
    """
    video_name = os.path.basename(video_path)
    max_retries = 2
    retry_delay = 1.0
    
    logging.debug(f"开始分析视频质量: {video_name}")
    
    for attempt in range(max_retries):
        try:
            # 获取详细的视频信息
            logging.debug(f"尝试获取完整视频信息 (第{attempt+1}次): {video_name}")
            cmd = [FFPROBE_PATH, '-v', 'quiet', '-print_format', 'json', 
                   '-show_format', '-show_streams', video_path]
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=30)
            
            if result.returncode != 0:
                logging.debug(f"ffprobe命令失败 (第{attempt+1}次): {video_name} -> 返回码: {result.returncode}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 1.5
                continue
            
            if not result.stdout.strip():
                logging.debug(f"ffprobe输出为空 (第{attempt+1}次): {video_name}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 1.5
                continue
            
            try:
                data = json.loads(result.stdout)
                logging.debug(f"JSON解析成功 (第{attempt+1}次): {video_name}")
            except json.JSONDecodeError as e:
                logging.warning(f"JSON解析失败 (第{attempt+1}次): {video_name} -> {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 1.5
                continue
                
            video_stream = None
            audio_stream = None
            
            # 找到视频流和音频流
            for stream in data.get('streams', []):
                if stream.get('codec_type') == 'video' and not video_stream:
                    video_stream = stream
                elif stream.get('codec_type') == 'audio' and not audio_stream:
                    audio_stream = stream
            
            if not video_stream:
                logging.warning(f"未找到视频流 (第{attempt+1}次): {video_name}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 1.5
                continue
            
            # 提取关键信息
            width = int(video_stream.get('width', 0))
            height = int(video_stream.get('height', 0))
            
            # 安全的帧率计算
            fps = 25.0  # 默认值
            try:
                fps_str = video_stream.get('r_frame_rate', '25/1')
                if '/' in fps_str:
                    numerator, denominator = fps_str.split('/')
                    if float(denominator) != 0:
                        fps = float(numerator) / float(denominator)
                else:
                    fps = float(fps_str)
            except (ValueError, ZeroDivisionError) as e:
                logging.debug(f"帧率解析失败，使用默认值: {video_name} -> {e}")
                fps = 25.0
            
            # 时长信息
            duration = 0.0
            try:
                # 优先从视频流获取时长
                stream_duration = video_stream.get('duration')
                if stream_duration and stream_duration != 'N/A':
                    duration = float(stream_duration)
                else:
                    # 备用：从格式信息获取时长
                    format_info = data.get('format', {})
                    format_duration = format_info.get('duration')
                    if format_duration and format_duration != 'N/A':
                        duration = float(format_duration)
            except (ValueError, TypeError) as e:
                logging.debug(f"时长解析失败: {video_name} -> {e}")
                duration = 0.0
            
            # 计算码率
            bitrate = 0
            try:
                format_info = data.get('format', {})
                format_bitrate = format_info.get('bit_rate')
                if format_bitrate and format_bitrate != 'N/A':
                    bitrate = int(float(format_bitrate))
            except (ValueError, TypeError) as e:
                logging.debug(f"码率解析失败: {video_name} -> {e}")
                bitrate = 0
            
            # 色彩空间信息
            color_space = video_stream.get('color_space', 'unknown')
            color_primaries = video_stream.get('color_primaries', 'unknown')
            pix_fmt = video_stream.get('pix_fmt', 'unknown')
            codec_name = video_stream.get('codec_name', 'unknown')
            
            # 音频信息
            audio_codec = 'unknown'
            audio_bitrate = 0
            if audio_stream:
                audio_codec = audio_stream.get('codec_name', 'unknown')
                try:
                    audio_bitrate_str = audio_stream.get('bit_rate')
                    if audio_bitrate_str and audio_bitrate_str != 'N/A':
                        audio_bitrate = int(float(audio_bitrate_str))
                except (ValueError, TypeError):
                    pass
            
            quality_info = {
                'width': width,
                'height': height,
                'fps': round(fps, 2),
                'duration': round(duration, 2),
                'bitrate': bitrate,
                'bitrate_mbps': round(bitrate / 1000000, 2) if bitrate > 0 else 0.0,
                'color_space': color_space,
                'color_primaries': color_primaries,
                'codec': codec_name,
                'pixel_format': pix_fmt,
                'audio_codec': audio_codec,
                'audio_bitrate': audio_bitrate,
                'audio_bitrate_kbps': round(audio_bitrate / 1000, 1) if audio_bitrate > 0 else 0.0,
                'file_size_mb': 0.0
            }
            
            # 添加文件大小信息
            try:
                file_size = os.path.getsize(video_path)
                quality_info['file_size_mb'] = round(file_size / (1024 * 1024), 2)
            except OSError:
                pass
            
            # 成功获取信息，输出详细日志
            logging.info(f"✅ 视频质量分析完成: {video_name}")
            logging.info(f"  分辨率: {width}x{height}")
            logging.info(f"  帧率: {fps:.1f} fps")
            logging.info(f"  时长: {duration:.1f}s")
            logging.info(f"  视频码率: {bitrate/1000000:.1f} Mbps")
            logging.info(f"  视频编码: {codec_name}")
            logging.info(f"  音频编码: {audio_codec}")
            if audio_bitrate > 0:
                logging.info(f"  音频码率: {audio_bitrate/1000:.0f} kbps")
            logging.info(f"  文件大小: {quality_info['file_size_mb']:.1f} MB")
            
            logging.debug(f"完整质量信息 {video_name}: {quality_info}")
            return quality_info
            
        except subprocess.TimeoutExpired:
            logging.warning(f"视频质量分析超时 (第{attempt+1}次): {video_name}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 1.5
        except Exception as e:
            logging.warning(f"视频质量分析异常 (第{attempt+1}次): {video_name} -> {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 1.5
    
    # 所有尝试都失败了
    logging.error(f"❌ 无法分析视频质量: {video_name}")
    try:
        file_size = os.path.getsize(video_path)
        logging.error(f"  文件大小: {file_size / (1024*1024):.2f} MB")
        logging.error(f"  建议手动检查此文件")
    except:
        pass
    
    return {}

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
    except Exception as e:
        logging.warning(f"保存检查点失败: {e}")

def load_progress_checkpoint(checkpoint_file: str) -> Optional[Dict]:
    """从文件加载进度检查点"""
    try:
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
            return checkpoint
    except Exception as e:
        logging.warning(f"加载检查点失败: {e}")
    return None

# ===================== END: 新增功能函数 =====================

# ==================== 高级进度管理器 (基于批量裁剪2.0架构) ====================
class ProgressManager:
    """增强的进度管理器"""
    
    def __init__(self, progress_file=None, temp_file=None, output_dir=None):
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
        return {'completed': [], 'processing': [], 'failed': [], 'start_time': None, 'performance_history': []}
    
    def save_progress(self):
        """保存进度数据"""
        # 使用线程锁防止并发写入
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
    
    def is_completed(self, video_path, output_dir=None):
        """检查视频是否已完成（优先使用内容检查）"""
        if output_dir:
            # 使用内容检查（推荐）
            completed, record = self.is_file_completed_by_content(video_path, output_dir)
            if completed:
                return True
        
        # 回退到文件名检查
        video_name = os.path.basename(video_path)
        return video_name in self.progress_data.get('completed', [])
    
    def mark_completed(self, video_path, output_path=None, processing_time=0.0):
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
                'output_name': os.path.basename(output_path) if output_path else video_signature['name'],
                'output_size': os.path.getsize(output_path) if output_path and os.path.exists(output_path) else 0,
                'completed_time': datetime.now().isoformat(),
                'processing_time': processing_time
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
            
            # 记录性能数据
            if processing_time > 0:
                self.progress_data['performance_history'].append({
                    'file': video_signature['name'],
                    'time': processing_time,
                    'timestamp': datetime.now().isoformat()
                })
        
        # 从处理中移除
        video_name = os.path.basename(video_path)
        if video_name in self.progress_data['processing']:
            self.progress_data['processing'].remove(video_name)
        # 从失败列表中移除
        self.progress_data['failed'] = [f for f in self.progress_data['failed'] if f.get('name') != video_name]
        
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
        # 清理错误消息
        clean_error = error_msg[:200] if error_msg else "处理失败"
        
        # 检查是否已经在失败列表中
        failed_names = [f['name'] for f in self.progress_data['failed'] if isinstance(f, dict)]
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
                if isinstance(fail_info, dict):
                    logging.info(f"  - {fail_info['name']}: {fail_info['error']}")
    
    def cleanup_invalid_records(self, output_dir):
        """清理无效的记录（输出文件不存在的记录）"""
        cleaned_count = 0
        valid_completed = []
        
        for record in self.progress_data.get('completed', []):
            if isinstance(record, dict):
                # 新格式记录
                output_name = record.get('output_name', record.get('name'))
                # 对于切头尾，输出文件直接在输出目录中
                base_name = os.path.splitext(record.get('name', ''))[0]
                possible_paths = [
                    os.path.join(output_dir, f"{base_name}_no_head_tail.mp4"),
                    os.path.join(output_dir, output_name),
                ]
                
                found = False
                for output_path in possible_paths:
                    if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
                        valid_completed.append(record)
                        found = True
                        break
                
                if not found:
                    cleaned_count += 1
                    logging.info(f"清理无效记录: {record.get('name')} (输出文件不存在)")
            else:
                # 旧格式记录
                base_name = os.path.splitext(record)[0]
                output_path = os.path.join(output_dir, f"{base_name}_no_head_tail.mp4")
                if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
                    valid_completed.append(record)
                else:
                    cleaned_count += 1
                    logging.info(f"清理无效记录: {record} (输出文件不存在)")
        
        self.progress_data['completed'] = valid_completed
        
        if cleaned_count > 0:
            self.save_progress()
            logging.info(f"清理完成，移除了 {cleaned_count} 个无效记录")
        
        return cleaned_count

# ==================== 配置验证和初始化函数 ====================

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
    if not os.path.exists(root_path):
        errors.append(f"输入目录不存在: {root_path}")
    if not os.path.exists(os.path.dirname(output_root)):
        warnings.append(f"输出目录的父目录不存在，将自动创建: {os.path.dirname(output_root)}")
    
    # 检查进度记录文件夹
    if not os.path.exists(PROGRESS_FOLDER):
        warnings.append(f"进度记录文件夹不存在，将自动创建: {PROGRESS_FOLDER}")
    
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

def detect_hardware_capabilities():
    """检测硬件能力并返回优化配置"""
    hardware_info = {}
    
    # CPU信息
    try:
        hardware_info['cpu_count'] = multiprocessing.cpu_count()
        hardware_info['cpu_brand'] = platform.processor() or "Unknown CPU"
    except:
        hardware_info['cpu_count'] = 4
        hardware_info['cpu_brand'] = "Unknown CPU"
    
    # 内存信息
    try:
        memory = psutil.virtual_memory()
        hardware_info['total_memory_gb'] = memory.total / (1024**3)
        hardware_info['available_memory_gb'] = memory.available / (1024**3)
    except:
        hardware_info['total_memory_gb'] = 8.0
        hardware_info['available_memory_gb'] = 4.0
    
    # GPU检测
    hardware_info['gpu_name'] = "未检测到GPU"
    hardware_info['encoder'] = 'libx264'  # 默认软件编码
    hardware_info['encoder_type'] = 'software'
    
    # 尝试检测NVIDIA GPU
    try:
        result = subprocess.run(['nvidia-smi', '--query-gpu=name', '--format=csv,noheader,nounits'], 
                               capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            gpu_name = result.stdout.strip().split('\n')[0]
            hardware_info['gpu_name'] = gpu_name
            hardware_info['encoder'] = 'h264_nvenc'
            hardware_info['encoder_type'] = 'nvidia'
    except:
        pass
    
    # 如果没有NVIDIA，尝试检测AMD
    if hardware_info['encoder_type'] == 'software':
        try:
            # AMD GPU检测逻辑（简化版）
            result = subprocess.run(['wmic', 'path', 'win32_VideoController', 'get', 'name'], 
                                   capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and 'AMD' in result.stdout.upper():
                hardware_info['gpu_name'] = "AMD GPU"
                hardware_info['encoder'] = 'h264_amf'
                hardware_info['encoder_type'] = 'amd'
        except:
            pass
    
    # 如果没有独显，尝试Intel集显
    if hardware_info['encoder_type'] == 'software':
        try:
            result = subprocess.run(['wmic', 'path', 'win32_VideoController', 'get', 'name'], 
                                   capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and 'INTEL' in result.stdout.upper():
                hardware_info['gpu_name'] = "Intel GPU"
                hardware_info['encoder'] = 'h264_qsv'
                hardware_info['encoder_type'] = 'intel'
        except:
            pass
    
    # 根据硬件配置确定最大并发数
    if hardware_info['encoder_type'] != 'software':
        # 有硬件加速，可以更高并发
        base_concurrent = min(6, max(2, hardware_info['cpu_count'] // 2))
    else:
        # 纯CPU编码，较低并发
        base_concurrent = min(3, max(1, hardware_info['cpu_count'] // 4))
    
    # 根据内存调整
    if hardware_info['available_memory_gb'] < 4:
        base_concurrent = max(1, base_concurrent // 2)
    elif hardware_info['available_memory_gb'] > 16:
        base_concurrent = min(8, base_concurrent + 2)
    
    hardware_info['max_concurrent'] = base_concurrent
    
    # 添加编码参数
    if hardware_info['encoder_type'] == 'nvidia':
        hardware_info['options'] = {
            'preset': 'p4',
            'rc': 'vbr',
            'cq': '23',
            'b:v': '0',
            'maxrate': '10M',
            'bufsize': '20M'
        }
    elif hardware_info['encoder_type'] == 'amd':
        hardware_info['options'] = {
            'quality': 'speed',
            'rc': 'vbr_peak',
            'qmin': '18',
            'qmax': '28',
            'b:v': '5M'
        }
    elif hardware_info['encoder_type'] == 'intel':
        hardware_info['options'] = {
            'preset': 'veryfast',
            'global_quality': '23'
        }
    else:  # software
        hardware_info['options'] = {
            'preset': 'veryfast',
            'crf': '23'
        }
    
    return hardware_info

# ==================== 全局变量初始化 ====================
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

def parse_progress(line):
    """解析FFmpeg进度信息 - 增强版"""
    info = {}
    patterns = {'frame': r'frame=\s*(\d+)', 'fps': r'fps=\s*([\d\.]+)', 'time': r'time=\s*(\d+):(\d+):([\d\.]+)',
                'speed': r'speed=\s*([\d\.]+)x', 'size': r'size=\s*(\d+)kB'}
    
    try:
        for key, pattern in patterns.items():
            match = re.search(pattern, line)
            if match:
                if key == 'time':
                    # 更安全的时间解析
                    hours = float(match.group(1))
                    minutes = float(match.group(2))
                    seconds = float(match.group(3))
                    info[key] = hours * 3600 + minutes * 60 + seconds
                elif key in ['fps', 'speed']:
                    try:
                        info[key] = float(match.group(1))
                    except ValueError:
                        info[key] = 0.0
                else:
                    try:
                        info[key] = int(match.group(1))
                    except ValueError:
                        info[key] = 0
    except Exception as e:
        # 如果解析失败，返回空字典而不是崩溃
        logging.debug(f"进度解析失败: {e}, 行内容: {line.strip()}")
        return {}
    
    return info

def build_ffmpeg_command(input_file, output_file, hardware_info, source_quality_info=None):
    """构建FFmpeg命令，基于批量裁剪2.0的思维"""
    cmd = [FFMPEG_PATH, '-y', '-nostdin']
    
    # 输入优化参数
    cmd.extend(['-probesize', hardware_info.get('probe_size', '50M')])
    cmd.extend(['-analyzeduration', hardware_info.get('probe_size', '50M')])
    
    # 切头尾时间设置
    cmd.extend(['-ss', str(head_cut_time)])
    cmd.extend(['-i', input_file])
    
    # 计算有效时长
    total_duration = get_media_duration_seconds(input_file)
    effective_duration = max(0, total_duration - head_cut_time - tail_cut_time)
    if effective_duration > 0:
        cmd.extend(['-t', str(effective_duration)])
    
    # 编码器设置
    cmd.extend(['-c:v', hardware_info['encoder']])
    
    # 根据编码器类型设置参数
    if hardware_info['encoder_type'] == 'nvidia':
        # 使用优化的NVIDIA参数
        options = hardware_info.get('options', {})
        for key, value in options.items():
            cmd.extend([f'-{key}', str(value)])
    elif hardware_info['encoder_type'] == 'amd':
        # 使用优化的AMD参数
        options = hardware_info.get('options', {})
        for key, value in options.items():
            cmd.extend([f'-{key}', str(value)])
    elif hardware_info['encoder_type'] == 'intel':
        # 使用优化的Intel参数
        options = hardware_info.get('options', {})
        for key, value in options.items():
            cmd.extend([f'-{key}', str(value)])
    else:  # software
        # 使用优化的软件编码参数
        options = hardware_info.get('options', {})
        for key, value in options.items():
            cmd.extend([f'-{key}', str(value)])
    
    # 音频处理
    cmd.extend(['-c:a', 'aac', '-b:a', '192k'])
    
    # 输出优化参数
    cmd.extend([
        '-movflags', '+faststart',  # 快速启动
        '-map_metadata', '-1',      # 移除元数据
        '-vsync', 'cfr',            # 恒定帧率
        '-avoid_negative_ts', 'make_zero',
        '-fflags', '+genpts',
        '-max_muxing_queue_size', hardware_info.get('buffer_size', '2048').replace('M', ''),
        output_file
    ])
    
    return cmd

def run_ffmpeg_process(cmd, duration, pbar, video_path=None):
    """运行FFmpeg进程并监控进度 - 修复版本"""
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                               encoding='utf-8', errors='ignore', bufsize=1)
    last_percentage = 0
    last_update_time = time.time()
    no_progress_count = 0
    
    # 对于长视频，调整超时参数 - 增强稳定性
    is_long_video = duration > 3600  # 超过1小时算长视频
    is_very_long_video = duration > 7200  # 超过2小时算超长视频
    
    if is_very_long_video:
        max_stall_time = 600  # 超长视频10分钟
        max_no_progress_time = 1200  # 超长视频20分钟
    elif is_long_video:
        max_stall_time = 300  # 长视频5分钟
        max_no_progress_time = 600  # 长视频10分钟
    else:
        max_stall_time = 120  # 短视频2分钟
        max_no_progress_time = 300  # 短视频5分钟
    
    logging.info(f"视频时长: {duration:.1f}秒, 长视频模式: {is_long_video}, 超长视频: {is_very_long_video}")
    
    # 监控变量
    stall_count = 0
    total_stall_time = 0
    
    while process.poll() is None:
        try:
            line = process.stderr.readline()
            if line:
                progress_info = parse_progress(line)
                if 'time' in progress_info:
                    last_update_time = time.time()
                    no_progress_count = 0  # 重置无进度计数
                    stall_count = 0  # 重置停滞计数
                    
                    # 修复进度计算，避免超过100% - 只在有time键时执行
                    processed_time = progress_info['time']  # ffmpeg报告的当前处理时间
                    if processed_time > duration:
                        processed_time = duration
                    
                    # 真实进度计算
                    percentage = min(100, (processed_time / duration) * 100)
                    
                    if percentage > last_percentage:
                        pbar.update(percentage - last_percentage)
                        last_percentage = percentage
                        
                        postfix = {'FPS': f"{progress_info.get('fps', 0):.1f}",
                                   '速度': f"{progress_info.get('speed', 0):.1f}x",
                                   '大小': f"{progress_info.get('size', 0) / 1024:.1f}MB",
                                   '时间': f"{processed_time:.1f}s/{duration:.1f}s"}
                        pbar.set_postfix(postfix)
                        
                        # 每隔10%输出详细日志
                        if int(percentage) % 10 == 0 and int(percentage) != int(last_percentage):
                            logging.info(f"处理进度: {percentage:.1f}% - FPS: {progress_info.get('fps', 0):.1f}, 速度: {progress_info.get('speed', 0):.1f}x")
            else:
                no_progress_count += 1
                stall_count += 1
                
                # 检查是否停滞太久
                current_time = time.time()
                if current_time - last_update_time > max_no_progress_time:
                    logging.error(f"处理超时，{max_no_progress_time}秒内无任何进度更新")
                    try:
                        process.terminate()
                        time.sleep(5)  # 等待进程结束
                        if process.poll() is None:
                            process.kill()  # 强制终止
                    except:
                        pass
                    raise Exception(f"处理超时，{max_no_progress_time}秒内无任何进度更新")
        
        except Exception as e:
            logging.error(f"监控FFmpeg进程时出错: {e}")
            # 确保进程被终止
            try:
                if process.poll() is None:
                    process.terminate()
                    time.sleep(2)
                    if process.poll() is None:
                        process.kill()
            except:
                pass
            raise
        
        # 动态调整睡眠时间
        if is_very_long_video:
            time.sleep(2)  # 超长视频进一步减少检查频率
        elif is_long_video:
            time.sleep(1)
        else:
            time.sleep(0.5)
    
    # 检查返回码
    if process.returncode != 0:
        remaining_stderr = process.stderr.read()
        error_lines = []
        for line in remaining_stderr.split('\n'):
            if (line.strip() and 
                not line.startswith('frame=') and 
                not line.startswith('size=') and
                not line.startswith('time=') and
                not line.startswith('bitrate=') and
                not line.startswith('speed=') and
                'fps=' not in line):
                error_lines.append(line.strip())
        
        filtered_errors = error_lines[-10:] if error_lines else ["无具体错误信息"]
        error_msg = '\n'.join(filtered_errors)
        raise Exception(f"ffmpeg处理失败 (代码 {process.returncode}): {error_msg}")

def update_final_progress(pbar, video_path, stage_name="最终处理"):
    """更新95-100%的进度"""
    # 95% -> 97%: 验证阶段
    if pbar.n < 97:
        pbar.update(97 - pbar.n)
        pbar.set_postfix_str(f"{stage_name} - 验证中...")
        time.sleep(0.5)
    
    # 97% -> 99%: 完成处理
    if pbar.n < 99:
        pbar.update(99 - pbar.n)
        pbar.set_postfix_str(f"{stage_name} - 完成处理...")
        time.sleep(0.3)
    
    # 99% -> 100%: 最终完成
    if pbar.n < 100:
        pbar.update(100 - pbar.n)

# ==================== FFmpeg进程管理函数 ====================

def run_ffmpeg_process(cmd, expected_duration, pbar, video_path):
    """
    执行FFmpeg进程并监控进度 - 优化版本
    
    Args:
        cmd: FFmpeg命令列表
        expected_duration: 预期处理时长(秒)
        pbar: 进度条对象
        video_path: 视频路径(用于日志)
    """
    video_name = os.path.basename(video_path)
    process = None
    
    try:
        # 启动FFmpeg进程
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8',
            creationflags=subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0
        )
        
        # 监控进度
        progress_pattern = re.compile(r'time=(\d+):(\d+):(\d+)\.(\d+)')
        last_progress = 0
        
        pbar.set_postfix_str("🎬 处理中...")
        
        while True:
            output = process.stderr.readline()
            if output == '' and process.poll() is not None:
                break
            
            if output:
                # 解析进度
                match = progress_pattern.search(output)
                if match:
                    hours = int(match.group(1))
                    minutes = int(match.group(2))
                    seconds = int(match.group(3))
                    current_time = hours * 3600 + minutes * 60 + seconds
                    
                    if expected_duration > 0:
                        progress = min(95, (current_time / expected_duration) * 95)  # 最多到95%
                        if progress > last_progress:
                            pbar.n = progress
                            pbar.set_postfix_str(f"🎬 处理中... {progress:.1f}%")
                            pbar.refresh()
                            last_progress = progress
        
        # 等待进程完成
        return_code = process.wait()
        
        if return_code != 0:
            stderr_output = process.stderr.read() if process.stderr else ""
            raise RuntimeError(f"FFmpeg处理失败 (退出码: {return_code}): {stderr_output}")
            
    except Exception as e:
        logging.error(f"FFmpeg进程执行失败: {video_name} -> {e}")
        raise
        
    finally:
        # 确保进程被正确终止和清理
        if process:
            try:
                if process.poll() is None:  # 进程仍在运行
                    process.terminate()
                    try:
                        process.wait(timeout=5)  # 等待5秒
                    except subprocess.TimeoutExpired:
                        process.kill()  # 强制杀死
                        process.wait()
                
                # 关闭文件描述符
                if process.stdout:
                    process.stdout.close()
                if process.stderr:
                    process.stderr.close()
                    
            except Exception as cleanup_error:
                logging.warning(f"FFmpeg进程清理失败: {cleanup_error}")
            
            finally:
                process = None  # 释放引用
                
        # 主动垃圾回收
        gc.collect()

# ==================== 核心处理函数 (基于批量裁剪2.0极致优化) ====================

def process_video(video_path, output_video_path, hardware_info, video_idx=0, total_videos=1):
    """
    处理单个视频文件 - 基于批量裁剪2.0的极致性能优化
    
    Args:
        video_path: 输入视频路径
        output_video_path: 输出视频路径
        hardware_info: 硬件信息字典
        video_idx: 当前视频索引
        total_videos: 总视频数量
    
    Returns:
        tuple: (是否成功, 处理时间, 错误信息)
    """
    start_time = time.time()
    video_name = os.path.basename(video_path)
    
    # 额外的稳定性检查：再次验证分辨率
    if SKIP_LOW_RESOLUTION_VIDEOS:
        should_skip, resolution, skip_reason = should_skip_low_resolution_video(video_path)
        if should_skip:
            logging.warning(f"处理时发现低分辨率视频，跳过: {video_name} -> {skip_reason}")
            return False, 0.0, f"低分辨率跳过: {skip_reason}"
    
    # 获取进度条位置
    position = get_progress_bar_position()
    
    try:
        # 增强的文件安全性验证
        if not os.path.exists(video_path):
            raise FileNotFoundError(f"输入文件不存在: {video_path}")
        
        # 验证文件权限
        if not os.access(video_path, os.R_OK):
            raise PermissionError(f"无法读取输入文件: {video_path}")
        
        # 验证文件完整性
        file_size = os.path.getsize(video_path)
        if file_size == 0:
            raise ValueError(f"输入文件为空: {video_path}")
        
        # 验证文件扩展名
        valid_extensions = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.3gp'}
        file_ext = os.path.splitext(video_path)[1].lower()
        if file_ext not in valid_extensions:
            logging.warning(f"文件扩展名可能不支持: {file_ext}")
        
        # 检查文件是否被占用
        try:
            with open(video_path, 'rb') as f:
                f.read(1024)  # 尝试读取1KB数据
        except PermissionError:
            raise PermissionError(f"文件被其他程序占用: {video_name}")
        except Exception as e:
            raise ValueError(f"文件访问异常: {video_name} -> {e}")
        
        # 获取视频信息 - 增加重试机制和验证，参考批量裁剪2.0.py的详细日志
        logging.info(f"🔍 开始分析视频文件: {video_name}")
        
        # 先获取基本视频质量信息
        quality_info = analyze_video_quality(video_path)
        if quality_info:
            logging.info(f"✅ 视频质量分析成功: {video_name}")
            # 从质量信息中获取时长（更可靠）
            duration = quality_info.get('duration', 0.0)
            if duration > 0:
                logging.info(f"✅ 从质量分析获取时长: {duration:.1f}s - {video_name}")
            else:
                logging.warning(f"⚠️  质量分析中时长无效，尝试独立获取: {video_name}")
        else:
            logging.warning(f"⚠️  视频质量分析失败，尝试独立获取时长: {video_name}")
            duration = 0.0
        
        # 如果质量分析中没有有效时长，独立获取时长
        if duration <= 0:
            logging.info(f"🔄 独立获取视频时长: {video_name}")
            for attempt in range(3):  # 最多重试3次
                try:
                    duration = get_media_duration_seconds(video_path)
                    if duration > 0:
                        logging.info(f"✅ 独立获取时长成功 (第{attempt+1}次): {duration:.1f}s - {video_name}")
                        break
                    else:
                        logging.warning(f"⚠️  获取时长返回无效值 (第{attempt+1}次): {duration} - {video_name}")
                except Exception as e:
                    if attempt < 2:
                        logging.warning(f"获取时长失败，第{attempt+1}次重试: {video_name} -> {e}")
                        time.sleep(1)
                    else:
                        logging.error(f"❌ 所有获取时长尝试都失败: {video_name} -> {e}")
                        raise ValueError(f"无法获取视频信息，文件可能损坏: {e}")
        
        if duration <= 0:
            logging.error(f"❌ 最终时长验证失败: {duration} - {video_name}")
            raise ValueError("无法获取视频时长或时长为0，文件可能损坏")
        else:
            logging.info(f"✅ 时长验证通过: {duration:.1f}s - {video_name}")
        
        # 检查切割时间的合理性 - 智能处理短视频
        effective_duration = duration - head_cut_time - tail_cut_time
        if effective_duration <= 0:
            # 对于短视频，智能跳过而不是失败
            short_video_msg = f"视频时长({duration:.1f}s)小于切割总时间({head_cut_time + tail_cut_time}s)，智能跳过"
            logging.warning(f"⏭️ 智能跳过短视频: {video_name} -> {short_video_msg}")
            
            # 标记为跳过而不是失败
            if progress_manager:
                # 使用特殊标记记录跳过的短视频
                progress_manager.mark_completed(video_path, None, 0.0)
            
            return False, 0.0, f"智能跳过: {short_video_msg}"
        
        # 额外的文件大小检查
        file_size = os.path.getsize(video_path)
        if file_size < 1024 * 1024:  # 小于1MB
            logging.warning(f"视频文件过小: {video_name} ({file_size} bytes)")
        
        # 检查可用磁盘空间
        output_dir = os.path.dirname(output_video_path)
        if os.path.exists(output_dir):
            free_space = shutil.disk_usage(output_dir).free
            estimated_size = file_size * 0.8  # 估算输出文件大小
            if free_space < estimated_size * 2:  # 至少需要2倍空间作为缓冲
                logging.warning(f"磁盘空间可能不足: 可用{free_space/1024/1024:.1f}MB, 估算需要{estimated_size/1024/1024:.1f}MB")
        
        # 计算有效时长
        effective_duration = duration - head_cut_time - tail_cut_time
        
        # 创建输出目录
        output_dir = os.path.dirname(output_video_path)
        if output_dir:  # 只有当输出目录不是空时才创建
            os.makedirs(output_dir, exist_ok=True)
        
        # 创建进度条
        desc = f"[{video_idx+1}/{total_videos}] {video_name[:30]}..."
        pbar = tqdm(total=100, desc=desc, position=position, 
                   leave=False, unit='%', ncols=120,
                   bar_format='{l_bar}{bar}| {n:.1f}/{total:.0f}% [{elapsed}<{remaining}, {postfix}]')
        
        # 标记为处理中
        if progress_manager:
            progress_manager.mark_processing(video_path)
        
        # 构建FFmpeg命令
        cmd = build_ffmpeg_command(video_path, output_video_path, hardware_info)
        
        # 记录命令信息
        logging.info(f"开始处理视频 [{video_idx+1}/{total_videos}]: {video_name}")
        logging.info(f"原始时长: {duration:.1f}s, 有效时长: {effective_duration:.1f}s")
        logging.info(f"使用编码器: {hardware_info.get('encoder', 'unknown')}")
        logging.debug(f"FFmpeg命令: {' '.join(cmd)}")
        
        # 执行FFmpeg处理
        run_ffmpeg_process(cmd, effective_duration, pbar, video_path)
        
        # 更新最终进度
        update_final_progress(pbar, video_path, "切头尾处理")
        
        # 增强的输出文件验证
        if not os.path.exists(output_video_path):
            raise FileNotFoundError("输出文件未生成")
        
        # 验证输出文件权限
        if not os.access(output_video_path, os.R_OK):
            raise PermissionError(f"无法读取输出文件: {output_video_path}")
        
        output_size = os.path.getsize(output_video_path)
        if output_size < 1024:  # 小于1KB认为处理失败
            raise ValueError(f"输出文件过小 ({output_size} bytes)")
        
        # 验证输出文件完整性 - 尝试读取文件头
        try:
            with open(output_video_path, 'rb') as f:
                header = f.read(32)  # 读取前32字节
                if len(header) < 8:
                    raise ValueError("输出文件头部数据不完整")
        except Exception as e:
            raise ValueError(f"输出文件完整性验证失败: {e}")
        
        # 验证输出视频时长和媒体信息
        try:
            output_duration = get_media_duration_seconds(output_video_path)
            if output_duration <= 0:
                raise ValueError("输出视频时长无效，文件可能损坏")
                
            expected_duration = effective_duration
            duration_diff = abs(output_duration - expected_duration)
            
            if duration_diff > 5:  # 允许5秒误差
                logging.warning(f"输出时长与预期不符: 预期{expected_duration:.1f}s, 实际{output_duration:.1f}s, 差异{duration_diff:.1f}s")
                
            # 如果差异过大，标记为可能的处理问题
            if duration_diff > expected_duration * 0.1:  # 超过10%差异
                logging.warning(f"输出时长差异过大，可能存在处理问题")
                
        except Exception as e:
            # 如果无法获取输出文件信息，尝试删除可能损坏的文件
            try:
                os.remove(output_video_path)
                logging.warning(f"已删除可能损坏的输出文件: {os.path.basename(output_video_path)}")
            except:
                pass
            raise ValueError(f"输出文件媒体信息验证失败: {e}")
        
        processing_time = time.time() - start_time
        
        # 标记完成
        if progress_manager:
            progress_manager.mark_completed(video_path, output_video_path, processing_time)
        
        # 成功日志
        logging.info(f"✅ 处理完成: {video_name} -> 耗时: {processing_time:.1f}s, 输出大小: {output_size/1024/1024:.1f}MB")
        
        pbar.set_postfix_str("✅ 完成")
        pbar.close()
        
        # 释放进度条位置
        release_progress_bar_position(position)
        
        # 主动触发内存清理
        if video_idx % 5 == 0:  # 每处理5个视频进行一次内存清理
            gc.collect()
        
        return True, processing_time, None
        
    except KeyboardInterrupt:
        logging.info(f"⏹️  用户中断处理: {video_name}")
        if 'pbar' in locals():
            pbar.set_postfix_str("❌ 中断")
            pbar.close()
        release_progress_bar_position(position)
        raise
        
    except Exception as e:
        processing_time = time.time() - start_time
        error_msg = str(e)
        error_type = type(e).__name__
        
        # 详细的错误分类和处理
        if isinstance(e, FileNotFoundError):
            logging.error(f"❌ 文件未找到: {video_name} -> {error_msg}")
        elif isinstance(e, PermissionError):
            logging.error(f"❌ 权限错误: {video_name} -> {error_msg}")
        elif isinstance(e, OSError):
            logging.error(f"❌ 系统错误: {video_name} -> {error_msg}")
        elif isinstance(e, ValueError):
            logging.error(f"❌ 参数错误: {video_name} -> {error_msg}")
        elif isinstance(e, MemoryError):
            logging.error(f"❌ 内存不足: {video_name} -> {error_msg}")
        else:
            logging.error(f"❌ 未知错误 ({error_type}): {video_name} -> 耗时: {processing_time:.1f}s, 错误: {error_msg}")
        
        # 标记失败
        if progress_manager:
            progress_manager.mark_failed(video_path, f"{error_type}: {error_msg}")
        
        if 'pbar' in locals():
            pbar.set_postfix_str(f"❌ {error_type}")
            pbar.close()
        
        # 安全清理失败的输出文件
        cleanup_attempts = 0
        max_cleanup_attempts = 3
        while cleanup_attempts < max_cleanup_attempts:
            try:
                if os.path.exists(output_video_path):
                    # 确保文件没有被占用
                    time.sleep(0.1)
                    os.remove(output_video_path)
                    logging.info(f"已清理失败的输出文件: {os.path.basename(output_video_path)}")
                break
            except PermissionError:
                cleanup_attempts += 1
                if cleanup_attempts < max_cleanup_attempts:
                    logging.warning(f"文件被占用，等待后重试清理... (尝试 {cleanup_attempts}/{max_cleanup_attempts})")
                    time.sleep(1)
                else:
                    logging.warning(f"无法清理失败文件 (文件被占用): {os.path.basename(output_video_path)}")
            except Exception as cleanup_error:
                logging.warning(f"清理失败文件时出错: {cleanup_error}")
                break
        
        # 确保释放进度条位置
        try:
            release_progress_bar_position(position)
        except Exception:
            pass  # 忽略释放进度条位置时的错误
            
        return False, processing_time, f"{error_type}: {error_msg}"

def process_video_batch(video_list, hardware_info, max_workers=None):
    """
    批量处理视频 - 基于批量裁剪2.0的并发优化
    
    Args:
        video_list: 视频文件列表
        hardware_info: 硬件信息字典
        max_workers: 最大并发数
    
    Returns:
        dict: 处理结果统计
    """
    if not video_list:
        logging.warning("没有视频需要处理")
        return {'success': 0, 'failed': 0, 'skipped': 0, 'total_time': 0}
    
    total_videos = len(video_list)
    logging.info(f"开始批量处理 {total_videos} 个视频文件")
    
    # 确定并发数
    if max_workers is None:
        max_workers = min(hardware_info.get('max_concurrent', 2), total_videos)
    
    logging.info(f"使用 {max_workers} 个并发处理线程")
    
    # 统计变量
    success_count = 0
    failed_count = 0
    skipped_count = 0
    total_processing_time = 0
    
    # 设置开始时间
    if progress_manager:
        progress_manager.set_start_time()
    
    batch_start_time = time.time()
    
    # 创建主进度条
    main_pbar = tqdm(total=total_videos, desc="总体进度", position=0, 
                     leave=True, unit='个', ncols=120,
                     bar_format='{l_bar}{bar}| {n}/{total} [{elapsed}<{remaining}, {rate_fmt}]')
    
    def update_main_progress():
        """更新主进度条"""
        completed = success_count + failed_count + skipped_count
        main_pbar.n = completed
        main_pbar.set_postfix({
            '成功': success_count,
            '失败': failed_count, 
            '跳过': skipped_count,
            '剩余': total_videos - completed
        })
        main_pbar.refresh()
    
    def process_single_video(video_info):
        """处理单个视频的包装函数 - 增强异常处理"""
        nonlocal success_count, failed_count, skipped_count, total_processing_time
        
        video_path, output_path, video_idx = video_info
        video_name = os.path.basename(video_path)
        
        try:
            # 检查是否已完成
            if progress_manager and progress_manager.is_completed(video_path, os.path.dirname(output_path)):
                with progress_save_lock:
                    skipped_count += 1
                    update_main_progress()
                logging.info(f"⏭️  跳过已完成: {video_name}")
                return
            
            # 检查是否正在处理中
            if progress_manager and progress_manager.is_processing(video_path):
                logging.info(f"⏳ 跳过处理中: {video_name}")
                with progress_save_lock:
                    skipped_count += 1
                    update_main_progress()
                return
            
            # 处理视频
            logging.info(f"🚀 开始处理: [{video_idx+1}/{total_videos}] {video_name}")
            success, processing_time, error_msg = process_video(
                video_path, output_path, hardware_info, video_idx, total_videos
            )
            
            # 更新统计
            with progress_save_lock:
                if success:
                    success_count += 1
                    logging.info(f"✅ 处理成功: {video_name} (耗时: {processing_time:.1f}s)")
                else:
                    # 检查是否是智能跳过
                    if error_msg and "智能跳过" in error_msg:
                        skipped_count += 1
                        logging.info(f"⏭️ 智能跳过: {video_name} -> {error_msg}")
                    else:
                        failed_count += 1
                        logging.error(f"❌ 处理失败: {video_name} -> {error_msg}")
                total_processing_time += processing_time
                update_main_progress()
                
        except KeyboardInterrupt:
            logging.info(f"⏹️ 用户中断批量处理: {video_name}")
            with progress_save_lock:
                failed_count += 1
                update_main_progress()
            raise
        except Exception as e:
            logging.error(f"💥 处理视频时发生未预期异常: {video_name} -> {e}", exc_info=True)
            with progress_save_lock:
                failed_count += 1
                if progress_manager:
                    progress_manager.mark_failed(video_path, f"未预期异常: {str(e)}")
                update_main_progress()
    
    try:
        # 准备视频信息列表
        video_info_list = []
        for idx, video_path in enumerate(video_list):
            # 构建输出路径 - 直接输出到根目录，不创建子文件夹
            base_name = os.path.splitext(os.path.basename(video_path))[0]
            output_path = os.path.join(output_root, f"{base_name}_no_head_tail.mp4")
            video_info_list.append((video_path, output_path, idx))
        
        # 使用线程池并发处理 - 增强稳定性
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="VideoProcessor"
        ) as executor:
            # 提交所有任务
            futures = []
            active_futures = set()
            
            for video_info in video_info_list:
                try:
                    future = executor.submit(process_single_video, video_info)
                    futures.append(future)
                    active_futures.add(future)
                except Exception as e:
                    logging.error(f"提交任务失败: {e}")
                    with progress_save_lock:
                        failed_count += 1
                        update_main_progress()
            
            # 监控任务完成情况
            completed_futures = 0
            total_futures = len(futures)
            
            try:
                # 等待所有任务完成 - 增强稳定性和进度跟踪（移除全局超时）
                logging.info(f"开始等待 {total_futures} 个任务完成...")
                
                for future in concurrent.futures.as_completed(futures):  # 移除全局超时，让所有任务都能完成
                    try:
                        result = future.result(timeout=1800)  # 单个任务30分钟超时（处理超长视频）
                        completed_futures += 1
                        active_futures.discard(future)
                        
                        # 每完成10个任务输出进度日志
                        if completed_futures % 10 == 0 or completed_futures == total_futures:
                            remaining = total_futures - completed_futures
                            logging.info(f"📊 任务进度: {completed_futures}/{total_futures}, 剩余: {remaining}")
                            logging.info(f"📈 当前统计: 成功: {success_count}, 失败: {failed_count}, 跳过: {skipped_count}")
                        
                        # 定期进行内存清理
                        if completed_futures % 20 == 0:
                            gc.collect()
                            logging.debug(f"🧹 执行内存清理 (完成 {completed_futures} 个任务)")
                            
                    except concurrent.futures.TimeoutError:
                        logging.warning(f"⏰ 任务超时 (30分钟)，正在取消...")
                        try:
                            future.cancel()
                        except Exception:
                            pass
                        completed_futures += 1
                        active_futures.discard(future)
                        with progress_save_lock:
                            failed_count += 1
                            update_main_progress()
                            
                    except KeyboardInterrupt:
                        logging.info("⏹️ 收到中断信号，正在停止所有处理...")
                        # 优雅关闭所有剩余任务
                        for remaining_future in active_futures:
                            try:
                                remaining_future.cancel()
                            except Exception:
                                pass
                        executor.shutdown(wait=False)
                        raise
                        
                    except Exception as e:
                        logging.error(f"💥 处理任务时发生异常: {e}", exc_info=True)
                        completed_futures += 1
                        active_futures.discard(future)
                        # 不增加失败计数，因为 process_single_video 内部已经处理了
                        
                
            except KeyboardInterrupt:
                logging.info("用户中断，正在清理剩余任务...")
                for remaining_future in active_futures:
                    remaining_future.cancel()
                raise
                
            finally:
                # 确保所有任务都被清理
                for future in futures:
                    if not future.done():
                        future.cancel()
                
                # 等待所有任务真正结束
                try:
                    executor.shutdown(wait=True, timeout=10)
                except:
                    logging.warning("线程池关闭超时，强制终止")
    
    except KeyboardInterrupt:
        logging.info("批量处理被用户中断")
        main_pbar.set_postfix_str("❌ 用户中断")
    
    finally:
        main_pbar.close()
        
        # 强制清理内存和资源
        gc.collect()
        
        # 计算总体统计
        batch_time = time.time() - batch_start_time
        
        # 打印最终统计
        # 输出详细的处理结果摘要
        success_rate = (success_count / total_videos * 100) if total_videos > 0 else 0
        print(f"\n{'='*80}")
        print(f"🎬 批量视频处理完成! 📊")
        print(f"{'='*80}")
        print(f"📈 处理统计:")
        print(f"   ✅ 成功处理: {success_count:>4} 个 ({success_rate:.1f}%)")
        print(f"   ❌ 处理失败: {failed_count:>4} 个")
        print(f"   ⏭️  跳过文件: {skipped_count:>4} 个")
        print(f"   📁 总计文件: {total_videos:>4} 个")
        print(f"{'─'*80}")
        print(f"⏱️  性能统计:")
        print(f"   🕐 批量总耗时: {batch_time:.1f} 秒")
        print(f"   ⚡ 实际处理时间: {total_processing_time:.1f} 秒")
        if success_count > 0:
            avg_speed = total_processing_time / success_count
            print(f"   📊 平均处理速度: {avg_speed:.1f} 秒/个")
            if batch_time > 0:
                efficiency = (total_processing_time / batch_time * 100)
                print(f"   🎯 处理效率: {efficiency:.1f}%")
        
        # 成功率评估
        if success_rate >= 95:
            print(f"   🌟 处理质量: 优秀!")
        elif success_rate >= 80:
            print(f"   👍 处理质量: 良好")
        elif success_rate >= 60:
            print(f"   ⚠️  处理质量: 一般，建议检查失败原因")
        else:
            print(f"   🚨 处理质量: 较差，请检查输入文件和参数设置")
            
        print(f"{'='*80}")
        
        # 记录到日志
        logging.info(f"🎯 批量处理完成: 成功{success_count}, 失败{failed_count}, 跳过{skipped_count}, 总计{total_videos}")
        logging.info(f"📊 时间统计: 总耗时{batch_time:.1f}秒, 处理耗时{total_processing_time:.1f}秒, 成功率{success_rate:.1f}%")
        
        # 打印进度摘要
        if progress_manager:
            progress_manager.print_summary()
    
    return {
        'success': success_count,
        'failed': failed_count, 
        'skipped': skipped_count,
        'total_time': batch_time,
        'processing_time': total_processing_time
    }

# ==================== 主程序入口 (基于批量裁剪2.0架构) ====================

def setup_logging():
    """设置日志记录"""
    # 创建日志目录
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # 配置日志
    log_file = os.path.join(log_dir, f'video_cut_process_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # 同时输出到控制台
        ]
    )
    
    logging.info(f"日志文件: {log_file}")
    return log_file

def main():
    """主程序入口 - 基于批量裁剪2.0的完整流程"""
    global progress_manager
    
    print("🎬 批量视频切头尾处理工具 v2.0 (增强稳定性版)")
    print("=" * 60)
    
    # 设置日志记录
    log_file = setup_logging()
    logging.info("程序启动")
    
    try:
        # 1. 配置验证
        if not validate_config():
            print("❌ 配置验证失败，程序退出")
            return
        
        # 2. 硬件检测
        print("🔍 正在检测硬件配置...")
        hardware_info = detect_hardware_capabilities()
        print(f"✅ 硬件检测完成:")
        print(f"   - GPU: {hardware_info.get('gpu_name', 'N/A')}")
        print(f"   - 编码器: {hardware_info.get('encoder', 'N/A')}")
        print(f"   - 并发数: {hardware_info.get('max_concurrent', 'N/A')}")
        print()
        
        # 3. 创建进度管理器
        print("📊 初始化进度管理...")
        progress_manager = ProgressManager(output_dir=output_root)
        print(f"✅ 进度管理器初始化完成")
        print()
        
        # 4. 扫描视频文件
        print("📁 正在扫描视频文件...")
        video_files = []
        
        # 支持的视频格式
        video_extensions = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v'}
        
        # 递归扫描
        for root, dirs, files in os.walk(root_path):
            for file in files:
                if os.path.splitext(file.lower())[1] in video_extensions:
                    video_path = os.path.join(root, file)
                    video_files.append(video_path)
        
        if not video_files:
            print("❌ 未找到任何视频文件")
            return
        
        print(f"✅ 找到 {len(video_files)} 个视频文件")
        
        # 显示前几个文件的基本信息（参考批量裁剪2.0.py风格）
        if video_files:
            print(f"\n📄 视频文件示例:")
            for i, video_path in enumerate(video_files[:5]):
                try:
                    file_size = os.path.getsize(video_path) / (1024*1024)
                    print(f"   {i+1}. {os.path.basename(video_path)} ({file_size:.1f}MB)")
                except:
                    print(f"   {i+1}. {os.path.basename(video_path)} (大小未知)")
            if len(video_files) > 5:
                print(f"   ... 还有 {len(video_files)-5} 个文件")
            print()
        
        # 4.1 低分辨率视频跳过处理
        print("🔍 检查视频分辨率...")
        filtered_videos = []
        skipped_videos = []
        skipped_count = 0
        
        for video_path in video_files:
            should_skip, resolution, skip_reason = should_skip_low_resolution_video(video_path)
            
            if should_skip:
                skipped_videos.append({
                    'path': video_path,
                    'resolution': resolution,
                    'reason': skip_reason
                })
                skipped_count += 1
            else:
                filtered_videos.append(video_path)
        
        # 显示跳过统计
        if skipped_count > 0:
            print(f"\n📊 跳过低分辨率视频统计:")
            print(f"   总计跳过: {skipped_count} 个视频")
            print(f"   跳过原因: 分辨率低于 {MIN_RESOLUTION_WIDTH}px 宽度")
            if SKIP_VIDEOS_MOVE_DIR and SKIP_VIDEOS_MOVE_DIR.strip():
                print(f"   移动目录: {SKIP_VIDEOS_MOVE_DIR}")
            else:
                print(f"   处理方式: 仅跳过，不移动")
            
            # 显示前几个跳过的视频
            print(f"   跳过视频示例:")
            for i, skipped_info in enumerate(skipped_videos[:5]):
                resolution = skipped_info['resolution']
                if resolution:
                    res_str = f"{resolution[0]}x{resolution[1]}"
                else:
                    res_str = "未知分辨率"
                print(f"     {i+1}. {os.path.basename(skipped_info['path'])} ({res_str})")
            
            if len(skipped_videos) > 5:
                print(f"     ... 还有 {len(skipped_videos)-5} 个")
            print()
        else:
            print(f"✅ 所有视频分辨率均符合要求 (>= {MIN_RESOLUTION_WIDTH}px)")
        
        # 更新视频列表为过滤后的列表
        video_files = filtered_videos
        print(f"📹 待处理视频: {len(video_files)} 个")
        
        if not video_files:
            print("❌ 没有符合分辨率要求的视频文件")
            # 处理跳过的视频
            if skipped_count > 0 and SKIP_VIDEOS_MOVE_DIR and SKIP_VIDEOS_MOVE_DIR.strip():
                print(f"\n📦 移动跳过的视频...")
                moved_count = 0
                for skipped_info in skipped_videos:
                    if move_skipped_video(skipped_info['path'], skipped_info['reason']):
                        moved_count += 1
                print(f"✅ 已移动 {moved_count} 个跳过的视频到: {SKIP_VIDEOS_MOVE_DIR}")
            return
        
        # 5. 过滤已完成的文件
        print("🔄 检查处理状态...")
        pending_videos = []
        completed_count = 0
        
        for video_path in video_files:
            if progress_manager.is_completed(video_path, output_root):
                completed_count += 1
            else:
                pending_videos.append(video_path)
        
        print(f"✅ 状态统计:")
        print(f"   - 待处理: {len(pending_videos)} 个")
        print(f"   - 已完成: {completed_count} 个")
        print(f"   - 失败文件: {progress_manager.get_failed_count()} 个")
        print()
        
        if not pending_videos:
            print("🎉 所有视频都已处理完成！")
            
            # 处理跳过的低分辨率视频
            if skipped_count > 0:
                print(f"\n📦 处理跳过的低分辨率视频...")
                logging.info(f"开始处理 {skipped_count} 个跳过的低分辨率视频")
                
                if SKIP_VIDEOS_MOVE_DIR and SKIP_VIDEOS_MOVE_DIR.strip():
                    moved_count = 0
                    failed_move_count = 0
                    
                    for skipped_info in skipped_videos:
                        if move_skipped_video(skipped_info['path'], skipped_info['reason']):
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
                
                # 最终统计汇总
                print(f"\n📈 最终统计汇总:")
                total_all_files = len(video_files) + skipped_count
                print(f"   总扫描视频: {total_all_files} 个")
                print(f"   已完成处理: {completed_count} 个")
                print(f"   跳过低分辨率: {skipped_count} 个")
                print(f"   待处理视频: 0 (全部完成)")
            
            return
        
        # 6. 用户确认
        print(f"📋 处理配置:")
        print(f"   - 输入目录: {root_path}")
        print(f"   - 输出目录: {output_root}")
        print(f"   - 切头时间: {head_cut_time}秒")
        print(f"   - 切尾时间: {tail_cut_time}秒")
        print(f"   - 并发处理: {hardware_info.get('max_concurrent', 2)}个")
        if SKIP_LOW_RESOLUTION_VIDEOS:
            print(f"   - 跳过低分辨率: 启用 (最小宽度: {MIN_RESOLUTION_WIDTH}px)")
            if SKIP_VIDEOS_MOVE_DIR and SKIP_VIDEOS_MOVE_DIR.strip():
                print(f"   - 跳过视频移动目录: {SKIP_VIDEOS_MOVE_DIR}")
            else:
                print(f"   - 跳过视频处理: 仅跳过，不移动")
        else:
            print(f"   - 跳过低分辨率: 禁用")
        print()
        
        # 显示前几个待处理文件
        print("📄 待处理文件预览:")
        for i, video_path in enumerate(pending_videos[:5]):
            print(f"   {i+1}. {os.path.basename(video_path)}")
        if len(pending_videos) > 5:
            print(f"   ... 还有 {len(pending_videos)-5} 个文件")
        print()
        
        # 自动开始处理，无需用户确认
        print("🚀 自动开始处理所有待处理文件...")
        time.sleep(1)  # 给用户1秒时间查看配置信息
        
        # 7. 开始批量处理
        print("\n🚀 开始批量处理...")
        print("=" * 50)
        
        # 清理无效记录
        cleaned = progress_manager.cleanup_invalid_records(output_root)
        if cleaned > 0:
            print(f"🧹 清理了 {cleaned} 个无效记录")
        
        # 执行批量处理
        results = process_video_batch(
            pending_videos, 
            hardware_info, 
            max_workers=hardware_info.get('max_concurrent', 2)
        )
        
        # 7.1 处理跳过的低分辨率视频
        if skipped_count > 0:
            print(f"\n📦 处理跳过的低分辨率视频...")
            logging.info(f"开始处理 {skipped_count} 个跳过的低分辨率视频")
            
            if SKIP_VIDEOS_MOVE_DIR and SKIP_VIDEOS_MOVE_DIR.strip():
                moved_count = 0
                failed_move_count = 0
                
                for skipped_info in skipped_videos:
                    if move_skipped_video(skipped_info['path'], skipped_info['reason']):
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
        
        # 8. 处理完成总结
        print("\n🎉 批量处理完成！")
        print("=" * 50)
        
        # 生成处理报告
        total_files = len(video_files)
        success_rate = (results['success'] / total_files * 100) if total_files > 0 else 0
        
        print(f"📊 最终统计报告:")
        if skipped_count > 0:
            # 包含跳过视频的总体统计
            total_all_files = total_files + skipped_count
            print(f"   - 总扫描文件: {total_all_files} 个")
            print(f"   - 跳过低分辨率: {skipped_count} 个")
            print(f"   - 符合分辨率: {total_files} 个")
        else:
            print(f"   - 总计文件: {total_files} 个")
        print(f"   - 成功处理: {results['success']} 个")
        print(f"   - 处理失败: {results['failed']} 个")
        print(f"   - 断点续传跳过: {results.get('skipped', 0)} 个")
        print(f"   - 成功率: {success_rate:.1f}%")
        print(f"   - 总耗时: {results['total_time']:.1f}秒")
        
        if results['success'] > 0:
            avg_time = results['processing_time'] / results['success']
            print(f"   - 平均处理时间: {avg_time:.1f}秒/个")
        
        print(f"   - 输出目录: {output_root}")
        print()
        
        # 失败文件报告
        if results['failed'] > 0:
            print("❌ 失败文件详情:")
            failed_files = progress_manager.progress_data.get('failed', [])
            for fail_info in failed_files[-10:]:  # 显示最近10个失败
                if isinstance(fail_info, dict):
                    print(f"   - {fail_info['name']}: {fail_info['error'][:50]}...")
            if len(failed_files) > 10:
                print(f"   ... 还有 {len(failed_files)-10} 个失败文件")
            print()
        
        print("✅ 程序执行完成！")
        
    except KeyboardInterrupt:
        print("\n⏹️  用户中断程序")
        logging.info("用户中断程序执行")
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
        logging.error(f"程序执行出错: {e}", exc_info=True)
    finally:
        # 确保保存进度
        if progress_manager:
            progress_manager.save_progress()
            logging.info("程序退出前已保存进度")

def safe_main():
    """
    安全的主函数包装器 - 增强稳定性
    """
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  程序被用户中断")
        logging.info("程序被用户中断")
        
        # 确保保存进度
        if 'progress_manager' in globals() and progress_manager:
            try:
                progress_manager.save_progress()
                print("✅ 已保存当前进度")
            except Exception as e:
                print(f"⚠️  保存进度失败: {e}")
        
        sys.exit(0)
        
    except MemoryError:
        print("\n\n❌ 内存不足，程序退出")
        logging.critical("内存不足，程序退出")
        
        # 强制清理内存
        gc.collect()
        sys.exit(1)
        
    except Exception as e:
        print(f"\n\n💥 程序发生未预期的错误: {e}")
        logging.critical(f"程序发生未预期的错误: {e}", exc_info=True)
        
        # 尝试保存进度
        if 'progress_manager' in globals() and progress_manager:
            try:
                progress_manager.save_progress()
                print("✅ 已保存当前进度")
            except:
                pass
        
        print("请检查日志文件获取详细错误信息")
        sys.exit(1)
        
    finally:
        # 最终清理
        try:
            gc.collect()
        except:
            pass

def test_video_info_extraction(video_path: str = None):
    """
    测试视频信息提取功能 - 参考批量裁剪2.0.py的测试函数
    """
    if not video_path:
        print("请提供视频文件路径进行测试")
        return
    
    if not os.path.exists(video_path):
        print(f"❌ 文件不存在: {video_path}")
        return
    
    video_name = os.path.basename(video_path)
    print(f"🧪 测试视频信息提取: {video_name}")
    print("=" * 60)
    
    # 测试1: 获取时长
    print(f"📏 测试时长获取:")
    duration = get_media_duration_seconds(video_path)
    if duration > 0:
        print(f"✅ 时长: {duration:.1f}秒 ({duration/60:.1f}分钟)")
    else:
        print(f"❌ 无法获取时长")
    
    # 测试2: 获取分辨率
    print(f"\n📐 测试分辨率获取:")
    resolution = get_video_resolution(video_path)
    if resolution:
        width, height = resolution
        print(f"✅ 分辨率: {width}x{height}")
        if width >= MIN_RESOLUTION_WIDTH:
            print(f"✅ 分辨率符合处理要求 (>= {MIN_RESOLUTION_WIDTH}px)")
        else:
            print(f"⚠️  分辨率低于处理要求 (< {MIN_RESOLUTION_WIDTH}px)")
    else:
        print(f"❌ 无法获取分辨率")
    
    # 测试3: 视频质量分析
    print(f"\n🔍 测试视频质量分析:")
    quality_info = analyze_video_quality(video_path)
    if quality_info:
        print(f"✅ 质量分析成功:")
        print(f"  视频编码: {quality_info.get('codec', 'unknown')}")
        print(f"  音频编码: {quality_info.get('audio_codec', 'unknown')}")
        print(f"  帧率: {quality_info.get('fps', 0):.1f} fps")
        print(f"  码率: {quality_info.get('bitrate_mbps', 0):.1f} Mbps")
        print(f"  文件大小: {quality_info.get('file_size_mb', 0):.1f} MB")
    else:
        print(f"❌ 质量分析失败")
    
    # 测试4: 切割时间验证
    print(f"\n✂️  测试切割时间验证:")
    if duration > 0:
        effective_duration = duration - head_cut_time - tail_cut_time
        if effective_duration > 0:
            print(f"✅ 切割时间验证通过:")
            print(f"  原始时长: {duration:.1f}s")
            print(f"  切头时间: {head_cut_time}s")
            print(f"  切尾时间: {tail_cut_time}s")
            print(f"  有效时长: {effective_duration:.1f}s")
        else:
            print(f"❌ 切割时间验证失败:")
            print(f"  原始时长: {duration:.1f}s")
            print(f"  切割总时间: {head_cut_time + tail_cut_time}s")
            print(f"  有效时长: {effective_duration:.1f}s (无效)")
    
    print("=" * 60)
    print(f"🏁 测试完成")

if __name__ == "__main__":
    # 检查是否是测试模式
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        if len(sys.argv) > 2:
            test_video_info_extraction(sys.argv[2])
        else:
            print("测试模式: python 批量切头尾2.0.py test <视频文件路径>")
    else:
        safe_main()
