# _*_ coding: utf-8 _*_
"""
综合视频处理器 - 终极版
整合视频裁剪、切头尾、智能缓存、数据库去重等所有核心功能

作者: L
版本: 3.0 Ultimate
功能: 
- 视频裁剪 + ROI选择
- 视频切头尾处理
- 智能缓存管理
- 数据库去重系统
- 流水线并行处理
- 硬件优化加速
- 多电脑协作
"""

# ==================== 用户配置区域 ====================
# !!! 请根据你的实际情况修改以下配置 !!!

# --- FFmpeg 路径配置 ---
FFMPEG_PATH = r"D:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe"
FFPROBE_PATH = r"D:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffprobe.exe"
# --- 数据库记录字段配置 ---
# !!! 重要：每次视频处理成功后，以下11个字段信息会插入Video_Editing表 !!!
OPERATOR_NAME = "L"                   # 操作人员姓名/用户名 (必填)
COMPUTER_NAME = "大09"                # 电脑名称 (留空则自动获取当前电脑名)
COMPUTER_IP = ""                      # 电脑IP地址 (留空则自动获取本机IP)
AUTO_GET_COMPUTER_INFO = True         # 自动获取电脑信息(名称和IP)
# 其他字段由程序自动生成：input_path, output_path, video_name, 
# pre_processing_size, resolution, hash_value, status, log_path
# --- 输入输出路径配置 ---
INPUT_DIR = r"D:\一般类"
OUTPUT_DIR = r"D:\测试完"
PROGRESS_FOLDER = r"Z:\personal_folder\L\处理完数据记录"

# --- 处理模式配置 ---
PROCESSING_MODE = "both"  # "crop" = 裁剪模式, "trim" = 切头尾模式, "both" = 两种都做
TARGET_RESOLUTION = (1920, 1080)  # 目标分辨率 (仅裁剪模式)

# --- 切头尾配置 (仅切头尾模式) ---
CUT_HEAD_SECONDS = 47     # 切掉开头秒数
CUT_TAIL_SECONDS = 47      # 切掉结尾秒数
SEGMENT_DURATION = 0   # 分段时长(秒), 0=不分段

# --- 智能缓存配置 ---
ENABLE_CACHE = True                    # 启用智能缓存
CACHE_DIR = r"D:\VideoCache"          # 缓存目录
MAX_CACHE_SIZE_GB = 500               # 最大缓存大小(GB)
CACHE_CLEANUP_THRESHOLD = 0.9         # 清理阈值(90%)
PRELOAD_COUNT = 2                     # 预加载视频数量

# --- 数据库配置 ---
ENABLE_DATABASE = True                # 启用数据库去重
DB_HOST = "192.168.10.70"            # 数据库主机
DB_PORT = 3306                        # 数据库端口
DB_NAME = "data_sql"                  # 数据库名
DB_USER = "root"                      # 数据库用户
DB_PASSWORD = "zq828079"              # 数据库密码
TABLE_NAME = "Video_Editing"          # 固定表名



# --- 性能配置 ---
MAX_PARALLEL_WORKERS = 6              # 最大并行数
QUALITY_MODE = 'highest'              # 质量模式: 'highest' | 'high' | 'balanced' | 'fast'
AUTO_BITRATE = True                   # 自动码率调整
VIDEO_BITRATE = "10M"                 # 固定码率(AUTO_BITRATE=False时使用)

# --- 长时间处理稳定性配置 ---
MEMORY_CLEANUP_INTERVAL = 50          # 每处理N个视频清理一次内存
MAX_MEMORY_USAGE_MB = 8192            # 最大内存使用量(MB)，超过则强制清理
HEARTBEAT_INTERVAL = 300              # 心跳检测间隔(秒)
MAX_PROCESSING_TIME_HOURS = 24        # 单个视频最大处理时间(小时)
ENABLE_WATCHDOG = True                # 启用看门狗机制
AUTO_RESTART_ON_ERROR = True          # 错误时自动重启
BATCH_SIZE = 100                      # 分批处理大小，大数据集分批处理

# --- 多电脑协作配置 ---
ENABLE_DISTRIBUTED_PROCESSING = True  # 启用分布式处理
COMPUTER_LOCK_TIMEOUT = 1800          # 电脑锁定超时时间(秒)
TASK_CLAIM_RETRY_INTERVAL = 60        # 任务声明重试间隔(秒)
ENABLE_LOAD_BALANCING = True          # 启用负载均衡

# --- 低分辨率跳过配置 ---
SKIP_LOW_RESOLUTION = True            # 跳过低分辨率视频
MIN_RESOLUTION_WIDTH = 1920           # 最小分辨率宽度
SKIP_VIDEOS_MOVE_DIR = r"Z:\personal_folder\L\跳过的低分辨率视频"

# --- 支持的视频格式 ---
SUPPORTED_VIDEO_FORMATS = ['.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm', '.ts', '.m4v', '.3gp', '.f4v']

# --- 处理器信息 ---
PROCESSOR_NAME = "综合视频处理器_终极版"  # 操作员名称

# ==================== 依赖检查 ====================
def check_dependencies():
    """检查必要的依赖库"""
    required_packages = {
        'cv2': 'opencv-python',
        'numpy': 'numpy',
        'tqdm': 'tqdm',
        'psutil': 'psutil'
    }
    
    optional_packages = {
        'pymysql': 'pymysql'
    }
    
    missing_required = []
    missing_optional = []
    
    for module, package in required_packages.items():
        try:
            __import__(module)
        except ImportError:
            missing_required.append(package)
    
    for module, package in optional_packages.items():
        try:
            __import__(module)
        except ImportError:
            missing_optional.append(package)
    
    if missing_required:
        print("❌ 缺少必要依赖库:")
        for package in missing_required:
            print(f"   - {package}")
        print("\n请运行以下命令安装:")
        print(f"pip install {' '.join(missing_required)}")
        return False
    
    if missing_optional:
        print("⚠️ 缺少可选依赖库（功能会被禁用）:")
        for package in missing_optional:
            print(f"   - {package}")
        print("\n要启用完整功能，请运行:")
        print(f"pip install {' '.join(missing_optional)}")
    
    return True

# ==================== 导入模块 ====================
# 检查依赖
if not check_dependencies():
    sys.exit(1)

import time
import cv2
import os
import sys
import json
import shutil
import logging
import hashlib
import sqlite3
import threading
import subprocess
import concurrent.futures
import multiprocessing
import platform
import socket
import uuid
import psutil
import pickle
import glob
import re
import signal
import traceback
import queue
from pathlib import Path
from tqdm import tqdm
from datetime import datetime
from typing import List, Tuple, Dict, Optional, Any
from dataclasses import dataclass, field
from contextlib import contextmanager
import numpy as np
import enum

# ==================== 日志配置 ====================
def setup_logging():
    """配置日志系统"""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / "unified_video_processor.log", encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    # 设置第三方库日志级别
    logging.getLogger('PIL').setLevel(logging.WARNING)
    logging.getLogger('matplotlib').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

logger = setup_logging()

# ==================== 核心数据结构 ====================
class ProcessingMode(enum.Enum):
    """处理模式枚举"""
    CROP = "crop"
    TRIM = "trim"
    BOTH = "both"

class PipelineStage(enum.Enum):
    """流水线阶段状态"""
    PENDING = "pending"
    CACHING = "caching"
    CACHED = "cached"
    CHECKING = "checking"
    DUPLICATE = "duplicate"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"

@dataclass
class VideoTask:
    """视频处理任务"""
    video_path: str
    task_id: str
    mode: ProcessingMode
    stage: PipelineStage = PipelineStage.PENDING
    cache_path: Optional[str] = None
    hash_value: Optional[str] = None
    error_msg: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    created_time: float = field(default_factory=time.time)
    stage_start_time: float = field(default_factory=time.time)
    
    # ROI设置 (裁剪模式)
    roi: Optional[Tuple[int, int, int, int]] = None
    
    # 切片设置 (切头尾模式)
    cut_head: float = 0
    cut_tail: float = 0
    segment_duration: float = 0
    
    def update_stage(self, new_stage: PipelineStage, error_msg: str = None):
        """更新任务阶段"""
        self.stage = new_stage
        self.stage_start_time = time.time()
        if error_msg:
            self.error_msg = error_msg
    
    def can_retry(self) -> bool:
        """是否可以重试"""
        return self.retry_count < self.max_retries
    
    def increment_retry(self):
        """增加重试次数"""
        self.retry_count += 1

# ==================== 内存和健康监控器 ====================
class MemoryHealthMonitor:
    """内存健康监控器 - 长时间处理稳定性保障"""
    
    def __init__(self):
        self.start_time = time.time()
        self.last_cleanup_time = time.time()
        self.processed_count = 0
        self.memory_samples = []
        self.max_memory_seen = 0
        self.cleanup_count = 0
        
    def should_cleanup_memory(self) -> bool:
        """检查是否需要清理内存"""
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            self.max_memory_seen = max(self.max_memory_seen, memory_mb)
            
            # 记录内存样本
            if len(self.memory_samples) >= 100:
                self.memory_samples.pop(0)
            self.memory_samples.append(memory_mb)
            
            # 检查是否超过内存阈值
            if memory_mb > MAX_MEMORY_USAGE_MB:
                logger.warning(f"⚠️ 内存使用超限: {memory_mb:.1f}MB > {MAX_MEMORY_USAGE_MB}MB")
                return True
            
            # 检查是否达到定期清理间隔
            if self.processed_count % MEMORY_CLEANUP_INTERVAL == 0 and self.processed_count > 0:
                return True
                
            return False
            
        except Exception as e:
            logger.warning(f"内存检查失败: {e}")
            return False
    
    def cleanup_memory(self, force: bool = False):
        """清理内存"""
        try:
            import gc
            import psutil
            
            process = psutil.Process()
            before_mb = process.memory_info().rss / 1024 / 1024
            
            # 执行垃圾回收
            collected = gc.collect()
            
            # 强制清理时执行额外操作
            if force:
                gc.collect()  # 二次清理
                gc.collect()  # 三次清理
                
            after_mb = process.memory_info().rss / 1024 / 1024
            freed_mb = before_mb - after_mb
            
            self.cleanup_count += 1
            self.last_cleanup_time = time.time()
            
            logger.info(f"🧹 内存清理完成 #{self.cleanup_count}: {before_mb:.1f}MB → {after_mb:.1f}MB "
                       f"(释放{freed_mb:.1f}MB, 回收{collected}个对象)")
            
            return freed_mb
            
        except Exception as e:
            logger.error(f"内存清理失败: {e}")
            return 0
    
    def update_processed_count(self):
        """更新处理计数"""
        self.processed_count += 1
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """获取内存统计信息"""
        try:
            import psutil
            process = psutil.Process()
            current_mb = process.memory_info().rss / 1024 / 1024
            
            avg_memory = sum(self.memory_samples) / len(self.memory_samples) if self.memory_samples else 0
            
            return {
                'current_memory_mb': current_mb,
                'max_memory_mb': self.max_memory_seen,
                'avg_memory_mb': avg_memory,
                'cleanup_count': self.cleanup_count,
                'processed_count': self.processed_count,
                'uptime_hours': (time.time() - self.start_time) / 3600
            }
        except Exception:
            return {}

class DistributedTaskManager:
    """分布式任务管理器 - 多电脑协作核心"""
    
    def __init__(self, db_manager: 'DatabaseManager', computer_name: str):
        self.db_manager = db_manager
        self.computer_name = computer_name
        self.heartbeat_thread = None
        self.shutdown_event = threading.Event()
        self.last_heartbeat = time.time()
        
        if ENABLE_DISTRIBUTED_PROCESSING:
            self._start_heartbeat()
    
    def _start_heartbeat(self):
        """启动心跳机制"""
        def heartbeat_worker():
            while not self.shutdown_event.is_set():
                try:
                    self._send_heartbeat()
                    time.sleep(HEARTBEAT_INTERVAL)
                except Exception as e:
                    logger.error(f"心跳发送失败: {e}")
                    time.sleep(60)  # 错误时等待1分钟
        
        self.heartbeat_thread = threading.Thread(target=heartbeat_worker, daemon=True)
        self.heartbeat_thread.start()
        logger.info(f"💓 启动心跳机制，间隔{HEARTBEAT_INTERVAL}秒")
    
    def _send_heartbeat(self):
        """发送心跳"""
        if not self.db_manager.is_enabled:
            return
            
        try:
            import psutil
            
            # 获取系统状态
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            heartbeat_data = {
                'computer_name': self.computer_name,
                'timestamp': time.time(),
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'status': 'active'
            }
            
            # 这里可以扩展为向数据库发送心跳信息
            self.last_heartbeat = time.time()
            logger.debug(f"💓 心跳发送: CPU {cpu_percent:.1f}%, 内存 {memory.percent:.1f}%")
            
        except Exception as e:
            logger.error(f"心跳数据收集失败: {e}")
    
    def claim_video_task(self, video_path: str) -> bool:
        """声明视频处理任务 - 使用Video_Editing表检查是否已处理"""
        if not ENABLE_DISTRIBUTED_PROCESSING or not self.db_manager.is_enabled:
            return True
        
        try:
            # 简化分布式处理：直接检查Video_Editing表中是否已被处理
            cursor = self.db_manager.connection_pool.cursor()
            
            video_name = os.path.basename(video_path)
            # 检查是否已被其他电脑处理完成
            cursor.execute(f"""
                SELECT computer_name, updated_time FROM `{TABLE_NAME}` 
                WHERE video_name = %s AND status = 1
                ORDER BY updated_time DESC LIMIT 1
            """, (video_name,))
            
            existing_record = cursor.fetchone()
            if existing_record:
                computer_name = existing_record[0]
                updated_time = existing_record[1]
                logger.info(f"🔒 视频已被 {computer_name} 在 {updated_time} 处理完成: {video_name}")
                return False
            
            logger.debug(f"🔐 任务可以处理: {video_name}")
            return True
            
        except Exception as e:
            logger.error(f"任务声明检查失败: {e}")
            return True  # 失败时允许处理，避免阻塞
    
    def release_video_task(self, video_path: str):
        """释放视频处理任务 - 简化版本，不使用额外表"""
        if not ENABLE_DISTRIBUTED_PROCESSING or not self.db_manager.is_enabled:
            return
        
        # 简化版本：不需要额外的锁表，任务完成时会写入Video_Editing表
        logger.debug(f"🔓 任务处理完成: {os.path.basename(video_path)}")
    
    def cleanup_expired_locks(self):
        """清理过期的任务锁 - 简化版本"""
        if not ENABLE_DISTRIBUTED_PROCESSING or not self.db_manager.is_enabled:
            return
        
        # 简化版本：不需要清理额外的锁表
        logger.debug("🧹 分布式处理检查完成（基于Video_Editing表）")
    
    def shutdown(self):
        """关闭分布式任务管理器"""
        self.shutdown_event.set()
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=5)
        logger.info("🛑 分布式任务管理器已关闭")

# ==================== 硬件检测器 ====================
class HardwareDetector:
    """硬件检测和优化类"""
    
    def __init__(self):
        self.cpu_count = multiprocessing.cpu_count()
        self.memory_gb = psutil.virtual_memory().total / (1024**3)
        self.is_i9 = self._detect_i9_processor()
        
    def _detect_i9_processor(self) -> bool:
        """检测是否为i9处理器"""
        try:
            cpu_info = platform.processor().lower()
            return 'i9' in cpu_info
        except Exception:
            return False
    
    def detect_hardware_capabilities(self) -> Dict[str, Any]:
        """检测硬件能力"""
        # 检测GPU编码器
        gpu_info = self._detect_gpu_capabilities()
        
        # 计算最佳并行数
        if self.is_i9:
            max_parallel = min(self.cpu_count - 2, 20)
        else:
            max_parallel = min(self.cpu_count // 2, 8)
        
        # 内存优化
        if self.memory_gb >= 32:
            max_parallel = min(max_parallel, int(self.memory_gb // 1.5))
        elif self.memory_gb >= 16:
            max_parallel = min(max_parallel, 10)
        else:
            max_parallel = min(max_parallel, 6)
        
        return {
            'cpu_cores': self.cpu_count,
            'memory_gb': self.memory_gb,
            'is_i9': self.is_i9,
            'max_parallel': min(max_parallel, MAX_PARALLEL_WORKERS),
            'encoder_type': gpu_info.get('encoder_type', 'software'),
            'encoder': gpu_info.get('encoder', 'libx264'),
            'encoder_options': gpu_info.get('options', {}),
        }
    
    def _detect_gpu_capabilities(self) -> Dict[str, Any]:
        """检测GPU编码能力"""
        try:
            result = subprocess.run(
                [FFMPEG_PATH, '-hide_banner', '-encoders'], 
                capture_output=True, text=True, timeout=10
            )
            
            if result.returncode != 0:
                return self._get_software_config()
            
            encoders = result.stdout.lower()
            
            # NVIDIA检测
            if 'h264_nvenc' in encoders:
                return {
                    'encoder_type': 'nvidia',
                    'encoder': 'h264_nvenc',
                    'options': {'preset': 'p2', 'rc': 'vbr', 'cq': '25'}
                }
            
            # AMD检测
            if 'h264_amf' in encoders:
                return {
                    'encoder_type': 'amd',
                    'encoder': 'h264_amf',
                    'options': {'quality': 'balanced', 'rc': 'vbr_peak_constrained'}
                }
            
            # Intel检测
            if 'h264_qsv' in encoders:
                return {
                    'encoder_type': 'intel',
                    'encoder': 'h264_qsv',
                    'options': {'preset': 'fast', 'global_quality': '25'}
                }
            
            return self._get_software_config()
            
        except Exception as e:
            logger.warning(f"GPU检测失败: {e}")
            return self._get_software_config()
    
    def _get_software_config(self) -> Dict[str, Any]:
        """获取软件编码配置"""
        preset_map = {
            'highest': 'slow',
            'high': 'medium',
            'balanced': 'fast',
            'fast': 'veryfast'
        }
        
        return {
            'encoder_type': 'software',
            'encoder': 'libx264',
            'options': {
                'preset': preset_map.get(QUALITY_MODE, 'fast'),
                'crf': '23',
                'threads': '0'
            }
        }

# ==================== 数据库管理器 ====================
class DatabaseManager:
    """数据库管理器 - 增强连接池和长时间稳定性"""
    
    def __init__(self):
        self.is_enabled = ENABLE_DATABASE
        self.connection_pool = None
        self.connection_lock = threading.RLock()
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.last_connection_check = 0
        self.connection_check_interval = 300  # 5分钟检查一次连接
        
        # 根据配置获取电脑信息
        if AUTO_GET_COMPUTER_INFO:
            self.computer_name = COMPUTER_NAME if COMPUTER_NAME else socket.gethostname()
            self.computer_ip = COMPUTER_IP if COMPUTER_IP else self._get_local_ip()
        else:
            self.computer_name = COMPUTER_NAME
            self.computer_ip = COMPUTER_IP
        
        # 操作员信息
        self.operator = PROCESSOR_NAME if 'PROCESSOR_NAME' in globals() else "综合处理器"
        
        if self.is_enabled:
            self._init_database()
    
    def _get_local_ip(self):
        """获取本地IP地址"""
        try:
            # 连接到远程地址来获取本地IP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"
    
    def _init_database(self):
        """初始化数据库连接池"""
        try:
            # 尝试导入pymysql
            try:
                import pymysql
                from pymysql import pooling
            except ImportError:
                logger.warning("pymysql模块未安装，禁用数据库功能")
                logger.info("要启用数据库功能，请运行: pip install pymysql")
                self.is_enabled = False
                return
            
            # 创建连接池配置
            pool_config = {
                'host': DB_HOST,
                'port': DB_PORT,
                'user': DB_USER,
                'password': DB_PASSWORD,
                'database': DB_NAME,
                'charset': 'utf8mb4',
                'autocommit': True,
                'ping_interval': 300,  # 5分钟ping一次
                'max_connections': 20,
                'max_idle_time': 3600,  # 1小时空闲超时
                'retry_on_error': True
            }
            
            # 使用简单连接（因为pymysql可能没有连接池）
            self.connection_pool = self._create_robust_connection()
            self._create_tables()
            logger.info("✅ 数据库连接池初始化成功")
            
        except Exception as e:
            logger.warning(f"数据库连接失败，禁用数据库功能: {e}")
            self.is_enabled = False
    
    def _create_robust_connection(self):
        """创建健壮的数据库连接"""
        import pymysql
        
        for attempt in range(self.max_reconnect_attempts):
            try:
                connection = pymysql.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    database=DB_NAME,
                    charset='utf8mb4',
                    autocommit=True,
                    connect_timeout=30,
                    read_timeout=30,
                    write_timeout=30
                )
                
                # 测试连接
                connection.ping(reconnect=True)
                self.reconnect_attempts = 0
                logger.info(f"✅ 数据库连接成功 (尝试 {attempt + 1})")
                return connection
                
            except Exception as e:
                self.reconnect_attempts += 1
                logger.warning(f"⚠️ 数据库连接失败 (尝试 {attempt + 1}/{self.max_reconnect_attempts}): {e}")
                
                if attempt < self.max_reconnect_attempts - 1:
                    wait_time = min(2 ** attempt, 30)  # 指数退避，最大30秒
                    time.sleep(wait_time)
        
        raise Exception(f"数据库连接失败，已达到最大重试次数 ({self.max_reconnect_attempts})")
    
    def get_connection(self):
        """获取数据库连接（带健康检查）"""
        if not self.is_enabled:
            return None
        
        with self.connection_lock:
            current_time = time.time()
            
            # 定期检查连接健康状态
            if current_time - self.last_connection_check > self.connection_check_interval:
                try:
                    if self.connection_pool:
                        self.connection_pool.ping(reconnect=True)
                        self.last_connection_check = current_time
                        logger.debug("💓 数据库连接健康检查通过")
                except Exception as e:
                    logger.warning(f"⚠️ 数据库连接检查失败，尝试重连: {e}")
                    self.connection_pool = self._create_robust_connection()
                    self.last_connection_check = current_time
            
            return self.connection_pool
    
    def execute_with_retry(self, query: str, params: tuple = None, max_retries: int = 3):
        """执行SQL语句（带重试机制）"""
        if not self.is_enabled:
            return None
        
        for attempt in range(max_retries):
            try:
                connection = self.get_connection()
                if not connection:
                    return None
                
                with connection.cursor() as cursor:
                    cursor.execute(query, params)
                    if query.strip().upper().startswith('SELECT'):
                        return cursor.fetchall()
                    else:
                        return cursor.rowcount
                        
            except Exception as e:
                logger.warning(f"⚠️ SQL执行失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                
                # 连接错误时重新创建连接
                if "connection" in str(e).lower() or "lost" in str(e).lower():
                    try:
                        self.connection_pool = self._create_robust_connection()
                    except Exception as conn_e:
                        logger.error(f"❌ 重新连接失败: {conn_e}")
                
                if attempt == max_retries - 1:
                    logger.error(f"❌ SQL执行最终失败: {query[:100]}...")
                    raise e
                
                time.sleep(1 * (attempt + 1))  # 递增等待时间
        
        return None
    
    def _create_tables(self):
        """创建数据库表 - 严格使用11个固定字段"""
        if not self.is_enabled:
            return
            
        try:
            cursor = self.connection_pool.cursor()
            
            # 【固定】创建Video_Editing表，严格使用11个字段，不添加任何额外字段
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
                `input_path` VARCHAR(1024) NOT NULL COMMENT '输入路径',
                `output_path` VARCHAR(1024) COMMENT '输出路径',
                `video_name` VARCHAR(255) NOT NULL COMMENT '视频名',
                `pre_processing_size` BIGINT COMMENT '处理前视频大小 (Bytes)',
                `resolution` VARCHAR(50) COMMENT '分辨率',
                `hash_value` VARCHAR(64) NOT NULL COMMENT '哈希值',
                `operator` VARCHAR(100) COMMENT '处理人',
                `computer_name` VARCHAR(100) COMMENT '电脑号',
                `computer_ip` VARCHAR(50) COMMENT '电脑IP',
                `status` TINYINT NOT NULL COMMENT '处理状态 0: 失败, 1: 成功',
                `log_path` VARCHAR(1024) COMMENT '日志路径',
                `created_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `hash_value_unique` (`hash_value`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(create_table_query)
            
            self.connection_pool.commit()
            logger.info("✅ Video_Editing表创建成功 (11个固定字段)")
            
        except Exception as e:
            logger.error(f"创建数据库表失败: {e}")
    
    def is_video_processed(self, video_path: str, file_hash: str) -> Tuple[bool, Optional[Dict]]:
        """检查视频是否已处理 - 使用11个固定字段"""
        if not self.is_enabled:
            return False, None
            
        try:
            cursor = self.connection_pool.cursor()
            cursor.execute(f"""
                SELECT input_path, output_path, computer_name, created_time 
                FROM `{TABLE_NAME}` 
                WHERE hash_value = %s AND status = 1
                ORDER BY created_time DESC LIMIT 1
            """, (file_hash,))
            
            result = cursor.fetchone()
            if result:
                return True, {
                    'input_path': result[0],
                    'output_path': result[1],
                    'computer_name': result[2],
                    'created_time': result[3]
                }
            return False, None
            
        except Exception as e:
            logger.error(f"查询数据库失败: {e}")
            return False, None
    
    def enhanced_duplicate_check(self, video_path: str) -> Tuple[bool, str]:
        """增强的查重检查 - 使用11个固定字段"""
        if not self.is_enabled:
            return False, "数据库未启用"
            
        try:
            # 计算文件哈希
            file_hash = self._calculate_video_sha256(video_path)
            if not file_hash:
                return False, "无法计算文件哈希"
            
            video_name = os.path.basename(video_path)
            
            cursor = self.connection_pool.cursor()
            # 使用hash_value和video_name双重验证（只查询11个固定字段）
            cursor.execute(f"""
                SELECT computer_name, updated_time, output_path
                FROM `{TABLE_NAME}`
                WHERE hash_value = %s AND video_name = %s AND status = 1
                ORDER BY updated_time DESC LIMIT 1
            """, (file_hash, video_name))
            
            result = cursor.fetchone()
            if result:
                computer_name = result[0]
                updated_time = result[1]
                output_path = result[2]
                return True, f"已被 {computer_name} 在 {updated_time} 处理过，输出: {output_path}"
            else:
                return False, "未发现重复"
                
        except Exception as e:
            logger.error(f"查重检查失败: {e}")
            return False, f"查重检查异常: {e}"
    
    def _calculate_video_sha256(self, video_path: str, quick_mode: bool = False) -> str:
        """计算视频文件SHA256 - 参考MC_L脚本"""
        try:
            if not os.path.exists(video_path):
                return ""
                
            import hashlib
            hash_sha256 = hashlib.sha256()
            
            if quick_mode:
                # 快速模式：采样文件头、中间、尾部
                file_size = os.path.getsize(video_path)
                sample_size = min(1024 * 1024, file_size // 10)  # 1MB或文件的1/10
                
                with open(video_path, 'rb') as f:
                    # 文件头
                    hash_sha256.update(f.read(sample_size))
                    
                    # 文件中间
                    if file_size > sample_size * 2:
                        f.seek(file_size // 2)
                        hash_sha256.update(f.read(sample_size))
                    
                    # 文件尾
                    if file_size > sample_size * 3:
                        f.seek(-sample_size, 2)
                        hash_sha256.update(f.read(sample_size))
            else:
                # 完整模式：计算整个文件
                with open(video_path, 'rb') as f:
                    while chunk := f.read(8192):
                        hash_sha256.update(chunk)
            
            return hash_sha256.hexdigest()
            
        except Exception as e:
            logger.error(f"计算SHA256失败 {video_path}: {e}")
            return ""
    
    
    def record_processing_complete(self, video_path: str, output_path: str, 
                                  processing_time: float = 0.0, log_path: str = None) -> bool:
        """记录视频处理完成 - 严格写入11个固定字段"""
        if not self.is_enabled:
            return True
            
        try:
            # 获取真实的视频信息
            video_name = os.path.basename(video_path)
            pre_processing_size = os.path.getsize(video_path) if os.path.exists(video_path) else 0
            
            # 计算真实的哈希值
            hash_value = self._calculate_video_sha256(video_path, quick_mode=False)
            if not hash_value:
                logger.warning(f"无法计算哈希值: {video_name}")
                return False
                
            # 获取真实的视频分辨率
            resolution = None
            try:
                video_info = self._get_real_video_info(video_path)
                if video_info and video_info.get('width') and video_info.get('height'):
                    resolution = f"{video_info['width']}x{video_info['height']}"
            except Exception as e:
                logger.warning(f"获取视频分辨率失败: {e}")
            
            cursor = self.connection_pool.cursor()
            
            # 【固定】严格插入11个字段，不添加任何额外字段
            insert_sql = f"""
            INSERT INTO `{TABLE_NAME}` 
            (input_path, output_path, video_name, pre_processing_size, resolution, 
             hash_value, operator, computer_name, computer_ip, status, log_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s)
            ON DUPLICATE KEY UPDATE
            output_path = VALUES(output_path),
            status = 1,
            updated_time = CURRENT_TIMESTAMP
            """
            
            cursor.execute(insert_sql, (
                video_path,                # input_path
                output_path,               # output_path  
                video_name,                # video_name
                pre_processing_size,       # pre_processing_size
                resolution,                # resolution
                hash_value,                # hash_value
                OPERATOR_NAME,             # operator (使用配置的操作员名称)
                self.computer_name,        # computer_name
                self.computer_ip,          # computer_ip
                log_path                   # log_path
            ))
            
            self.connection_pool.commit()
            logger.info(f"✅ 数据库记录完成 (11个固定字段): {video_name}")
            logger.debug(f"   哈希值: {hash_value}")
            logger.debug(f"   分辨率: {resolution}")
            logger.debug(f"   文件大小: {pre_processing_size} bytes")
            return True
            
        except Exception as e:
            logger.error(f"❌ 数据库记录失败: {e}")
            return False
    
    def _get_real_video_info(self, video_path: str) -> Dict[str, Any]:
        """获取真实的视频信息"""
        try:
            # 使用ffprobe获取准确的视频信息
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams',
                '-select_streams', 'v:0', video_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, 
                                  encoding='utf-8', errors='ignore')
            
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                streams = data.get('streams', [])
                
                if streams:
                    video_stream = streams[0]
                    return {
                        'width': video_stream.get('width', 0),
                        'height': video_stream.get('height', 0),
                        'duration': float(video_stream.get('duration', 0)),
                        'bit_rate': video_stream.get('bit_rate', ''),
                        'codec_name': video_stream.get('codec_name', '')
                    }
            
            return {}
            
        except Exception as e:
            logger.error(f"获取视频信息失败: {e}")
            return {}

# ==================== 智能缓存管理器 ====================
class SmartCacheManager:
    """智能缓存管理器 - 增强缓存后查重功能"""
    
    def __init__(self, db_manager: DatabaseManager = None):
        self.cache_dir = Path(CACHE_DIR)
        self.cache_dir.mkdir(exist_ok=True)
        self.max_cache_size = MAX_CACHE_SIZE_GB * 1024**3  # 转换为字节
        self.cache_records = {}
        self.download_queue = queue.Queue()
        self.download_threads = []
        self.is_enabled = ENABLE_CACHE
        self.db_manager = db_manager  # 用于缓存后查重
        
        # 缓存完成记录
        self.cache_completion_file = self.cache_dir / ".cache_completion.json"
        self.completed_cache_videos = set()
        
        if self.is_enabled:
            self._start_cache_threads()
            self._load_cache_records()
            self._load_cache_completion_record()
    
    def _start_cache_threads(self):
        """启动缓存线程"""
        # 下载线程
        for i in range(2):
            thread = threading.Thread(target=self._download_worker, daemon=True)
            thread.start()
            self.download_threads.append(thread)
        
        # 清理线程
        cleanup_thread = threading.Thread(target=self._cleanup_monitor, daemon=True)
        cleanup_thread.start()
    
    def _load_cache_records(self):
        """加载缓存记录"""
        cache_record_file = self.cache_dir / "cache_records.json"
        if cache_record_file.exists():
            try:
                with open(cache_record_file, 'r', encoding='utf-8') as f:
                    self.cache_records = json.load(f)
                logger.info(f"加载缓存记录: {len(self.cache_records)} 个文件")
            except Exception as e:
                logger.warning(f"加载缓存记录失败: {e}")
                self.cache_records = {}
    
    def _save_cache_records(self):
        """保存缓存记录"""
        cache_record_file = self.cache_dir / "cache_records.json"
        try:
            with open(cache_record_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache_records, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存缓存记录失败: {e}")
    
    def _load_cache_completion_record(self):
        """加载缓存完成记录"""
        try:
            if self.cache_completion_file.exists():
                with open(self.cache_completion_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_cache_videos = set(data.get('completed_videos', []))
                    logger.debug(f"📋 加载缓存完成记录: {len(self.completed_cache_videos)}个视频")
            else:
                self.completed_cache_videos = set()
        except Exception as e:
            logger.warning(f"⚠️ 加载缓存完成记录失败: {e}")
            self.completed_cache_videos = set()
    
    def _save_cache_completion_record(self):
        """保存缓存完成记录"""
        try:
            with open(self.cache_completion_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'completed_videos': list(self.completed_cache_videos),
                    'last_updated': time.time()
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"💾 保存缓存完成记录失败: {e}")
    
    def mark_cache_completed(self, video_path: str):
        """标记视频缓存完成"""
        self.completed_cache_videos.add(video_path)
        self._save_cache_completion_record()
        logger.debug(f"✅ 标记缓存完成: {os.path.basename(video_path)}")
    
    def is_cache_completed(self, video_path: str) -> bool:
        """检查视频是否已完成缓存"""
        return video_path in self.completed_cache_videos
    
    def get_cached_path(self, video_path: str) -> Optional[str]:
        """获取缓存路径"""
        if not self.is_enabled:
            return None
        
        video_name = os.path.basename(video_path)
        cache_path = self.cache_dir / video_name
        
        if cache_path.exists() and self._verify_cache_integrity(str(cache_path), video_path):
            # 更新访问时间
            self.cache_records[video_path] = {
                'cache_path': str(cache_path),
                'last_access': time.time(),
                'file_size': cache_path.stat().st_size
            }
            self._save_cache_records()
            return str(cache_path)
        
        return None
    
    def _verify_cache_integrity(self, cache_path: str, original_path: str) -> bool:
        """验证缓存完整性"""
        try:
            if not os.path.exists(cache_path):
                return False
            
            # 比较文件大小
            cache_size = os.path.getsize(cache_path)
            if os.path.exists(original_path):
                original_size = os.path.getsize(original_path)
                if abs(cache_size - original_size) > 1024:  # 允许1KB差异
                    return False
            
            # 验证视频可读性
            probe_cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format=duration', 
                        '-of', 'default=noprint_wrappers=1:nokey=1', cache_path]
            result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=10, encoding='utf-8', errors='ignore')
            
            return result.returncode == 0 and result.stdout.strip()
            
        except Exception as e:
            logger.warning(f"缓存完整性验证失败: {e}")
            return False
    
    def start_async_download(self, video_path: str, priority: int = 0) -> bool:
        """开始异步下载"""
        if not self.is_enabled:
            return False
        
        if self.get_cached_path(video_path):
            return True  # 已缓存
        
        try:
            self.download_queue.put((priority, video_path))
            return True
        except Exception as e:
            logger.error(f"添加下载任务失败: {e}")
            return False
    
    def _download_worker(self):
        """下载工作线程"""
        while True:
            try:
                priority, video_path = self.download_queue.get(timeout=1)
                self._download_video_to_cache(video_path)
                self.download_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"下载工作线程异常: {e}")
    
    def _download_video_to_cache(self, video_path: str) -> bool:
        """下载视频到缓存 - 增强缓存后查重功能"""
        try:
            if not os.path.exists(video_path):
                logger.error(f"源文件不存在: {video_path}")
                return False
            
            video_name = os.path.basename(video_path)
            cache_path = self.cache_dir / video_name
            
            # 检查缓存空间
            if not self._ensure_cache_space(os.path.getsize(video_path)):
                logger.warning(f"缓存空间不足，跳过: {video_name}")
                return False
            
            # 复制文件
            logger.info(f"🚀 开始缓存: {video_name}")
            start_time = time.time()
            
            shutil.copy2(video_path, cache_path)
            
            download_time = time.time() - start_time
            
            # 记录缓存信息
            self.cache_records[video_path] = {
                'cache_path': str(cache_path),
                'last_access': time.time(),
                'download_time': download_time,
                'file_size': cache_path.stat().st_size
            }
            self._save_cache_records()
            
            # 🎯 关键功能：缓存完成后立即执行查重检查
            self.mark_cache_completed(video_path)
            
            # 执行缓存后查重（参考MC_L脚本逻辑）
            if self.db_manager and self.db_manager.is_enabled:
                try:
                    is_duplicate, message = self.db_manager.enhanced_duplicate_check(video_path)
                    if is_duplicate:
                        logger.info(f"🔍 缓存后发现重复: {video_name} - {message}")
                        
                        # 清理缓存文件（节省磁盘空间）
                        try:
                            if cache_path.exists():
                                cache_path.unlink()
                                logger.debug(f"🗑️ 清理重复文件缓存: {video_name}")
                                
                                # 从缓存记录中移除
                                if video_path in self.cache_records:
                                    del self.cache_records[video_path]
                                    self._save_cache_records()
                        except Exception as e:
                            logger.warning(f"清理重复缓存失败: {e}")
                        
                        return False  # 返回False表示应跳过处理
                    else:
                        logger.debug(f"📝 缓存后查重检查通过: {video_name} - {message}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ 缓存后查重检查失败: {e}")
                    # 查重失败不影响缓存，继续处理
            
            logger.info(f"缓存完成: {video_name} ({download_time:.1f}s)")
            return True
            
        except Exception as e:
            logger.error(f"缓存失败 {video_path}: {e}")
            return False
    
    def _ensure_cache_space(self, required_space: int) -> bool:
        """确保缓存空间充足"""
        current_size = self._get_cache_size()
        
        while current_size + required_space > self.max_cache_size:
            if not self._cleanup_oldest_cache():
                return False
            current_size = self._get_cache_size()
        
        return True
    
    def _get_cache_size(self) -> int:
        """获取当前缓存大小"""
        total_size = 0
        try:
            for cache_file in self.cache_dir.iterdir():
                if cache_file.is_file():
                    total_size += cache_file.stat().st_size
        except Exception as e:
            logger.warning(f"计算缓存大小失败: {e}")
        return total_size
    
    def _cleanup_oldest_cache(self) -> bool:
        """清理最旧的缓存"""
        if not self.cache_records:
            return False
        
        # 按最后访问时间排序
        sorted_records = sorted(
            self.cache_records.items(),
            key=lambda x: x[1].get('last_access', 0)
        )
        
        for video_path, record in sorted_records:
            cache_path = record['cache_path']
            try:
                if os.path.exists(cache_path):
                    os.remove(cache_path)
                    logger.info(f"清理缓存: {os.path.basename(cache_path)}")
                
                del self.cache_records[video_path]
                self._save_cache_records()
                return True
                
            except Exception as e:
                logger.warning(f"清理缓存失败: {e}")
        
        return False
    
    def _cleanup_monitor(self):
        """缓存清理监控线程"""
        while True:
            try:
                time.sleep(300)  # 每5分钟检查一次
                
                current_size = self._get_cache_size()
                if current_size > self.max_cache_size * CACHE_CLEANUP_THRESHOLD:
                    logger.info("开始自动清理缓存...")
                    self._cleanup_old_cache()
                
            except Exception as e:
                logger.error(f"缓存清理监控异常: {e}")
    
    def _cleanup_old_cache(self):
        """清理旧缓存"""
        # 清理超过7天未访问的缓存
        cutoff_time = time.time() - 7 * 24 * 3600
        
        to_remove = []
        for video_path, record in self.cache_records.items():
            if record.get('last_access', 0) < cutoff_time:
                to_remove.append(video_path)
        
        for video_path in to_remove:
            record = self.cache_records[video_path]
            cache_path = record['cache_path']
            try:
                if os.path.exists(cache_path):
                    os.remove(cache_path)
                del self.cache_records[video_path]
            except Exception as e:
                logger.warning(f"清理旧缓存失败: {e}")
        
        if to_remove:
            self._save_cache_records()
            logger.info(f"清理了 {len(to_remove)} 个旧缓存文件")
    
    def wait_for_cache(self, video_path: str, timeout: float = 300) -> Optional[str]:
        """等待缓存完成"""
        if not self.is_enabled:
            return None
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            cached_path = self.get_cached_path(video_path)
            if cached_path:
                return cached_path
            time.sleep(1)
        
        return None
    
    def preload_videos(self, video_paths: List[str], current_index: int):
        """预加载视频"""
        if not self.is_enabled or PRELOAD_COUNT <= 0:
            return
        
        # 预加载接下来的几个视频
        for i in range(1, PRELOAD_COUNT + 1):
            next_index = current_index + i
            if next_index < len(video_paths):
                self.start_async_download(video_paths[next_index], priority=-i)

# ==================== ROI选择器 ====================
class ROISelector:
    """ROI区域选择器"""
    
    def __init__(self):
        self.gui_available = self._init_opencv_gui()
    
    def _init_opencv_gui(self) -> bool:
        """初始化OpenCV GUI"""
        try:
            # 检查OpenCV版本
            current_version = cv2.__version__
            logger.info(f"OpenCV版本: {current_version}")
            
            # 测试GUI功能
            test_img = np.zeros((100, 100, 3), dtype=np.uint8)
            cv2.namedWindow("test_window", cv2.WINDOW_NORMAL)
            cv2.imshow("test_window", test_img)
            cv2.waitKey(1)
            cv2.destroyWindow("test_window")
            
            logger.info("OpenCV GUI初始化成功")
            return True
            
        except Exception as e:
            logger.warning(f"OpenCV GUI初始化失败: {e}")
            return False
    
    def extract_preview_frame(self, video_path: str) -> Optional[np.ndarray]:
        """提取预览帧"""
        try:
            # 获取视频时长
            probe_cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format=duration',
                        '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
            result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=10, encoding='utf-8', errors='ignore')
            
            if result.returncode != 0:
                return None
            
            duration = float(result.stdout.strip())
            seek_time = duration / 2  # 从中间提取帧
            
            # 提取帧
            temp_frame_path = Path("temp_preview_frame.jpg")
            extract_cmd = [
                FFMPEG_PATH, '-ss', str(seek_time), '-i', video_path,
                '-vframes', '1', '-q:v', '2', str(temp_frame_path), '-y'
            ]
            
            result = subprocess.run(extract_cmd, capture_output=True, timeout=30, encoding='utf-8', errors='ignore')
            
            if result.returncode == 0 and temp_frame_path.exists():
                frame = cv2.imread(str(temp_frame_path))
                temp_frame_path.unlink()  # 删除临时文件
                return frame
            
            return None
            
        except Exception as e:
            logger.error(f"提取预览帧失败: {e}")
            return None
    
    def select_roi_for_video(self, video_path: str) -> Optional[Tuple[int, int, int, int, int, int]]:
        """为视频选择ROI区域，返回 (x, y, w, h, base_width, base_height)"""
        frame = self.extract_preview_frame(video_path)
        if frame is None:
            logger.error(f"无法提取预览帧: {video_path}")
            return None
        
        height, width = frame.shape[:2]
        print(f"📐 基准视频尺寸: {width}x{height}")
        print("💡 提示: 在此分辨率下选择的裁剪框将自动适配2K/4K等不同分辨率视频")
        
        if self.gui_available:
            roi = self._gui_select_roi(frame)
        else:
            roi = self._fallback_roi_input(frame)
        
        if roi is None:
            return None
        
        # 返回ROI + 基准分辨率，用于后续比例缩放
        return roi + (width, height)
    
    def _gui_select_roi(self, frame: np.ndarray) -> Optional[Tuple[int, int, int, int]]:
        """GUI方式选择ROI"""
        try:
            # 调整显示尺寸 - 使用更大的窗口
            height, width = frame.shape[:2]
            
            # 设置目标显示高度为1200像素（更大的窗口）
            target_height = 1200
            if height > target_height:
                scale = target_height / height
                new_width = int(width * scale)
                display_frame = cv2.resize(frame, (new_width, target_height))
            elif height < 600:  # 如果原始图像太小，放大显示
                scale = 600 / height
                new_width = int(width * scale)
                display_frame = cv2.resize(frame, (new_width, 600))
            else:
                display_frame = frame.copy()
                scale = 1.0
            
            # 添加提示文字和分辨率信息
            info_text = f"Video: {width}x{height} (Base Resolution for Scaling)"
            cv2.putText(display_frame, info_text, (20, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 255), 2)
            cv2.putText(display_frame, "Select ROI area, then press SPACE or ENTER", 
                       (20, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 255, 255), 2)
            cv2.putText(display_frame, "This selection will auto-scale for 2K/4K videos", 
                       (20, 90), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
            
            # 选择ROI - 设置窗口为可调整大小
            window_name = "Select ROI Area (Drag to select, SPACE/ENTER to confirm, ESC to cancel)"
            cv2.namedWindow(window_name, cv2.WINDOW_NORMAL | cv2.WINDOW_KEEPRATIO)
            
            # 设置窗口初始大小（更大）
            cv2.resizeWindow(window_name, display_frame.shape[1], display_frame.shape[0])
            
            # 显示图像并等待用户选择
            cv2.imshow(window_name, display_frame)
            cv2.waitKey(100)  # 等待窗口完全显示
            
            roi = cv2.selectROI(window_name, display_frame, fromCenter=False, showCrosshair=True)
            cv2.destroyAllWindows()
            
            if roi[2] == 0 or roi[3] == 0:
                logger.warning("未选择有效ROI区域")
                return None
            
            # 转换为原始尺寸坐标
            x = int(roi[0] / scale)
            y = int(roi[1] / scale)
            w = int(roi[2] / scale)
            h = int(roi[3] / scale)
            
            # 用户选择的原始ROI
            original_roi = (x, y, w, h)
            print(f'您选择的裁剪框 (原始尺寸): {original_roi}')
            
            # 调整为16:9比例
            final_roi = self._adjust_roi_to_16_9((x, y, w, h), width, height)
            print(f'脚本计算出的最终16:9裁剪参数: {final_roi}')
            
            # 显示最终裁剪框预览
            self._show_roi_preview(frame, original_roi, final_roi, scale)
            
            return final_roi
            
        except Exception as e:
            logger.error(f"GUI选择ROI失败: {e}")
            return None
    
    def _fallback_roi_input(self, frame: np.ndarray) -> Optional[Tuple[int, int, int, int]]:
        """备用命令行输入ROI"""
        height, width = frame.shape[:2]
        
        print(f"📐 视频尺寸: {width}x{height} (基准分辨率)")
        print("💡 提示: 选择的裁剪框将自动适配2K/4K等不同分辨率视频")
        print("请输入ROI坐标 (x y w h)，以空格分隔:")
        
        while True:
            try:
                user_input = input().strip()
                coords = list(map(int, user_input.split()))
                
                if len(coords) != 4:
                    print("请输入4个数字: x y w h")
                    continue
                
                x, y, w, h = coords
                
                # 验证坐标有效性
                if (x >= 0 and y >= 0 and w > 0 and h > 0 and 
                    x + w <= width and y + h <= height):
                    
                    original_roi = (x, y, w, h)
                    print(f'您输入的裁剪框 (原始尺寸): {original_roi}')
                    
                    # 调整为16:9比例
                    final_roi = self._adjust_roi_to_16_9((x, y, w, h), width, height)
                    print(f'脚本计算出的最终16:9裁剪参数: {final_roi}')
                    
                    # 显示预览（如果可能的话）
                    try:
                        scale = 800 / height if height > 0 else 1
                        self._show_roi_preview(frame, original_roi, final_roi, scale)
                    except Exception as e:
                        print(f"⚠️ 无法显示预览: {e}")
                    
                    return final_roi
                else:
                    print("坐标超出视频范围，请重新输入")
                    
            except ValueError:
                print("输入格式错误，请输入4个整数")
            except KeyboardInterrupt:
                return None
    
    def _adjust_roi_to_16_9(self, roi: Tuple[int, int, int, int], 
                           video_width: int, video_height: int) -> Tuple[int, int, int, int]:
        """调整ROI为16:9比例"""
        x, y, w, h = roi
        target_aspect = 16 / 9
        current_aspect = w / h if h > 0 else target_aspect
        
        # 在选择区域内找到最大的16:9矩形
        if current_aspect > target_aspect:
            # 当前区域更宽，以高度为准
            new_h = h
            new_w = int(h * target_aspect)
            new_x = x + (w - new_w) // 2
            new_y = y
        else:
            # 当前区域更高，以宽度为准
            new_w = w
            new_h = int(w / target_aspect)
            new_x = x
            new_y = y + (h - new_h) // 2
        
        # 确保不超出视频边界
        new_x = max(0, min(new_x, video_width - new_w))
        new_y = max(0, min(new_y, video_height - new_h))
        
        if new_x + new_w > video_width:
            new_w = video_width - new_x
            new_h = int(new_w / target_aspect)
        
        if new_y + new_h > video_height:
            new_h = video_height - new_y
            new_w = int(new_h * target_aspect)
        
        logger.info(f"ROI调整: {roi} -> ({new_x}, {new_y}, {new_w}, {new_h})")
        return (new_x, new_y, new_w, new_h)
    
    def _show_roi_preview(self, frame: np.ndarray, original_roi: Tuple[int, int, int, int], 
                         final_roi: Tuple[int, int, int, int], scale: float):
        """显示ROI预览对比
        
        Args:
            frame: 原始视频帧
            original_roi: 用户选择的原始ROI (x, y, w, h)
            final_roi: 调整后的16:9 ROI (x, y, w, h)
            scale: 显示缩放比例
        """
        try:
            # 创建预览图像
            preview_image = frame.copy()
            
            # 画出用户选择的框 (红色)
            cv2.rectangle(preview_image, 
                         (original_roi[0], original_roi[1]),
                         (original_roi[0] + original_roi[2], original_roi[1] + original_roi[3]), 
                         (0, 0, 255), 3)
            cv2.putText(preview_image, 'Your Selection', 
                       (original_roi[0], original_roi[1] - 15), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 255), 2)
            
            # 画出脚本计算出的16:9框 (绿色)
            cv2.rectangle(preview_image, 
                         (final_roi[0], final_roi[1]),
                         (final_roi[0] + final_roi[2], final_roi[1] + final_roi[3]), 
                         (0, 255, 0), 3)
            cv2.putText(preview_image, 'Final 16:9 Crop', 
                       (final_roi[0], final_roi[1] - 15), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)
            
            # 缩放以便显示
            height, width = preview_image.shape[:2]
            display_height = 800
            display_scale = display_height / height if height > 0 else 1
            display_width = int(width * display_scale)
            display_frame = cv2.resize(preview_image, (display_width, display_height))
            
            # 添加说明文字
            cv2.putText(display_frame, "Press any key to start processing...", 
                       (20, display_height - 30), 
                       cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 0), 2)
            
            # 添加比例信息
            aspect_ratio = final_roi[2] / final_roi[3] if final_roi[3] > 0 else 0
            info_text = f"16:9 Ratio: {aspect_ratio:.3f}, Size: {final_roi[2]}x{final_roi[3]}"
            cv2.putText(display_frame, info_text, 
                       (20, 40), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
            
            # 显示预览窗口
            window_name = "Final Crop Preview (Red: Your Selection, Green: 16:9 Adjusted)"
            cv2.namedWindow(window_name, cv2.WINDOW_NORMAL | cv2.WINDOW_KEEPRATIO)
            cv2.resizeWindow(window_name, display_width, display_height)
            cv2.imshow(window_name, display_frame)
            
            print("\n🎯 最终裁剪框预览:")
            print(f"   红色框: 您的选择 {original_roi}")
            print(f"   绿色框: 16:9调整后 {final_roi}")
            print(f"   宽高比: {aspect_ratio:.3f} (目标: 1.778)")
            print("   按任意键开始处理...")
            
            cv2.waitKey(0)
            cv2.destroyAllWindows()
            
        except Exception as e:
            logger.warning(f"显示ROI预览失败: {e}")
            print("⚠️ 无法显示预览，直接开始处理...")
    
    def scale_roi_for_resolution(self, base_roi: Tuple[int, int, int, int, int, int], 
                                target_width: int, target_height: int) -> Tuple[int, int, int, int]:
        """根据目标分辨率缩放ROI
        
        Args:
            base_roi: (x, y, w, h, base_width, base_height) 基准ROI和基准分辨率
            target_width: 目标视频宽度
            target_height: 目标视频高度
            
        Returns:
            (x, y, w, h) 缩放后的ROI坐标
        """
        x, y, w, h, base_width, base_height = base_roi
        
        # 计算缩放比例
        width_scale = target_width / base_width
        height_scale = target_height / base_height
        
        # 按比例缩放ROI坐标
        scaled_x = int(x * width_scale)
        scaled_y = int(y * height_scale)
        scaled_w = int(w * width_scale)
        scaled_h = int(h * height_scale)
        
        # 确保缩放后的ROI不超出目标视频边界
        scaled_x = max(0, min(scaled_x, target_width - scaled_w))
        scaled_y = max(0, min(scaled_y, target_height - scaled_h))
        
        if scaled_x + scaled_w > target_width:
            scaled_w = target_width - scaled_x
        if scaled_y + scaled_h > target_height:
            scaled_h = target_height - scaled_y
        
        logger.info(f"ROI缩放: 基准{base_width}x{base_height} -> 目标{target_width}x{target_height}")
        logger.info(f"  原始ROI: ({x}, {y}, {w}, {h})")
        logger.info(f"  缩放ROI: ({scaled_x}, {scaled_y}, {scaled_w}, {scaled_h})")
        logger.info(f"  缩放比例: {width_scale:.3f}x{height_scale:.3f}")
        
        return (scaled_x, scaled_y, scaled_w, scaled_h)

# ==================== 进度管理器 ====================
class ProgressManager:
    """统一进度管理器"""
    
    def __init__(self, computer_name: str, db_manager: DatabaseManager):
        self.computer_name = computer_name
        self.db_manager = db_manager
        self.progress_file = Path(PROGRESS_FOLDER) / f"progress_{computer_name}.json"
        self.progress_data = self._load_progress()
        
        # 确保进度文件夹存在
        self.progress_file.parent.mkdir(exist_ok=True)
    
    def _load_progress(self) -> Dict[str, Any]:
        """加载进度数据"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logger.info(f"加载进度记录: {len(data.get('completed', []))} 个已完成")
                return data
            except Exception as e:
                logger.warning(f"加载进度文件失败: {e}")
        
        return {
            'completed': [],
            'processing': [],
            'failed': [],
            'roi_settings': None,
            'start_time': None
        }
    
    def save_progress(self):
        """保存进度数据"""
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存进度文件失败: {e}")
    
    def is_completed(self, video_path: str) -> bool:
        """检查视频是否已完成"""
        video_name = os.path.basename(video_path)
        
        # 检查本地进度记录
        for record in self.progress_data['completed']:
            if isinstance(record, dict):
                if record.get('name') == video_name:
                    return True
            elif record == video_name:
                return True
        
        return False
    
    def mark_completed(self, video_path: str, output_path: str = None, processing_time: float = 0.0):
        """标记为已完成"""
        video_name = os.path.basename(video_path)
        
        # 添加到已完成列表
        record = {
            'name': video_name,
            'path': video_path,
            'output_path': output_path,
            'processing_time': processing_time,
            'completed_time': datetime.now().isoformat(),
            'computer': self.computer_name
        }
        
        self.progress_data['completed'].append(record)
        
        # 从处理中移除
        self.progress_data['processing'] = [
            p for p in self.progress_data['processing'] if p != video_name
        ]
        
        self.save_progress()
        logger.info(f"标记完成: {video_name}")
    
    def mark_processing(self, video_path: str):
        """标记为处理中"""
        video_name = os.path.basename(video_path)
        if video_name not in self.progress_data['processing']:
            self.progress_data['processing'].append(video_name)
            self.save_progress()
    
    def mark_failed(self, video_path: str, error_msg: str = ""):
        """标记为失败"""
        video_name = os.path.basename(video_path)
        
        self.progress_data['failed'].append({
            'name': video_name,
            'error': error_msg,
            'time': datetime.now().isoformat()
        })
        
        # 从处理中移除
        self.progress_data['processing'] = [
            p for p in self.progress_data['processing'] if p != video_name
        ]
        
        self.save_progress()
        logger.error(f"标记失败: {video_name} - {error_msg}")
    
    def get_statistics(self) -> Dict[str, int]:
        """获取统计信息"""
        return {
            'completed': len(self.progress_data['completed']),
            'processing': len(self.progress_data['processing']),
            'failed': len(self.progress_data['failed'])
        }
    
    def set_roi_settings(self, roi: Tuple[int, int, int, int, int, int]):
        """保存ROI设置（包含基准分辨率）"""
        self.progress_data['roi_settings'] = roi
        self.save_progress()
        x, y, w, h, base_width, base_height = roi
        logger.info(f"保存ROI设置: 区域({x}, {y}, {w}, {h}), 基准分辨率{base_width}x{base_height}")
    
    def get_roi_settings(self) -> Optional[Tuple[int, int, int, int, int, int]]:
        """获取ROI设置（包含基准分辨率）"""
        saved_roi = self.progress_data.get('roi_settings')
        if saved_roi and len(saved_roi) == 6:
            return saved_roi
        elif saved_roi and len(saved_roi) == 4:
            # 兼容旧格式，假设基准分辨率是1080p
            x, y, w, h = saved_roi
            logger.warning("发现旧格式ROI设置，假设基准分辨率为1920x1080")
            return (x, y, w, h, 1920, 1080)
        return None

# ==================== 视频处理器 ====================
class VideoProcessor:
    """统一视频处理器"""
    
    def __init__(self, hardware_info: Dict[str, Any], progress_manager: ProgressManager):
        self.hardware_info = hardware_info
        self.progress_manager = progress_manager
        self.temp_dir = Path("temp_processing")
        self.temp_dir.mkdir(exist_ok=True)
    
    def calculate_file_hash(self, file_path: str) -> str:
        """计算文件哈希值"""
        hash_sha256 = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                # 读取文件开头、中间、结尾的样本
                file_size = os.path.getsize(file_path)
                chunk_size = 1024 * 1024  # 1MB
                
                # 开头
                chunk = f.read(min(chunk_size, file_size))
                hash_sha256.update(chunk)
                
                # 中间
                if file_size > chunk_size * 2:
                    f.seek(file_size // 2)
                    chunk = f.read(min(chunk_size, file_size - file_size // 2))
                    hash_sha256.update(chunk)
                
                # 结尾
                if file_size > chunk_size:
                    f.seek(-min(chunk_size, file_size), 2)
                    chunk = f.read()
                    hash_sha256.update(chunk)
                    
        except Exception as e:
            logger.error(f"计算文件哈希失败: {e}")
            return ""
        
        return hash_sha256.hexdigest()
    
    def get_video_info(self, video_path: str) -> Dict[str, Any]:
        """获取视频信息"""
        try:
            probe_cmd = [
                FFPROBE_PATH, '-v', 'quiet', '-print_format', 'json',
                '-show_format', '-show_streams', video_path
            ]
            
            result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=30, encoding='utf-8', errors='ignore')
            
            if result.returncode != 0:
                return {}
            
            data = json.loads(result.stdout)
            video_stream = None
            
            # 查找视频流
            for stream in data.get('streams', []):
                if stream.get('codec_type') == 'video':
                    video_stream = stream
                    break
            
            if not video_stream:
                return {}
            
            info = {
                'width': int(video_stream.get('width', 0)),
                'height': int(video_stream.get('height', 0)),
                'duration': float(video_stream.get('duration', 0)),
                'fps': eval(video_stream.get('r_frame_rate', '25/1')),
                'codec': video_stream.get('codec_name', 'unknown'),
                'file_size': os.path.getsize(video_path)
            }
            
            # 从format获取总时长
            format_info = data.get('format', {})
            if 'duration' in format_info:
                info['duration'] = float(format_info['duration'])
            
            return info
            
        except Exception as e:
            logger.error(f"获取视频信息失败: {e}")
            return {}
    
    def should_skip_low_resolution(self, video_path: str) -> Tuple[bool, str]:
        """检查是否应跳过低分辨率视频"""
        if not SKIP_LOW_RESOLUTION:
            return False, ""
        
        video_info = self.get_video_info(video_path)
        width = video_info.get('width', 0)
        height = video_info.get('height', 0)
        
        if width == 0 or height == 0:
            return False, "无法获取分辨率"
        
        if width < MIN_RESOLUTION_WIDTH:
            reason = f"分辨率({width}x{height})低于最小要求({MIN_RESOLUTION_WIDTH}px宽度)"
            return True, reason
        
        return False, ""
    
    def build_ffmpeg_command(self, input_file: str, output_file: str, 
                           mode: ProcessingMode, roi: Optional[Tuple[int, int, int, int]] = None,
                           cut_head: float = 0, cut_tail: float = 0,
                           segment_duration: float = 0) -> List[str]:
        """构建FFmpeg命令"""
        cmd = [FFMPEG_PATH, '-y', '-nostdin']
        
        # 输入参数
        if cut_head > 0:
            cmd.extend(['-ss', str(cut_head)])
        
        cmd.extend(['-i', input_file])
        
        # 视频处理参数
        video_filters = []
        
        if mode in [ProcessingMode.CROP, ProcessingMode.BOTH] and roi:
            x, y, w, h = roi
            crop_filter = f"crop={w}:{h}:{x}:{y}"
            scale_filter = f"scale={TARGET_RESOLUTION[0]}:{TARGET_RESOLUTION[1]}:force_original_aspect_ratio=disable"
            video_filters.extend([crop_filter, scale_filter])
        
        if video_filters:
            cmd.extend(['-vf', ','.join(video_filters)])
        
        # 编码器设置
        cmd.extend(['-c:v', self.hardware_info['encoder']])
        
        # 编码器选项
        for key, value in self.hardware_info['encoder_options'].items():
            cmd.extend([f'-{key}', str(value)])
        
        # 音频设置
        cmd.extend(['-c:a', 'aac', '-b:a', '192k'])
        
        # 时长限制
        if cut_tail > 0:
            video_info = self.get_video_info(input_file)
            duration = video_info.get('duration', 0)
            if duration > 0:
                end_time = duration - cut_tail
                cmd.extend(['-t', str(end_time - cut_head)])
        elif segment_duration > 0:
            cmd.extend(['-t', str(segment_duration)])
        
        # 输出设置
        cmd.extend([
            '-movflags', '+faststart',
            '-map_metadata', '-1',
            '-avoid_negative_ts', 'make_zero',
            output_file
        ])
        
        return cmd
    
    def run_ffmpeg_with_progress(self, cmd: List[str], expected_duration: float, 
                                video_name: str) -> bool:
        """运行FFmpeg并显示进度"""
        try:
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, encoding='utf-8', bufsize=1
            )
            
            with tqdm(total=100, desc=f"处理: {video_name[:30]}", 
                     unit='%', leave=False) as pbar:
                
                last_progress = 0
                
                while process.poll() is None:
                    line = process.stderr.readline()
                    if line:
                        progress_info = self._parse_ffmpeg_progress(line)
                        if 'time' in progress_info:
                            current_time = progress_info['time']
                            progress = min(95, (current_time / expected_duration) * 100)
                            
                            if progress > last_progress:
                                pbar.update(progress - last_progress)
                                last_progress = progress
                
                # 完成最后5%
                if last_progress < 100:
                    pbar.update(100 - last_progress)
            
            if process.returncode == 0:
                logger.info(f"FFmpeg处理成功: {video_name}")
                return True
            else:
                stderr_output = process.stderr.read()
                logger.error(f"FFmpeg处理失败: {video_name} - {stderr_output}")
                return False
                
        except Exception as e:
            logger.error(f"运行FFmpeg异常: {e}")
            return False
    
    def _parse_ffmpeg_progress(self, line: str) -> Dict[str, float]:
        """解析FFmpeg进度输出"""
        info = {}
        patterns = {
            'frame': r'frame=\s*(\d+)',
            'fps': r'fps=\s*([\d\.]+)',
            'time': r'time=\s*(\d+):(\d+):([\d\.]+)',
            'speed': r'speed=\s*([\d\.]+)x'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, line)
            if match:
                if key == 'time':
                    hours, minutes, seconds = match.groups()
                    info[key] = float(hours) * 3600 + float(minutes) * 60 + float(seconds)
                else:
                    info[key] = float(match.group(1))
        
        return info
    
    def process_video_crop(self, input_path: str, output_path: str, 
                          base_roi: Tuple[int, int, int, int, int, int], 
                          roi_selector: 'ROISelector' = None) -> bool:
        """处理视频裁剪，自动根据视频分辨率缩放ROI"""
        try:
            video_info = self.get_video_info(input_path)
            duration = video_info.get('duration', 0)
            video_width = video_info.get('width', 0)
            video_height = video_info.get('height', 0)
            
            if duration <= 0:
                logger.error(f"无法获取视频时长: {input_path}")
                return False
            
            if video_width <= 0 or video_height <= 0:
                logger.error(f"无法获取视频分辨率: {input_path}")
                return False
            
            # 根据实际视频分辨率缩放ROI
            if roi_selector:
                actual_roi = roi_selector.scale_roi_for_resolution(base_roi, video_width, video_height)
            else:
                # 兼容性处理：如果没有roi_selector，使用基础ROI（前4个值）
                actual_roi = base_roi[:4]
            
            x, y, w, h, base_width, base_height = base_roi
            print(f"📐 处理视频: {os.path.basename(input_path)} ({video_width}x{video_height})")
            print(f"   基准ROI: ({x}, {y}, {w}, {h}) @ {base_width}x{base_height}")
            print(f"   实际ROI: {actual_roi}")
            
            cmd = self.build_ffmpeg_command(
                input_path, output_path, ProcessingMode.CROP, roi=actual_roi
            )
            
            logger.info(f"开始裁剪处理: {os.path.basename(input_path)}")
            return self.run_ffmpeg_with_progress(cmd, duration, os.path.basename(input_path))
            
        except Exception as e:
            logger.error(f"视频裁剪失败: {e}")
            return False
    
    def process_video_trim(self, input_path: str, output_path: str,
                          cut_head: float = 0, cut_tail: float = 0,
                          segment_duration: float = 0) -> bool:
        """处理视频切头尾"""
        try:
            video_info = self.get_video_info(input_path)
            original_duration = video_info.get('duration', 0)
            
            if original_duration <= 0:
                logger.error(f"无法获取视频时长: {input_path}")
                return False
            
            # 如果需要分段处理
            if segment_duration > 0 and original_duration > segment_duration:
                return self._process_video_segments(
                    input_path, output_path, cut_head, cut_tail, segment_duration
                )
            
            # 单段处理
            effective_duration = original_duration - cut_head - cut_tail
            if effective_duration <= 0:
                logger.error(f"切头尾后视频时长无效: {effective_duration}")
                return False
            
            cmd = self.build_ffmpeg_command(
                input_path, output_path, ProcessingMode.TRIM,
                cut_head=cut_head, cut_tail=cut_tail
            )
            
            logger.info(f"开始切头尾处理: {os.path.basename(input_path)}")
            return self.run_ffmpeg_with_progress(cmd, effective_duration, os.path.basename(input_path))
            
        except Exception as e:
            logger.error(f"视频切头尾失败: {e}")
            return False
    
    def _process_video_segments(self, input_path: str, output_path: str,
                               cut_head: float, cut_tail: float, segment_duration: float) -> bool:
        """分段处理视频"""
        try:
            video_info = self.get_video_info(input_path)
            total_duration = video_info.get('duration', 0) - cut_head - cut_tail
            
            segments = []
            current_start = cut_head
            segment_index = 0
            
            # 生成分段
            while current_start < total_duration + cut_head:
                segment_end = min(current_start + segment_duration, total_duration + cut_head)
                
                if segment_end - current_start < 60:  # 最后一段少于1分钟，合并到前一段
                    if segments:
                        # 扩展最后一段
                        last_segment = segments[-1]
                        segments[-1] = (last_segment[0], segment_end, last_segment[2])
                    break
                
                base_name = os.path.splitext(os.path.basename(output_path))[0]
                segment_path = os.path.join(
                    os.path.dirname(output_path),
                    f"{base_name}_part{segment_index + 1:03d}.mp4"
                )
                
                segments.append((current_start, segment_end, segment_path))
                current_start = segment_end
                segment_index += 1
            
            # 处理每个分段
            for start_time, end_time, segment_path in segments:
                duration = end_time - start_time
                
                cmd = [FFMPEG_PATH, '-y', '-nostdin']
                cmd.extend(['-ss', str(start_time)])
                cmd.extend(['-i', input_path])
                cmd.extend(['-t', str(duration)])
                cmd.extend(['-c:v', self.hardware_info['encoder']])
                
                # 编码器选项
                for key, value in self.hardware_info['encoder_options'].items():
                    cmd.extend([f'-{key}', str(value)])
                
                cmd.extend(['-c:a', 'aac', '-b:a', '192k'])
                cmd.extend(['-movflags', '+faststart'])
                cmd.append(segment_path)
                
                logger.info(f"处理分段 {len(segments)}: {os.path.basename(segment_path)}")
                
                if not self.run_ffmpeg_with_progress(cmd, duration, os.path.basename(segment_path)):
                    logger.error(f"分段处理失败: {segment_path}")
                    return False
            
            logger.info(f"分段处理完成，共 {len(segments)} 个分段")
            return True
            
        except Exception as e:
            logger.error(f"分段处理失败: {e}")
            return False

# ==================== 流水线管理器 ====================
class VideoPipelineManager:
    """视频处理流水线管理器"""
    
    def __init__(self, max_concurrent: int = 4):
        self.max_concurrent = max_concurrent
        self.tasks: Dict[str, VideoTask] = {}
        self.cache_queue = queue.Queue()
        self.check_queue = queue.Queue()
        self.process_queue = queue.Queue()
        
        self.workers = []
        self.shutdown_event = threading.Event()
        
        # 启动工作线程
        self._start_workers()
    
    def _start_workers(self):
        """启动工作线程"""
        # 缓存工作线程
        cache_worker = threading.Thread(target=self._cache_worker, daemon=True)
        cache_worker.start()
        self.workers.append(cache_worker)
        
        # 检查工作线程
        check_worker = threading.Thread(target=self._check_worker, daemon=True)
        check_worker.start()
        self.workers.append(check_worker)
        
        # 处理工作线程
        for i in range(self.max_concurrent):
            process_worker = threading.Thread(target=self._process_worker, daemon=True)
            process_worker.start()
            self.workers.append(process_worker)
    
    def add_task(self, video_path: str, mode: ProcessingMode, **kwargs) -> str:
        """添加处理任务"""
        task_id = f"task_{int(time.time() * 1000)}_{len(self.tasks)}"
        
        task = VideoTask(
            video_path=video_path,
            task_id=task_id,
            mode=mode,
            **kwargs
        )
        
        self.tasks[task_id] = task
        
        # 添加到缓存队列
        if ENABLE_CACHE:
            self.cache_queue.put(task_id)
        else:
            # 直接进入检查队列
            task.update_stage(PipelineStage.CACHED)
            self.check_queue.put(task_id)
        
        return task_id
    
    def _cache_worker(self):
        """缓存工作线程"""
        while not self.shutdown_event.is_set():
            try:
                task_id = self.cache_queue.get(timeout=1)
                task = self.tasks.get(task_id)
                
                if not task:
                    continue
                
                task.update_stage(PipelineStage.CACHING)
                
                # 执行缓存逻辑（这里简化处理）
                time.sleep(0.1)  # 模拟缓存时间
                
                task.update_stage(PipelineStage.CACHED)
                self.check_queue.put(task_id)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"缓存工作线程异常: {e}")
    
    def _check_worker(self):
        """检查工作线程"""
        while not self.shutdown_event.is_set():
            try:
                task_id = self.check_queue.get(timeout=1)
                task = self.tasks.get(task_id)
                
                if not task:
                    continue
                
                task.update_stage(PipelineStage.CHECKING)
                
                # 执行去重检查逻辑（这里简化处理）
                time.sleep(0.1)  # 模拟检查时间
                
                # 假设没有重复
                self.process_queue.put(task_id)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"检查工作线程异常: {e}")
    
    def _process_worker(self):
        """处理工作线程"""
        while not self.shutdown_event.is_set():
            try:
                task_id = self.process_queue.get(timeout=1)
                task = self.tasks.get(task_id)
                
                if not task:
                    continue
                
                task.update_stage(PipelineStage.PROCESSING)
                
                # 执行实际的视频处理
                # 这里需要根据task.mode调用相应的处理函数
                
                task.update_stage(PipelineStage.COMPLETED)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"处理工作线程异常: {e}")
    
    def wait_completion(self, timeout: Optional[float] = None) -> bool:
        """等待所有任务完成"""
        start_time = time.time()
        
        while True:
            if timeout and time.time() - start_time > timeout:
                return False
            
            # 检查是否所有任务都完成
            all_done = True
            for task in self.tasks.values():
                if task.stage not in [PipelineStage.COMPLETED, PipelineStage.FAILED, 
                                    PipelineStage.CANCELLED, PipelineStage.DUPLICATE]:
                    all_done = False
                    break
            
            if all_done:
                return True
            
            time.sleep(1)
    
    def get_statistics(self) -> Dict[str, int]:
        """获取统计信息"""
        stats = {}
        for stage in PipelineStage:
            stats[stage.value] = sum(1 for task in self.tasks.values() if task.stage == stage)
        return stats
    
    def shutdown(self):
        """关闭流水线"""
        self.shutdown_event.set()

# ==================== 主处理器 ====================
class UnifiedVideoProcessor:
    """统合视频处理器 - 主控制器（增强稳定性版本）"""
    
    def __init__(self):
        self.computer_name = socket.gethostname()
        self.mode = ProcessingMode(PROCESSING_MODE)
        self.batch_id = f"batch_{int(time.time())}"
        self.start_time = time.time()
        
        # 初始化核心组件
        self.hardware_detector = HardwareDetector()
        self.db_manager = DatabaseManager()
        self.cache_manager = SmartCacheManager(self.db_manager) if ENABLE_CACHE else None
        self.progress_manager = ProgressManager(self.computer_name, self.db_manager)
        self.roi_selector = ROISelector()
        
        # 新增：健康监控和分布式管理
        self.memory_monitor = MemoryHealthMonitor()
        self.task_manager = DistributedTaskManager(self.db_manager, self.computer_name)
        
        # 检测硬件能力
        self.hardware_info = self.hardware_detector.detect_hardware_capabilities()
        
        # 创建视频处理器
        self.video_processor = VideoProcessor(self.hardware_info, self.progress_manager)
        
        # 创建流水线管理器
        self.pipeline_manager = VideoPipelineManager(
            max_concurrent=self.hardware_info['max_parallel']
        )
        
        # 错误恢复和监控
        self.error_count = 0
        self.max_consecutive_errors = 10
        self.last_successful_time = time.time()
        self.shutdown_event = threading.Event()
        
        # 注册优雅关闭
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"🚀 初始化完成 - 模式: {self.mode.value}, 硬件: {self.hardware_info['encoder_type']}")
        logger.info(f"📊 内存监控: 启用, 分布式处理: {'启用' if ENABLE_DISTRIBUTED_PROCESSING else '禁用'}")
    
    def _signal_handler(self, signum, frame):
        """优雅关闭信号处理器"""
        logger.info(f"📡 收到关闭信号 {signum}，开始优雅关闭...")
        self.shutdown_event.set()
        self.graceful_shutdown()
    
    def graceful_shutdown(self):
        """优雅关闭处理器"""
        logger.info("🛑 开始优雅关闭视频处理器...")
        
        try:
            # 关闭各个组件
            if hasattr(self, 'pipeline_manager'):
                self.pipeline_manager.shutdown()
            
            if hasattr(self, 'task_manager'):
                self.task_manager.shutdown()
            
            if hasattr(self, 'cache_manager') and self.cache_manager:
                # 缓存管理器的关闭逻辑
                pass
            
            # 最终统计
            if hasattr(self, 'memory_monitor'):
                stats = self.memory_monitor.get_memory_stats()
                logger.info(f"🏁 最终统计: 处理{stats.get('processed_count', 0)}个视频, "
                           f"运行{stats.get('uptime_hours', 0):.1f}小时, "
                           f"峰值内存{stats.get('max_memory_mb', 0):.0f}MB")
            
            logger.info("✅ 优雅关闭完成")
            
        except Exception as e:
            logger.error(f"❌ 关闭过程中出现异常: {e}")
        
        sys.exit(0)
    
    def find_video_files(self, directory: str) -> List[str]:
        """查找视频文件"""
        video_files = []
        
        try:
            for ext in SUPPORTED_VIDEO_FORMATS:
                pattern = os.path.join(directory, f'*{ext}')
                files = glob.glob(pattern, recursive=False)
                video_files.extend(files)
                
                # 也搜索大写扩展名
                pattern_upper = os.path.join(directory, f'*{ext.upper()}')
                files_upper = glob.glob(pattern_upper, recursive=False)
                video_files.extend(files_upper)
            
            # 去重并排序
            video_files = list(set(video_files))
            video_files.sort()
            
            logger.info(f"找到 {len(video_files)} 个视频文件")
            return video_files
            
        except Exception as e:
            logger.error(f"搜索视频文件失败: {e}")
            return []
    
    def setup_roi_for_crop_mode(self, video_files: List[str]) -> Optional[Tuple[int, int, int, int, int, int]]:
        """为裁剪模式设置ROI"""
        if self.mode not in [ProcessingMode.CROP, ProcessingMode.BOTH]:
            return None
        
        # 检查是否有保存的ROI设置
        saved_roi = self.progress_manager.get_roi_settings()
        if saved_roi:
            logger.info(f"使用保存的ROI设置: {saved_roi}")
            if len(saved_roi) == 6:
                x, y, w, h, base_width, base_height = saved_roi
                print(f"发现保存的ROI设置: 区域({x}, {y}, {w}, {h}) @ {base_width}x{base_height}")
            else:
                print(f"发现保存的ROI设置: {saved_roi}")
            print("按回车键使用保存的设置，或输入 'r' 重新选择: ", end="")
            user_input = input().strip().lower()
            
            if user_input != 'r':
                return saved_roi
        
        # 选择ROI
        if not video_files:
            logger.error("没有视频文件可用于ROI选择")
            return None
        
        print(f"使用第一个视频进行ROI选择: {os.path.basename(video_files[0])}")
        roi = self.roi_selector.select_roi_for_video(video_files[0])
        
        if roi:
            self.progress_manager.set_roi_settings(roi)
            logger.info(f"ROI设置完成: {roi}")
            x, y, w, h, base_width, base_height = roi
            print(f"✅ ROI设置完成:")
            print(f"   裁剪区域: ({x}, {y}, {w}, {h})")
            print(f"   基准分辨率: {base_width}x{base_height}")
            print(f"   将自动缩放到其他分辨率的相应位置")
        
        return roi
    
    def filter_videos(self, video_files: List[str]) -> List[str]:
        """过滤视频文件"""
        filtered_files = []
        skipped_count = 0
        
        for video_path in video_files:
            # 检查是否已完成
            if self.progress_manager.is_completed(video_path):
                logger.info(f"跳过已完成: {os.path.basename(video_path)}")
                continue
            
            # 检查低分辨率
            should_skip, reason = self.video_processor.should_skip_low_resolution(video_path)
            if should_skip:
                logger.info(f"跳过低分辨率: {os.path.basename(video_path)} - {reason}")
                skipped_count += 1
                
                # 移动到指定目录
                if SKIP_VIDEOS_MOVE_DIR and os.path.exists(SKIP_VIDEOS_MOVE_DIR):
                    try:
                        dest_path = os.path.join(SKIP_VIDEOS_MOVE_DIR, os.path.basename(video_path))
                        shutil.move(video_path, dest_path)
                        logger.info(f"已移动跳过的视频: {dest_path}")
                    except Exception as e:
                        logger.warning(f"移动跳过视频失败: {e}")
                continue
            
            # 🔍 【关键】处理前数据库查重检查 - 使用增强查重
            if self.db_manager.is_enabled:
                logger.info(f"🔍 处理前数据库查重: {os.path.basename(video_path)}")
                is_duplicate, message = self.db_manager.enhanced_duplicate_check(video_path)
                
                if is_duplicate:
                    logger.info(f"✋ 处理前发现重复，跳过: {os.path.basename(video_path)} - {message}")
                    continue
                else:
                    logger.debug(f"📝 处理前查重通过: {os.path.basename(video_path)} - {message}")
            
            filtered_files.append(video_path)
        
        logger.info(f"过滤完成: {len(filtered_files)} 个待处理, {skipped_count} 个跳过")
        return filtered_files
    
    def process_video_file(self, video_path: str, roi: Optional[Tuple[int, int, int, int]] = None) -> bool:
        """处理单个视频文件（增强稳定性版本）"""
        video_name = os.path.basename(video_path)
        original_video_path = video_path  # 🔑 保存原始路径，用于数据库记录
        
        try:
            # 1. 分布式任务声明
            if not self.task_manager.claim_video_task(video_path):
                logger.info(f"🔒 视频已被其他电脑处理: {video_name}")
                return False  # 被其他电脑锁定，跳过
            
            base_name = os.path.splitext(video_name)[0]
            
            # 2. 超时检查
            if self.shutdown_event.is_set():
                logger.info("🛑 收到关闭信号，停止处理")
                return False
            
            # 3. 内存检查和清理
            if self.memory_monitor.should_cleanup_memory():
                freed_mb = self.memory_monitor.cleanup_memory()
                logger.info(f"🧹 预处理内存清理: 释放 {freed_mb:.1f}MB")
            
            # 4. 智能缓存处理：使用缓存路径进行处理，但数据库记录使用原始路径
            actual_processing_path = video_path  # 默认使用原始路径
            
            if self.cache_manager and self.cache_manager.is_enabled:
                # 检查是否有缓存版本
                cached_path = self.cache_manager.get_cached_path(video_path)
                if cached_path:
                    logger.info(f"📦 使用缓存版本: {video_name}")
                    actual_processing_path = cached_path  # 🎯 使用缓存路径进行处理
                else:
                    logger.debug(f"📁 使用原始文件: {video_name}")
            
            # 5. 标记为处理中
            self.progress_manager.mark_processing(original_video_path)
            self.memory_monitor.update_processed_count()
            
            start_time = time.time()
            success = False
            
            # 6. 处理超时保护
            processing_timeout = MAX_PROCESSING_TIME_HOURS * 3600
            
            logger.info(f"🎬 开始处理: {video_name}")
            logger.info(f"   📂 原始路径: {original_video_path}")
            logger.info(f"   🔧 处理路径: {actual_processing_path}")
            
            if self.mode == ProcessingMode.CROP:
                if not roi:
                    logger.error("裁剪模式需要ROI参数")
                    return False
                
                output_path = os.path.join(OUTPUT_DIR, f"{base_name}_cropped.mp4")
                success = self.video_processor.process_video_crop(actual_processing_path, output_path, roi, self.roi_selector)
            
            elif self.mode == ProcessingMode.TRIM:
                output_path = os.path.join(OUTPUT_DIR, f"{base_name}_trimmed.mp4")
                success = self.video_processor.process_video_trim(
                    actual_processing_path, output_path, CUT_HEAD_SECONDS, CUT_TAIL_SECONDS, SEGMENT_DURATION
                )
            
            elif self.mode == ProcessingMode.BOTH:
                if not roi:
                    logger.error("混合模式需要ROI参数")
                    return False
                
                # 先裁剪
                temp_cropped = self.video_processor.temp_dir / f"{base_name}_temp_cropped.mp4"
                crop_success = self.video_processor.process_video_crop(actual_processing_path, str(temp_cropped), roi, self.roi_selector)
                
                if crop_success:
                    # 再切头尾
                    output_path = os.path.join(OUTPUT_DIR, f"{base_name}_processed.mp4")
                    success = self.video_processor.process_video_trim(
                        str(temp_cropped), output_path, CUT_HEAD_SECONDS, CUT_TAIL_SECONDS, SEGMENT_DURATION
                    )
                    
                    # 清理临时文件
                    try:
                        temp_cropped.unlink()
                    except Exception:
                        pass
            
            processing_time = time.time() - start_time
            
            # 6. 处理结果处理
            if success:
                # 成功计数和恢复
                self.error_count = 0
                self.last_successful_time = time.time()
                
                # 标记为完成
                self.progress_manager.mark_completed(original_video_path, output_path, processing_time)
                
                # 📝 关键功能：写入真实的11个字段到数据库 - 使用原始路径
                if self.db_manager.is_enabled:
                    logger.info(f"🎯 开始写入数据库真实信息: {video_name}")
                    logger.info(f"   📂 数据库记录路径: {original_video_path}")
                    logger.info(f"   🔧 实际处理路径: {actual_processing_path}")
                    
                    # 🔑 关键修复：使用原始路径进行数据库记录，确保哈希计算使用存在的文件
                    success_db = self.db_manager.record_processing_complete(
                        video_path=original_video_path,  # 🎯 使用原始路径，不是缓存路径
                        output_path=output_path, 
                        processing_time=processing_time,
                        log_path=None
                    )
                    
                    if success_db:
                        logger.info(f"✅ 数据库记录成功: {video_name}")
                    else:
                        logger.warning(f"⚠️ 数据库记录失败: {video_name}")
                        # 数据库记录失败不影响处理成功状态
                
                logger.info(f"✅ 处理成功: {video_name} ({processing_time:.1f}s)")
                return True
            else:
                # 错误计数
                self.error_count += 1
                
                self.progress_manager.mark_failed(original_video_path, "处理失败")
                
                # 不记录失败到数据库，只记录成功的处理结果
                
                logger.error(f"❌ 处理失败: {video_name}")
                return False
                
        except Exception as e:
            # 错误计数和恢复检查
            self.error_count += 1
            
            # 检查是否需要重启
            if AUTO_RESTART_ON_ERROR and self.error_count >= self.max_consecutive_errors:
                logger.error(f"🚨 连续错误达到阈值 ({self.max_consecutive_errors})，准备重启...")
                # 这里可以实现重启逻辑或通知管理员
            
            self.progress_manager.mark_failed(original_video_path, str(e))
            
            # 不记录异常到数据库，只记录成功的处理结果
            
            logger.error(f"❌ 处理异常: {video_path} - {e}")
            return False
        
        finally:
            # 7. 清理和释放资源
            try:
                # 释放分布式任务锁
                self.task_manager.release_video_task(original_video_path)
                
                # 处理后内存检查
                if self.memory_monitor.should_cleanup_memory():
                    freed_mb = self.memory_monitor.cleanup_memory()
                    logger.debug(f"🧹 后处理内存清理: 释放 {freed_mb:.1f}MB")
                
            except Exception as cleanup_e:
                logger.warning(f"⚠️ 清理资源时出现异常: {cleanup_e}")
    
    def process_batch(self, video_files: List[str], roi: Optional[Tuple[int, int, int, int]] = None):
        """批量处理视频（增强大规模处理能力）"""
        if not video_files:
            logger.info("没有需要处理的视频文件")
            return
        
        total_videos = len(video_files)
        logger.info(f"🚀 开始批量处理 {total_videos} 个视频文件")
        
        # 确保输出目录存在
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 大数据集分批处理
        if total_videos > BATCH_SIZE:
            logger.info(f"📦 大数据集检测，将分批处理，每批 {BATCH_SIZE} 个视频")
            return self._process_large_batch(video_files, roi)
        
        # 初始化批次统计
        batch_start_time = time.time()
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        # 预加载缓存
        if self.cache_manager:
            logger.info("🚀 启动智能预加载...")
            for i, video_path in enumerate(video_files[:PRELOAD_COUNT]):
                self.cache_manager.start_async_download(video_path, priority=-i)
        
        # 分布式处理：清理过期锁
        if ENABLE_DISTRIBUTED_PROCESSING:
            self.task_manager.cleanup_expired_locks()
        
        # 创建进度条
        with tqdm(total=total_videos, desc="🎬 处理进度", unit="video", 
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
            
            # 计算最优并行数
            max_workers = min(self.hardware_info['max_parallel'], total_videos, 32)  # 限制最大32并发
            
            logger.info(f"⚡ 使用 {max_workers} 个并行工作线程")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_video = {
                    executor.submit(self.process_video_file, video_path, roi): video_path
                    for video_path in video_files
                }
                
                # 处理完成的任务
                processed_count = 0
                for future in concurrent.futures.as_completed(future_to_video):
                    if self.shutdown_event.is_set():
                        logger.info("🛑 收到关闭信号，终止批量处理")
                        break
                    
                    video_path = future_to_video[future]
                    video_name = os.path.basename(video_path)
                    
                    try:
                        result = future.result(timeout=MAX_PROCESSING_TIME_HOURS * 3600)
                        if result is True:
                            success_count += 1
                        elif result is False:
                            # False 可能表示被其他电脑处理或跳过
                            skipped_count += 1
                        else:
                            failed_count += 1
                            
                    except concurrent.futures.TimeoutError:
                        logger.error(f"⏰ 处理超时: {video_name}")
                        failed_count += 1
                    except Exception as e:
                        logger.error(f"❌ 任务异常: {video_name} - {e}")
                        failed_count += 1
                    
                    processed_count += 1
                    
                    # 更新进度条
                    current_success_rate = (success_count / processed_count * 100) if processed_count > 0 else 0
                    pbar.update(1)
                    pbar.set_postfix({
                        '✅成功': success_count,
                        '❌失败': failed_count, 
                        '⏭️跳过': skipped_count,
                        '📊成功率': f"{current_success_rate:.1f}%",
                        '🧠内存': f"{self.memory_monitor.get_memory_stats().get('current_memory_mb', 0):.0f}MB"
                    })
                    
                    # 定期健康检查
                    if processed_count % 50 == 0:
                        self._health_check_during_batch(processed_count, total_videos)
        
        # 批次完成统计
        batch_duration = time.time() - batch_start_time
        total_processed = success_count + failed_count + skipped_count
        
        self._log_batch_completion(total_processed, success_count, failed_count, 
                                 skipped_count, batch_duration)
    
    def _process_large_batch(self, video_files: List[str], roi: Optional[Tuple[int, int, int, int]] = None):
        """处理大规模数据集（分批处理）"""
        total_videos = len(video_files)
        total_batches = (total_videos + BATCH_SIZE - 1) // BATCH_SIZE
        
        logger.info(f"📊 大规模处理统计: {total_videos} 个视频, {total_batches} 批次, 每批 {BATCH_SIZE} 个")
        
        overall_success = 0
        overall_failed = 0
        overall_skipped = 0
        
        for batch_idx in range(total_batches):
            if self.shutdown_event.is_set():
                logger.info("🛑 收到关闭信号，终止大规模处理")
                break
            
            start_idx = batch_idx * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, total_videos)
            batch_files = video_files[start_idx:end_idx]
            
            logger.info(f"📦 处理批次 {batch_idx + 1}/{total_batches} "
                       f"({len(batch_files)} 个视频, 索引 {start_idx}-{end_idx-1})")
            
            # 批次间内存强制清理
            if batch_idx > 0:
                freed_mb = self.memory_monitor.cleanup_memory(force=True)
                logger.info(f"🧹 批次间内存清理: 释放 {freed_mb:.1f}MB")
                time.sleep(2)  # 短暂休息
            
            # 处理当前批次
            batch_start = time.time()
            self.process_batch(batch_files, roi)  # 递归调用，但不会再进入大批次逻辑
            batch_duration = time.time() - batch_start
            
            # 批次完成统计（这里需要获取实际的成功/失败数，简化处理）
            logger.info(f"✅ 批次 {batch_idx + 1} 完成，耗时 {batch_duration:.1f}秒")
            
            # 批次间健康检查
            self._health_check_between_batches(batch_idx + 1, total_batches)
        
        logger.info(f"🏁 大规模处理完成: {total_batches} 批次全部处理完毕")
    
    def _health_check_during_batch(self, processed: int, total: int):
        """批次处理期间的健康检查"""
        try:
            stats = self.memory_monitor.get_memory_stats()
            progress_percent = (processed / total * 100) if total > 0 else 0
            
            logger.info(f"🏥 健康检查 [{processed}/{total}] "
                       f"进度 {progress_percent:.1f}%, "
                       f"内存 {stats.get('current_memory_mb', 0):.0f}MB, "
                       f"运行 {stats.get('uptime_hours', 0):.1f}h")
            
            # 检查错误率
            if hasattr(self, 'error_count') and processed > 50:
                error_rate = (self.error_count / processed) * 100
                if error_rate > 20:  # 错误率超过20%
                    logger.warning(f"⚠️ 高错误率警告: {error_rate:.1f}%")
                    
        except Exception as e:
            logger.warning(f"健康检查异常: {e}")
    
    def _health_check_between_batches(self, current_batch: int, total_batches: int):
        """批次间的健康检查"""
        try:
            stats = self.memory_monitor.get_memory_stats()
            
            logger.info(f"🔄 批次间检查 [{current_batch}/{total_batches}] "
                       f"内存峰值 {stats.get('max_memory_mb', 0):.0f}MB, "
                       f"清理次数 {stats.get('cleanup_count', 0)}")
            
            # 分布式锁清理
            if ENABLE_DISTRIBUTED_PROCESSING and current_batch % 5 == 0:
                self.task_manager.cleanup_expired_locks()
                
        except Exception as e:
            logger.warning(f"批次间检查异常: {e}")
    
    def _log_batch_completion(self, total: int, success: int, failed: int, skipped: int, duration: float):
        """记录批次完成统计"""
        success_rate = (success / total * 100) if total > 0 else 0
        avg_time = (duration / total) if total > 0 else 0
        
        logger.info(f"🎯 批量处理完成统计:")
        logger.info(f"   总计: {total} 个视频")
        logger.info(f"   ✅ 成功: {success} ({success_rate:.1f}%)")
        logger.info(f"   ❌ 失败: {failed}")
        logger.info(f"   ⏭️ 跳过: {skipped}")
        logger.info(f"   ⏱️ 总耗时: {duration:.1f}秒")
        logger.info(f"   📊 平均: {avg_time:.1f}秒/视频")
        
        # 获取内存统计
        stats = self.memory_monitor.get_memory_stats()
        logger.info(f"   🧠 内存峰值: {stats.get('max_memory_mb', 0):.0f}MB")
        logger.info(f"   🧹 清理次数: {stats.get('cleanup_count', 0)}")
        
        print(f"\n🎬 批量处理完成!")
        print(f"   📊 成功率: {success_rate:.1f}% ({success}/{total})")
        print(f"   ⏱️ 处理速度: {avg_time:.1f}秒/视频")
        print(f"   🧠 内存管理: {stats.get('cleanup_count', 0)} 次清理")
        if skipped > 0:
            print(f"   ℹ️ 跳过 {skipped} 个视频（可能被其他电脑处理）")
    
    def run(self):
        """运行主程序"""
        try:
            print("🚀 综合视频处理器 - 终极版")
            print(f"   处理模式: {self.mode.value}")
            print(f"   硬件编码: {self.hardware_info['encoder_type']}")
            print(f"   并行数量: {self.hardware_info['max_parallel']}")
            print(f"   缓存: {'启用' if ENABLE_CACHE else '禁用'}")
            print(f"   数据库: {'启用' if self.db_manager.is_enabled else '禁用'}")
            print()
            
            # 验证配置
            if not os.path.exists(FFMPEG_PATH):
                raise ValueError(f"FFmpeg路径不存在: {FFMPEG_PATH}")
            if not os.path.exists(INPUT_DIR):
                raise ValueError(f"输入目录不存在: {INPUT_DIR}")
            
            # 查找视频文件
            print("🔍 搜索视频文件...")
            video_files = self.find_video_files(INPUT_DIR)
            
            if not video_files:
                print("❌ 没有找到支持的视频文件")
                return
            
            print(f"✅ 找到 {len(video_files)} 个视频文件")
            
            # 设置ROI（如果需要）
            roi = self.setup_roi_for_crop_mode(video_files)
            
            # 过滤视频文件
            print("🔧 过滤视频文件...")
            filtered_files = self.filter_videos(video_files)
            
            if not filtered_files:
                print("✅ 所有视频都已处理完成！")
                return
            
            print(f"📋 待处理视频: {len(filtered_files)} 个")
            
            # 开始批量处理
            print("🎬 开始批量处理...")
            self.process_batch(filtered_files, roi)
            
            # 显示最终统计
            stats = self.progress_manager.get_statistics()
            print(f"\n📊 最终统计:")
            print(f"   已完成: {stats['completed']}")
            print(f"   处理中: {stats['processing']}")
            print(f"   失败: {stats['failed']}")
            
        except KeyboardInterrupt:
            print("\n⚠️ 用户中断处理")
            logger.info("用户中断处理")
        except Exception as e:
            print(f"\n❌ 程序异常: {e}")
            logger.error(f"程序异常: {e}", exc_info=True)
        finally:
            # 清理资源
            if hasattr(self, 'pipeline_manager'):
                self.pipeline_manager.shutdown()
            logger.info("程序结束")

# ==================== 主函数 ====================
def main():
    """主函数"""
    # 设置信号处理器
    def signal_handler(signum, frame):
        logger.info("收到退出信号，正在清理...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 创建并运行处理器
        processor = UnifiedVideoProcessor()
        processor.run()
        
    except Exception as e:
        logger.error(f"主程序异常: {e}", exc_info=True)
        print(f"❌ 程序启动失败: {e}")

if __name__ == '__main__':
    main()


