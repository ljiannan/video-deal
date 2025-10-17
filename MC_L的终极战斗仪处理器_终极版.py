# _*_ coding: utf-8 _*_
"""
MC_L的终极战斗仪 - NAS极限优化版
整合批量切头尾2.1.py和批量裁剪2.1.py的所有功能
支持：切头尾 + 裁剪 + 缩放 + 硬件加速 + ROI选择 + 断点续传

🚀 MC_L的顶尖优化策略：
- 智能本地缓存系统 (500TB+ NAS优化)
- 异步预读取和断点续传
- i9处理器极致性能调优
- 网络I/O瓶颈突破
- 内存和存储智能管理

基于批量切头尾2.1.py和批量裁剪2.1.py整合优化
版本: MC_L的NAS Extreme V2.0
日期: 2025年
"""

# ==================== START: 用户配置区域 ====================
# !!! 请根据你的实际情况修改以下配置 !!!

# --- 处理人员配置 ---
PROCESSOR_NAME = "Ljn"  # 处理人名称，可以修改为你的名字
COMPUTER_NAME = "大09"  # 电脑号，留空将自动获取
COMPUTER_IP = ""   # 电脑IP，留空将自动获取

# --- FFmpeg 路径配置 ---
FFMPEG_PATH = r"D:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe"
FFPROBE_PATH = r"D:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffprobe.exe"

# --- 输入输出路径配置 ---
INPUT_DIR = r"Z:\a项目\航拍特写\李建楠\9.22\野营"
OUTPUT_DIR = r"D:\9.22野营"

# --- 进度记录配置 ---
PROGRESS_FOLDER = r"Z:\personal_folder\L\处理完数据记录"

# --- 功能开关配置 ---
ENABLE_HEAD_TAIL_CUT = True   # 启用切头尾功能
ENABLE_CROPPING = True        # 启用裁剪功能

# --- 切头尾时间配置 ---
HEAD_CUT_TIME = 42    # 片头时间（秒）
TAIL_CUT_TIME = 42    # 片尾时间（秒）

# --- 裁剪配置 ---
TARGET_RESOLUTION = (1920, 1080)  # 目标分辨率 (必须是16:9比例)

# --- 低分辨率视频跳过配置 ---
SKIP_LOW_RESOLUTION_VIDEOS = True
MIN_RESOLUTION_WIDTH = 1920
SKIP_VIDEOS_MOVE_DIR = r"Z:\personal_folder\L\测试\跳过的低分辨率视频"

# --- 支持的视频格式 ---
SUPPORTED_VIDEO_FORMATS = ['.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm', '.ts', '.m4v', '.3gp', '.f4v']

# --- 硬件配置 ---
MAX_PARALLEL_WORKERS_OVERRIDE = 0
MAX_PARALLEL_WORKERS = 16  # 翻倍优化并行处理数量
QUALITY_MODE = 'highest'
AUTO_BITRATE = True
VIDEO_BITRATE = "10M"
MAX_BITRATE = "20M"
BUFFER_SIZE = "20M"

# --- NAS极限优化配置 (2025年顶尖策略) ---
# 本地缓存配置 - 🔧 临时禁用以解决卡死问题
LOCAL_CACHE_DIR = r"D:\NAS_VideoCache"  # 本地SSD缓存目录
MAX_CACHE_SIZE_GB = 200  # 最大缓存大小(GB) - 根据本地SSD空间调整
PRELOAD_COUNT = 0  # 🔧 禁用预加载以避免卡死
ASYNC_DOWNLOAD_THREADS = 8  # 异步下载线程数

# 🔧 緊急修復：禁用緩存功能以解決卡死問題
ENABLE_CACHE = False  # 設為False來禁用緩存

# 网络优化配置 - 已清理未使用的配置

# i9性能优化配置
ENABLE_I9_TURBO = True  # 启用i9涡轮优化
CPU_AFFINITY_OPTIMIZATION = True  # CPU亲和性优化

# 存储优化配置
TEMP_PROCESSING_DIR = r"D:\VideoProcessing_Temp"  # 临时处理目录
AUTO_CLEANUP_CACHE = True  # 自动清理缓存
MONITOR_DISK_SPACE = True  # 监控磁盘空间
MIN_FREE_SPACE_GB = 50  # 最小剩余空间(GB)
MEMORY_POOL_SIZE_GB = 8  # 内存池大小(GB)

# --- 数据库配置 (多电脑协作) ---
MYSQL_CONFIG = {
    'host': '192.168.10.70',
    'user': 'root',
    'password': 'zq828079',
    'database': 'data_sql',
}

# 启用多电脑协作功能
ENABLE_MULTI_COMPUTER_SYNC = True  # 设为True启用数据库同步

# ===================== END: 用户配置区域 =====================

import time
import os
import subprocess
import logging
import json
import re
import concurrent.futures
import threading
import psutil
import platform
import hashlib
import uuid
from collections import deque
import socket
import numpy as np
import cv2
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any, Set
from tqdm import tqdm
from datetime import datetime
import multiprocessing
import queue
from dataclasses import dataclass, field
from collections import deque, defaultdict
import mysql.connector
from mysql.connector import Error, pooling
from loguru import logger
import enum
import signal
import atexit
import shutil
from threading import Lock, Event, Thread, RLock, Condition

# ==================== 编码配置 ====================
os.environ['PYTHONIOENCODING'] = 'utf-8'
if platform.system() == "Windows":
    os.environ['PYTHONUTF8'] = '1'

# ==================== 流水线状态枚举 ====================
class PipelineStage(enum.Enum):
    """流水线阶段状态"""
    PENDING = "pending"        # 等待处理
    CACHING = "caching"        # 正在缓存
    CACHED = "cached"          # 缓存完成
    CHECKING = "checking"      # 正在查重
    DUPLICATE = "duplicate"    # 发现重复
    PROCESSING = "processing"  # 正在处理
    COMPLETED = "completed"    # 处理完成
    FAILED = "failed"          # 处理失败
    TIMEOUT = "timeout"        # 超时失败
    CANCELLED = "cancelled"    # 已取消

@dataclass
class PipelineTask:
    """流水线任务单元"""
    video_path: str
    task_id: str
    stage: PipelineStage = PipelineStage.PENDING
    cache_path: Optional[str] = None
    hash_value: Optional[str] = None
    error_msg: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    created_time: float = field(default_factory=time.time)
    stage_start_time: float = field(default_factory=time.time)
    timeout_seconds: Dict[PipelineStage, float] = field(default_factory=lambda: {
        PipelineStage.CACHING: 7200.0,    # 2小时缓存超时（大文件需要更多时间）
        PipelineStage.CHECKING: 600.0,    # 10分钟查重超时（增加容错）
        PipelineStage.PROCESSING: 7200.0  # 2小时处理超时（4K视频需要更多时间）
    })
    
    def is_timeout(self) -> bool:
        """检查当前阶段是否超时 - 智能超时机制"""
        if self.stage in self.timeout_seconds:
            elapsed = time.time() - self.stage_start_time
            base_timeout = self.timeout_seconds[self.stage]
            
            # 根据文件大小动态调整超时时间
            try:
                if os.path.exists(self.video_path):
                    file_size_gb = os.path.getsize(self.video_path) / (1024**3)
                    # 每GB增加30分钟超时时间（最多额外增加4小时）
                    size_bonus = min(file_size_gb * 1800, 14400)  
                    adjusted_timeout = base_timeout + size_bonus
                    return elapsed > adjusted_timeout
            except Exception:
                pass  # 如果获取文件大小失败，使用基础超时
            
            return elapsed > base_timeout
        return False
    
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

class VideoPipelineManager:
    """视频处理流水线管理器 - 防卡死加固版"""
    
    def __init__(self, max_concurrent_cache: int = 6, max_concurrent_check: int = 4, max_concurrent_process: int = 4):
        # 队列系统
        self.pending_queue = queue.Queue()           # 待缓存队列
        self.cache_queue = queue.Queue()             # 缓存完成队列（待查重）
        self.process_queue = queue.Queue()           # 查重完成队列（待处理）
        self.completed_queue = queue.Queue()         # 完成队列
        self.failed_queue = queue.Queue()            # 失败队列
        
        # 任务跟踪
        self.tasks: Dict[str, PipelineTask] = {}     # 所有任务
        self.active_tasks: Dict[PipelineStage, Set[str]] = defaultdict(set)  # 活跃任务分组
        
        # 并发控制
        self.max_concurrent_cache = max_concurrent_cache
        self.max_concurrent_check = max_concurrent_check  
        self.max_concurrent_process = max_concurrent_process
        
        # 线程控制
        self.cache_threads: List[Thread] = []
        self.check_threads: List[Thread] = []
        self.process_threads: List[Thread] = []
        self.monitor_thread: Optional[Thread] = None
        
        # 同步控制
        self.task_lock = RLock()
        self.shutdown_event = Event()
        self.pipeline_condition = Condition()
        
        # 统计信息
        self.stats = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'duplicate_tasks': 0,
            'cache_hits': 0,
            'avg_cache_time': 0.0,
            'avg_check_time': 0.0,
            'avg_process_time': 0.0
        }
        
        # 错误恢复
        self.error_recovery_enabled = True
        self.deadlock_detection_enabled = True
        self.last_activity_time = time.time()
        
        # 注册退出处理
        atexit.register(self.cleanup)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logging.info("🚀 视频处理流水线管理器初始化完成")
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        logging.info(f"📡 接收到信号 {signum}，开始优雅关闭...")
        self.shutdown()
    
    def bulk_database_check(self, video_paths: List[str], progress_manager) -> Tuple[List[str], List[Dict]]:
        """批量数据库快速检查 - 在缓存前过滤已处理视频"""
        videos_to_process = []
        skipped_videos = []
        
        if not progress_manager or not hasattr(progress_manager, 'video_record_manager'):
            return video_paths, []
            
        video_record_manager = progress_manager.video_record_manager
        if not video_record_manager or not video_record_manager.db_manager.is_available():
            return video_paths, []
        
        logging.info(f"🚀 开始批量数据库快速检查 ({len(video_paths)} 个视频)...")
        
        for video_path in video_paths:
            try:
                is_processed, db_record = video_record_manager.quick_database_check_before_cache(video_path)
                if is_processed:
                    video_name = os.path.basename(video_path)
                    processor = db_record.get('computer_name', 'unknown') if db_record else 'unknown'
                    logging.info(f"🔍 跳过已处理视频: {video_name} (处理者: {processor})")
                    skipped_videos.append({
                        'video_path': video_path,
                        'video_name': video_name,
                        'processor': processor,
                        'record': db_record
                    })
                    # 直接标记为已完成
                    progress_manager.mark_completed(video_path, db_record.get('output_path', '') if db_record else '', 0)
                else:
                    videos_to_process.append(video_path)
                    
            except Exception as e:
                logging.warning(f"⚠️ 数据库检查失败，将继续处理: {os.path.basename(video_path)} - {e}")
                videos_to_process.append(video_path)
        
        logging.info(f"✅ 批量数据库检查完成: 跳过 {len(skipped_videos)} 个，需处理 {len(videos_to_process)} 个")
        return videos_to_process, skipped_videos

    def add_task(self, video_path: str) -> str:
        """添加任务到流水线"""
        task_id = f"{int(time.time() * 1000)}_{hash(video_path) % 10000}"
        task = PipelineTask(video_path=video_path, task_id=task_id)
        
        with self.task_lock:
            self.tasks[task_id] = task
            self.active_tasks[PipelineStage.PENDING].add(task_id)
            self.pending_queue.put(task_id)
            self.stats['total_tasks'] += 1
            
        with self.pipeline_condition:
            self.pipeline_condition.notify_all()
            
        logging.debug(f"📝 任务已添加: {os.path.basename(video_path)} (ID: {task_id})")
        return task_id
    
    def start_pipeline(self, cache_manager, progress_manager, video_processor):
        """启动流水线处理"""
        logging.info("🔄 启动视频处理流水线...")
        
        # 启动缓存线程
        for i in range(self.max_concurrent_cache):
            thread = Thread(
                target=self._cache_worker,
                args=(cache_manager,),
                name=f"CacheWorker-{i}",
                daemon=True
            )
            thread.start()
            self.cache_threads.append(thread)
        
        # 启动查重线程
        for i in range(self.max_concurrent_check):
            thread = Thread(
                target=self._check_worker,
                args=(progress_manager,),
                name=f"CheckWorker-{i}",
                daemon=True
            )
            thread.start()
            self.check_threads.append(thread)
        
        # 启动处理线程
        for i in range(self.max_concurrent_process):
            thread = Thread(
                target=self._process_worker,
                args=(video_processor,),
                name=f"ProcessWorker-{i}",
                daemon=True
            )
            thread.start()
            self.process_threads.append(thread)
        
        # 启动监控线程
        self.monitor_thread = Thread(
            target=self._monitor_worker,
            name="PipelineMonitor",
            daemon=True
        )
        self.monitor_thread.start()
        
        logging.info(f"✅ 流水线已启动: 缓存×{self.max_concurrent_cache}, 查重×{self.max_concurrent_check}, 处理×{self.max_concurrent_process}")
    
    def _cache_worker(self, cache_manager):
        """缓存工作线程"""
        thread_name = threading.current_thread().name
        logging.debug(f"🔄 {thread_name} 启动")
        
        while not self.shutdown_event.is_set():
            try:
                # 等待任务
                task_id = self.pending_queue.get(timeout=1.0)
                
                with self.task_lock:
                    if task_id not in self.tasks:
                        continue
                    task = self.tasks[task_id]
                    
                    # 更新状态
                    self.active_tasks[PipelineStage.PENDING].discard(task_id)
                    self.active_tasks[PipelineStage.CACHING].add(task_id)
                    task.update_stage(PipelineStage.CACHING)
                
                print(f"📥 开始缓存: {os.path.basename(task.video_path)}")
                logging.debug(f"📥 {thread_name} 开始缓存: {os.path.basename(task.video_path)}")
                
                # 执行缓存
                start_time = time.time()
                cache_path = cache_manager.get_cached_path(task.video_path)
                
                # 如果缓存不存在，执行下载
                if not cache_path:
                    logging.info(f"🔄 缓存不存在，开始下载: {os.path.basename(task.video_path)}")
                    download_success = cache_manager._download_video_to_cache(task.video_path)
                    if download_success:
                        # 下载成功后重新获取缓存路径
                        cache_path = cache_manager.get_cached_path(task.video_path)
                
                cache_time = time.time() - start_time
                
                with self.task_lock:
                    if cache_path:
                        # 缓存成功
                        task.cache_path = cache_path
                        self.active_tasks[PipelineStage.CACHING].discard(task_id)
                        self.active_tasks[PipelineStage.CACHED].add(task_id)
                        task.update_stage(PipelineStage.CACHED)
                        
                        # 添加到查重队列
                        self.cache_queue.put(task_id)
                        self.stats['cache_hits'] += 1
                        self.stats['avg_cache_time'] = (self.stats['avg_cache_time'] + cache_time) / 2
                        
                        print(f"💾 缓存完成: {os.path.basename(task.video_path)} ({cache_time:.1f}s)")
                        logging.info(f"✅ {thread_name} 缓存完成: {os.path.basename(task.video_path)} ({cache_time:.1f}s)")
                    else:
                        # 缓存失败
                        self._handle_task_failure(task_id, "缓存失败", can_retry=True)
                
                with self.pipeline_condition:
                    self.pipeline_condition.notify_all()
                    
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"❌ {thread_name} 缓存异常: {e}")
                if 'task_id' in locals():
                    self._handle_task_failure(task_id, f"缓存异常: {e}")
                    
        logging.debug(f"🛑 {thread_name} 已停止")
    
    def _check_worker(self, progress_manager):
        """查重工作线程"""
        thread_name = threading.current_thread().name
        logging.debug(f"🔄 {thread_name} 启动")
        
        while not self.shutdown_event.is_set():
            try:
                # 等待任务
                task_id = self.cache_queue.get(timeout=1.0)
                
                with self.task_lock:
                    if task_id not in self.tasks:
                        continue
                    task = self.tasks[task_id]
                    
                    # 更新状态
                    self.active_tasks[PipelineStage.CACHED].discard(task_id)
                    self.active_tasks[PipelineStage.CHECKING].add(task_id)
                    task.update_stage(PipelineStage.CHECKING)
                
                logging.debug(f"🔍 {thread_name} 开始查重: {os.path.basename(task.video_path)}")
                
                # 执行查重
                start_time = time.time()
                is_duplicate = self._perform_duplicate_check(task, progress_manager)
                check_time = time.time() - start_time
                
                with self.task_lock:
                    self.active_tasks[PipelineStage.CHECKING].discard(task_id)
                    
                    if is_duplicate:
                        # 发现重复
                        task.update_stage(PipelineStage.DUPLICATE)
                        self.failed_queue.put(task_id)
                        self.stats['duplicate_tasks'] += 1
                        logging.info(f"🔄 {thread_name} 发现重复: {os.path.basename(task.video_path)}")
                    else:
                        # 无重复，进入处理队列
                        self.active_tasks[PipelineStage.CACHED].add(task_id)
                        task.update_stage(PipelineStage.CACHED)
                        self.process_queue.put(task_id)
                        print(f"🔍 查重通过: {os.path.basename(task.video_path)} ({check_time:.1f}s)")
                        logging.info(f"✅ {thread_name} 查重通过: {os.path.basename(task.video_path)} ({check_time:.1f}s)")
                    
                    self.stats['avg_check_time'] = (self.stats['avg_check_time'] + check_time) / 2
                
                with self.pipeline_condition:
                    self.pipeline_condition.notify_all()
                    
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"❌ {thread_name} 查重异常: {e}")
                if 'task_id' in locals():
                    self._handle_task_failure(task_id, f"查重异常: {e}")
                    
        logging.debug(f"🛑 {thread_name} 已停止")
    
    def _process_worker(self, video_processor):
        """处理工作线程"""
        thread_name = threading.current_thread().name
        logging.debug(f"🔄 {thread_name} 启动")
        
        while not self.shutdown_event.is_set():
            try:
                # 等待任务
                task_id = self.process_queue.get(timeout=1.0)
                
                with self.task_lock:
                    if task_id not in self.tasks:
                        continue
                    task = self.tasks[task_id]
                    
                    # 更新状态
                    self.active_tasks[PipelineStage.CACHED].discard(task_id)
                    self.active_tasks[PipelineStage.PROCESSING].add(task_id)
                    task.update_stage(PipelineStage.PROCESSING)
                
                print(f"⚙️ 开始处理: {os.path.basename(task.video_path)}")
                logging.info(f"⚙️ {thread_name} 开始处理: {os.path.basename(task.video_path)}")
                
                # 执行处理
                start_time = time.time()
                success = self._perform_video_processing(task, video_processor)
                process_time = time.time() - start_time
                
                with self.task_lock:
                    self.active_tasks[PipelineStage.PROCESSING].discard(task_id)
                    
                    if success:
                        # 处理成功
                        task.update_stage(PipelineStage.COMPLETED)
                        self.completed_queue.put(task_id)
                        self.stats['completed_tasks'] += 1
                        print(f"🎬 处理完成: {os.path.basename(task.video_path)} ({process_time:.1f}s)")
                        logging.info(f"🎉 {thread_name} 处理完成: {os.path.basename(task.video_path)} ({process_time:.1f}s)")
                    else:
                        # 处理失败
                        self._handle_task_failure(task_id, "处理失败", can_retry=True)
                    
                    self.stats['avg_process_time'] = (self.stats['avg_process_time'] + process_time) / 2
                
                with self.pipeline_condition:
                    self.pipeline_condition.notify_all()
                    
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"❌ {thread_name} 处理异常: {e}")
                if 'task_id' in locals():
                    self._handle_task_failure(task_id, f"处理异常: {e}")
                    
        logging.debug(f"🛑 {thread_name} 已停止")
    
    def _monitor_worker(self):
        """监控工作线程 - 防卡死检测"""
        logging.debug("🔄 Pipeline监控线程启动")
        
        while not self.shutdown_event.is_set():
            try:
                time.sleep(10)  # 每10秒检查一次
                
                current_time = time.time()
                tasks_to_timeout = []
                
                with self.task_lock:
                    # 检查超时任务
                    for task_id, task in self.tasks.items():
                        if task.stage in [PipelineStage.CACHING, PipelineStage.CHECKING, PipelineStage.PROCESSING]:
                            if task.is_timeout():
                                tasks_to_timeout.append(task_id)
                    
                    # 处理超时任务
                    for task_id in tasks_to_timeout:
                        self._handle_task_timeout(task_id)
                    
                    # 死锁检测
                    if self.deadlock_detection_enabled:
                        self._detect_deadlock()
                    
                    # 更新活跃时间
                    if tasks_to_timeout or self._has_active_tasks():
                        self.last_activity_time = current_time
                
                # 打印状态
                if current_time - self.last_activity_time < 60:  # 最近1分钟有活动
                    self._print_pipeline_status()
                    
            except Exception as e:
                logging.error(f"❌ 监控线程异常: {e}")
                
        logging.debug("🛑 Pipeline监控线程已停止")
    
    def _handle_task_failure(self, task_id: str, error_msg: str, can_retry: bool = False):
        """处理任务失败"""
        with self.task_lock:
            if task_id not in self.tasks:
                return
                
            task = self.tasks[task_id]
            
            # 从活跃任务中移除
            for stage_set in self.active_tasks.values():
                stage_set.discard(task_id)
            
            # 检查是否可以重试
            if can_retry and task.can_retry():
                task.increment_retry()
                task.update_stage(PipelineStage.PENDING, error_msg)
                self.active_tasks[PipelineStage.PENDING].add(task_id)
                self.pending_queue.put(task_id)
                logging.warning(f"🔄 任务重试 ({task.retry_count}/{task.max_retries}): {os.path.basename(task.video_path)} - {error_msg}")
            else:
                # 标记失败
                task.update_stage(PipelineStage.FAILED, error_msg)
                self.failed_queue.put(task_id)
                self.stats['failed_tasks'] += 1
                logging.error(f"❌ 任务失败: {os.path.basename(task.video_path)} - {error_msg}")
    
    def _handle_task_timeout(self, task_id: str):
        """处理任务超时"""
        with self.task_lock:
            if task_id not in self.tasks:
                return
                
            task = self.tasks[task_id]
            stage_name = task.stage.value
            elapsed = time.time() - task.stage_start_time
            
            logging.warning(f"⏰ 任务超时: {os.path.basename(task.video_path)} 在 {stage_name} 阶段超时 ({elapsed:.1f}s)")
            
            # 从活跃任务中移除
            for stage_set in self.active_tasks.values():
                stage_set.discard(task_id)
            
            # 检查是否可以重试
            if task.can_retry():
                task.increment_retry()
                task.update_stage(PipelineStage.PENDING, f"阶段 {stage_name} 超时")
                self.active_tasks[PipelineStage.PENDING].add(task_id)
                self.pending_queue.put(task_id)
                logging.info(f"🔄 超时任务重试: {os.path.basename(task.video_path)}")
            else:
                task.update_stage(PipelineStage.TIMEOUT, f"阶段 {stage_name} 超时")
                self.failed_queue.put(task_id)
                self.stats['failed_tasks'] += 1
    
    def _detect_deadlock(self):
        """死锁检测"""
        # 简单的死锁检测：如果所有队列都空且有活跃任务但很久没有进展
        if (self.pending_queue.empty() and self.cache_queue.empty() and 
            self.process_queue.empty() and self._has_active_tasks() and
            time.time() - self.last_activity_time > 120):  # 2分钟无进展
            
            logging.warning("⚠️ 检测到可能的死锁情况，尝试恢复...")
            
            # 重置超时任务
            with self.task_lock:
                for task_id, task in self.tasks.items():
                    if task.stage in [PipelineStage.CACHING, PipelineStage.CHECKING, PipelineStage.PROCESSING]:
                        if time.time() - task.stage_start_time > 60:  # 超过1分钟的任务
                            self._handle_task_timeout(task_id)
    
    def _has_active_tasks(self) -> bool:
        """检查是否有活跃任务"""
        return any(len(tasks) > 0 for stage, tasks in self.active_tasks.items() 
                  if stage != PipelineStage.COMPLETED)
    
    def _print_pipeline_status(self):
        """打印流水线状态 - 已禁用，使用tqdm统一显示"""
        pass  # 不再输出，避免与tqdm进度条冲突
    
    def _perform_duplicate_check(self, task: PipelineTask, progress_manager) -> bool:
        """执行查重检查 - 使用增强的智能查重系统 🔧 修复：使用原始路径进行查重"""
        try:
            if not progress_manager.video_record_manager:
                return False
                
            # 🔧 关键修复：使用原始视频路径而不是缓存路径进行查重检查
            # 这确保数据库中记录的是原始NAS路径的信息，而不是本地缓存路径
            is_duplicate, message = progress_manager.video_record_manager.enhanced_duplicate_check(task.video_path)
            
            if is_duplicate:
                logging.info(f"🔍 智能查重发现重复: {os.path.basename(task.video_path)} - {message}")
                
                # 清理缓存文件（如果存在）
                try:
                    if task.cache_path and os.path.exists(task.cache_path):
                        os.remove(task.cache_path)
                        logging.debug(f"🗑️ 清理重复文件缓存: {task.cache_path}")
                except Exception as e:
                    logging.warning(f"清理缓存失败: {e}")
                return True
            else:
                logging.debug(f"📝 智能查重检查完成: {os.path.basename(task.video_path)} - {message}")
                return False
                
        except Exception as e:
            logging.error(f"智能查重检查失败: {e}")
            return False
    
    def _perform_video_processing(self, task: PipelineTask, video_processor) -> bool:
        """执行视频处理"""
        try:
            if not task.cache_path or not os.path.exists(task.cache_path):
                return False
                
            # 这里调用实际的视频处理函数
            # 需要根据具体的视频处理器接口调整
            result = video_processor.process_single_video(
                input_path=task.cache_path,
                original_path=task.video_path
            )
            
            return result is not None and result
            
        except Exception as e:
            logging.error(f"视频处理失败: {e}")
            return False
    
    def wait_completion(self, timeout: Optional[float] = None) -> bool:
        """等待所有任务完成 - 增强稳定性版本"""
        start_time = time.time()
        last_progress_time = start_time
        last_completed_count = 0
        
        while self._has_active_tasks():
            current_time = time.time()
            
            # 检查总体超时（如果设置了）
            if timeout and (current_time - start_time) > timeout:
                logging.warning(f"⏰ 等待完成超时 ({timeout}s)")
                return False
            
            # 检查是否有进展（防止卡死）
            current_completed = self.stats['completed_tasks']
            if current_completed > last_completed_count:
                last_progress_time = current_time
                last_completed_count = current_completed
                logging.info(f"📈 处理进度: {current_completed}/{self.stats['total_tasks']} 完成")
            
            # 如果30分钟没有任何进展，警告但不退出（让任务自然超时）
            elif current_time - last_progress_time > 1800:  # 30分钟
                logging.warning(f"⚠️ 30分钟无进展，当前状态: {current_completed}/{self.stats['total_tasks']} 完成")
                logging.warning(f"   活跃任务: 缓存{len(self.active_tasks[PipelineStage.CACHING])}, "
                              f"处理{len(self.active_tasks[PipelineStage.PROCESSING])}")
                last_progress_time = current_time  # 重置时间，避免重复警告
                
            with self.pipeline_condition:
                self.pipeline_condition.wait(timeout=10.0)  # 增加等待间隔
                
            if self.shutdown_event.is_set():
                logging.info("🛑 收到关闭信号，退出等待")
                break
                
        logging.info("✅ 所有任务已完成")
        return True
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self.task_lock:
            active_stats = {stage.value: len(tasks) for stage, tasks in self.active_tasks.items()}
            
        return {
            **self.stats,
            'active_by_stage': active_stats,
            'total_active': sum(active_stats.values()),
            'queue_sizes': {
                'pending': self.pending_queue.qsize(),
                'cache': self.cache_queue.qsize(),
                'process': self.process_queue.qsize(),
                'completed': self.completed_queue.qsize(),
                'failed': self.failed_queue.qsize()
            }
        }
    
    def shutdown(self):
        """关闭流水线"""
        logging.info("🛑 开始关闭视频处理流水线...")
        
        self.shutdown_event.set()
        
        with self.pipeline_condition:
            self.pipeline_condition.notify_all()
        
        # 等待线程结束
        all_threads = self.cache_threads + self.check_threads + self.process_threads
        if self.monitor_thread:
            all_threads.append(self.monitor_thread)
            
        for thread in all_threads:
            if thread.is_alive():
                thread.join(timeout=5.0)
                
        logging.info("✅ 视频处理流水线已关闭")
    
    def cleanup(self):
        """清理资源"""
        if not self.shutdown_event.is_set():
            self.shutdown()


# ==================== 2025年顶尖NAS优化核心类 ====================

class DatabaseManager:
    """数据库连接管理器 - 多电脑协作核心"""
    
    def __init__(self):
        self.connection_pool = None
        self.is_enabled = ENABLE_MULTI_COMPUTER_SYNC
        if self.is_enabled:
            self._create_connection_pool()
    
    def _create_connection_pool(self):
        """创建数据库连接池"""
        try:
            self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name="video_editing_pool",
                pool_size=5,
                **MYSQL_CONFIG
            )
            logger.info("🔗 数据库连接池创建成功")
            
            # 测试连接
            with self.get_connection() as conn:
                if conn and conn.is_connected():
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    logger.info("✅ 数据库连接测试成功")
                else:
                    raise Exception("数据库连接测试失败")
                    
        except Error as e:
            logger.error(f"❌ 数据库连接池创建失败: {e}")
            self.is_enabled = False
            self.connection_pool = None
    
    def get_connection(self, retries=3):
        """获取数据库连接（带重试机制）"""
        if not self.is_enabled or not self.connection_pool:
            return None
            
        for attempt in range(retries):
            try:
                connection = self.connection_pool.get_connection()
                if connection and connection.is_connected():
                    # 测试连接健康状态
                    try:
                        connection.ping(reconnect=True)
                        return connection
                    except Error:
                        connection.close()
                        continue
                        
            except Error as e:
                if attempt == retries - 1:  # 最后一次尝试
                    logger.error(f"❌ 获取数据库连接失败 (尝试 {attempt + 1}/{retries}): {e}")
                    # 尝试重新创建连接池
                    if e.errno in [2006, 2013]:  # MySQL server has gone away / Lost connection
                        logger.info("🔄 尝试重新创建数据库连接池...")
                        self._create_connection_pool()
                else:
                    logger.warning(f"⚠️ 获取数据库连接失败 (尝试 {attempt + 1}/{retries}): {e}")
                    time.sleep(0.5 * (attempt + 1))  # 指数退避
                    
        return None
    
    def is_available(self) -> bool:
        """检查数据库是否可用"""
        return self.is_enabled and self.connection_pool is not None
    
    def health_check(self) -> bool:
        """检查数据库连接池健康状态"""
        if not self.is_available():
            return False
            
        try:
            with self.get_connection() as conn:
                if conn and conn.is_connected():
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    return True
        except Exception as e:
            logger.warning(f"⚠️ 数据库健康检查失败: {e}")
            
        return False
    
    def rebuild_pool_if_needed(self) -> bool:
        """如果连接池不健康，尝试重建"""
        if not self.health_check():
            logger.info("🔄 检测到数据库连接问题，正在重建连接池...")
            self._create_connection_pool()
            return self.health_check()
        return True
    
    def initialize(self) -> bool:
        """初始化数据库管理器"""
        try:
            # 如果数据库功能未启用，直接返回True
            if not self.is_enabled:
                logger.info("🔧 数据库功能未启用，跳过初始化")
                return True
            
            # 检查连接池是否已创建
            if not self.connection_pool:
                logger.info("🔧 创建数据库连接池...")
                self._create_connection_pool()
            
            # 验证连接池状态
            if not self.is_available():
                logger.warning("⚠️ 数据库连接池不可用")
                return False
            
            # 执行健康检查
            if self.health_check():
                logger.info("✅ 数据库管理器初始化成功")
                return True
            else:
                logger.warning("⚠️ 数据库健康检查失败")
                return False
                
        except Exception as e:
            logger.error(f"❌ 数据库管理器初始化失败: {e}")
            return False
    
    def execute_with_retry(self, query: str, params: tuple = None, retries: int = 3, fetch: bool = False) -> bool:
        """执行SQL查询（带重试和事务管理）"""
        if not self.is_available():
            return False
            
        for attempt in range(retries):
            connection = None
            try:
                connection = self.get_connection()
                if not connection:
                    continue
                    
                cursor = connection.cursor()
                
                # 检查是否为DDL语句（CREATE, ALTER, DROP等）
                query_upper = query.strip().upper()
                is_ddl = query_upper.startswith(('CREATE', 'ALTER', 'DROP'))
                
                # DDL语句不需要显式事务管理
                if not is_ddl:
                    connection.start_transaction()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                result = None
                if fetch:
                    result = cursor.fetchall()
                
                # DDL语句自动提交，其他语句需要手动提交
                if not is_ddl:
                    connection.commit()
                
                cursor.close()
                return result if fetch else True
                
            except Error as e:
                if connection:
                    try:
                        connection.rollback()
                    except:
                        pass
                        
                if attempt == retries - 1:
                    logger.error(f"❌ SQL执行失败 (尝试 {attempt + 1}/{retries}): {e}")
                else:
                    logger.warning(f"⚠️ SQL执行失败 (尝试 {attempt + 1}/{retries}): {e}")
                    time.sleep(0.5 * (attempt + 1))
                    
            except Exception as e:
                if connection:
                    try:
                        connection.rollback()
                    except:
                        pass
                logger.error(f"❌ SQL执行时发生未知异常: {e}")
                break
                
            finally:
                if connection:
                    try:
                        connection.close()
                    except:
                        pass
                        
        return False
    
    def execute_batch_with_retry(self, queries: List[Tuple[str, tuple]], retries: int = 3) -> bool:
        """批量执行SQL查询（带重试和事务管理）"""
        if not self.is_available() or not queries:
            return False
            
        for attempt in range(retries):
            connection = None
            try:
                connection = self.get_connection()
                if not connection:
                    continue
                    
                cursor = connection.cursor()
                
                # 开始事务
                connection.start_transaction()
                
                # 执行所有查询
                for query, params in queries:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                
                # 提交事务
                connection.commit()
                cursor.close()
                logger.info(f"✅ 批量执行 {len(queries)} 条SQL语句成功")
                return True
                
            except Error as e:
                if connection:
                    try:
                        connection.rollback()
                    except:
                        pass
                        
                if attempt == retries - 1:
                    logger.error(f"❌ 批量SQL执行失败 (尝试 {attempt + 1}/{retries}): {e}")
                else:
                    logger.warning(f"⚠️ 批量SQL执行失败 (尝试 {attempt + 1}/{retries}): {e}")
                    time.sleep(0.5 * (attempt + 1))
                    
            except Exception as e:
                if connection:
                    try:
                        connection.rollback()
                    except:
                        pass
                logger.error(f"❌ 批量SQL执行时发生未知异常: {e}")
                break
                
            finally:
                if connection:
                    try:
                        connection.close()
                    except:
                        pass
                        
        return False
    
    def fetch_all_with_retry(self, query: str, params: tuple = None, retries: int = 3) -> list:
        """执行查询并获取所有结果（带重试）"""
        if not self.is_available():
            return []
            
        for attempt in range(retries):
            try:
                with self.get_connection() as conn:
                    if conn and conn.is_connected():
                        cursor = conn.cursor(dictionary=True)
                        
                        if params:
                            cursor.execute(query, params)
                        else:
                            cursor.execute(query)
                        
                        result = cursor.fetchall()
                        return result
                        
            except Exception as e:
                logger.warning(f"⚠️ 查询执行失败 (尝试 {attempt + 1}/{retries}): {e}")
                if attempt == retries - 1:
                    logger.error(f"❌ 查询最终失败: {query}")
                    return []
                time.sleep(0.5 * (attempt + 1))
        
        return []

    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取数据库性能指标"""
        if not self.is_available():
            return {}
            
        metrics = {}
        try:
            with self.get_connection() as conn:
                if conn and conn.is_connected():
                    cursor = conn.cursor()
                    
                    # 获取连接数
                    cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
                    result = cursor.fetchone()
                    if result:
                        metrics['active_connections'] = int(result[1])
                    
                    # 获取查询统计
                    cursor.execute("SHOW STATUS LIKE 'Questions'")
                    result = cursor.fetchone()
                    if result:
                        metrics['total_queries'] = int(result[1])
                    
                    # 获取缓冲池使用率
                    cursor.execute("SHOW STATUS LIKE 'Innodb_buffer_pool_pages_total'")
                    total_pages = cursor.fetchone()
                    cursor.execute("SHOW STATUS LIKE 'Innodb_buffer_pool_pages_free'")
                    free_pages = cursor.fetchone()
                    
                    if total_pages and free_pages:
                        total = int(total_pages[1])
                        free = int(free_pages[1])
                        used = total - free
                        metrics['buffer_pool_usage_percent'] = round((used / total) * 100, 2) if total > 0 else 0
                    
        except Exception as e:
            logger.warning(f"⚠️ 获取数据库性能指标失败: {e}")
            
        return metrics
    
    def cleanup(self):
        """清理数据库连接池资源"""
        try:
            if self.connection_pool:
                # 关闭连接池中的所有连接
                self.connection_pool.close()
                self.connection_pool = None
                logger.info("✅ 数据库连接池已关闭")
        except Exception as e:
            logger.warning(f"⚠️ 数据库连接池清理时发生异常: {e}")
        finally:
            self.is_enabled = False

class DatabaseMonitor:
    """数据库监控和诊断类"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.last_check_time = 0
        self.health_history = []
        
    def get_comprehensive_status(self) -> Dict[str, Any]:
        """获取数据库综合状态报告"""
        status = {
            'timestamp': datetime.now().isoformat(),
            'basic_health': self.db_manager.health_check(),
            'connection_pool_status': self.db_manager.is_available(),
            'performance_metrics': self.db_manager.get_performance_metrics(),
            'table_statistics': self._get_table_statistics(),
            'recent_errors': self._get_recent_errors(),
            'recommendations': []
        }
        
        # 添加建议
        if not status['basic_health']:
            status['recommendations'].append("数据库连接异常，建议检查网络和数据库服务状态")
        
        perf = status.get('performance_metrics', {})
        if perf.get('buffer_pool_usage_percent', 0) > 90:
            status['recommendations'].append("缓冲池使用率过高，建议增加innodb_buffer_pool_size")
        
        if perf.get('active_connections', 0) > 80:
            status['recommendations'].append("活跃连接数较高，建议检查连接池配置")
            
        return status
    
    def _get_table_statistics(self) -> Dict[str, Any]:
        """获取视频编辑表的统计信息"""
        if not self.db_manager.is_available():
            return {}
            
        stats = {}
        try:
            with self.db_manager.get_connection() as conn:
                if conn and conn.is_connected():
                    cursor = conn.cursor()
                    
                    # 总记录数
                    cursor.execute("SELECT COUNT(*) FROM Video_Editing")
                    result = cursor.fetchone()
                    stats['total_records'] = result[0] if result else 0
                    
                    # 成功处理数
                    cursor.execute("SELECT COUNT(*) FROM Video_Editing WHERE status = 1")
                    result = cursor.fetchone()
                    stats['successful_processes'] = result[0] if result else 0
                    
                    # 失败处理数
                    cursor.execute("SELECT COUNT(*) FROM Video_Editing WHERE status = 0")
                    result = cursor.fetchone()
                    stats['failed_processes'] = result[0] if result else 0
                    
                    # 今日处理数
                    cursor.execute("""
                        SELECT COUNT(*) FROM Video_Editing 
                        WHERE DATE(updated_time) = CURDATE()
                    """)
                    result = cursor.fetchone()
                    stats['today_processes'] = result[0] if result else 0
                    
                    # 各计算机处理统计
                    cursor.execute("""
                        SELECT computer_name, COUNT(*) as count, 
                               SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) as success_count
                        FROM Video_Editing 
                        GROUP BY computer_name
                        ORDER BY count DESC
                    """)
                    results = cursor.fetchall()
                    stats['computer_statistics'] = [
                        {
                            'computer_name': row[0],
                            'total_count': row[1],
                            'success_count': row[2],
                            'success_rate': round(row[2] / row[1] * 100, 2) if row[1] > 0 else 0
                        }
                        for row in results
                    ]
                    
        except Exception as e:
            logger.warning(f"⚠️ 获取表统计信息失败: {e}")
            
        return stats
    
    def _get_recent_errors(self) -> List[Dict[str, Any]]:
        """获取最近的错误记录"""
        if not self.db_manager.is_available():
            return []
            
        errors = []
        try:
            with self.db_manager.get_connection() as conn:
                if conn and conn.is_connected():
                    cursor = conn.cursor(dictionary=True)
                    
                    cursor.execute("""
                        SELECT video_name, log_path, updated_time, computer_name
                        FROM Video_Editing 
                        WHERE status = 0 AND updated_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                        ORDER BY updated_time DESC
                        LIMIT 10
                    """)
                    
                    results = cursor.fetchall()
                    errors = [
                        {
                            'video_name': row['video_name'],
                            'error_message': row['log_path'],
                            'time': row['updated_time'].isoformat() if row['updated_time'] else None,
                            'computer': row['computer_name']
                        }
                        for row in results
                    ]
                    
        except Exception as e:
            logger.warning(f"⚠️ 获取错误记录失败: {e}")
            
        return errors
    
    def log_health_status(self):
        """记录健康状态历史"""
        current_time = time.time()
        if current_time - self.last_check_time > 300:  # 每5分钟检查一次
            health_status = {
                'timestamp': current_time,
                'healthy': self.db_manager.health_check(),
                'metrics': self.db_manager.get_performance_metrics()
            }
            
            self.health_history.append(health_status)
            
            # 只保留最近24小时的记录
            cutoff_time = current_time - 86400
            self.health_history = [h for h in self.health_history if h['timestamp'] > cutoff_time]
            
            self.last_check_time = current_time
            
            if not health_status['healthy']:
                logger.warning("⚠️ 数据库健康检查失败，尝试重建连接池...")
                self.db_manager.rebuild_pool_if_needed()

class VideoRecordManager:
    """视频记录管理器 - 多电脑协作防重复处理"""
    
    def __init__(self, computer_name: str, db_manager: DatabaseManager, log_file_path: str = None):
        self.computer_name = computer_name
        self.db_manager = db_manager
        self.computer_ip = self._get_local_ip()
        self.log_file_path = log_file_path  # 日志文件路径
        
        # 新增查重相关配置
        self.video_extensions = ('.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm', '.m4v', '.3gp')
        self.duplicate_output_path = None  # 重复视频移动目标路径，可配置
        self.opencv_timeout = 30000  # OpenCV超时设置(毫秒)
        
        # 数据库表名配置（固定为Video_Editing）
        self.videos_table = 'Video_Editing'
        # 移除额外表，统一使用Video_Editing表
        
        # 成功插入数据库的视频计数器
        self.db_insert_success_count = 0
        self.db_insert_total_count = 0
        
        # 初始化增强数据库表
        self._create_enhanced_tables()
        
    def _get_local_ip(self) -> str:
        """获取本机IP地址"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"
    
    def _get_config_hash(self) -> str:
        """获取当前配置的哈希值"""
        config_data = {
            'enable_head_tail_cut': ENABLE_HEAD_TAIL_CUT,
            'enable_cropping': ENABLE_CROPPING,
            'head_cut_time': HEAD_CUT_TIME if ENABLE_HEAD_TAIL_CUT else 0,
            'tail_cut_time': TAIL_CUT_TIME if ENABLE_HEAD_TAIL_CUT else 0,
            'target_resolution': TARGET_RESOLUTION if ENABLE_CROPPING else None,
            'quality_mode': QUALITY_MODE
        }
        config_str = json.dumps(config_data, sort_keys=True)
        return hashlib.sha256(config_str.encode()).hexdigest()
    
    def _create_enhanced_tables(self):
        """创建增强的查重数据库表结构"""
        if not self.db_manager.is_available():
            return
        
        try:
            # 创建固定的Video_Editing表（与用户规范完全一致 + 兼容代码需求）
            TABLE_NAME = "Video_Editing"
            videos_table_sql = f"""
            CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
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
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            
            # 只创建 Video_Editing 表，移除所有额外表
            
            # 执行表创建
            success = self.db_manager.execute_with_retry(videos_table_sql)
            if success:
                logger.info(f"✅ 数据表 {TABLE_NAME} 创建/验证成功")
            else:
                logger.warning(f"⚠️ 数据表 {TABLE_NAME} 创建失败")
            
            # 检查并添加缺失的列（兼容性处理）
            self._add_missing_columns_if_needed(TABLE_NAME)
            
            # 确认表名配置
            self.videos_table = TABLE_NAME
                    
        except Exception as e:
            logger.error(f"❌ 创建增强数据库表失败: {e}")
    
    def _add_missing_columns_if_needed(self, table_name: str):
        """检查并添加缺失的列（兼容性处理）- 已禁用兼容字段添加"""
        # 不再添加任何兼容字段，保持表结构简洁
        logger.info("✅ 跳过兼容列检查 - 使用固定表结构")
        pass
    
    def calculate_video_sha256(self, video_path: str, quick_mode: bool = False) -> str:
        """计算视频文件SHA256（支持快速模式和完整模式）"""
        try:
            # 验证文件存在性和可读性
            if not os.path.exists(video_path):
                logger.error(f"文件不存在: {video_path}")
                return ""
                
            if not os.access(video_path, os.R_OK):
                logger.error(f"文件无法读取: {video_path}")
                return ""
            
            # 快速模式：使用新的快速哈希计算方法
            if quick_mode:
                return self.calculate_file_hash_fast(video_path)
            
            # 完整模式：计算整个文件的哈希值
            hash_sha256 = hashlib.sha256()
            pre_processing_size = os.path.getsize(video_path)
            
            with open(video_path, "rb") as f:
                # 分块读取，避免大文件内存占用
                bytes_read = 0
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_sha256.update(chunk)
                    bytes_read += len(chunk)
                    
                    # 验证读取进度，确保文件完整性
                    if bytes_read > pre_processing_size:
                        logger.warning(f"文件大小异常: {video_path}, 预期{pre_processing_size}, 实际读取{bytes_read}")
                        break
                        
            # 验证实际读取的字节数
            if bytes_read != pre_processing_size:
                logger.warning(f"文件读取不完整: {video_path}, 预期{pre_processing_size}, 实际{bytes_read}")
                
            return hash_sha256.hexdigest()
        except Exception as e:
            logger.error(f"计算视频SHA256失败 {video_path}: {e}")
            return ""
    
    def _calculate_quick_hash(self, video_path: str) -> str:
        """快速哈希计算（仅计算文件的关键部分，增强严谨性）"""
        try:
            pre_processing_size = os.path.getsize(video_path)
            file_mtime = os.path.getmtime(video_path)
            video_name = os.path.basename(video_path)
            
            hash_sha256 = hashlib.sha256()
            
            # 添加文件基本信息到哈希计算
            file_info = f"{video_name}|{pre_processing_size}|{file_mtime:.6f}"
            hash_sha256.update(file_info.encode('utf-8'))
            
            with open(video_path, "rb") as f:
                # 读取文件头部 1MB（包含视频元数据）
                header = f.read(1024 * 1024)
                if header:
                    hash_sha256.update(b"HEADER:")
                    hash_sha256.update(header)
                
                # 如果文件大于 10MB，读取多个采样点
                if pre_processing_size > 10 * 1024 * 1024:
                    # 读取1/4位置的1MB
                    quarter_pos = pre_processing_size // 4
                    f.seek(quarter_pos)
                    quarter = f.read(1024 * 1024)
                    if quarter:
                        hash_sha256.update(b"QUARTER:")
                        hash_sha256.update(quarter)
                    
                    # 读取中部 1MB
                    middle_pos = pre_processing_size // 2
                    f.seek(middle_pos)
                    middle = f.read(1024 * 1024)
                    if middle:
                        hash_sha256.update(b"MIDDLE:")
                        hash_sha256.update(middle)
                    
                    # 读取3/4位置的1MB
                    three_quarter_pos = pre_processing_size * 3 // 4
                    f.seek(three_quarter_pos)
                    three_quarter = f.read(1024 * 1024)
                    if three_quarter:
                        hash_sha256.update(b"THREE_QUARTER:")
                        hash_sha256.update(three_quarter)
                    
                    # 读取尾部 1MB
                    tail_pos = max(0, pre_processing_size - 1024 * 1024)
                    f.seek(tail_pos)
                    tail = f.read(1024 * 1024)
                    if tail:
                        hash_sha256.update(b"TAIL:")
                        hash_sha256.update(tail)
                
                # 添加配置信息到哈希（确保相同文件不同配置有不同哈希）
                config_hash = self._get_config_hash()
                hash_sha256.update(f"CONFIG:{config_hash}".encode())
                
            result_hash = hash_sha256.hexdigest()
            logger.debug(f"快速哈希计算完成: {video_name} -> {result_hash[:16]}...")
            return result_hash
            
        except Exception as e:
            logger.error(f"快速哈希计算失败 {video_path}: {e}")
            return ""
    
    def verify_video_integrity(self, video_path: str) -> bool:
        """验证视频文件完整性和可播放性"""
        try:
            # 基本文件检查
            if not os.path.exists(video_path):
                logger.error(f"视频文件不存在: {video_path}")
                return False
                
            if not os.access(video_path, os.R_OK):
                logger.error(f"视频文件无法读取: {video_path}")
                return False
            
            pre_processing_size = os.path.getsize(video_path)
            if pre_processing_size == 0:
                logger.error(f"视频文件为空: {video_path}")
                return False
            
            # 使用ffprobe验证视频格式和完整性
            try:
                video_info = get_video_info(video_path)
                if not video_info:
                    logger.error(f"无法获取视频信息: {video_path}")
                    return False
                
                # 检查关键信息
                duration = video_info.get('duration', 0)
                width = video_info.get('width', 0)
                height = video_info.get('height', 0)
                
                if duration <= 0:
                    logger.error(f"视频时长无效: {video_path}, 时长: {duration}")
                    return False
                    
                if width <= 0 or height <= 0:
                    logger.error(f"视频分辨率无效: {video_path}, 分辨率: {width}x{height}")
                    return False
                
                # 检查视频编码器信息
                codec = video_info.get('codec_name', '')
                if not codec:
                    logger.warning(f"无法获取视频编码器信息: {video_path}")
                
                logger.debug(f"视频完整性验证通过: {os.path.basename(video_path)} "
                           f"({width}x{height}, {duration:.2f}s, {codec})")
                return True
                
            except Exception as ffprobe_error:
                logger.error(f"ffprobe验证失败 {video_path}: {ffprobe_error}")
                return False
                
        except Exception as e:
            logger.error(f"视频完整性验证异常 {video_path}: {e}")
            return False
    
    def resolve_hash_conflict(self, video_path: str, hash_value: str, existing_record: Dict) -> str:
        """解决哈希冲突：相同哈希但不同文件的情况"""
        try:
            video_name = os.path.basename(video_path)
            existing_path = existing_record.get('input_path', '')
            existing_name = existing_record.get('video_name', '')
            
            logger.warning(f"⚠️ 检测到哈希冲突:")
            logger.warning(f"   当前文件: {video_path}")
            logger.warning(f"   已存在文件: {existing_path}")
            logger.warning(f"   哈希值: {hash_value[:16]}...")
            
            # 策略1：如果是相同路径，认为是同一文件
            if video_path == existing_path:
                logger.info(f"✅ 确认为同一文件 (路径完全匹配)")
                return "SAME_FILE"
            
            # 策略2：如果文件名相同且大小相同，可能是移动了位置
            current_size = os.path.getsize(video_path)
            existing_size = existing_record.get('pre_processing_size', 0)
            
            if video_name == existing_name and current_size == existing_size:
                logger.info(f"✅ 可能是相同文件的不同位置 (名称+大小匹配)")
                return "MOVED_FILE"
            
            # 策略3：重新计算完整哈希进行对比
            logger.info(f"🔄 重新计算完整哈希进行精确对比...")
            full_hash = self.calculate_video_sha256(video_path, quick_mode=False)
            
            if full_hash == hash_value:
                logger.warning(f"⚠️ 真实哈希冲突 - 需要人工干预")
                return "REAL_CONFLICT"
            else:
                logger.info(f"✅ 快速哈希冲突，完整哈希不同 - 为不同文件")
                return "DIFFERENT_FILE"
                
        except Exception as e:
            logger.error(f"解决哈希冲突失败: {e}")
            return "RESOLUTION_ERROR"
    
    def quick_database_check_before_cache(self, video_path: str) -> Tuple[bool, Optional[Dict]]:
        """缓存前快速数据库检查 - 根据电脑号和视频名快速判断是否已处理"""
        if not self.db_manager.is_available():
            return False, None
        
        video_name = os.path.basename(video_path)
        
        try:
            # 快速查询：按照电脑号和视频名查找 status=1 的记录
            query = """
                SELECT id, computer_name, computer_ip, video_name, output_path, updated_time
                FROM Video_Editing 
                WHERE video_name = %s AND status = 1
                ORDER BY updated_time DESC
                LIMIT 1
            """
            params = (video_name,)
            results = self.db_manager.fetch_all_with_retry(query, params)
            
            if results and len(results) > 0:
                record = results[0]
                db_record = {
                    'id': record[0],
                    'computer_name': record[1], 
                    'computer_ip': record[2],
                    'video_name': record[3],
                    'output_path': record[4],
                    'updated_time': record[5]
                }
                
                # 如果是本电脑处理的，直接跳过
                if record[1] == self.computer_name:
                    logging.info(f"🔍 快速检查: 本电脑已处理过 {video_name}")
                    return True, db_record
                
                # 如果是其他电脑处理的，也跳过（避免重复处理）
                logging.info(f"🔍 快速检查: 其他电脑({record[1]})已处理过 {video_name}")
                return True, db_record
            
            # 未找到已处理记录
            logging.debug(f"🔍 快速检查: {video_name} 未找到处理记录，需要缓存")
            return False, None
            
        except Exception as e:
            logging.error(f"快速数据库检查失败 {video_name}: {e}")
            return False, None

    def is_video_processed(self, video_path: str, hash_value: str = None) -> Tuple[bool, Optional[Dict]]:
        """检查视频是否已被处理（使用新的组合查重方法）"""
        if not self.db_manager.is_available():
            return False, None
        
        # 第一步：验证视频文件完整性
        if not self.verify_video_integrity(video_path):
            logger.error(f"视频文件完整性验证失败，跳过处理: {video_path}")
            return False, None
            
        # 获取文件基本信息
        try:
            pre_processing_size = os.path.getsize(video_path)
            video_name = os.path.basename(video_path)
            video_info = self.get_video_info_fast(video_path)
            resolution = f"{video_info.get('width', 0)}x{video_info.get('height', 0)}" if video_info else "unknown"
        except Exception as e:
            logger.error(f"获取文件信息失败 {video_path}: {e}")
            return False, None
            
        if not hash_value:
            # 使用快速哈希进行数据库查询
            hash_value = self.calculate_file_hash_fast(video_path)
            if not hash_value:
                logger.warning(f"无法计算哈希值: {video_path}")
                return False, None
        
        # 使用新的查重方法：check_if_processed_successfully（四重验证）
        logger.info(f"🔍 开始新查重检查: {video_name} | 大小:{pre_processing_size} | 分辨率:{resolution}")
        try:
            is_processed = self.check_if_processed_successfully(
                video_name=video_name,
                pre_processing_size=pre_processing_size,
                resolution=resolution,
                hash_value=hash_value
            )
            
            if is_processed:
                # 获取详细记录信息
                detail_query = """
                    SELECT id, input_path, output_path, video_name, pre_processing_size, 
                           resolution, hash_value, operator, computer_name, computer_ip, 
                           status, created_time, updated_time
                    FROM Video_Editing 
                    WHERE video_name = %s 
                    AND pre_processing_size = %s 
                    AND resolution = %s
                    AND hash_value = %s
                    AND status = 1
                    ORDER BY updated_time DESC LIMIT 1
                """
                params = (video_name, pre_processing_size, resolution, hash_value)
                results = self.db_manager.fetch_all_with_retry(detail_query, params)
                record = results[0] if results else None
                
                if record:
                    logger.info(f"🔍 发现已处理视频 (四重匹配): {video_name} "
                              f"(处理者: {record.get('computer_name', 'unknown')}, "
                              f"处理时间: {record.get('updated_time', 'unknown')})")
                    return True, record
                else:
                    logger.info(f"🔍 发现已处理视频 (四重匹配): {video_name}")
                    return True, None
            
            return False, None
            
        except Exception as e:
            logger.error(f"查重检查失败 {video_path}: {e}")
            return False, None
    
    def is_video_processing(self, video_path: str, hash_value: str = None) -> Tuple[bool, Optional[Dict]]:
        """检查视频是否正在被其他电脑处理（使用增强的重试机制）"""
        if not self.db_manager.is_available():
            return False, None
            
        if not hash_value:
            hash_value = self.calculate_video_sha256(video_path)
            if not hash_value:
                return False, None
        
        # 在新表结构中，我们只检查是否存在相同hash_value且状态为成功的记录
        # 如果不存在，则认为可以处理（简化逻辑，避免复杂的正在处理状态管理）
        query = """
            SELECT * FROM Video_Editing 
            WHERE hash_value = %s AND status = 1
            ORDER BY updated_time DESC LIMIT 1
        """
        
        results = self.db_manager.fetch_all_with_retry(query, (hash_value,))
        result = results[0] if results else None
        
        if result:
            logger.info(f"🔄 视频已被处理: {os.path.basename(video_path)} "
                      f"(处理者: {result['computer_name']}, "
                      f"完成时间: {result['updated_time']})")
            return True, result
            
        return False, None
    
    def _cleanup_zombie_record(self, record_id: int):
        """清理僵尸处理记录（在新表结构中，直接删除记录）"""
        try:
            with self.db_manager.get_connection() as connection:
                if not connection or not connection.is_connected():
                    return
                
                cursor = connection.cursor()
                cursor.execute(
                    "DELETE FROM Video_Editing WHERE id = %s",
                    (record_id,)
                )
                connection.commit()
                
        except Error as e:
            error_code = getattr(e, 'errno', 0)
            logger.error(f"❌ 清理僵尸记录失败 (错误代码: {error_code}): {e}")
        except Exception as e:
            logger.error(f"❌ 清理僵尸记录时发生未知异常: {e}")
    
    def start_processing(self, video_path: str, hash_value: str = None, 
                        video_info: Dict = None) -> bool:
        """标记视频开始处理（在新表结构中暂时跳过，处理完成后直接插入成功记录）"""
        # 在新的表结构中，我们简化流程：不插入处理中状态，直接在完成时插入成功记录
        logger.info(f"📝 准备处理: {os.path.basename(video_path)}")
        return True
    
    def complete_processing(self, video_path: str, output_path: str, 
                          processing_time: float, hash_value: str = None) -> bool:
        """标记视频处理完成（使用增强的重试机制）"""
        if not self.db_manager.is_available():
            return True
            
        if not hash_value:
            hash_value = self.calculate_video_sha256(video_path)
            if not hash_value:
                return True
        
        video_name = os.path.basename(video_path)
        
        # 获取视频信息
        pre_processing_size = os.path.getsize(video_path) if os.path.exists(video_path) else 0
        resolution = None
        try:
            video_info = get_video_info(video_path)
            if video_info:
                width = video_info.get('width', 0)
                height = video_info.get('height', 0)
                if width and height:
                    resolution = f"{width}x{height}"
        except Exception as e:
            logger.warning(f"⚠️ 获取视频信息失败: {e}")
        
        # 使用增强的数据库执行方法
        insert_query = """
            INSERT INTO Video_Editing 
            (input_path, output_path, video_name, pre_processing_size, resolution, 
             hash_value, operator, computer_name, computer_ip, status, log_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s)
            ON DUPLICATE KEY UPDATE
            output_path = VALUES(output_path),
            status = 1,
            updated_time = CURRENT_TIMESTAMP
        """
        
        params = (video_path, output_path, video_name, pre_processing_size, resolution, hash_value,
                 PROCESSOR_NAME, self.computer_name, self.computer_ip, self.log_file_path)
        
        if self.db_manager.execute_with_retry(insert_query, params):
            logger.info(f"✅ 标记处理完成: {video_name} (处理时间: {processing_time:.2f}s)")
            return True
        else:
            logger.error(f"❌ 标记处理完成失败: {video_name}")
            return True  # 仍然返回True，不影响视频处理流程
    
    def fail_processing(self, video_path: str, error_message: str, 
                       hash_value: str = None) -> bool:
        """标记视频处理失败（使用增强的重试机制）"""
        if not self.db_manager.is_available():
            return True
            
        if not hash_value:
            hash_value = self.calculate_video_sha256(video_path)
            if not hash_value:
                return True
        
        video_name = os.path.basename(video_path)
        
        # 获取视频信息
        pre_processing_size = os.path.getsize(video_path) if os.path.exists(video_path) else 0
        resolution = None
        try:
            video_info = get_video_info(video_path)
            if video_info:
                width = video_info.get('width', 0)
                height = video_info.get('height', 0)
                if width and height:
                    resolution = f"{width}x{height}"
        except Exception as e:
            logger.warning(f"⚠️ 获取视频信息失败: {e}")
        
        # 使用增强的数据库执行方法
        insert_query = """
            INSERT INTO Video_Editing 
            (input_path, output_path, video_name, pre_processing_size, resolution, 
             hash_value, operator, computer_name, computer_ip, status, log_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s)
            ON DUPLICATE KEY UPDATE
            status = 0,
            updated_time = CURRENT_TIMESTAMP
        """
        
        # 限制错误信息长度，避免数据库字段溢出
        truncated_error = error_message[:1000] if error_message else None
        
        params = (video_path, None, video_name, pre_processing_size, resolution, hash_value,
                 PROCESSOR_NAME, self.computer_name, self.computer_ip, self.log_file_path)
        
        if self.db_manager.execute_with_retry(insert_query, params):
            logger.info(f"❌ 标记处理失败: {video_name} (错误: {error_message[:100]}...)")
            return True
        else:
            logger.error(f"❌ 标记处理失败记录失败: {video_name}")
            return True  # 仍然返回True，不影响视频处理流程
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """获取处理统计信息（使用增强的重试机制）"""
        if not self.db_manager.is_available():
            return {}
        
        # 总体统计查询
        status_query = """
            SELECT 
                status,
                COUNT(*) as count
            FROM Video_Editing 
            WHERE created_time > DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY status
        """
        
        # 各电脑统计查询
        computer_query = """
            SELECT 
                computer_name,
                COUNT(*) as total,
                SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) as completed
            FROM Video_Editing 
            WHERE created_time > DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY computer_name
            ORDER BY completed DESC
        """
        
        # 使用增强的数据库查询方法
        status_stats = self.db_manager.fetch_all_with_retry(status_query)
        computer_stats = self.db_manager.fetch_all_with_retry(computer_query)
        
        return {
            'status_stats': status_stats if status_stats else [],
            'computer_stats': computer_stats if computer_stats else []
        }
    
    # ================== 新增智能查重系统方法 ==================
    
    def check_if_processed_successfully(self, video_name, pre_processing_size, resolution, hash_value):
        """
        核心检查1阶段：视频名、大小、分辨率和哈希值联合查重判断
        只有所有条件都匹配才认为该视频已被成功处理过。
        """
        if not all([video_name, pre_processing_size, resolution, hash_value]):
            logger.warning("去重检查缺少必要参数，无法执行。")
            return False
        
        if not self.db_manager.is_available():
            logger.error("无法连接数据库，跳过去重检查。")
            return False  # 无法连接数据库，默认未处理，以免漏处理
        
        try:
            # 【保险检查】：SQL查询确保每个WHERE条件
            query = """
                SELECT id FROM Video_Editing
                WHERE video_name = %s
                AND pre_processing_size = %s  
                AND resolution = %s
                AND hash_value = %s
                AND status = 1
            """
            params = (video_name, pre_processing_size, resolution, hash_value)
            results = self.db_manager.fetch_all_with_retry(query, params)
            
            if len(results) > 0:
                logger.info(f"✅ 四重验证匹配成功: {video_name}")
                return True
            else:
                logger.debug(f"❌ 四重验证未匹配: {video_name}")
                return False
        except Exception as e:
            logger.error(f"检查视频处理状态出错: {e}")
            return False

    def calculate_file_hash_fast(self, input_path: str, chunk_size: int = 1048576, sample_size: int = 3) -> str:
        """
        快速计算文件哈希值，带有5次重试逻辑
        对于大文件，直接采样头文件、中、尾计算哈希，以提高效率。
        """
        max_retries = 5  # 【新增】定义最大重试次数
        retry_delay = 5   # 【新增】定义重试间隔时间（秒）
        
        for attempt in range(max_retries):
            try:
                pre_processing_size = os.path.getsize(input_path)
                hash_sha256 = hashlib.sha256()

                # 对于小文件，直接计算完整哈希
                if pre_processing_size <= chunk_size * sample_size:
                    with open(input_path, "rb") as f:
                        for chunk in iter(lambda: f.read(chunk_size), b""):
                            hash_sha256.update(chunk)
                    return hash_sha256.hexdigest()  # 成功则直接返回

                # 对于大文件，采样计算
                with open(input_path, "rb") as f:
                    # 读取开头
                    f.seek(0)
                    hash_sha256.update(f.read(chunk_size))

                    # 读取中间部分（多个采样点）
                    for i in range(1, sample_size):
                        pos = int(pre_processing_size * i / sample_size) 
                        f.seek(max(0, pos - chunk_size // 2))
                        hash_sha256.update(f.read(chunk_size))

                    # 读取结尾
                    f.seek(max(0, pre_processing_size - chunk_size))
                    hash_sha256.update(f.read(chunk_size))

                # 添加文件大小信息，提高哈希唯一性
                hash_sha256.update(str(pre_processing_size).encode())
                return hash_sha256.hexdigest()  # 成功则直接返回
            except Exception as e:
                # 【新增】进行错误分析和重试策略
                logger.warning(f"计算文件哈希失败 (尝试 {attempt + 1}/{max_retries}): {input_path} - {e}")
                if attempt < max_retries - 1:
                    logger.info(f"等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                else:
                    # 【新增】如果所有重试都失败，返回 None
                    logger.error(f"计算文件哈希彻底失败: {input_path}")
                    return None
        
        # 【新增】兜底，如果所有重试都失败，确保返回 None
        return None

    def calculate_file_hash_full(self, input_path: str, chunk_size: int = 1048576) -> str:
        """计算文件的完整SHA256哈希值"""
        try:
            hash_sha256 = hashlib.sha256()
            with open(input_path, "rb") as f:
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            logger.error(f"计算文件哈希失败 {input_path}: {e}")
            return None

    def get_video_info_with_ffmpeg(self, input_path: str) -> Dict[str, Any]:
        """使用FFmpeg获取视频信息（备选方案）"""
        try:
            # 检查FFmpeg是否可用
            result = subprocess.run(
                ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', input_path],
                capture_output=True, text=True, timeout=30)

            if result.returncode != 0:
                return None

            info = json.loads(result.stdout)

            # 提取视频流信息
            video_stream = None
            for stream in info.get('streams', []):
                if stream.get('codec_type') == 'video':
                    video_stream = stream
                    break

            if not video_stream:
                return None

            # 获取视频信息
            pre_processing_size = os.path.getsize(input_path)
            duration = float(info['format'].get('duration', 0))
            frame_rate = self._parse_frame_rate(video_stream.get('r_frame_rate', '0/0'))
            width = video_stream.get('width', 0)
            height = video_stream.get('height', 0)
            resolution = f"{width}x{height}"

            return {
                'pre_processing_size': pre_processing_size,
                'duration': duration,
                'frame_rate': frame_rate,
                'resolution': resolution
            }
        except Exception as e:
            logger.warning(f"使用FFmpeg获取视频信息失败 {input_path}: {e}")
            return None

    def _parse_frame_rate(self, rate_str: str) -> float:
        """解析帧率字符串"""
        try:
            if '/' in rate_str:
                num, den = rate_str.split('/')
                if float(den) != 0:
                    return float(num) / float(den)
            return float(rate_str) if rate_str else 0
        except:
            return 0

    def get_video_info_fast(self, input_path: str) -> Dict[str, Any]:
        """快速获取视频信息（优化版本）"""
        # 首先尝试使用现有的get_video_info函数
        try:
            video_info = get_video_info(input_path)
            if video_info:
                # 转换为标准格式
                pre_processing_size = os.path.getsize(input_path)
                duration = video_info.get('duration', 0)
                frame_rate = video_info.get('fps', 0)
                width = video_info.get('width', 0)
                height = video_info.get('height', 0)
                resolution = f"{width}x{height}"

                return {
                    'pre_processing_size': pre_processing_size,
                    'duration': duration,
                    'frame_rate': frame_rate,
                    'resolution': resolution
                }
        except Exception as e:
            logger.warning(f"使用get_video_info获取视频信息失败: {e}")

        # 如果失败，尝试使用FFmpeg
        return self.get_video_info_with_ffmpeg(input_path)

    def check_file_already_processed(self, input_path: str) -> Dict[str, Any]:
        """检查文件是否已经处理过（通过文件路径）"""
        if not self.db_manager.is_available():
            return None
            
        try:
            query = f"""
                SELECT id, hash_value, pre_processing_size, created_time 
                FROM {self.videos_table} 
                WHERE input_path = %s
            """
            
            results = self.db_manager.fetch_all_with_retry(query, (input_path,))
            result = results[0] if results else None
            return result
        except Exception as e:
            logger.error(f"检查文件是否已处理失败: {e}")
            return None

    def check_duplicate_by_attributes(self, video_info: Dict[str, Any], exclude_path: str = None) -> List[Dict[str, Any]]:
        """根据属性检查是否存在重复视频（排除指定路径）"""
        if not self.db_manager.is_available():
            return []
            
        try:
            if exclude_path:
                query = f"""
                    SELECT id, hash_value, input_path, video_name 
                    FROM {self.videos_table} 
                    WHERE pre_processing_size = %s AND resolution = %s
                    AND input_path != %s
                    AND status = 1
                """
                params = (
                    video_info['pre_processing_size'],
                    video_info['resolution'],
                    exclude_path
                )
            else:
                query = f"""
                    SELECT id, hash_value, input_path, video_name 
                    FROM {self.videos_table} 
                    WHERE pre_processing_size = %s AND resolution = %s
                    AND status = 1
                """
                params = (
                    video_info['pre_processing_size'],
                    video_info['resolution']
                )

            results = self.db_manager.fetch_all_with_retry(query, params)
            return results if results else []
        except Exception as e:
            logger.error(f"检查重复视频失败: {e}")
            return []

    # 移除record_to_tobe_deleted方法 - 不再使用额外表

    def move_duplicate_video(self, input_path: str, moved_path: str) -> Tuple[bool, str]:
        """移动重复视频到指定目录"""
        if not moved_path or not self.duplicate_output_path:
            logger.warning(f"未配置重复视频输出路径，跳过移动: {input_path}")
            return False, moved_path
            
        try:
            # 确保目标目录存在
            os.makedirs(os.path.dirname(moved_path), exist_ok=True)

            # 如果目标文件已存在，添加后缀
            counter = 1
            original_moved_path = moved_path
            while os.path.exists(moved_path):
                name, ext = os.path.splitext(original_moved_path)
                moved_path = f"{name}_{counter}{ext}"
                counter += 1

            # 移动文件
            shutil.move(input_path, moved_path)
            logger.info(f"成功移动重复视频: {os.path.basename(input_path)} -> {moved_path}")
            return True, moved_path
        except Exception as e:
            logger.error(f"移动重复视频失败 {input_path}: {e}")
            return False, moved_path

    # 移除record_delete_fail方法 - 不再使用额外表

    # 移除record_problem_video方法 - 不再使用额外表

    def insert_video_record(self, input_path: str, video_name: str, hash_value: str, video_info: Dict[str, Any]) -> bool:
        """插入视频记录到数据库 - 带重试机制"""
        import time
        
        self.db_insert_total_count += 1  # 统计总尝试次数
        
        if not self.db_manager.is_available():
            return True
            
        max_retries = 5  # 最大重试次数
        retry_delays = [1, 2, 5, 10, 30]  # 重试间隔（秒）
        
        for attempt in range(max_retries):
            try:
                query = f"""
                    INSERT INTO {self.videos_table} (input_path, output_path, video_name, pre_processing_size, resolution, hash_value, operator, computer_name, computer_ip, status, log_path)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    video_name = VALUES(video_name),
                    pre_processing_size = VALUES(pre_processing_size),
                    resolution = VALUES(resolution),
                    updated_time = CURRENT_TIMESTAMP
                """
                
                params = (
                    input_path,  # input_path (required field)
                    None,       # output_path (null for video records)
                    video_name,  # video_name (required field)
                    video_info.get('pre_processing_size', 0),  # pre_processing_size
                    video_info.get('resolution', ''),  # resolution
                    hash_value, # hash_value (required field)
                    PROCESSOR_NAME,  # operator
                    self.computer_name,  # computer_name
                    getattr(self, 'computer_ip', ''),  # computer_ip
                    1,  # status (required field, 1 = success)
                    getattr(self, 'log_file_path', '')  # log_path
                )
                
                if self.db_manager.execute_with_retry(query, params):
                    self.db_insert_success_count += 1  # 统计成功次数
                    if attempt > 0:
                        logger.info(f"✅ 重试成功插入视频记录: {video_name} (第{attempt+1}次尝试) (成功: {self.db_insert_success_count}/{self.db_insert_total_count})")
                    else:
                        logger.info(f"✅ 成功插入/更新视频记录: {video_name} (成功: {self.db_insert_success_count}/{self.db_insert_total_count})")
                    return True
                else:
                    if attempt < max_retries - 1:
                        delay = retry_delays[attempt]
                        logger.warning(f"⚠️ 插入视频记录失败: {video_name} (第{attempt+1}次尝试)，{delay}秒后重试...")
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"❌ 插入视频记录最终失败: {video_name} (已尝试{max_retries}次)")
                        return False
                        
            except Exception as e:
                if attempt < max_retries - 1:
                    delay = retry_delays[attempt]
                    logger.warning(f"⚠️ 插入视频记录异常: {video_name} (第{attempt+1}次尝试) - {e}，{delay}秒后重试...")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"❌ 插入视频记录最终异常: {video_name} (已尝试{max_retries}次) - {e}")
                    return False
        
        return False
    
    def get_db_insert_statistics(self) -> Dict[str, int]:
        """获取数据库插入统计信息"""
        return {
            'success_count': self.db_insert_success_count,
            'total_count': self.db_insert_total_count,
            'failed_count': self.db_insert_total_count - self.db_insert_success_count,
            'success_rate': (self.db_insert_success_count / self.db_insert_total_count * 100) if self.db_insert_total_count > 0 else 0
        }
    
    def reset_db_insert_statistics(self):
        """重置数据库插入统计信息"""
        self.db_insert_success_count = 0
        self.db_insert_total_count = 0

    def record_processing_result(self, video_path: str, input_sha256: str = None, 
                               output_path: str = None, output_sha256: str = None,
                               processing_status: str = 'completed', metadata: Dict = None):
        """记录视频处理结果到 Video_Editing 表
        
        Args:
            video_path: 原始视频路径
            input_sha256: 输入文件SHA256
            output_path: 输出文件路径
            output_sha256: 输出文件SHA256
            processing_status: 处理状态 ('completed', 'failed', 'skipped')
            metadata: 额外元数据 (如ROI信息)
        """
        if not self.db_manager.is_available():
            return
            
        try:
            import socket
            import getpass
            
            # 获取视频基本信息
            video_name = os.path.basename(video_path)
            pre_processing_size = os.path.getsize(video_path) if os.path.exists(video_path) else 0
            
            # 如果没有提供输入哈希，尝试计算
            if not input_sha256 and os.path.exists(video_path):
                try:
                    input_sha256 = self.calculate_video_sha256(video_path, quick_mode=True)
                except Exception as e:
                    logger.debug(f"无法计算输入文件哈希: {e}")
                    input_sha256 = ""
                    
            # 获取视频分辨率信息
            resolution = "0x0"
            try:
                video_info = self.get_video_info_fast(video_path)
                if video_info:
                    resolution = video_info.get('resolution', '0x0')
            except Exception as e:
                logger.debug(f"无法获取视频分辨率: {e}")
            
            # 获取系统信息
            try:
                computer_name = COMPUTER_NAME if COMPUTER_NAME else socket.gethostname()
                computer_ip = COMPUTER_IP if COMPUTER_IP else socket.gethostbyname(socket.gethostname())
                operator = PROCESSOR_NAME
            except Exception as e:
                logger.debug(f"无法获取系统信息: {e}")
                computer_name = "unknown"
                computer_ip = "unknown"
                operator = "unknown"
            
            # 处理状态映射
            status_mapping = {
                'completed': 1,  # 成功
                'failed': 0,     # 失败
                'skipped': 0     # 跳过视为失败
            }
            status = status_mapping.get(processing_status, 0)
            
            # 插入到 Video_Editing 表
            query = """
                INSERT INTO Video_Editing (
                    input_path, output_path, video_name, pre_processing_size, 
                    resolution, hash_value, operator, computer_name, computer_ip, status, log_path
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                output_path = VALUES(output_path),
                video_name = VALUES(video_name),
                pre_processing_size = VALUES(pre_processing_size),
                resolution = VALUES(resolution),
                operator = VALUES(operator),
                computer_name = VALUES(computer_name),
                computer_ip = VALUES(computer_ip),
                status = VALUES(status),
                log_path = VALUES(log_path),
                updated_time = CURRENT_TIMESTAMP
            """
            
            params = (
                video_path,                    # input_path
                output_path or '',             # output_path
                video_name,                    # video_name
                pre_processing_size,           # pre_processing_size
                resolution,                    # resolution
                input_sha256 or '',           # hash_value
                operator,                     # operator
                computer_name,                # computer_name
                computer_ip,                  # computer_ip
                status,                       # status
                self.log_file_path or ''      # log_path
            )
            
            # 带重试机制的执行
            max_retries = 5
            retry_delays = [1, 2, 5, 10, 30]
            
            for attempt in range(max_retries):
                try:
                    success = self.db_manager.execute_with_retry(query, params)
                    
                    if success:
                        if attempt > 0:
                            logger.info(f"✅ 重试成功记录处理结果到Video_Editing表: {video_name} -> {processing_status} (第{attempt+1}次尝试)")
                        else:
                            logger.info(f"✅ 成功记录处理结果到Video_Editing表: {video_name} -> {processing_status}")
                        break
                    else:
                        if attempt < max_retries - 1:
                            delay = retry_delays[attempt]
                            logger.warning(f"⚠️ 记录处理结果到Video_Editing表失败: {video_name} (第{attempt+1}次尝试)，{delay}秒后重试...")
                            import time
                            time.sleep(delay)
                            continue
                        else:
                            logger.error(f"❌ 记录处理结果到Video_Editing表最终失败: {video_name} (已尝试{max_retries}次)")
                            
                except Exception as e:
                    if attempt < max_retries - 1:
                        delay = retry_delays[attempt]
                        logger.warning(f"⚠️ 记录处理结果异常: {video_name} (第{attempt+1}次尝试) - {e}，{delay}秒后重试...")
                        import time
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"❌ 记录处理结果最终异常: {video_name} (已尝试{max_retries}次) - {e}")
                        break
                
        except Exception as e:
            logger.error(f"记录处理结果异常 {os.path.basename(video_path)}: {e}")

    def set_duplicate_output_path(self, output_path: str):
        """设置重复视频移动目标路径"""
        self.duplicate_output_path = output_path
        if output_path and not os.path.exists(output_path):
            try:
                os.makedirs(output_path, exist_ok=True)
                logger.info(f"创建重复视频输出目录: {output_path}")
            except Exception as e:
                logger.error(f"创建重复视频输出目录失败: {e}")

    def enhanced_duplicate_check(self, video_path: str) -> Tuple[bool, str]:
        """
        增强的重复检查流程
        返回: (是否为重复, 处理结果消息)
        """
        video_name = os.path.basename(video_path)
        
        try:
            # 第一步：检查文件是否已经处理过
            existing_record = self.check_file_already_processed(video_path)
            if existing_record:
                # 检查文件是否被修改（通过文件大小）
                current_pre_processing_size = os.path.getsize(video_path)
                if existing_record['pre_processing_size'] == current_pre_processing_size:
                    logger.info(f"文件未修改，跳过: {video_name}")
                    return True, "文件已存在且未修改"
                else:
                    logger.info(f"文件已修改，重新处理: {video_name}")

            # 第二步：计算文件哈希值 (使用快速SHA256)
            hash_value = self.calculate_file_hash_fast(video_path)
            if not hash_value:
                error_msg = "无法计算文件哈希值"
                logger.error(f"{error_msg}: {video_name}")
                # 直接记录到日志，不再使用额外表
                return False, error_msg

            # 第三步：获取视频信息
            video_info = self.get_video_info_fast(video_path)
            if not video_info:
                error_msg = "无法获取视频信息"
                logger.error(f"{error_msg}: {video_name}")
                # 直接记录到日志，不再使用额外表
                return False, error_msg

            # 第四步：检查是否存在属性相同的视频（排除当前文件）
            duplicates = self.check_duplicate_by_attributes(video_info, exclude_path=video_path)

            if duplicates:
                logger.info(f"找到 {len(duplicates)} 个属性相同的视频: {video_name}")

                for duplicate in duplicates:
                    # 检查哈希值是否也相同
                    if duplicate['hash_value'] == hash_value:
                        logger.info(f"发现完全重复的视频: {duplicate['video_name']}")

                        # 生成移动后的路径（如果配置了重复视频输出路径）
                        moved_path = None
                        if self.duplicate_output_path:
                            file_ext = os.path.splitext(video_name)[1]
                            new_filename = f"{hash_value}{file_ext}"
                            moved_path = os.path.join(self.duplicate_output_path, new_filename)
                        
                        # 如果配置了输出路径，则移动文件
                        if moved_path:
                            success, final_path = self.move_duplicate_video(video_path, moved_path)
                            if not success:
                                logger.error(f"移动重复视频失败: {video_name}")
                                return True, "发现重复，但移动失败"
                            logger.info(f"重复视频已移动: {video_name} -> {final_path}")
                        else:
                            logger.info(f"发现重复视频但未配置移动路径: {video_name}")

                        return True, "发现重复视频，已处理"

            # 第五步：插入当前视频记录 - 必须成功才能继续
            if existing_record:
                # 更新现有记录
                success = self.insert_video_record(video_path, video_name, hash_value, video_info)
            else:
                # 插入新记录
                success = self.insert_video_record(video_path, video_name, hash_value, video_info)
                
            if success:
                return False, "新视频，已记录到数据库"
            else:
                # 数据库插入失败，等待并重新尝试
                logger.error(f"❌ 数据库插入失败，视频将等待处理: {video_name}")
                return True, "数据库插入失败，跳过处理等待重试"
                
        except Exception as e:
            error_msg = f"查重检查异常: {str(e)}"
            logger.error(f"处理视频文件时发生错误 {video_path}: {e}")
            # 直接记录到日志，不再使用额外表
            return False, error_msg

class DatabaseSystemManager:
    """数据库系统管理器 - 整合所有数据库功能"""
    
    def __init__(self, computer_name: str, log_file_path: str = None):
        self.computer_name = computer_name
        self.db_manager = DatabaseManager()
        self.db_monitor = DatabaseMonitor(self.db_manager)
        self.record_manager = VideoRecordManager(computer_name, self.db_manager, log_file_path)
        self.last_health_check = 0
        self.health_check_interval = 300  # 5分钟检查一次
        
    def initialize_system(self) -> bool:
        """初始化数据库系统"""
        try:
            logger.info("🔧 正在初始化数据库系统...")
            
            # 初始化数据库管理器
            if not self.db_manager.initialize():
                logger.error("❌ 数据库管理器初始化失败")
                return False
            
            # 执行健康检查
            health_status = self.db_manager.health_check()
            if health_status:
                logger.info("✅ 数据库健康检查通过")
            else:
                logger.warning("⚠️ 数据库健康检查失败，但系统可继续运行")
            
            # 获取性能指标
            metrics = self.db_manager.get_performance_metrics()
            if metrics:
                logger.info(f"📊 数据库性能指标: 连接数={metrics.get('active_connections', 'N/A')}, "
                           f"缓冲池使用率={metrics.get('buffer_pool_usage_percent', 'N/A')}%")
            
            # 获取统计信息
            stats = self.record_manager.get_processing_statistics()
            if stats:
                status_stats = stats.get('status_stats', [])
                total_processed = sum(item.get('count', 0) for item in status_stats)
                logger.info(f"📈 近7天处理统计: 总计{total_processed}个视频")
            
            logger.info("✅ 数据库系统初始化完成")
            return True
            
        except Exception as e:
            logger.error(f"❌ 数据库系统初始化失败: {e}")
            return False
    
    def periodic_health_check(self):
        """定期健康检查"""
        current_time = time.time()
        if current_time - self.last_health_check > self.health_check_interval:
            try:
                # 记录健康状态
                self.db_monitor.log_health_status()
                
                # 检查连接池状态
                if not self.db_manager.is_available():
                    logger.warning("⚠️ 数据库连接池不可用，尝试重建...")
                    self.db_manager.rebuild_pool_if_needed()
                
                self.last_health_check = current_time
                
            except Exception as e:
                logger.error(f"❌ 定期健康检查失败: {e}")
    
    def get_system_status_report(self) -> Dict[str, Any]:
        """获取系统状态报告"""
        try:
            return self.db_monitor.get_comprehensive_status()
        except Exception as e:
            logger.error(f"❌ 获取系统状态报告失败: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def is_video_already_processed(self, video_path: str, hash_value: str = None) -> Tuple[bool, Optional[Dict]]:
        """检查视频是否已被处理"""
        self.periodic_health_check()  # 执行定期检查
        return self.record_manager.is_already_processed(video_path, hash_value)
    
    def mark_video_complete(self, video_path: str, output_path: str, 
                          processing_time: float, hash_value: str = None) -> bool:
        """标记视频处理完成"""
        return self.record_manager.complete_processing(video_path, output_path, processing_time, hash_value)
    
    def mark_video_failed(self, video_path: str, error_message: str, hash_value: str = None) -> bool:
        """标记视频处理失败"""
        return self.record_manager.fail_processing(video_path, error_message, hash_value)
    
    def cleanup_resources(self):
        """清理资源"""
        try:
            if hasattr(self, 'db_manager') and self.db_manager:
                self.db_manager.cleanup()
            logger.info("✅ 数据库系统资源清理完成")
        except Exception as e:
            logger.error(f"❌ 数据库系统资源清理失败: {e}")

@dataclass
class CacheEntry:
    """缓存条目数据结构"""
    video_path: str
    local_path: str
    pre_processing_size: int
    download_time: float
    last_access: float
    is_complete: bool
    priority: int
    access_count: int = 0

class NetworkPerformanceMonitor:
    """网络性能监控器 - 2025年智能监控"""
    
    def __init__(self):
        self.transfer_history = deque(maxlen=100)
        self.current_speed = 0.0
        self.avg_speed = 0.0
        self.peak_speed = 0.0
        self.error_count = 0
        self.last_test_time = 0
        
    def record_transfer(self, bytes_transferred: int, duration: float):
        """记录传输性能"""
        if duration <= 0:
            return
            
        # 计算传输速度（字节/秒）
        speed = bytes_transferred / duration
        current_time = time.time()
        
        # 记录传输历史
        self.transfer_history.append((current_time, speed))
        
        # 更新峰值速度
        if speed > self.peak_speed:
            self.peak_speed = speed
            
        # 计算滑动平均速度（最近60秒内的数据）
        recent_transfers = [(t, s) for t, s in self.transfer_history if current_time - t < 60]
        if recent_transfers:
            total_bytes = sum(s * 1 for _, s in recent_transfers)  # 假设每次记录代表1秒
            self.avg_speed = total_bytes / len(recent_transfers)
        else:
            self.avg_speed = 0
            
        self.current_speed = speed
        
        # 更新最后测试时间
        self.last_test_time = current_time
        
    def get_optimal_chunk_size(self) -> int:
        """根据网络性能动态调整块大小"""
        if self.avg_speed > 100 * 1024 * 1024:  # > 100MB/s
            return 100 * 1024 * 1024  # 100MB chunks
        elif self.avg_speed > 50 * 1024 * 1024:  # > 50MB/s
            return 50 * 1024 * 1024   # 50MB chunks
        elif self.avg_speed > 10 * 1024 * 1024:  # > 10MB/s
            return 25 * 1024 * 1024   # 25MB chunks
        else:
            return 10 * 1024 * 1024   # 10MB chunks
            
    def get_performance_stats(self) -> Dict[str, Any]:
        """获取性能统计信息"""
        return {
            'current_speed_mbps': self.current_speed / (1024 * 1024),
            'avg_speed_mbps': self.avg_speed / (1024 * 1024),
            'peak_speed_mbps': self.peak_speed / (1024 * 1024),
            'error_count': self.error_count,
            'last_test_time': self.last_test_time,
            'transfer_count': len(self.transfer_history)
        }
        
    def record_error(self):
        """记录传输错误"""
        self.error_count += 1
        
    def reset_stats(self):
        """重置统计信息"""
        self.transfer_history.clear()
        self.current_speed = 0.0
        self.avg_speed = 0.0
        self.peak_speed = 0.0
        self.error_count = 0
            

class SmartCacheManager:
    """智能缓存管理器 - 2025年极限优化版本"""
    
    def __init__(self):
        self.cache_dir = Path(LOCAL_CACHE_DIR)
        self.temp_dir = Path(TEMP_PROCESSING_DIR)
        self.cache_entries: Dict[str, CacheEntry] = {}
        self.cache_sha256_map: Dict[str, str] = {}  # SHA256 -> 缓存路径映射
        self.download_queue = queue.PriorityQueue()
        self.active_downloads: Dict[str, threading.Thread] = {}
        self.cache_lock = threading.RLock()
        self.total_cache_size = 0
        self._network_monitor = None  # 延迟初始化
        
        # 缓存统计
        self.cache_hits = 0
        self.cache_misses = 0
        self.total_requests = 0
        
        # 创建目录
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # 延迟加载，提升启动速度
        self._cache_loaded = False
        self._monitors_started = False
        
        logging.info(f"🚀 智能缓存管理器初始化完成 (延迟加载模式)")
        logging.info(f"   缓存目录: {self.cache_dir}")
        logging.info(f"   临时目录: {self.temp_dir}")
        logging.info(f"   最大缓存: {MAX_CACHE_SIZE_GB}GB")
    
    @property
    def network_monitor(self):
        """延迟初始化网络监控器"""
        if self._network_monitor is None:
            self._network_monitor = NetworkPerformanceMonitor()
        return self._network_monitor
    
    def _ensure_cache_loaded(self):
        """确保缓存已加载"""
        if not self._cache_loaded:
            self._scan_existing_cache()
            self._cache_loaded = True
    
    def _ensure_monitors_started(self):
        """确保监控线程已启动"""
        if not self._monitors_started:
            self._start_monitor_threads()
            self._monitors_started = True
        logging.info(f"   现有缓存: {len(self.cache_entries)}个文件")
        
    def _scan_existing_cache(self):
        """扫描现有缓存文件并计算SHA256"""
        try:
            for cache_file in self.cache_dir.glob("*"):
                if cache_file.is_file():
                    pre_processing_size = cache_file.stat().st_size
                    access_time = cache_file.stat().st_atime
                    self.total_cache_size += pre_processing_size
                    
                    # 计算SHA256哈希值
                    try:
                        hash_value = self._calculate_file_sha256(cache_file)
                    except Exception as e:
                        logging.warning(f"计算SHA256失败 {cache_file.name}: {e}")
                        hash_value = ""
                    
                    # 🔧 创建缓存条目 - 使用文件名作为键以支持快速查找
                    entry = CacheEntry(
                        video_path=f"cached://{cache_file.name}",
                        local_path=str(cache_file),
                        pre_processing_size=pre_processing_size,
                        download_time=cache_file.stat().st_mtime,
                        last_access=access_time,
                        is_complete=True,
                        priority=1,
                        access_count=1
                    )
                    
                    # 🔧 修复：使用文件名（不含扩展名）作为键添加到cache_entries
                    # 这样可以通过文件名匹配到缓存，提高缓存命中率
                    file_basename = cache_file.stem  # 不含扩展名的文件名
                    self.cache_entries[file_basename] = entry
                    
                    # 建立SHA256映射（用于精确匹配）
                    if hash_value:
                        self.cache_sha256_map[hash_value] = str(cache_file)
                    
            logging.info(f"扫描到 {len(self.cache_sha256_map)} 个缓存文件，总大小: {self.total_cache_size/1024/1024/1024:.2f}GB")
            logging.info(f"建立SHA256映射 {len(self.cache_sha256_map)} 条记录")
        except Exception as e:
            logging.error(f"扫描缓存失败: {e}")
            
    def _calculate_file_sha256(self, file_path: Path) -> str:
        """计算文件SHA256哈希值"""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            # 分块读取大文件
            for chunk in iter(lambda: f.read(8192), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
        
    def _start_monitor_threads(self):
        """启动监控线程"""
        # 磁盘空间监控线程
        if MONITOR_DISK_SPACE:
            threading.Thread(target=self._disk_space_monitor, daemon=True).start()
            
        # 缓存清理线程
        if AUTO_CLEANUP_CACHE:
            threading.Thread(target=self._cache_cleanup_monitor, daemon=True).start()
            
    def _disk_space_monitor(self):
        """磁盘空间监控线程"""
        while True:
            try:
                cache_disk_usage = psutil.disk_usage(str(self.cache_dir))
                free_gb = cache_disk_usage.free / (1024**3)
                
                if free_gb < MIN_FREE_SPACE_GB:
                    logging.warning(f"⚠️ 磁盘空间不足: {free_gb:.1f}GB < {MIN_FREE_SPACE_GB}GB")
                    self._emergency_cleanup()
                    
                time.sleep(30)  # 每30秒检查一次
            except Exception as e:
                logging.error(f"磁盘空间监控错误: {e}")
                time.sleep(60)
                
    def _cache_cleanup_monitor(self):
        """缓存清理监控线程"""
        while True:
            try:
                self._cleanup_old_cache()
                time.sleep(300)  # 每5分钟清理一次
            except Exception as e:
                logging.error(f"缓存清理错误: {e}")
                time.sleep(60)
                
    def _emergency_cleanup(self):
        """紧急清理缓存"""
        with self.cache_lock:
            # 按访问时间排序，删除最老的缓存
            sorted_entries = sorted(
                self.cache_entries.values(),
                key=lambda x: x.last_access
            )
            
            cleaned_size = 0
            target_size = 5 * 1024**3  # 清理5GB空间
            
            for entry in sorted_entries:
                if cleaned_size >= target_size:
                    break
                    
                try:
                    if os.path.exists(entry.local_path):
                        pre_processing_size = os.path.getsize(entry.local_path)
                        os.remove(entry.local_path)
                        cleaned_size += pre_processing_size
                        self.total_cache_size -= pre_processing_size
                        
                    del self.cache_entries[entry.video_path]
                    logging.info(f"🧹 紧急清理缓存: {os.path.basename(entry.local_path)}")
                    
                except Exception as e:
                    logging.error(f"清理缓存失败 {entry.local_path}: {e}")
                    
            logging.info(f"🧹 紧急清理完成，释放 {cleaned_size/(1024**3):.2f}GB 空间")
    
    def _cleanup_old_cache(self):
        """清理过期缓存"""
        current_time = time.time()
        max_cache_bytes = MAX_CACHE_SIZE_GB * 1024**3
        
        with self.cache_lock:
            # 如果缓存超出限制，清理最旧的文件
            if self.total_cache_size > max_cache_bytes:
                excess = self.total_cache_size - max_cache_bytes
                
                sorted_entries = sorted(
                    self.cache_entries.values(),
                    key=lambda x: x.last_access
                )
                
                cleaned_size = 0
                for entry in sorted_entries:
                    if cleaned_size >= excess:
                        break
                        
                    # 根据访问频率和时间决定清理策略
                    access_age = current_time - entry.last_access
                    if (access_age > 7200 or  # 2小时未访问
                        (access_age > 1800 and entry.access_count < 2)):  # 30分钟未访问且访问次数少
                        try:
                            if os.path.exists(entry.local_path):
                                pre_processing_size = os.path.getsize(entry.local_path)
                                os.remove(entry.local_path)
                                cleaned_size += pre_processing_size
                                self.total_cache_size -= pre_processing_size
                                
                            del self.cache_entries[entry.video_path]
                            
                        except Exception as e:
                            logging.error(f"清理过期缓存失败 {entry.local_path}: {e}")
                            
    def get_cached_path(self, video_path: str) -> Optional[str]:
        """获取缓存路径 - 🔧 增强版：支持SHA256完整性验证和断点续传检测"""
        self.total_requests += 1
        
        with self.cache_lock:
            # 1️⃣ 检查是否有完整的缓存
            if video_path in self.cache_entries:
                entry = self.cache_entries[video_path]
                if entry.is_complete and os.path.exists(entry.local_path):
                    # 🔒 验证文件完整性（SHA256检查）
                    if self._verify_cache_integrity(entry.local_path, video_path):
                        entry.last_access = time.time()
                        entry.access_count += 1
                        self.cache_hits += 1
                        logging.info(f"✅ 缓存命中且完整: {os.path.basename(video_path)}")
                        return entry.local_path
                    else:
                        logging.warning(f"⚠️ 缓存文件损坏，标记为不完整: {os.path.basename(video_path)}")
                        entry.is_complete = False
                else:
                    # 缓存文件损坏，移除记录
                    if video_path in self.cache_entries:
                        del self.cache_entries[video_path]
            
            # 2️⃣ 🔧 改进：通过文件名索引查找现有缓存
            video_name = os.path.basename(video_path)
            video_basename = os.path.splitext(video_name)[0]  # 不含扩展名的文件名
            
            # 首先尝试通过文件名索引快速查找
            if video_basename in self.cache_entries:
                entry = self.cache_entries[video_basename]
                if entry.is_complete and os.path.exists(entry.local_path):
                    logging.info(f"🎯 通过文件名索引找到现有缓存: {video_name}")
                    entry.last_access = time.time()
                    entry.access_count += 1
                    self.cache_hits += 1
                    return entry.local_path
                else:
                    # 清理无效缓存条目
                    del self.cache_entries[video_basename]
            
            # 3️⃣ 🔧 备用：通过文件名模式匹配查找现有缓存
            for cache_file in self.cache_dir.glob(f"*_{video_name}"):
                if cache_file.is_file():
                    logging.info(f"🎯 通过文件名模式找到现有缓存: {video_name}")
                    # 创建缓存条目并添加到索引
                    cache_size = cache_file.stat().st_size
                    entry = CacheEntry(
                        video_path=video_path,
                        local_path=str(cache_file),
                        pre_processing_size=cache_size,
                        download_time=cache_file.stat().st_mtime,
                        last_access=time.time(),
                        is_complete=True,
                        priority=1,
                        access_count=1
                    )
                    self.cache_entries[video_path] = entry
                    self.cache_entries[video_basename] = entry  # 同时添加到文件名索引
                    self.cache_hits += 1
                    return str(cache_file)
            
            # 3️⃣ 🔧 通过SHA256映射查找现有缓存（如果原始文件可访问）
            try:
                if os.path.exists(video_path):
                    video_sha256 = self._calculate_file_sha256(Path(video_path))
                    if video_sha256 in self.cache_sha256_map:
                        cached_path = self.cache_sha256_map[video_sha256]
                        if os.path.exists(cached_path):
                            logging.info(f"🎯 通过SHA256找到现有缓存: {os.path.basename(video_path)} ({video_sha256[:8]})")
                            # 创建缓存条目
                            cache_size = os.path.getsize(cached_path)
                            entry = CacheEntry(
                                video_path=video_path,
                                local_path=cached_path,
                                pre_processing_size=cache_size,
                                download_time=os.path.getmtime(cached_path),
                                last_access=time.time(),
                                is_complete=True,
                                priority=1,
                                access_count=1
                            )
                            self.cache_entries[video_path] = entry
                            self.cache_hits += 1
                            return cached_path
            except Exception as e:
                logging.debug(f"SHA256查找失败 {os.path.basename(video_path)}: {e}")
            
            # 4️⃣ 检查是否有部分缓存文件（用于断点续传）
            video_name = os.path.basename(video_path)
            cache_file_pattern = f"cache_{hashlib.md5(video_path.encode()).hexdigest()[:8]}_{video_name}"
            potential_cache_path = self.cache_dir / cache_file_pattern
            
            if potential_cache_path.exists():
                cache_size = potential_cache_path.stat().st_size
                try:
                    original_size = os.path.getsize(video_path)
                    completion_ratio = cache_size / original_size if original_size > 0 else 0
                    
                    if completion_ratio >= 0.99:  # 99%以上认为完整
                        logging.info(f"🎯 发现近似完整缓存文件: {video_name} ({completion_ratio:.1%})")
                        # 创建或更新缓存条目
                        entry = CacheEntry(
                            video_path=video_path,
                            local_path=str(potential_cache_path),
                            pre_processing_size=cache_size,
                            download_time=potential_cache_path.stat().st_mtime,
                            last_access=time.time(),
                            is_complete=True,
                            priority=1,
                            access_count=1
                        )
                        self.cache_entries[video_path] = entry
                        self.cache_hits += 1
                        return str(potential_cache_path)
                    elif completion_ratio >= 0.1:  # 10%以上的部分文件
                        logging.info(f"📂 发现部分缓存文件: {video_name} ({completion_ratio:.1%}) - 可用于断点续传")
                        # 不返回路径，让调用方知道需要继续下载
                        return None
                except Exception as e:
                    logging.warning(f"检查部分缓存文件失败 {video_name}: {e}")
            
            self.cache_misses += 1
            return None
    
    def _verify_cache_integrity(self, cache_path: str, original_path: str) -> bool:
        """验证缓存文件完整性 - 🔒 通过SHA256和文件大小双重验证"""
        try:
            cache_file = Path(cache_path)
            if not cache_file.exists():
                return False
            
            # 🔍 检查文件大小
            try:
                cache_size = cache_file.stat().st_size
                original_size = os.path.getsize(original_path)
                
                if cache_size != original_size:
                    logging.warning(f"📏 缓存文件大小不匹配: 缓存{cache_size} vs 原始{original_size}")
                    return False
            except Exception as e:
                logging.warning(f"文件大小检查失败: {e}")
                return False
            
            # 🔐 SHA256完整性验证（仅对小于1000MB的文件进行SHA256验证以节省时间）
            if cache_size < 1000 * 1024 * 1024:  # 1000MB
                try:
                    cache_sha256 = self._calculate_file_sha256(cache_file)
                    original_sha256 = self._calculate_file_sha256(Path(original_path))
                    
                    if cache_sha256 != original_sha256:
                        logging.warning(f"🔐 SHA256校验失败: {os.path.basename(original_path)}")
                        return False
                    
                    logging.debug(f"✅ SHA256校验通过: {os.path.basename(original_path)} ({cache_sha256[:8]})")
                    return True
                    
                except Exception as e:
                    logging.warning(f"SHA256校验过程失败: {e}")
                    # SHA256失败但大小匹配，仍认为有效
                    return True
            else:
                # 大文件仅验证大小，跳过SHA256以提升性能
                logging.debug(f"📁 大文件跳过SHA256校验，仅验证大小: {os.path.basename(original_path)}")
                return True
                
        except Exception as e:
            logging.error(f"缓存完整性验证失败: {e}")
            return False
            
    def start_async_download(self, video_path: str, priority: int = 0) -> bool:
        """启动异步下载"""
        if video_path in self.active_downloads:
            return True  # 已在下载中
            
        if video_path in self.cache_entries:
            if self.cache_entries[video_path].is_complete:
                return True  # 已下载完成
                
        # 添加到下载队列
        self.download_queue.put((priority, time.time(), video_path))
        
        # 启动下载线程
        if len(self.active_downloads) < ASYNC_DOWNLOAD_THREADS:
            thread = threading.Thread(target=self._download_worker, daemon=True)
            thread.start()
            
        return True
        
    def _download_worker(self):
        """下载工作线程"""
        while True:
            try:
                # 从队列获取下载任务
                try:
                    priority, timestamp, video_path = self.download_queue.get(timeout=60)
                except queue.Empty:
                    break
                    
                if video_path in self.active_downloads:
                    continue
                    
                # 标记为活跃下载
                self.active_downloads[video_path] = threading.current_thread()
                
                try:
                    self._download_video_to_cache(video_path)
                finally:
                    # 清理活跃下载记录
                    if video_path in self.active_downloads:
                        del self.active_downloads[video_path]
                    self.download_queue.task_done()
                    
            except Exception as e:
                logging.error(f"下载工作线程异常: {e}")
                
    def _download_video_to_cache(self, video_path: str) -> bool:
        """下载视频到本地缓存 - 🚀 增强版：支持断点续传、网络下载和SHA256去重"""
        try:
            video_name = os.path.basename(video_path)
            
            # 🔍 首先检查源文件是否可直接访问（本地文件或已挂载的网络驱动器）
            source_accessible = False
            try:
                # 尝试快速访问文件（1秒超时）
                if os.path.exists(video_path):
                    test_size = os.path.getsize(video_path)
                    if test_size > 0:
                        source_accessible = True
                        logging.debug(f"✅ 源文件可直接访问: {video_name}")
            except Exception as e:
                logging.debug(f"❌ 源文件不可直接访问: {video_name} - {e}")
                source_accessible = False
            
            # 先计算文件SHA256，检查是否已存在相同文件（仅当源文件可访问时）
            source_sha256 = ""
            if source_accessible:
                try:
                    source_sha256 = self._calculate_file_sha256(Path(video_path))
                    if source_sha256 in self.cache_sha256_map:
                        existing_cache_path = self.cache_sha256_map[source_sha256]
                        if Path(existing_cache_path).exists():
                            logging.info(f"🎯 发现相同文件缓存: {video_name} (SHA256: {source_sha256[:8]})")
                            # 更新访问时间
                            with self.cache_lock:
                                for entry in self.cache_entries.values():
                                    if entry.local_path == existing_cache_path:
                                        entry.last_access = time.time()
                                        entry.access_count += 1
                                        break
                            return True
                except Exception as e:
                    logging.warning(f"SHA256检测失败 {video_name}: {e}")
                    source_sha256 = ""
            
            # 🚀 如果源文件不可直接访问，尝试网络下载方式
            if not source_accessible:
                return self._download_from_network(video_path, video_name)
            
            # 🔄 源文件可访问，继续原有的文件复制逻辑
            return self._copy_file_to_cache(video_path, video_name, source_sha256)
            
        except Exception as e:
            logging.error(f"缓存下载失败 {video_path}: {e}")
            return False
    
    def _download_from_network(self, video_path: str, video_name: str) -> bool:
        """从网络共享访问文件到缓存"""
        try:
            # 🔧 只支持SMB/UNC网络共享访问，不支持HTTP/FTP等网络下载
            if video_path.startswith(('http://', 'https://', 'ftp://')):
                logging.warning(f"⚠️ 不支持网络下载协议 {video_path.split('://')[0]}://, 跳过: {video_name}")
                return False
            else:
                # 🔧 尝试SMB/网络共享访问
                return self._try_network_share_access(video_path, video_name)
        except Exception as e:
            logging.error(f"网络访问失败 {video_name}: {e}")
            return False
    
    def _try_network_share_access(self, video_path: str, video_name: str) -> bool:
        """尝试访问网络共享文件"""
        try:
            import time
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    logging.info(f"🔄 尝试网络访问 (第{attempt+1}/{max_retries}次): {video_name}")
                    
                    # 🕐 设置较长超时时间进行网络访问（Windows兼容）
                    import threading
                    import time
                    
                    # 使用线程超时机制（Windows兼容）
                    result = [None]
                    exception = [None]
                    
                    def access_file():
                        try:
                            if os.path.exists(video_path):
                                pre_processing_size = os.path.getsize(video_path)
                                if pre_processing_size > 0:
                                    result[0] = True
                        except Exception as e:
                            exception[0] = e
                    
                    # 启动访问线程
                    access_thread = threading.Thread(target=access_file, daemon=True)
                    access_thread.start()
                    
                    # 等待30秒超时
                    access_thread.join(timeout=30)
                    
                    if access_thread.is_alive():
                        logging.warning(f"⏰ 网络访问超时 (30秒): {video_name}")
                        continue
                    elif exception[0]:
                        raise exception[0]
                    elif result[0]:
                        logging.info(f"✅ 网络访问成功: {video_name}")
                        return self._copy_file_to_cache(video_path, video_name, "")
                        
                except (TimeoutError, OSError, IOError) as e:
                    logging.warning(f"⚠️ 网络访问失败 (第{attempt+1}次): {video_name} - {e}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2  # 指数退避
                    continue
            
            logging.error(f"❌ 网络访问最终失败: {video_name}")
            return False
            
        except Exception as e:
            logging.error(f"网络共享访问异常 {video_name}: {e}")
            return False
    
    def _copy_file_to_cache(self, video_path: str, video_name: str, source_sha256: str) -> bool:
        """将文件复制到缓存目录"""
        try:
            
            # 生成本地缓存路径
            local_path = self.cache_dir / f"cache_{hashlib.md5(video_path.encode()).hexdigest()[:8]}_{video_name}"
            
            # 🔄 检查是否支持断点续传
            resume_from = 0
            if local_path.exists():
                existing_size = local_path.stat().st_size
                original_size = os.path.getsize(video_path)
                
                if existing_size < original_size:
                    resume_from = existing_size
                    completion_ratio = existing_size / original_size
                    logging.info(f"📂 发现部分文件，从 {resume_from:,} 字节处续传 ({completion_ratio:.1%})")
                elif existing_size == original_size:
                    # 文件大小相同，验证完整性
                    if self._verify_cache_integrity(str(local_path), video_path):
                        logging.info(f"✅ 文件已完整缓存: {video_name}")
                        return True
                    else:
                        logging.warning(f"⚠️ 缓存文件损坏，重新下载: {video_name}")
                        resume_from = 0
            
            logging.info(f"🔄 开始缓存下载: {video_name}")
            start_time = time.time()
            
            # 检查文件大小
            try:
                pre_processing_size = os.path.getsize(video_path)
            except:
                logging.error(f"无法获取文件大小: {video_path}")
                return False
                
            # 检查磁盘空间
            cache_disk_usage = psutil.disk_usage(str(self.cache_dir))
            if cache_disk_usage.free < pre_processing_size * 1.2:  # 留20%余量
                logging.warning(f"磁盘空间不足，跳过缓存: {video_name}")
                return False
                
            # 创建缓存条目
            with self.cache_lock:
                self.cache_entries[video_path] = CacheEntry(
                    video_path=video_path,
                    local_path=str(local_path),
                    pre_processing_size=pre_processing_size,
                    download_time=start_time,
                    last_access=start_time,
                    is_complete=False,
                    priority=0,
                    access_count=1
                )
            
            # 🚀 分块复制文件（支持断点续传）
            chunk_size = self.network_monitor.get_optimal_chunk_size()
            
            try:
                # 打开文件：源文件从断点位置开始读取，目标文件以追加模式写入
                with open(video_path, 'rb') as src:
                    # 🎯 断点续传：跳到指定位置
                    if resume_from > 0:
                        src.seek(resume_from)
                        
                    # 根据是否续传选择写入模式
                    mode = 'ab' if resume_from > 0 else 'wb'
                    with open(local_path, mode) as dst:
                        copied = resume_from  # 从断点开始计算
                        last_progress_report = resume_from
                        
                        while True:
                            chunk_start = time.time()
                            try:
                                chunk = src.read(chunk_size)
                                if not chunk:
                                    break
                                    
                                dst.write(chunk)
                                copied += len(chunk)
                                chunk_duration = time.time() - chunk_start
                                
                                # 记录传输性能
                                if chunk_duration > 0:
                                    self.network_monitor.record_transfer(len(chunk), chunk_duration)
                                
                                # 🚀 显示实时传输进度（每1%或每10MB显示一次）
                                progress = copied / pre_processing_size * 100
                                
                                # 更频繁的进度显示：每1%或每10MB
                                should_update = (
                                    (copied - last_progress_report >= 10 * 1024 * 1024) or  # 每10MB
                                    (progress - (last_progress_report / pre_processing_size * 100) >= 1)  # 每1%
                                )
                                
                                if should_update:
                                    stats = self.network_monitor.get_performance_stats()
                                    resume_info = f" (续传)" if resume_from > 0 else ""
                                    speed_mb = stats['current_speed_mbps']
                                    avg_speed = stats['avg_speed_mbps']
                                    
                                    # 📊 计算剩余时间
                                    remaining_bytes = pre_processing_size - copied
                                    if speed_mb > 0:
                                        eta_seconds = remaining_bytes / (speed_mb * 1024 * 1024)
                                        if eta_seconds < 60:
                                            eta_str = f"{eta_seconds:.0f}s"
                                        elif eta_seconds < 3600:
                                            eta_str = f"{eta_seconds/60:.1f}min"
                                        else:
                                            eta_str = f"{eta_seconds/3600:.1f}h"
                                    else:
                                        eta_str = "∞"
                                    
                                    # 🎯 显示完整的下载进度信息
                                    progress_msg = (f"📥 缓存传输: {video_name} {progress:.1f}%{resume_info} "
                                                  f"[{copied/(1024**2):.1f}MB/{pre_processing_size/(1024**2):.1f}MB] "
                                                  f"速度:{speed_mb:.1f}MB/s 剩余:{eta_str}")
                                    
                                    # 只在console显示动态进度，不记录到日志避免混乱
                                    print(f"\r\033[K{progress_msg}", end="", flush=True)
                                    last_progress_report = copied
                                    
                            except Exception as e:
                                self.network_monitor.record_error()
                                logging.error(f"块复制错误: {e}")
                                raise
                                
            except Exception as e:
                logging.error(f"文件复制失败 {video_name}: {e}")
                # 清理不完整的文件
                if local_path.exists():
                    try:
                        local_path.unlink()
                    except:
                        pass
                return False
            
            # 更新缓存条目和SHA256映射
            download_time = time.time() - start_time
            with self.cache_lock:
                # 更新缓存条目
                entry = CacheEntry(
                    video_path=video_path,
                    local_path=str(local_path),
                    pre_processing_size=pre_processing_size,
                    download_time=download_time,
                    last_access=time.time(),
                    is_complete=True,
                    priority=1,
                    access_count=1
                )
                self.cache_entries[video_path] = entry
                self.total_cache_size += pre_processing_size
                
                # 建立SHA256映射
                if source_sha256:
                    self.cache_sha256_map[source_sha256] = str(local_path)
                    logging.debug(f"建立SHA256映射: {source_sha256[:8]} -> {local_path.name}")
                    
            speed_mb = pre_processing_size / (1024 * 1024) / download_time
            
            # 🎉 缓存完成，输出最终状态
            final_msg = (f"✅ 缓存完成: {video_name} "
                        f"大小:{pre_processing_size/(1024**3):.2f}GB "
                        f"耗时:{download_time:.1f}s "
                        f"平均速度:{speed_mb:.1f}MB/s")
            
            print(f"\n{final_msg}")  # 新行显示完成信息，避免被进度条覆盖
            logging.info(final_msg)
            
            return True
            
        except Exception as e:
            logging.error(f"文件复制到缓存失败 {video_name}: {e}")
            
            # 清理失败的缓存文件
            try:
                if local_path.exists():
                    local_path.unlink()
                with self.cache_lock:
                    if video_path in self.cache_entries:
                        del self.cache_entries[video_path]
            except:
                pass
                
            return False
            
    def preload_videos(self, video_paths: List[str], current_index: int = 0):
        """智能预加载视频"""
        # 预加载当前视频之后的几个视频
        for i in range(PRELOAD_COUNT):
            next_index = current_index + i + 1
            if next_index < len(video_paths):
                video_path = video_paths[next_index]
                if not self.get_cached_path(video_path):
                    self.start_async_download(video_path, priority=i)
                    
    def wait_for_cache(self, video_path: str, timeout: float = 7200) -> Optional[str]:
        """等待缓存完成"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            cached_path = self.get_cached_path(video_path)
            if cached_path:
                return cached_path
                
            # 如果没有在下载中，启动下载
            if video_path not in self.active_downloads:
                self.start_async_download(video_path, priority=-1)  # 高优先级
                
            time.sleep(1)
            
        logging.warning(f"⏰ 缓存超时: {os.path.basename(video_path)}")
        return None
        
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        self._ensure_cache_loaded()  # 确保缓存已加载
        with self.cache_lock:
            return {
                'total_entries': len(self.cache_entries),
                'total_size_gb': self.total_cache_size / (1024**3),
                'cache_hit_ratio': len([e for e in self.cache_entries.values() if e.is_complete]) / max(len(self.cache_entries), 1) * 100,
                'active_downloads': len(self.active_downloads),
                'avg_network_speed_mbps': self.network_monitor.avg_speed / (1024*1024),
                'peak_network_speed_mbps': self.network_monitor.peak_speed / (1024*1024)
            }
    
    def remove_from_cache(self, video_path: str) -> bool:
        """从缓存中移除视频文件（用于查重后清理）"""
        try:
            with self.cache_lock:
                # 查找并删除缓存条目
                if video_path in self.cache_entries:
                    entry = self.cache_entries[video_path]
                    
                    # 删除物理文件
                    if os.path.exists(entry.local_path):
                        pre_processing_size = os.path.getsize(entry.local_path)
                        os.remove(entry.local_path)
                        self.total_cache_size -= pre_processing_size
                        logging.debug(f"🗑️ 已删除缓存文件: {entry.local_path}")
                    
                    # 从内存中移除记录
                    del self.cache_entries[video_path]
                    
                    # 从SHA256映射中移除（如果存在）
                    sha256_to_remove = None
                    for sha256, path in self.cache_sha256_map.items():
                        if path == entry.local_path:
                            sha256_to_remove = sha256
                            break
                    if sha256_to_remove:
                        del self.cache_sha256_map[sha256_to_remove]
                    
                    logging.info(f"✅ 成功清理重复视频缓存: {os.path.basename(video_path)}")
                    return True
                else:
                    # 尝试按文件名匹配清理
                    video_name = os.path.basename(video_path)
                    for cache_file in self.cache_dir.glob(f"*_{video_name}"):
                        if cache_file.is_file():
                            pre_processing_size = cache_file.stat().st_size
                            cache_file.unlink()
                            self.total_cache_size -= pre_processing_size
                            logging.debug(f"🗑️ 按文件名匹配删除: {cache_file}")
                            return True
                    
                    logging.debug(f"📂 缓存中未找到文件: {os.path.basename(video_path)}")
                    return False
                    
        except Exception as e:
            logging.error(f"清理缓存失败 {video_path}: {e}")
            return False

class I9PerformanceOptimizer:
    """i9处理器性能优化器 - 简化版"""
    
    def __init__(self):
        """i9性能优化器初始化 - 防死锁加固版"""
        try:
            self.cpu_count = multiprocessing.cpu_count()
            self.is_i9 = self._detect_i9_processor()
            self.numa_nodes = self._detect_numa_topology()
            self.cpu_affinity_map = self._create_cpu_affinity_map()
            
            if self.is_i9 and ENABLE_I9_TURBO:
                # 使用超时保护来避免死锁
                import threading
                def turbo_thread():
                    try:
                        self._enable_i9_turbo()
                    except Exception as e:
                        logging.warning(f"i9睿频优化线程异常: {e}")
                
                thread = threading.Thread(target=turbo_thread, daemon=True)
                thread.start()
                thread.join(timeout=30)  # 最多等待30秒
                if thread.is_alive():
                    logging.warning("⚠️  i9睿频优化超时，跳过但不影响主程序")
                
            logging.info(f"🔥 i9性能优化器初始化完成")
            logging.info(f"   i9处理器: {self.is_i9}")
            logging.info(f"   CPU核心数: {self.cpu_count}")
            logging.info(f"   NUMA节点: {len(self.numa_nodes)}")
            
        except Exception as e:
            logging.error(f"⚠️  i9性能优化器初始化失败: {e}")
            logging.info("🔄 使用默认配置继续运行，不影响核心功能")
            # 设置默认值确保程序能继续运行
            self.cpu_count = multiprocessing.cpu_count()
            self.is_i9 = False
            self.numa_nodes = [list(range(self.cpu_count))]
            self.cpu_affinity_map = {}
        
    def _detect_i9_processor(self) -> bool:
        """检测是否为i9或高端处理器"""
        try:
            cpu_info = platform.processor().lower()
            cpu_count = multiprocessing.cpu_count()
            
            # 直接检测i9
            if 'i9' in cpu_info or 'intel(r) core(tm) i9' in cpu_info:
                return True
            
            # 根据核心数判断高端CPU (16核心以上视为高端)
            if cpu_count >= 16:
                logging.info(f"检测到高核心数CPU ({cpu_count}核心)，启用i9级优化")
                return True
            
            # 检测其他高端CPU标识
            high_end_keywords = [
                'xeon', 'threadripper', 'ryzen 9', 'ryzen 7 5', 'ryzen 7 7',
                'i7-12', 'i7-13', 'i7-14', 'i5-13', 'i5-14'
            ]
            
            for keyword in high_end_keywords:
                if keyword in cpu_info:
                    logging.info(f"检测到高端CPU关键词: {keyword}")
                    return True
                    
            return False
        except Exception as e:
            logging.warning(f"CPU检测失败: {e}")
            # 如果检测失败但核心数较多，仍然启用优化
            try:
                cpu_count = multiprocessing.cpu_count()
                return cpu_count >= 16
            except:
                return False
    
    def _detect_numa_topology(self) -> List[List[int]]:
        """检测NUMA拓扑结构"""
        numa_nodes = []
        try:
            if platform.system() == "Windows":
                # Windows下的NUMA检测
                cores_per_node = self.cpu_count // 2  # 假设双NUMA节点
                numa_nodes.append(list(range(0, cores_per_node)))
                numa_nodes.append(list(range(cores_per_node, self.cpu_count)))
            else:
                # Linux下的NUMA检测
                numa_nodes = [list(range(self.cpu_count))]
                
        except Exception as e:
            logging.warning(f"NUMA检测失败: {e}")
            numa_nodes = [list(range(self.cpu_count))]
            
        return numa_nodes
            
        
    def _create_cpu_affinity_map(self) -> Dict[str, List[int]]:
        """创建CPU亲和性映射"""
        if not CPU_AFFINITY_OPTIMIZATION:
            return {}
            
        # 为不同任务类型分配专用核心
        total_cores = self.cpu_count
        
        return {
            'ffmpeg_encoding': list(range(0, total_cores // 2)),  # 编码任务使用前一半核心
            'file_io': list(range(total_cores // 2, total_cores // 2 + 2)),  # I/O任务使用2个核心
            'download': list(range(total_cores // 2 + 2, total_cores // 2 + 4)),  # 下载任务使用2个核心
            'system': list(range(total_cores // 2 + 4, total_cores))  # 系统任务使用其余核心
        }
        
    def _enable_i9_turbo(self):
        """启用i9睿频优化 - 防死锁加固版"""
        try:
            if platform.system() == "Windows":
                # Windows下设置高性能电源计划 - 添加超时保护
                try:
                    result1 = subprocess.run([
                        'powercfg', '/setactive', '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c'
                    ], capture_output=True, check=False, timeout=10)  # 10秒超时
                    logging.debug(f"电源计划设置结果: {result1.returncode}")
                except subprocess.TimeoutExpired:
                    logging.warning("电源计划设置超时，跳过")
                except Exception as e:
                    logging.warning(f"电源计划设置失败: {e}")
                
                # 设置处理器状态为100% - 添加超时保护
                try:
                    result2 = subprocess.run([
                        'powercfg', '/setacvalueindex', 'scheme_current', 
                        'sub_processor', 'procthrottlemax', '100'
                    ], capture_output=True, check=False, timeout=10)  # 10秒超时
                    logging.debug(f"处理器状态设置结果: {result2.returncode}")
                except subprocess.TimeoutExpired:
                    logging.warning("处理器状态设置超时，跳过")
                except Exception as e:
                    logging.warning(f"处理器状态设置失败: {e}")
                
                logging.info("🚀 i9睿频优化已启用")
                
        except Exception as e:
            logging.warning(f"i9睿频优化设置失败: {e}")
            logging.info("⚠️  i9优化失败不影响核心功能，程序将继续运行")
            
    def set_process_affinity(self, process: subprocess.Popen, task_type: str = 'ffmpeg_encoding'):
        """设置进程CPU亲和性 - 防死锁加固版"""
        if not CPU_AFFINITY_OPTIMIZATION or task_type not in self.cpu_affinity_map:
            return
            
        try:
            cpu_list = self.cpu_affinity_map[task_type]
            if platform.system() == "Windows":
                # Windows下设置亲和性 - 添加异常保护
                try:
                    import ctypes
                    handle = ctypes.windll.kernel32.OpenProcess(0x0200, False, process.pid)
                    if handle:
                        mask = sum(1 << cpu for cpu in cpu_list)
                        result = ctypes.windll.kernel32.SetProcessAffinityMask(handle, mask)
                        ctypes.windll.kernel32.CloseHandle(handle)
                        if not result:
                            logging.debug(f"CPU亲和性设置失败，但不影响功能")
                except ImportError:
                    logging.debug("ctypes库导入失败，跳过CPU亲和性设置")
                except Exception as e:
                    logging.debug(f"Windows CPU亲和性设置异常: {e}")
            else:
                # Linux下设置亲和性
                try:
                    os.sched_setaffinity(process.pid, cpu_list)
                except Exception as e:
                    logging.debug(f"Linux CPU亲和性设置失败: {e}")
                
            logging.debug(f"尝试设置进程 {process.pid} 亲和性到CPU {cpu_list}")
            
        except Exception as e:
            logging.debug(f"CPU亲和性设置失败（不影响功能）: {e}")
            
    def get_optimal_worker_count(self, task_type: str = 'encoding') -> int:
        """获取最优工作线程数"""
        if self.is_i9:
            if task_type == 'encoding':
                return min(self.cpu_count - 4, 20)  # i9编码任务
            elif task_type == 'download':
                return min(8, self.cpu_count // 4)  # 下载任务
            else:
                return min(self.cpu_count // 2, 12)  # 其他任务
        else:
            return min(self.cpu_count // 2, 8)

class HardwareDetector:
    """硬件检测和优化类 - 2025年NAS极限优化版"""
    
    # 类级别缓存，避免重复检测
    _gpu_cache = None
    _gpu_cache_time = 0
    _cache_timeout = 300  # 5分钟缓存
    
    def __init__(self, log_file_path: str = None):
        self.computer_name = self.get_computer_unique_id()
        self.hardware_info = None
        self.i9_optimizer = I9PerformanceOptimizer()
        self.cache_manager = SmartCacheManager()
        
        # 🔗 多电脑协作组件
        self.db_manager = DatabaseManager()
        self.video_record_manager = VideoRecordManager(self.computer_name, self.db_manager, log_file_path)

    def get_computer_unique_id(self) -> str:
        """获取电脑的唯一标识符"""
        try:
            mac = uuid.getnode()
            mac_str = f"{mac:012x}"
            hostname = socket.gethostname()
            
            disk_serial = "unknown"
            if platform.system() == "Windows":
                try:
                    result = subprocess.run(['wmic', 'diskdrive', 'get', 'serialnumber'], 
                                          capture_output=True, text=True, timeout=10)
                    if result.returncode == 0:
                        lines = result.stdout.strip().split('\n')
                        for line in lines[1:]:
                            serial = line.strip()
                            if serial and serial != "SerialNumber":
                                disk_serial = serial[:16]
                                break
                except Exception:
                    pass
            
            combined = f"{hostname}_{mac_str}_{disk_serial}"
            unique_hash = hashlib.md5(combined.encode()).hexdigest()[:12]
            return f"{hostname}_{unique_hash}"
        
        except Exception as e:
            logging.warning(f"获取电脑唯一标识失败: {e}")
            return f"{platform.node()}_{uuid.uuid4().hex[:8]}"

    def detect_hardware_capabilities(self) -> Dict[str, Any]:
        """🚀 2025年极限硬件检测和性能优化"""
        try:
            cpu_count = multiprocessing.cpu_count()
            cpu_freq = psutil.cpu_freq()
            memory = psutil.virtual_memory()
            
            cpu_info = platform.processor()
            is_i9 = self.i9_optimizer.is_i9
            is_high_end = any(x in cpu_info.lower() for x in ['i9', 'i7', 'ryzen 9', 'ryzen 7'])
            
            gpu_info = self.detect_gpu_capabilities()
            
            # 🔥 2025年i9极限优化参数
            if is_i9:
                max_parallel = self.i9_optimizer.get_optimal_worker_count('encoding')
                buffer_size = "200M"  # i9处理器大缓冲
                probe_size = "500M"   # i9处理器大探测
                memory_pool_gb = MEMORY_POOL_SIZE_GB
            elif is_high_end:
                max_parallel = min(cpu_count - 1, 16)
                buffer_size = "100M" 
                probe_size = "200M"
                memory_pool_gb = 6
            else:
                max_parallel = min(cpu_count // 2, 8)
                buffer_size = "50M"
                probe_size = "100M"
                memory_pool_gb = 4
            
            # 🧠 内存智能优化
            memory_gb = memory.total / (1024**3)
            if memory_gb >= 64:  # 64GB+内存的极限配置
                max_parallel = min(max_parallel, int(memory_gb // 2))
                memory_pool_gb = min(memory_pool_gb, 16)
            elif memory_gb >= 32:
                max_parallel = min(max_parallel, int(memory_gb // 1.5))
                memory_pool_gb = min(memory_pool_gb, 12)
            elif memory_gb >= 16:
                max_parallel = min(max_parallel, 12)
                memory_pool_gb = min(memory_pool_gb, 8)
            else:
                max_parallel = min(max_parallel, 6)
                memory_pool_gb = 4
            
            if MAX_PARALLEL_WORKERS_OVERRIDE > 0:
                max_parallel = MAX_PARALLEL_WORKERS_OVERRIDE
            
            # 🚀 NAS缓存优化参数 - 快速获取基础信息
            try:
                # 使用快速模式获取缓存统计，避免网络检测延迟
                cache_stats = {
                    'total_entries': len(self.cache_manager.cache_entries),
                    'total_size_gb': self.cache_manager.total_cache_size / (1024**3),
                    'cache_hit_ratio': 0.0,
                    'active_downloads': len(self.cache_manager.active_downloads),
                    'avg_network_speed_mbps': 0.0,  # 延迟检测
                    'peak_network_speed_mbps': 0.0  # 延迟检测
                }
            except Exception:
                cache_stats = {
                    'total_entries': 0,
                    'total_size_gb': 0.0,
                    'cache_hit_ratio': 0.0,
                    'active_downloads': 0,
                    'avg_network_speed_mbps': 0.0,
                    'peak_network_speed_mbps': 0.0
                }
            
            hw_info = {
                'cpu_cores': cpu_count,
                'cpu_freq_max': cpu_freq.max if cpu_freq else 0,
                'memory_gb': memory_gb,
                'memory_pool_gb': memory_pool_gb,
                'is_i9': is_i9,
                'is_high_end': is_high_end,
                'max_parallel': max_parallel,
                'buffer_size': buffer_size,
                'probe_size': probe_size,
                'gpu_info': gpu_info,
                'i9_optimizer': self.i9_optimizer,
                'cache_manager': self.cache_manager,
                'cache_stats': cache_stats,
                'numa_nodes': len(self.i9_optimizer.numa_nodes),
                # 🔗 多电脑协作组件
                'db_manager': self.db_manager,
                'video_record_manager': self.video_record_manager,
                'computer_name': self.computer_name
            }
            
            hw_info.update(gpu_info)
            
            logging.info(f"🚀 2025年极限硬件检测完成:")
            logging.info(f"   CPU: {cpu_count}核心 (i9={is_i9})")
            logging.info(f"   内存: {memory_gb:.1f}GB (池={memory_pool_gb}GB)")
            logging.info(f"   并行数: {max_parallel} (缓冲={buffer_size})")
            logging.info(f"   GPU: {gpu_info.get('encoder_type', 'unknown')}")
            logging.info(f"   缓存: {cache_stats['total_entries']}条目 ({cache_stats['total_size_gb']:.1f}GB)")
            logging.info(f"   网速: 延迟检测中... (提升启动速度)")
            
            # 启动后台任务异步更新网络速度
            threading.Thread(target=self._update_network_stats_async, args=(hw_info,), daemon=True).start()
            
            self.hardware_info = hw_info
            return hw_info
            
        except Exception as e:
            logging.error(f"极限硬件检测失败: {e}")
            return self.get_fallback_hardware()
    
    def _update_network_stats_async(self, hw_info: Dict[str, Any]):
        """异步更新网络统计信息"""
        try:
            # 给系统一些时间完成初始化
            time.sleep(2)
            
            # 获取完整的缓存统计信息（包括网络速度）
            cache_stats = self.cache_manager.get_cache_stats()
            
            # 更新硬件信息
            hw_info['cache_stats'] = cache_stats
            
            logging.info(f"📡 网络速度检测完成: 平均{cache_stats['avg_network_speed_mbps']:.1f}MB/s, 峰值{cache_stats['peak_network_speed_mbps']:.1f}MB/s")
            
        except Exception as e:
            logging.warning(f"异步网络检测失败: {e}")

    def detect_gpu_capabilities(self) -> Dict[str, Any]:
        """检测GPU能力和优化编码器选择 - 带缓存优化"""
        # 检查缓存
        current_time = time.time()
        if (HardwareDetector._gpu_cache is not None and 
            current_time - HardwareDetector._gpu_cache_time < HardwareDetector._cache_timeout):
            return HardwareDetector._gpu_cache.copy()
        
        gpu_info = {"encoder_type": "software", "encoder": "libx264", "options": {}}
        
        try:
            # 减少超时时间，加快检测速度
            result = subprocess.run([FFMPEG_PATH, '-hide_banner', '-encoders'], 
                                  capture_output=True, text=True, encoding='utf-8', timeout=5)
            
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
                        "options": self.get_nvidia_options(),
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
                        "options": self.get_amd_options(),
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
                        "options": self.get_intel_options(),
                        "max_parallel": 4
                    })
                    logging.info(f"检测到Intel编码器: {encoder}")
                    return gpu_info
            
            # 软件编码器优化
            gpu_info.update({
                "encoder_type": "software",
                "encoder": "libx264",
                "options": self.get_software_options(),
                "max_parallel": min(multiprocessing.cpu_count() // 2, 8)
            })
            logging.info("使用优化的软件编码器")
            
        except Exception as e:
            logging.warning(f"GPU检测失败: {e}")
        
        # 缓存检测结果
        HardwareDetector._gpu_cache = gpu_info.copy()
        HardwareDetector._gpu_cache_time = time.time()
        
        return gpu_info

    def get_nvidia_options(self) -> Dict[str, str]:
        """NVIDIA编码器优化参数"""
        if QUALITY_MODE == 'highest':
            return {'preset': 'p2', 'rc': 'vbr', 'cq': '20'}
        elif QUALITY_MODE == 'high':
            return {'preset': 'p2', 'rc': 'vbr', 'cq': '23'}
        elif QUALITY_MODE == 'balanced':
            return {'preset': 'p4', 'rc': 'vbr', 'cq': '25'}
        else:  # fast
            return {'preset': 'p6', 'rc': 'cbr'}

    def get_amd_options(self) -> Dict[str, str]:
        """AMD编码器选项"""
        return {'quality': 'balanced', 'rc': 'vbr_peak_constrained'}
    
    def get_intel_options(self) -> Dict[str, str]:
        """Intel编码器选项"""
        return {'preset': 'fast', 'global_quality': '25'}
    
    def get_software_options(self) -> Dict[str, str]:
        """软件编码器优化参数"""
        if QUALITY_MODE == 'highest':
            return {'preset': 'slow', 'crf': '20', 'threads': '0'}
        elif QUALITY_MODE == 'high':
            return {'preset': 'medium', 'crf': '23', 'threads': '0'}
        elif QUALITY_MODE == 'balanced':
            return {'preset': 'fast', 'crf': '25', 'threads': '0'}
        else:  # fast
            return {'preset': 'veryfast', 'crf': '28', 'threads': '0'}

    def get_fallback_hardware(self) -> Dict[str, Any]:
        """回退硬件配置"""
        cpu_count = multiprocessing.cpu_count()
        return {
            "cpu_cores": cpu_count,
            "encoder_type": "software",
            "encoder": "libx264",
            "options": self.get_software_options(),
            "max_parallel": min(cpu_count // 2, 4),
            "buffer_size": "30M",
            "probe_size": "50M"
        }

class ROISelector:
    """ROI区域选择器 - 增强版本"""
    
    def __init__(self):
        self.gui_available = self.init_opencv_gui()
        self.roi_16_9_mode = True  # 默认启用16:9强制模式
        
    def init_opencv_gui(self) -> bool:
        """初始化OpenCV GUI后端"""
        try:
            current_version = cv2.__version__
            logging.info(f"OpenCV版本: {current_version}")
            
            test_img = np.zeros((100, 100, 3), dtype=np.uint8)
            cv2.namedWindow("test_window", cv2.WINDOW_NORMAL)
            cv2.imshow("test_window", test_img)
            cv2.waitKey(1)
            cv2.destroyWindow("test_window")
            logging.info("OpenCV GUI后端初始化成功")
            return True
        except Exception as e:
            logging.warning(f"OpenCV GUI后端初始化失败: {e}")
            return False

    def extract_preview_frame(self, video_path: str) -> Optional[np.ndarray]:
        """提取视频预览帧"""
        try:
            # 获取视频时长
            probe_cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format=duration', 
                        '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
            result = subprocess.run(probe_cmd, capture_output=True, text=True, encoding='utf-8', timeout=15)
            
            if result.returncode != 0 or not result.stdout.strip():
                return None
                
            try:
                duration = float(result.stdout.strip())
            except ValueError:
                duration = 60  # 默认值
            
            # 提取中间帧作为预览
            temp_preview_path = Path("temp_preview_frame.jpg")
            extract_cmd = [
                FFMPEG_PATH, '-ss', str(duration / 2), '-i', video_path, 
                '-vframes', '1', '-q:v', '2', str(temp_preview_path), '-y'
            ]
            
            result = subprocess.run(extract_cmd, capture_output=True, timeout=30)
            if result.returncode != 0:
                return None
            
            frame = self._load_frame(temp_preview_path)
            
            # 清理临时文件
            try:
                if temp_preview_path.exists():
                    temp_preview_path.unlink()
            except Exception:
                pass
                
            return frame
            
        except Exception as e:
            logging.warning(f"提取预览帧失败 {video_path}: {e}")
            return None

    def _load_frame(self, temp_path: Path) -> Optional[np.ndarray]:
        """加载帧图像"""
        try:
            if temp_path.exists() and temp_path.stat().st_size > 1024:
                frame = cv2.imread(str(temp_path))
                if frame is not None and frame.size > 0:
                    return frame
            return None
        except Exception as e:
            logging.warning(f"加载帧失败: {e}")
            return None

    def adjust_roi_to_16_9(self, roi: Tuple[int, int, int, int], 
                          video_width: int, video_height: int) -> Tuple[Tuple[int, int, int, int], bool]:
        """智能调整ROI为16:9比例"""
        x, y, w, h = roi
        original_roi = roi
        was_adjusted = False
        
        # 处理边界问题
        if x < 0:
            w = w + x
            x = 0
        if y < 0:
            h = h + y
            y = 0
        
        if x + w > video_width:
            w = video_width - x
        if y + h > video_height:
            h = video_height - y
        
        # 确保ROI有效
        if w <= 0 or h <= 0:
            w = min(video_width * 0.8, video_width)
            h = min(video_height * 0.8, video_height)
            x = (video_width - w) // 2
            y = (video_height - h) // 2
            was_adjusted = True
            logging.warning(f"ROI无效，重置为中心区域: ({x}, {y}, {w}, {h})")
        
        # 调整为16:9比例
        target_aspect = 16 / 9
        roi_aspect = w / h if h > 0 else target_aspect
        
        if roi_aspect > target_aspect:
            # ROI比16:9更宽，以高度为准
            new_h = h
            new_w = int(h * target_aspect)
            new_x = x + (w - new_w) // 2
            new_y = y
        else:
            # ROI比16:9更高，以宽度为准
            new_w = w
            new_h = int(w / target_aspect)
            new_x = x
            new_y = y + (h - new_h) // 2
        
        # 确保在边界内
        new_x = max(0, min(new_x, video_width - new_w))
        new_y = max(0, min(new_y, video_height - new_h))
        
        # 检查调整后是否仍超出边界
        if new_x + new_w > video_width or new_y + new_h > video_height:
            max_w = video_width - new_x
            max_h = video_height - new_y
            
            if max_w / max_h > target_aspect:
                new_h = max_h
                new_w = int(max_h * target_aspect)
            else:
                new_w = max_w
                new_h = int(max_w / target_aspect)
            
            was_adjusted = True
        
        # 检查比例缩小
        roi_to_video_ratio_w = new_w / video_width
        roi_to_video_ratio_h = new_h / video_height
        
        max_ratio = 0.9
        if roi_to_video_ratio_w > max_ratio or roi_to_video_ratio_h > max_ratio:
            scale_factor = min(max_ratio / roi_to_video_ratio_w, max_ratio / roi_to_video_ratio_h)
            
            scaled_w = int(new_w * scale_factor)
            scaled_h = int(scaled_w / target_aspect)
            
            center_x = new_x + new_w // 2
            center_y = new_y + new_h // 2
            
            new_x = center_x - scaled_w // 2
            new_y = center_y - scaled_h // 2
            
            new_x = max(0, min(new_x, video_width - scaled_w))
            new_y = max(0, min(new_y, video_height - scaled_h))
            
            new_w, new_h = scaled_w, scaled_h
            was_adjusted = True
        
        # 确保最小尺寸
        min_w = 64
        min_h = int(min_w / target_aspect)
        
        if new_w < min_w or new_h < min_h:
            new_w = max(min_w, new_w)
            new_h = int(new_w / target_aspect)
            
            if new_x + new_w > video_width:
                new_x = video_width - new_w
            if new_y + new_h > video_height:
                new_y = video_height - new_h
            
            was_adjusted = True
        
        final_roi = (new_x, new_y, new_w, new_h)
        
        if was_adjusted or final_roi != original_roi:
            was_adjusted = True
            logging.info(f"ROI智能调整为16:9: {original_roi} -> {final_roi}")
            
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
        
        return final_roi, was_adjusted

    def gui_select_roi_16_9(self, frame: np.ndarray) -> Optional[Tuple[int, int, int, int]]:
        """16:9固定比例拖拽选框模式 - 类似剪映的交互体验"""
        if not self.gui_available:
            return None
            
        try:
            video_height, video_width = frame.shape[:2]
            target_aspect = 16.0 / 9.0
            
            # 调整显示尺寸
            display_height = 800
            scale_factor = display_height / video_height if video_height > 0 else 1
            display_width = int(video_width * scale_factor)
            
            # 计算默认16:9区域 (居中，最大化)
            frame_aspect = video_width / video_height
            if frame_aspect > target_aspect:
                # 视频比16:9更宽，以高度为准
                roi_h = int(video_height * 0.8)  # 80%高度
                roi_w = int(roi_h * target_aspect)
                roi_x = (video_width - roi_w) // 2
                roi_y = (video_height - roi_h) // 2
            else:
                # 视频比16:9更高，以宽度为准  
                roi_w = int(video_width * 0.8)  # 80%宽度
                roi_h = int(roi_w / target_aspect)
                roi_x = (video_width - roi_w) // 2
                roi_y = (video_height - roi_h) // 2
                
            # 确保在边界内
            roi_x = max(0, min(roi_x, video_width - roi_w))
            roi_y = max(0, min(roi_y, video_height - roi_h))
            
            # 拖拽状态变量
            dragging = False
            drag_start = None
            drag_offset = (0, 0)
            resizing = False
            resize_corner = None
            
            def mouse_callback(event, x, y, flags, param):
                nonlocal dragging, drag_start, drag_offset, resizing, resize_corner, roi_x, roi_y, roi_w, roi_h
                
                # 转换显示坐标到原始视频坐标
                orig_x = int(x / scale_factor)
                orig_y = int(y / scale_factor)
                
                # 计算显示坐标下的ROI
                display_roi_x = int(roi_x * scale_factor)
                display_roi_y = int(roi_y * scale_factor)
                display_roi_w = int(roi_w * scale_factor)
                display_roi_h = int(roi_h * scale_factor)
                
                if event == cv2.EVENT_LBUTTONDOWN:
                    # 检查是否点击在角落（调整大小） - 优先检查
                    corner_detect_size = 15  # 角落检测范围
                    corners = {
                        'tl': (display_roi_x, display_roi_y),  # 左上
                        'tr': (display_roi_x + display_roi_w, display_roi_y),  # 右上
                        'bl': (display_roi_x, display_roi_y + display_roi_h),  # 左下
                        'br': (display_roi_x + display_roi_w, display_roi_y + display_roi_h)  # 右下
                    }
                    
                    for corner_name, (cx, cy) in corners.items():
                        if abs(x - cx) <= corner_detect_size and abs(y - cy) <= corner_detect_size:
                            resizing = True
                            resize_corner = corner_name
                            drag_start = (orig_x, orig_y)  # 使用原始坐标
                            return
                    
                    # 检查是否点击在ROI框内（移动）
                    if (display_roi_x <= x <= display_roi_x + display_roi_w and 
                        display_roi_y <= y <= display_roi_y + display_roi_h):
                        # 拖拽移动
                        dragging = True
                        drag_start = (orig_x, orig_y)  # 使用原始坐标
                        drag_offset = (orig_x - roi_x, orig_y - roi_y)
                
                elif event == cv2.EVENT_MOUSEMOVE:
                    if dragging and drag_start:
                        # 移动ROI框
                        new_x = orig_x - drag_offset[0]
                        new_y = orig_y - drag_offset[1]
                        
                        # 边界检查
                        new_x = max(0, min(new_x, video_width - roi_w))
                        new_y = max(0, min(new_y, video_height - roi_h))
                        
                        roi_x, roi_y = new_x, new_y
                        
                    elif resizing and drag_start and resize_corner:
                        # 调整ROI框大小（保持16:9比例）
                        dx = orig_x - drag_start[0]
                        dy = orig_y - drag_start[1]
                        
                        # 保存原始值
                        old_x, old_y, old_w, old_h = roi_x, roi_y, roi_w, roi_h
                        
                        # 根据拖拽的角落调整大小
                        if resize_corner == 'br':  # 右下角
                            new_w = roi_w + dx
                            new_h = roi_h + dy
                        elif resize_corner == 'tr':  # 右上角
                            new_w = roi_w + dx
                            new_h = roi_h - dy
                            new_y = roi_y + dy
                        elif resize_corner == 'bl':  # 左下角
                            new_w = roi_w - dx
                            new_h = roi_h + dy
                            new_x = roi_x + dx
                        elif resize_corner == 'tl':  # 左上角
                            new_w = roi_w - dx
                            new_h = roi_h - dy
                            new_x = roi_x + dx
                            new_y = roi_y + dy
                        else:
                            new_w, new_h = roi_w, roi_h
                            new_x, new_y = roi_x, roi_y
                        
                        # 保持16:9比例 - 以变化量大的为准
                        if abs(dx) > abs(dy):
                            # 以宽度为准调整高度
                            new_h = int(new_w / target_aspect)
                        else:
                            # 以高度为准调整宽度
                            new_w = int(new_h * target_aspect)
                        
                        # 确保最小尺寸
                        min_w, min_h = 64, int(64 / target_aspect)
                        if new_w < min_w:
                            new_w = min_w
                            new_h = int(new_w / target_aspect)
                        if new_h < min_h:
                            new_h = min_h
                            new_w = int(new_h * target_aspect)
                        
                        # 根据角落重新计算位置
                        if resize_corner == 'br':  # 右下角 - 左上角不变
                            new_x, new_y = roi_x, roi_y
                        elif resize_corner == 'tr':  # 右上角 - 左下角不变
                            new_x = roi_x
                            new_y = roi_y + roi_h - new_h
                        elif resize_corner == 'bl':  # 左下角 - 右上角不变
                            new_x = roi_x + roi_w - new_w
                            new_y = roi_y
                        elif resize_corner == 'tl':  # 左上角 - 右下角不变
                            new_x = roi_x + roi_w - new_w
                            new_y = roi_y + roi_h - new_h
                        
                        # 边界检查
                        if (new_x >= 0 and new_y >= 0 and 
                            new_x + new_w <= video_width and new_y + new_h <= video_height):
                            roi_x, roi_y, roi_w, roi_h = new_x, new_y, new_w, new_h
                            drag_start = (orig_x, orig_y)  # 更新拖拽起点
                
                elif event == cv2.EVENT_LBUTTONUP:
                    dragging = False
                    resizing = False
                    drag_start = None
                    resize_corner = None
                
                elif event == cv2.EVENT_RBUTTONDOWN:
                    # 右键重置到默认位置
                    roi_x = (video_width - roi_w) // 2
                    roi_y = (video_height - roi_h) // 2
            
            # 创建窗口并设置鼠标回调
            cv2.namedWindow("16:9 拖拽选框", cv2.WINDOW_NORMAL)
            cv2.resizeWindow("16:9 拖拽选框", display_width, display_height)
            cv2.setWindowProperty("16:9 拖拽选框", cv2.WND_PROP_TOPMOST, 1)
            cv2.setMouseCallback("16:9 拖拽选框", mouse_callback)
            
            print("\n🎯 16:9 拖拽选框窗口已打开！")
            print(f"📺 原始视频: {video_width}x{video_height} -> 显示窗口: {display_width}x{display_height}")
            print(f"📐 初始16:9区域: ({roi_x}, {roi_y}, {roi_w}, {roi_h})")
            print("🖱️  操作提示：")
            print("   🖱️  左键拖拽框内 - 移动选框")
            print("   🔄 拖拽四个角点 - 调整大小（保持16:9）")
            print("   🖱️  右键 - 重置到中心位置")
            print("   ⌨️  ENTER - 确认选择")
            print("   ⌨️  ESC - 取消选择")
            
            while True:
                # 创建显示帧
                display_frame = cv2.resize(frame, (display_width, display_height))
                
                # 计算显示坐标下的ROI
                display_roi_x = int(roi_x * scale_factor)
                display_roi_y = int(roi_y * scale_factor)
                display_roi_w = int(roi_w * scale_factor)
                display_roi_h = int(roi_h * scale_factor)
                
                # 绘制半透明覆盖层
                overlay = display_frame.copy()
                cv2.rectangle(overlay, 
                             (display_roi_x, display_roi_y),
                             (display_roi_x + display_roi_w, display_roi_y + display_roi_h),
                             (0, 255, 0), -1)
                display_frame = cv2.addWeighted(display_frame, 0.7, overlay, 0.3, 0)
                
                # 绘制边框 - 减少厚度
                cv2.rectangle(display_frame,
                             (display_roi_x, display_roi_y),
                             (display_roi_x + display_roi_w, display_roi_y + display_roi_h),
                             (0, 255, 0), 1)  # 厚度从3改为1
                
                # 绘制角落调整点 - 更大更明显
                corner_visual_size = 6  # 显示大小
                corners = [
                    (display_roi_x, display_roi_y),  # 左上
                    (display_roi_x + display_roi_w, display_roi_y),  # 右上
                    (display_roi_x, display_roi_y + display_roi_h),  # 左下
                    (display_roi_x + display_roi_w, display_roi_y + display_roi_h)  # 右下
                ]
                for corner in corners:
                    # 绘制白色外圈
                    cv2.circle(display_frame, corner, corner_visual_size + 2, (255, 255, 255), -1)
                    # 绘制绿色内圆
                    cv2.circle(display_frame, corner, corner_visual_size, (0, 255, 0), -1)
                    # 绘制黑色边框
                    cv2.circle(display_frame, corner, corner_visual_size, (0, 0, 0), 1)
                
                # 添加信息文本
                cv2.putText(display_frame, "16:9 拖拽选框 (优化版)", 
                           (20, 40), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
                cv2.putText(display_frame, "拖拽框内移动 / 拖拽角点缩放 / 右键重置", 
                           (20, 80), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
                
                # 显示ROI信息
                roi_info = f"ROI: {roi_x},{roi_y},{roi_w}x{roi_h} (16:9)"
                cv2.putText(display_frame, roi_info, 
                           (20, display_height - 20), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 0), 2)
                
                cv2.imshow("16:9 拖拽选框", display_frame)
                key = cv2.waitKey(30) & 0xFF
                
                if key == 27:  # ESC
                    cv2.destroyAllWindows()
                    print("❌ 用户取消选择")
                    return None
                    
                elif key in [13, 10]:  # ENTER
                    cv2.destroyAllWindows()
                    final_roi = (roi_x, roi_y, roi_w, roi_h)
                    print(f"✅ 确认使用16:9区域: {final_roi}")
                    return final_roi
            
        except Exception as e:
            logging.warning(f"16:9拖拽选框失败: {e}")
            return None
    
    def gui_select_roi_manual(self, frame: np.ndarray) -> Optional[Tuple[int, int, int, int]]:
        """手动ROI选择模式（原版功能）"""
        if not self.gui_available:
            return None
            
        try:
            video_height, video_width = frame.shape[:2]
            
            # 调整显示尺寸
            display_height = 800
            scale_factor = display_height / video_height if video_height > 0 else 1
            display_width = int(video_width * scale_factor)
            display_frame = cv2.resize(frame, (display_width, display_height))
            
            cv2.putText(display_frame, "手动选择ROI区域，然后按'空格'或'回车'确认", 
                       (20, 40), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 255, 255), 2)
            cv2.putText(display_frame, "选择后将自动调整为16:9比例", 
                       (20, 80), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 0), 2)
            
            # 创建窗口
            cv2.namedWindow("手动ROI选择", cv2.WINDOW_NORMAL)
            cv2.resizeWindow("手动ROI选择", display_width, display_height)
            cv2.setWindowProperty("手动ROI选择", cv2.WND_PROP_TOPMOST, 1)
            
            print("\n🖱️  手动ROI选择模式:")
            print("   1. 用鼠标拖拽选择裁剪区域")
            print("   2. 按 SPACE/ENTER 确认选择") 
            print("   3. 按 ESC 取消选择")
            print("✨ 选择完成后会自动调整为16:9比例！")
            
            # OpenCV selectROI
            current_version = cv2.__version__
            if current_version.startswith('4.12'):
                print("⚠️ 检测到OpenCV 4.12.0版本，使用增强ROI选择模式")
                for attempt in range(3):
                    try:
                        cv2.imshow("手动ROI选择", display_frame)
                        cv2.waitKey(100)
                        r = cv2.selectROI("手动ROI选择", display_frame, fromCenter=False, showCrosshair=True)
                        break
                    except Exception as e:
                        print(f"ROI选择尝试 {attempt + 1}/3 失败: {e}")
                        if attempt == 2:
                            raise e
                        cv2.waitKey(500)
            else:
                r = cv2.selectROI("手动ROI选择", display_frame, fromCenter=False, showCrosshair=True)
            
            cv2.destroyAllWindows()
            
            if r[2] == 0 or r[3] == 0:
                print("❌ 未选择有效区域或已取消选择")
                return None
            
            # 转换为原始分辨率
            r_original = (
                int(r[0] / scale_factor),
                int(r[1] / scale_factor),
                int(r[2] / scale_factor),
                int(r[3] / scale_factor)
            )
            
            print(f"✅ ROI手动选择成功: {r_original}")
            return r_original
            
        except Exception as e:
            logging.warning(f"手动ROI选择失败: {e}")
            return None

    def gui_select_roi(self, frame: np.ndarray) -> Optional[Tuple[int, int, int, int]]:
        """图形化ROI选择 - 优先使用16:9强制模式"""
        if self.roi_16_9_mode:
            return self.gui_select_roi_16_9(frame)
        else:
            return self.gui_select_roi_manual(frame)

    def fallback_roi_input(self, frame: np.ndarray) -> Optional[Tuple[int, int, int, int]]:
        """命令行回退ROI输入"""
        try:
            video_height, video_width = frame.shape[:2]
            
            # 保存预览图
            preview_path = "roi_preview.jpg"
            display_height = 720
            scale_factor = display_height / video_height
            display_width = int(video_width * scale_factor)
            display_frame = cv2.resize(frame, (display_width, display_height))
            cv2.imwrite(preview_path, display_frame)
            
            print(f"\n🖼️  已生成预览图: {preview_path}")
            print(f"📺 预览分辨率: {display_width}x{display_height}")
            print(f"📺 原始分辨率: {video_width}x{video_height}")
            
            # 尝试自动打开预览图
            try:
                if platform.system() == "Windows":
                    os.startfile(preview_path)
                elif platform.system() == "Darwin":
                    subprocess.run(['open', preview_path], check=False, timeout=10)
                else:
                    subprocess.run(['xdg-open', preview_path], check=False, timeout=10)
            except Exception:
                pass
            
            print("\n请基于预览图输入ROI坐标:")
            print("格式: x y width height (以空格分隔)")
            print("例如: 100 50 800 600")
            
            while True:
                try:
                    user_input = input("请输入ROI坐标: ").strip()
                    coords = list(map(int, user_input.split()))
                    
                    if len(coords) != 4:
                        print("❌ 请输入4个数值: x y width height")
                        continue
                    
                    x_disp, y_disp, w_disp, h_disp = coords
                    
                    # 转换为原始分辨率
                    x = int(x_disp / scale_factor)
                    y = int(y_disp / scale_factor)
                    w = int(w_disp / scale_factor)
                    h = int(h_disp / scale_factor)
                    
                    # 边界检查
                    x = max(0, min(x, video_width - 1))
                    y = max(0, min(y, video_height - 1))
                    w = max(1, min(w, video_width - x))
                    h = max(1, min(h, video_height - y))
                    
                    print(f"✅ ROI输入成功: ({x}, {y}, {w}, {h})")
                    
                    # 清理预览图
                    try:
                        os.remove(preview_path)
                    except Exception:
                        pass
                    
                    return (x, y, w, h)
                    
                except ValueError:
                    print("❌ 输入格式错误，请输入4个整数")
                except KeyboardInterrupt:
                    print("\n用户取消输入")
                    return None
                except Exception as e:
                    print(f"❌ 输入错误: {e}")
        
        except Exception as e:
            logging.error(f"命令行ROI输入失败: {e}")
            return None

    def select_roi_for_video(self, video_path: str) -> Optional[Tuple[int, int, int, int, int, int]]:
        """为视频选择ROI区域，返回(x, y, w, h, base_width, base_height)"""
        frame = self.extract_preview_frame(video_path)
        if frame is None:
            logging.error(f"无法提取预览帧: {video_path}")
            return None
        
        video_height, video_width = frame.shape[:2]
        logging.info(f"基准视频尺寸: {video_width}x{video_height}")
        
        # 尝试GUI选择
        roi = self.gui_select_roi(frame)
        if roi is None and self.gui_available:
            print("🔄 GUI选择失败，切换到命令行输入模式")
        
        # 如果GUI失败，使用命令行输入
        if roi is None:
            roi = self.fallback_roi_input(frame)
        
        if roi is None:
            return None
        
        # 调整为16:9比例
        adjusted_roi, was_adjusted = self.adjust_roi_to_16_9(roi, video_width, video_height)
        
        if was_adjusted:
            print(f"🔄 ROI已自动调整为16:9比例")
        
        # 返回ROI + 基准尺寸
        return adjusted_roi + (video_width, video_height)

class ProgressManager:
    """统一进度管理器 - 增强版本 + 多电脑协作"""
    
    def __init__(self, computer_name: str, video_record_manager: Optional[VideoRecordManager] = None):
        self.computer_name = computer_name
        self.video_record_manager = video_record_manager
        self.progress_folder = os.path.join(PROGRESS_FOLDER, computer_name)
        self.progress_file = os.path.join(self.progress_folder, "unified_progress.json")
        self.temp_file = os.path.join(self.progress_folder, "unified_progress.tmp")
        self.crash_recovery_file = os.path.join(self.progress_folder, "crash_recovery.json")
        
        os.makedirs(self.progress_folder, exist_ok=True)
        self.progress_data = self.load_progress()

    def calculate_file_sha256(self, file_path: str) -> str:
        """计算文件SHA256哈希值"""
        try:
            hash_sha256 = hashlib.sha256()
            with open(file_path, "rb") as f:
                # 分块读取，避免大文件内存占用过高
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            logging.warning(f"计算SHA256失败 {file_path}: {e}")
            return ""
    
    def get_video_signature(self, video_path: str) -> str:
        """获取视频唯一签名（文件名+大小+修改时间的哈希）"""
        try:
            stat = os.stat(video_path)
            signature_data = f"{os.path.basename(video_path)}_{stat.st_size}_{stat.st_mtime}"
            return hashlib.md5(signature_data.encode()).hexdigest()[:16]
        except Exception as e:
            logging.warning(f"获取视频签名失败 {video_path}: {e}")
            return hashlib.md5(os.path.basename(video_path).encode()).hexdigest()[:16]

    def check_crash_recovery(self) -> Dict[str, Any]:
        """检查是否存在崩溃恢复数据"""
        try:
            if os.path.exists(self.crash_recovery_file):
                with open(self.crash_recovery_file, 'r', encoding='utf-8') as f:
                    recovery_data = json.load(f)
                    return recovery_data
        except Exception as e:
            logging.warning(f"读取崩溃恢复文件失败: {e}")
        return {}

    def save_crash_recovery(self, current_video: str, roi_settings: Optional[Tuple[int, int, int, int]]):
        """保存崩溃恢复数据"""
        try:
            recovery_data = {
                'last_session_time': datetime.now().isoformat(),
                'current_video': current_video,
                'roi_settings': roi_settings,
                'config': {
                    'enable_head_tail_cut': ENABLE_HEAD_TAIL_CUT,
                    'enable_cropping': ENABLE_CROPPING,
                    'head_cut_time': HEAD_CUT_TIME,
                    'tail_cut_time': TAIL_CUT_TIME,
                    'target_resolution': TARGET_RESOLUTION
                }
            }
            
            with open(self.crash_recovery_file, 'w', encoding='utf-8') as f:
                json.dump(recovery_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"保存崩溃恢复数据失败: {e}")

    def clear_crash_recovery(self):
        """清除崩溃恢复数据"""
        try:
            if os.path.exists(self.crash_recovery_file):
                os.remove(self.crash_recovery_file)
        except Exception as e:
            logging.warning(f"清除崩溃恢复文件失败: {e}")
            
    def detect_partial_outputs(self) -> List[Dict[str, Any]]:
        """检测输出目录中的部分处理文件"""
        partial_files = []
        try:
            if not os.path.exists(OUTPUT_DIR):
                return partial_files
                
            for video_name in os.listdir(OUTPUT_DIR):
                if video_name.lower().endswith('.mp4'):
                    file_path = os.path.join(OUTPUT_DIR, video_name)
                    file_stat = os.stat(file_path)
                    
                    # 检查文件大小是否异常小（可能是部分文件）
                    if file_stat.st_size < 1024 * 1024:  # 小于1MB
                        partial_files.append({
                            'path': file_path,
                            'name': video_name,
                            'size': file_stat.st_size,
                            'modified': file_stat.st_mtime
                        })
                        continue
                    
                    # 尝试快速检查文件完整性
                    try:
                        probe_cmd = [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', 
                                   '-show_entries', 'format=duration', '-of', 'csv=p=0', file_path]
                        result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=5)
                        
                        if result.returncode != 0 or not result.stdout.strip():
                            partial_files.append({
                                'path': file_path,
                                'name': video_name,
                                'size': file_stat.st_size,
                                'modified': file_stat.st_mtime,
                                'error': 'probe_failed'
                            })
                    except:
                        # 如果检查超时，假设文件可能有问题
                        partial_files.append({
                            'path': file_path,
                            'name': video_name,  
                            'size': file_stat.st_size,
                            'modified': file_stat.st_mtime,
                            'error': 'probe_timeout'
                        })
                        
        except Exception as e:
            logging.warning(f"检测部分文件失败: {e}")
            
        return partial_files

    def load_progress(self) -> Dict[str, Any]:
        """加载进度数据"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logging.info(f"加载进度记录: {len(data.get('completed', []))} 个已完成")
                    return data
        except Exception as e:
            logging.warning(f"加载进度文件失败: {e}")
        
        return {
            'completed': [], 'processing': [], 'failed': [],
            'start_time': None, 'roi_settings': None,
            'video_signatures': {},  # SHA256签名映射
            'config': {'enable_head_tail_cut': ENABLE_HEAD_TAIL_CUT, 'enable_cropping': ENABLE_CROPPING}
        }

    def save_progress(self):
        """保存进度数据"""
        try:
            with open(self.temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, ensure_ascii=False, indent=2)
            
            if os.path.exists(self.progress_file):
                os.remove(self.progress_file)
            os.rename(self.temp_file, self.progress_file)
        except Exception as e:
            logging.error(f"保存进度文件失败: {e}")

    def is_completed(self, video_path: str, quick_check: bool = False) -> bool:
        """检查视频是否已完成（优先数据库，后备本地记录）"""
        video_name = os.path.basename(video_path)
        
        # 快速检查模式：仅检查本地记录和输出文件
        if quick_check:
            # 1. 检查输出文件是否存在
            base_name = os.path.splitext(video_name)[0]
            output_path = os.path.join(OUTPUT_DIR, f"{base_name}.mp4")
            if os.path.exists(output_path) and os.path.getsize(output_path) > 1024 * 1024:  # 大于1MB
                logger.debug(f"✅ 输出文件存在: {video_name}")
                return True
            
            # 2. 检查本地签名记录（使用快速签名）
            video_signature = self.get_video_signature(video_path)
            signatures = self.progress_data.get('video_signatures', {})
            if video_signature in signatures:
                logger.debug(f"✅ 本地签名命中: {video_name}")
                return True
            
            # 3. 检查旧版本记录（基于文件名）
            for record in self.progress_data.get('completed', []):
                if isinstance(record, dict) and record.get('name') == video_name:
                    logger.debug(f"✅ 旧记录命中: {video_name}")
                    return True
                elif record == video_name:
                    logger.debug(f"✅ 旧记录命中: {video_name}")
                    return True
            
            return False
        
        # 完整检查模式（原逻辑）
        # 🔗 优先检查数据库记录
        if self.video_record_manager and self.video_record_manager.db_manager.is_available():
            is_processed, db_record = self.video_record_manager.is_video_processed(video_path)
            if is_processed:
                logger.info(f"🔍 数据库发现已处理视频: {os.path.basename(video_path)}")
                return True
        
        # 后备：检查本地记录
        video_signature = self.get_video_signature(video_path)
        
        # 检查SHA256签名映射
        signatures = self.progress_data.get('video_signatures', {})
        if video_signature in signatures:
            return True
            
        # 兼容旧版本记录（基于文件名）
        for record in self.progress_data.get('completed', []):
            if isinstance(record, dict):
                if record.get('name') == video_name:
                    # 迁移到新的签名系统
                    signatures[video_signature] = {
                        'name': video_name,
                        'path': video_path,
                        'migrated_from_old': True
                    }
                    self.progress_data['video_signatures'] = signatures
                    self.save_progress()
                    return True
            else:
                if record == video_name:
                    # 迁移到新系统
                    signatures[video_signature] = {
                        'name': video_name,
                        'path': video_path,
                        'migrated_from_old': True
                    }
                    self.progress_data['video_signatures'] = signatures
                    self.save_progress()
                    return True
        return False

    def mark_completed(self, video_path: str, output_path: str = None, processing_time: float = 0.0):
        """标记视频为已完成（数据库+本地记录）"""
        video_name = os.path.basename(video_path)
        video_signature = self.get_video_signature(video_path)
        
        # 🔗 优先更新数据库记录
        if self.video_record_manager and self.video_record_manager.db_manager.is_available():
            self.video_record_manager.complete_processing(video_path, output_path, processing_time)
        
        # 继续维护本地记录（兼容性）
        # 移除旧记录（兼容性处理）
        self.progress_data['completed'] = [
            record for record in self.progress_data['completed']
            if not (isinstance(record, dict) and record.get('name') == video_name) and record != video_name
        ]
        
        # 添加新记录到传统列表（兼容性）
        completed_record = {
            'name': video_name,
            'path': video_path,
            'output_path': output_path,
            'processing_time': processing_time,
            'completed_time': datetime.now().isoformat(),
            'signature': video_signature,
            'config': {
                'head_tail_cut': ENABLE_HEAD_TAIL_CUT,
                'cropping': ENABLE_CROPPING,
                'head_cut_time': HEAD_CUT_TIME if ENABLE_HEAD_TAIL_CUT else 0,
                'tail_cut_time': TAIL_CUT_TIME if ENABLE_HEAD_TAIL_CUT else 0,
                'target_resolution': TARGET_RESOLUTION if ENABLE_CROPPING else None
            }
        }
        
        self.progress_data['completed'].append(completed_record)
        
        # 添加到新的签名映射系统
        if 'video_signatures' not in self.progress_data:
            self.progress_data['video_signatures'] = {}
        
        self.progress_data['video_signatures'][video_signature] = {
            'name': video_name,
            'path': video_path,
            'output_path': output_path,
            'processing_time': processing_time,
            'completed_time': datetime.now().isoformat(),
            'config': completed_record['config'].copy()
        }
        
        # 从处理中移除
        if video_name in self.progress_data['processing']:
            self.progress_data['processing'].remove(video_name)
        
        # 从失败列表移除
        self.progress_data['failed'] = [
            record for record in self.progress_data['failed']
            if not (isinstance(record, dict) and record.get('name') == video_name)
        ]
        
        self.save_progress()

    def mark_processing(self, video_path: str, video_info: Dict = None):
        """标记视频为处理中（数据库+本地记录）"""
        video_name = os.path.basename(video_path)
        
        # 🔗 优先更新数据库记录
        if self.video_record_manager and self.video_record_manager.db_manager.is_available():
            self.video_record_manager.start_processing(video_path, video_info=video_info)
        
        # 继续维护本地记录（兼容性）
        if video_name not in self.progress_data['processing']:
            self.progress_data['processing'].append(video_name)
        self.save_progress()

    def mark_failed(self, video_path: str, error_msg: str = ""):
        """标记视频为失败（数据库+本地记录）"""
        video_name = os.path.basename(video_path)
        
        # 清理错误消息
        clean_error = self.clean_error_message(error_msg)
        
        # 🔗 优先更新数据库记录
        if self.video_record_manager and self.video_record_manager.db_manager.is_available():
            self.video_record_manager.fail_processing(video_path, clean_error)
        
        # 继续维护本地记录（兼容性）
        # 检查是否已在失败列表中
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

    def clean_error_message(self, error_msg: str) -> str:
        """清理错误消息"""
        if not error_msg:
            return "处理失败"
        
        if 'frame=' in error_msg or 'fps=' in error_msg:
            lines = error_msg.split('\n')
            clean_lines = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if not any(x in line for x in ['frame=', 'fps=', 'time=', 'bitrate=', 'speed=']):
                    clean_lines.append(line)
            return '\n'.join(clean_lines[:3]) if clean_lines else "FFmpeg处理失败"
        
        return error_msg

    def get_statistics(self) -> Dict[str, int]:
        """获取统计信息"""
        return {
            'completed': len(self.progress_data['completed']),
            'processing': len(self.progress_data['processing']),
            'failed': len(self.progress_data['failed'])
        }

def get_video_info(video_path: str) -> Dict[str, Any]:
    """获取视频信息"""
    try:
        cmd = [FFPROBE_PATH, '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', video_path]
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=30)
        
        if result.returncode != 0:
            return {}
        
        data = json.loads(result.stdout)
        video_stream = None
        
        for stream in data.get('streams', []):
            if stream.get('codec_type') == 'video':
                video_stream = stream
                break
        
        if not video_stream:
            return {}
        
        format_info = data.get('format', {})
        
        return {
            'width': int(video_stream.get('width', 0)),
            'height': int(video_stream.get('height', 0)),
            'duration': float(video_stream.get('duration', 0)) or float(format_info.get('duration', 0)),
            'fps': eval(video_stream.get('r_frame_rate', '25/1')),
            'bitrate': int(format_info.get('bit_rate', 0)),
            'codec': video_stream.get('codec_name', 'unknown'),
            'pixel_format': video_stream.get('pix_fmt', 'unknown')
        }
        
    except Exception as e:
        logging.warning(f"获取视频信息失败 {video_path}: {e}")
        return {}

def adjust_roi_by_ratio(roi_with_base: Tuple[int, int, int, int, int, int], current_video_path: str) -> Tuple[int, int, int, int]:
    """根据预览视频与当前视频的尺寸比例调整ROI参数
    
    Args:
        roi_with_base: (x, y, w, h, base_width, base_height) - 基于预览视频的ROI和预览视频尺寸
        current_video_path: 当前要处理的视频路径
    
    Returns:
        调整后的ROI: (x, y, w, h)
    """
    x, y, w, h, base_width, base_height = roi_with_base
    
    try:
        # 获取当前视频的实际尺寸
        current_info = get_video_info(current_video_path)
        if not current_info:
            logging.warning(f"无法获取视频信息，使用原始ROI: {current_video_path}")
            return (x, y, w, h)
        
        current_width = current_info.get('width', 0)
        current_height = current_info.get('height', 0)
        
        if current_width <= 0 or current_height <= 0:
            logging.warning(f"视频尺寸无效，使用原始ROI: {current_width}x{current_height}")
            return (x, y, w, h)
        
        # 计算缩放比例
        width_ratio = current_width / base_width
        height_ratio = current_height / base_height
        
        # 按比例调整ROI参数
        new_x = int(x * width_ratio)
        new_y = int(y * height_ratio)
        new_w = int(w * width_ratio)
        new_h = int(h * height_ratio)
        
        # 边界检查和修正
        new_x = max(0, min(new_x, current_width - 1))
        new_y = max(0, min(new_y, current_height - 1))
        new_w = min(new_w, current_width - new_x)
        new_h = min(new_h, current_height - new_y)
        
        # 确保裁剪区域有效
        if new_w <= 0 or new_h <= 0:
            logging.error(f"调整后的ROI无效: {new_w}x{new_h}")
            return (x, y, w, h)  # 返回原始ROI
        
        # 记录调整信息
        if (new_x != x or new_y != y or new_w != w or new_h != h):
            logging.info(f"🔄 ROI按比例调整: 基准({base_width}x{base_height}) -> 当前({current_width}x{current_height})")
            logging.info(f"   缩放比例: {width_ratio:.3f}x{height_ratio:.3f}")
            logging.info(f"   ROI调整: ({x},{y},{w},{h}) -> ({new_x},{new_y},{new_w},{new_h})")
        
        return (new_x, new_y, new_w, new_h)
        
    except Exception as e:
        logging.error(f"ROI比例调整失败: {e}")
        return (x, y, w, h)  # 返回原始ROI

def should_skip_low_resolution(video_path: str) -> Tuple[bool, Optional[Tuple[int, int]], str]:
    """检查是否应该跳过低分辨率视频"""
    if not SKIP_LOW_RESOLUTION_VIDEOS:
        return False, None, ""
    
    video_info = get_video_info(video_path)
    if not video_info:
        return False, None, "无法获取视频信息"
    
    width = video_info.get('width', 0)
    height = video_info.get('height', 0)
    
    if width < MIN_RESOLUTION_WIDTH:
        return True, (width, height), f"分辨率({width}x{height})低于最小要求({MIN_RESOLUTION_WIDTH}px宽度)"
    
    return False, (width, height), ""

def build_unified_ffmpeg_command(input_file: str, output_file: str, 
                                roi: Optional[Tuple[int, int, int, int, int, int]] = None,
                                hardware_info: Dict[str, Any] = None) -> List[str]:
    """构建统一的FFmpeg命令，支持切头尾+裁剪"""
    # 确保路径使用正确的分隔符并处理特殊字符
    input_file = os.path.normpath(input_file)
    output_file = os.path.normpath(output_file)
    
    # 确保路径是绝对路径以避免路径解析问题
    if not os.path.isabs(input_file):
        input_file = os.path.abspath(input_file)
    if not os.path.isabs(output_file):
        output_file = os.path.abspath(output_file)
    
    # 提供默认硬件信息
    if hardware_info is None:
        hardware_info = {
            'probe_size': '50M',
            'buffer_size': '1024M',
            'encoder_type': 'software',
            'options': {}
        }
    
    cmd = [FFMPEG_PATH, '-y', '-nostdin']
    
    # 输入优化参数
    cmd.extend(['-probesize', hardware_info.get('probe_size', '50M')])
    cmd.extend(['-analyzeduration', hardware_info.get('probe_size', '50M')])
    
    # 切头尾时间设置
    if ENABLE_HEAD_TAIL_CUT and HEAD_CUT_TIME > 0:
        cmd.extend(['-ss', str(HEAD_CUT_TIME)])
    
    # 使用引号包围输入文件路径以处理特殊字符
    cmd.extend(['-i', input_file])
    
    # 计算有效时长（如果启用了切头尾）
    if ENABLE_HEAD_TAIL_CUT:
        video_info = get_video_info(input_file)
        total_duration = video_info.get('duration', 0)
        effective_duration = max(0, total_duration - HEAD_CUT_TIME - TAIL_CUT_TIME)
        if effective_duration > 0:
            cmd.extend(['-t', str(effective_duration)])
    
    # 视频滤镜设置
    if ENABLE_CROPPING and roi:
        # 如果ROI包含基准尺寸信息，则进行比例调整
        if len(roi) == 6:
            # 包含基准尺寸的ROI，需要按比例调整
            x, y, w, h = adjust_roi_by_ratio(roi, input_file)
        else:
            # 传统的4参数ROI，直接使用
            x, y, w, h = roi
        
        # 获取原视频尺寸进行边界检查
        video_info = get_video_info(input_file)
        video_width = video_info.get('width', 0)
        video_height = video_info.get('height', 0)
        
        # 边界检查和自动修正
        if video_width > 0 and video_height > 0:
            # 确保裁剪区域不超出视频边界
            max_x = max(0, min(x, video_width - 1))
            max_y = max(0, min(y, video_height - 1))
            max_w = min(w, video_width - max_x)
            max_h = min(h, video_height - max_y)
            
            # 如果调整了参数，记录日志
            if (max_x != x or max_y != y or max_w != w or max_h != h):
                logging.warning(f"🔧 裁剪参数边界调整: 原({x},{y},{w},{h}) -> 新({max_x},{max_y},{max_w},{max_h}), 视频尺寸: {video_width}x{video_height}")
                x, y, w, h = max_x, max_y, max_w, max_h
            
            # 最终验证：确保裁剪区域有效
            if w > 0 and h > 0:
                filter_complex = f"crop={w}:{h}:{x}:{y},scale={TARGET_RESOLUTION[0]}:{TARGET_RESOLUTION[1]}:force_original_aspect_ratio=disable"
                cmd.extend(['-vf', filter_complex])
            else:
                logging.error(f"❌ 裁剪参数无效: crop={w}:{h}:{x}:{y}, 视频尺寸: {video_width}x{video_height}")
                raise ValueError(f"裁剪参数超出视频边界，无法处理")
        else:
            logging.error(f"❌ 无法获取视频尺寸信息: {input_file}")
            raise ValueError(f"无法获取视频尺寸信息，无法验证裁剪参数")
        cmd.extend(['-s', f'{TARGET_RESOLUTION[0]}x{TARGET_RESOLUTION[1]}'])
    
    # 编码器设置
    cmd.extend(['-c:v', hardware_info['encoder']])
    
    # 编码器参数
    options = hardware_info.get('options', {})
    for key, value in options.items():
        cmd.extend([f'-{key}', str(value)])
    
    # GOP设置
    if hardware_info['encoder_type'] == 'nvidia':
        cmd.extend(['-g', '120', '-keyint_min', '60'])
    elif hardware_info['encoder_type'] in ['amd', 'intel']:
        cmd.extend(['-g', '120', '-keyint_min', '60'])
    else:  # software
        cmd.extend(['-g', '120', '-keyint_min', '60', '-sc_threshold', '40', '-bf', '2'])
    
    # 音频处理
    cmd.extend(['-c:a', 'aac', '-b:a', '192k'])
    
    # 输出优化参数
    cmd.extend([
        '-movflags', '+faststart',
        '-map_metadata', '-1',
        '-fps_mode', 'cfr',
        '-avoid_negative_ts', 'make_zero',
        '-fflags', '+genpts',
        '-max_muxing_queue_size', hardware_info.get('buffer_size', '2048').replace('M', ''),
        '-f', 'mp4',  # 明确指定输出格式为mp4
        output_file
    ])
    
    return cmd

def parse_progress(line: str) -> Dict[str, Any]:
    """解析FFmpeg进度信息"""
    info = {}
    patterns = {
        'frame': r'frame=\s*(\d+)',
        'fps': r'fps=\s*([\d\.]+)',
        'time': r'time=\s*(\d+):(\d+):([\d\.]+)',
        'speed': r'speed=\s*([\d\.]+)x',
        'size': r'size=\s*(\d+)kB'
    }
    
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

def run_ffmpeg_process(cmd: List[str], expected_duration: float, pbar, video_path: str, 
                      i9_optimizer: Optional[I9PerformanceOptimizer] = None):
    """🚀 运行FFmpeg进程并监控进度 - 2025年极限优化版"""
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
        universal_newlines=True, encoding='utf-8', errors='ignore', bufsize=1
    )
    
    # 🔥 i9处理器CPU亲和性优化
    if i9_optimizer:
        i9_optimizer.set_process_affinity(process, 'ffmpeg_encoding')
    
    last_percentage = 0
    last_update_time = time.time()
    stalled_time = 0
    no_progress_count = 0
    
    # 对长视频调整超时参数
    is_long_video = expected_duration > 3600
    max_stall_time = 300 if is_long_video else 120
    max_no_progress_time = 600 if is_long_video else 300
    
    print(f"🎬 开始FFmpeg处理: 时长={expected_duration:.1f}s, 长视频模式={is_long_video}")
    logging.info(f"开始处理: 时长={expected_duration:.1f}s, 长视频模式={is_long_video}")
    
    while process.poll() is None:
        line = process.stderr.readline()
        if line:
            progress_info = parse_progress(line)
            if 'time' in progress_info:
                last_update_time = time.time()
                no_progress_count = 0
                
                current_time = progress_info['time']
                if current_time > expected_duration:
                    current_time = expected_duration
                
                # 进度计算：0-95%范围
                if current_time >= expected_duration * 0.95:
                    percentage = 95
                else:
                    percentage = min(95, 10 + current_time * 85 / expected_duration)
                
                if percentage > last_percentage:
                    pbar.update(percentage - last_percentage)
                    last_percentage = percentage
                    
                    postfix = {
                        'FPS': f"{progress_info.get('fps', 0):.1f}",
                        '速度': f"{progress_info.get('speed', 0):.1f}x",
                        '时间': f"{current_time:.1f}s/{expected_duration:.1f}s",
                        '进度': f"{current_time/expected_duration*100:.1f}%"
                    }
                    pbar.set_postfix(postfix)
                
                # 卡死检测
                speed = progress_info.get('speed', 1.0)
                if speed < 0.01:
                    stalled_time += 1
                elif speed < 0.1 and is_long_video:
                    stalled_time += 0.5
                else:
                    stalled_time = 0
                
                if stalled_time > max_stall_time:
                    process.terminate()
                    raise Exception(f"处理速度过慢，可能已卡死 (速度: {speed}x)")
        else:
            no_progress_count += 1
            if time.time() - last_update_time > max_no_progress_time:
                process.terminate()
                raise Exception(f"处理超时，{max_no_progress_time}秒内无进度更新")
        
        time.sleep(1 if is_long_video else 0.5)
    
    # 检查返回码
    if process.returncode != 0:
        remaining_stderr = process.stderr.read()
        error_lines = []
        for line in remaining_stderr.split('\n'):
            if (line.strip() and 
                not line.startswith('frame=') and 
                not line.startswith('size=') and
                'fps=' not in line):
                error_lines.append(line.strip())
        
        filtered_errors = error_lines[-10:] if error_lines else ["无具体错误信息"]
        error_msg = '\n'.join(filtered_errors)
        raise Exception(f"ffmpeg处理失败 (代码 {process.returncode}): {error_msg}")
    
    # 确保进度条到达95%
    if last_percentage < 95:
        pbar.update(95 - last_percentage)

def process_single_video(video_path: str, output_path: str, roi: Optional[Tuple[int, int, int, int]], 
                        hardware_info: Dict[str, Any], video_idx: int, total_videos: int) -> Tuple[bool, float, Optional[str]]:
    """处理单个视频"""
    filename = os.path.basename(video_path)
    start_time = time.time()
    
    # 创建进度条 - 🔧 线程安全优化
    safe_position = None if video_idx >= 8 else video_idx + 1  # 限制同时显示的进度条数量
    show_individual_bar = video_idx < 8  # 只显示前8个视频的进度条，避免屏幕混乱
    
    pbar = tqdm(
        total=100, 
        desc=f"视频 {video_idx + 1}/{total_videos}: {filename[:25]:<25}",
        position=safe_position,
        leave=False,
        disable=not show_individual_bar,  # 超过8个时禁用个别进度条
        bar_format='{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]',
        smoothing=0.3,
        mininterval=1.0,  # 个别进度条更新间隔更长
        maxinterval=3.0
    )
    
    try:
        pbar.set_postfix_str("分析视频信息...")
        
        # 获取视频信息
        video_info = get_video_info(video_path)
        if not video_info:
            raise Exception("无法获取视频信息")
        
        duration = video_info.get('duration', 0)
        if duration <= 0:
            raise Exception("视频时长无效")
        
        # 计算有效时长（考虑切头尾）
        if ENABLE_HEAD_TAIL_CUT:
            effective_duration = max(0, duration - HEAD_CUT_TIME - TAIL_CUT_TIME)
        else:
            effective_duration = duration
        
        if effective_duration <= 0:
            raise Exception("切头尾后时长无效")
        
        pbar.set_postfix_str("构建处理命令...")
        
        # 构建FFmpeg命令
        cmd = build_unified_ffmpeg_command(video_path, output_path, roi, hardware_info)
        
        logging.info(f"执行命令: {' '.join(cmd)}")
        
        pbar.set_postfix_str("开始处理视频...")
        
        # 🚀 运行FFmpeg (使用i9优化器)
        i9_optimizer = hardware_info.get('i9_optimizer')
        run_ffmpeg_process(cmd, effective_duration, pbar, video_path, i9_optimizer)
        
        # 更新最终进度 (95-100%)
        if pbar.n < 100:
            pbar.update(100 - pbar.n)
            pbar.set_postfix_str("处理完成✓")
        
        # 验证输出文件
        if not os.path.exists(output_path) or os.path.getsize(output_path) < 1024:
            raise Exception(f"输出文件无效: {output_path}")
        
        processing_time = time.time() - start_time
        
        pbar.close()
        print(f"✅ FFmpeg处理完成: {filename}, 耗时: {processing_time:.2f}s")
        logging.info(f"视频处理完成: {filename}, 耗时: {processing_time:.2f}s")
        
        return True, processing_time, None
        
    except Exception as e:
        pbar.set_postfix_str("处理失败✗")
        pbar.close()
        
        error_msg = str(e)
        print(f"❌ FFmpeg处理失败: {filename} - {error_msg}")
        logging.error(f"视频处理失败 {filename}: {error_msg}")
        
        return False, time.time() - start_time, error_msg

def find_video_files(directory: str) -> List[str]:
    """查找目录中所有支持的视频文件"""
    video_files = []
    
    try:
        for format_ext in SUPPORTED_VIDEO_FORMATS:
            pattern = os.path.join(directory, f'*{format_ext}')
            files = [f for f in os.listdir(directory) 
                    if f.lower().endswith(format_ext.lower())]
            video_files.extend([os.path.join(directory, f) for f in files])
        
        video_files = list(set(video_files))  # 去重
        video_files.sort()
        
        logging.info(f"在目录 {directory} 中找到 {len(video_files)} 个支持的视频文件")
        
    except Exception as e:
        logging.error(f"搜索视频文件时出错: {e}")
    
    return video_files

def process_video_batch_with_pipeline(video_paths: List[str], roi: Optional[Tuple[int, int, int, int]], 
                                    hardware_info: Dict[str, Any], progress_manager: ProgressManager) -> Dict[str, Any]:
    """🚀 使用流水线管理器批量处理视频 - 防卡死加固版"""
    
    # 获取组件
    cache_manager = hardware_info.get('cache_manager')
    i9_optimizer = hardware_info.get('i9_optimizer')
    
    # 创建流水线管理器 - 2025年极限优化：翻倍处理效率
    pipeline_manager = VideoPipelineManager(
        max_concurrent_cache=6,    # 6个缓存线程（翻倍）
        max_concurrent_check=4,    # 4个查重线程（翻倍） 
        max_concurrent_process=4   # 4个处理线程（翻倍）
    )
    
    # 创建视频处理器适配器
    class VideoProcessor:
        def __init__(self, roi, hardware_info, progress_manager):
            self.roi = roi
            self.hardware_info = hardware_info
            self.progress_manager = progress_manager
            
        def process_single_video(self, input_path: str, original_path: str) -> bool:
            """处理单个视频"""
            try:
                # 调用原有的视频处理逻辑
                return process_single_video_file(
                    input_path=input_path,
                    original_path=original_path,
                    roi=self.roi,
                    hardware_info=self.hardware_info,
                    progress_manager=self.progress_manager
                )
            except Exception as e:
                logging.error(f"视频处理异常 {os.path.basename(original_path)}: {e}")
                return False
    
    video_processor = VideoProcessor(roi, hardware_info, progress_manager)
    
    # 统计变量
    start_time = time.time()
    
    try:
        logging.info(f"🚀 启动视频处理流水线...")
        
        # 🚀 批量数据库快速检查 - 在添加任务前过滤已处理视频
        videos_to_process, skipped_videos = pipeline_manager.bulk_database_check(video_paths, progress_manager)
        
        if skipped_videos:
            logging.info(f"🔍 批量检查跳过 {len(skipped_videos)} 个已处理视频")
            for skipped in skipped_videos:
                logging.debug(f"  └─ {skipped['video_name']} (处理者: {skipped['processor']})")
        
        # 启动流水线
        pipeline_manager.start_pipeline(cache_manager, progress_manager, video_processor)
        
        # 添加需要处理的任务
        if videos_to_process:
            logging.info(f"📝 添加 {len(videos_to_process)} 个任务到流水线...")
            for video_path in videos_to_process:
                pipeline_manager.add_task(video_path)
        else:
            logging.info("🎯 所有视频都已处理完成，无需添加新任务")
        
        # 创建进度监控 
        actual_task_count = len(videos_to_process) if videos_to_process else 0
        progress_thread = Thread(
            target=_monitor_pipeline_progress,
            args=(pipeline_manager, actual_task_count),
            daemon=True
        )
        progress_thread.start()
        
        # 等待完成
        logging.info("⏳ 等待所有任务完成...")
        success = pipeline_manager.wait_completion(timeout=None)  # 无限等待，让任务自然完成
        
        if not success:
            logging.warning("⏰ 流水线处理超时")
        else:
            logging.info("✅ 流水线处理正常完成")
        
        # 获取统计信息
        stats = pipeline_manager.get_statistics()
        
        # 计算总体统计（包括批量检查跳过的视频）
        total_skipped = len(skipped_videos)
        pipeline_completed = stats['completed_tasks']
        pipeline_failed = stats['failed_tasks']
        pipeline_duplicates = stats['duplicate_tasks']
        
        results = {
            'success_count': pipeline_completed + total_skipped,  # 包括批量检查跳过的
            'failed_count': pipeline_failed + pipeline_duplicates,
            'total_count': len(video_paths),  # 原始总数
            'duplicate_count': pipeline_duplicates + total_skipped,  # 包括批量检查发现的重复
            'pipeline_stats': stats,
            'bulk_check_skipped': total_skipped,
            'actually_processed': pipeline_completed
        }
        
        elapsed_time = time.time() - start_time
        logging.info(f"✅ 流水线处理完成: {results['success_count']}/{results['total_count']} "
                    f"成功, 耗时 {elapsed_time:.1f}s")
        
        return results
        
    except Exception as e:
        logging.error(f"❌ 流水线处理异常: {e}")
        return {
            'success_count': 0,
            'failed_count': len(video_paths),
            'total_count': len(video_paths),
            'duplicate_count': 0,
            'error': str(e)
        }
    finally:
        # 确保流水线关闭
        pipeline_manager.shutdown()

def _monitor_pipeline_progress(pipeline_manager: VideoPipelineManager, total_tasks: int):
    """监控流水线进度 - 简化版"""
    last_completed = 0
    
    # 简化进度条 - 只显示完成数量
    pbar = tqdm(
        total=total_tasks,
        desc="📹 视频处理",
        bar_format='{l_bar}{bar:40}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
        position=0,
        leave=True,
        mininterval=0.5
    )
    
    try:
        last_stats_time = time.time()
        
        while not pipeline_manager.shutdown_event.is_set():
            stats = pipeline_manager.get_statistics()
            completed = stats['completed_tasks']
            failed = stats['failed_tasks']
            duplicates = stats['duplicate_tasks']
            total_finished = completed + failed + duplicates
            
            # 更新进度条
            if total_finished > last_completed:
                pbar.update(total_finished - last_completed)
                last_completed = total_finished
            
            # 每30秒显示详细统计
            current_time = time.time()
            if current_time - last_stats_time >= 30:
                active_by_stage = stats.get('active_by_stage', {})
                queue_sizes = stats.get('queue_sizes', {})
                
                pending = active_by_stage.get('pending', 0)
                caching = active_by_stage.get('caching', 0)
                checking = active_by_stage.get('checking', 0)
                processing = active_by_stage.get('processing', 0)
                cached = active_by_stage.get('cached', 0)
                
                avg_time = stats.get('avg_process_time', 0)
                
                print(f"\n📊 当前状态: 完成={completed}, 失败={failed}, 重复={duplicates}")
                print(f"📊 活动任务: 等待={pending}, 缓存中={caching}, 检查中={checking}, 处理中={processing}, 已缓存={cached}")
                print(f"📊 队列大小: {queue_sizes}")
                if avg_time > 0:
                    print(f"📊 平均处理时间: {avg_time:.1f}秒")
                last_stats_time = current_time
            
            # 检查是否完成
            if total_finished >= total_tasks:
                break
            
            time.sleep(1)
            
    except Exception as e:
        logging.error(f"进度监控异常: {e}")
    finally:
        pbar.close()

def process_single_video_file(input_path: str, original_path: str, roi: Optional[Tuple[int, int, int, int]], 
                            hardware_info: Dict[str, Any], progress_manager: ProgressManager) -> bool:
    """处理单个视频文件"""
    try:
        video_name = os.path.basename(original_path)
        logging.info(f"⚙️ 开始处理: {video_name}")
        
        # 生成输出路径
        base_name = os.path.splitext(os.path.basename(original_path))[0]
        output_path = os.path.join(OUTPUT_DIR, f"{base_name}.mp4")
        
        # 检查输出文件是否已存在
        if os.path.exists(output_path):
            logging.info(f"✅ 输出文件已存在，跳过: {video_name}")
            progress_manager.mark_completed(original_path, output_path)
            return True
        
        # 创建临时输出路径
        temp_output = output_path + ".tmp"
        
        # 构建FFmpeg命令（使用已有函数）
        cmd = build_unified_ffmpeg_command(input_path, temp_output, roi, hardware_info)
        
        # 执行处理
        success = _execute_ffmpeg_safe(cmd, video_name)
        
        if success and os.path.exists(temp_output):
            # 移动到最终位置
            shutil.move(temp_output, output_path)
            logging.info(f"✅ 处理完成: {video_name}")
            
            # 记录完成状态
            progress_manager.mark_completed(original_path, output_path)
            
            # 计算并记录SHA256（如果启用数据库）- 🔧 修复：使用原始路径计算哈希
            if progress_manager.video_record_manager:
                try:
                    # 🔧 关键修复：使用原始路径而不是缓存路径计算输入哈希
                    original_sha256 = progress_manager.video_record_manager.calculate_video_sha256(original_path, quick_mode=False)
                    output_sha256 = progress_manager.video_record_manager.calculate_video_sha256(output_path, quick_mode=False) 
                    
                    if original_sha256 and output_sha256:
                        progress_manager.video_record_manager.record_processing_result(
                            video_path=original_path,
                            input_sha256=original_sha256,
                            output_path=output_path,
                            output_sha256=output_sha256,
                            processing_status='completed',
                            metadata={'roi': roi} if roi else {}
                        )
                        logging.debug(f"📝 数据库记录已更新 (原始路径): {video_name}")
                except Exception as e:
                    logging.warning(f"数据库记录失败 {video_name}: {e}")
            
            return True
        else:
            # 清理临时文件
            if os.path.exists(temp_output):
                os.remove(temp_output)
            logging.error(f"❌ 处理失败: {video_name}")
            return False
            
    except Exception as e:
        logging.error(f"❌ 处理异常 {os.path.basename(original_path)}: {e}")
        return False

def _execute_ffmpeg_safe(cmd: List[str], video_name: str, timeout: int = 1800) -> bool:
    """安全执行FFmpeg命令，带超时和错误处理"""
    input_file = None
    output_file = None
    
    try:
        # 检查FFmpeg是否存在
        if not os.path.exists(cmd[0]):
            logging.error(f"❌ FFmpeg不存在: {cmd[0]}")
            return False
            
        # 从命令中提取输入和输出文件路径
        if '-i' in cmd:
            input_idx = cmd.index('-i')
            if input_idx + 1 < len(cmd):
                input_file = cmd[input_idx + 1]
        if len(cmd) > 1:
            output_file = cmd[-1]  # 通常输出文件是最后一个参数
            
        logging.debug(f"🔧 执行FFmpeg: {' '.join(cmd[:5])}... (共{len(cmd)}个参数)")
        
        # 详细记录完整命令用于调试
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"完整FFmpeg命令: {' '.join(cmd)}")
        
        # 使用subprocess.run而不是其他方式，确保稳定性
        # 在Windows上设置正确的编码和创建标志
        startupinfo = None
        if os.name == 'nt':  # Windows
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = subprocess.SW_HIDE
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',  # 处理编码错误
            timeout=timeout,
            check=False,  # 不自动抛出异常
            startupinfo=startupinfo,
            cwd=os.path.dirname(cmd[0]) if os.path.dirname(cmd[0]) else None  # 设置工作目录
        )
        
        if result.returncode == 0:
            logging.debug(f"✅ FFmpeg执行成功: {video_name}")
            return True
        else:
            # 详细记录错误信息
            error_msg = result.stderr if result.stderr else "未知错误"
            logging.error(f"❌ FFmpeg执行失败 {video_name}: 返回码 {result.returncode}")
            
            # 输出完整命令用于调试
            logging.error(f"完整命令: {' '.join(cmd)}")
            
            if error_msg.strip():
                logging.error(f"FFmpeg错误输出: {error_msg}")
            
            # 检查常见错误原因
            if result.returncode == 4294967274 or result.returncode == -22:
                logging.error("可能原因: 文件路径包含特殊字符、权限不足或编码器不支持")
                logging.error(f"输入文件存在: {os.path.exists(input_file) if input_file else '未知'}")
                logging.error(f"输出目录存在: {os.path.exists(os.path.dirname(output_file)) if output_file else '未知'}")
            elif result.returncode == 1:
                logging.error("FFmpeg通用错误，可能是编码参数问题")
            elif result.returncode < 0:
                logging.error(f"FFmpeg被信号终止: {abs(result.returncode)}")
            
            return False
            
    except subprocess.TimeoutExpired:
        logging.error(f"⏰ FFmpeg执行超时 {video_name}: {timeout}秒")
        return False
    except Exception as e:
        logging.error(f"❌ FFmpeg执行异常 {video_name}: {e}")
        return False

def process_video_batch(video_paths: List[str], roi: Optional[Tuple[int, int, int, int]], 
                       hardware_info: Dict[str, Any], progress_manager: ProgressManager) -> Dict[str, Any]:
    """🚀 批量处理视频 - 2025年NAS极限优化版"""
    
    # 🚀 获取NAS优化组件
    cache_manager = hardware_info.get('cache_manager')
    i9_optimizer = hardware_info.get('i9_optimizer')
    
    # 💡 启动智能预加载 - 🔧 添加緊急修復檢查
    if cache_manager and ENABLE_CACHE and PRELOAD_COUNT > 0:
        logging.info(f"🚀 启动智能预加载系统...")
        cache_manager.preload_videos(video_paths, 0)
        
        # 显示缓存状态
        cache_stats = cache_manager.get_cache_stats()
        logging.info(f"📊 缓存状态: {cache_stats['total_entries']}条目, "
                    f"{cache_stats['total_size_gb']:.1f}GB, "
                    f"命中率{cache_stats['cache_hit_ratio']:.1f}%")
    else:
        logging.info(f"⚠️ 緩存功能已禁用 - 直接處理視頻文件")
    
    # 创建总进度条
    # 🔧 线程安全的进度条
    import threading
    progress_lock = threading.Lock()
    
    total_pbar = tqdm(
        total=len(video_paths), 
        desc="📁 总体进度", 
        position=0, 
        leave=True,
        bar_format='{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
        smoothing=0.1,  # 减少跳跃
        mininterval=0.5,  # 最小更新间隔
        maxinterval=2.0   # 最大更新间隔
    )
    
    success_count = 0
    failed_count = 0
    
    def update_main_progress():
        nonlocal success_count, failed_count
        with progress_lock:
            try:
                total_pbar.update(1)
                completed = success_count + failed_count
                success_rate = success_count / completed * 100 if completed > 0 else 0
                
                total_pbar.set_postfix({
                    '成功': success_count,
                    '失败': failed_count,
                    '成功率': f"{success_rate:.1f}%"
                })
                
                # 强制刷新显示
                total_pbar.refresh()
                
            except Exception as e:
                logging.debug(f"进度条更新异常: {e}")
                pass

    def process_single_wrapper(video_info):
        """带有内存管理的视频处理包装器"""
        video_path, video_idx = video_info
        
        try:
            # 确保输出文件为.mp4格式（纯视频格式）
            base_name = os.path.splitext(os.path.basename(video_path))[0]
            output_path = os.path.join(OUTPUT_DIR, f"{base_name}.mp4")
            
            # 🚀 NAS缓存优化：获取缓存路径或等待缓存完成 - 🔧 緊急修復
            actual_video_path = video_path
            if cache_manager and ENABLE_CACHE:
                cached_path = cache_manager.get_cached_path(video_path)
                if cached_path:
                    actual_video_path = cached_path
                    logging.info(f"🎯 使用缓存: {os.path.basename(video_path)}")
                else:
                    # 🔧 緊急修復：縮短等待時間並增加超時處理
                    logging.info(f"⏰ 缓存未就绪，降级到30秒快速等待: {os.path.basename(video_path)}")
                    cached_path = cache_manager.wait_for_cache(video_path, timeout=30)
                    if cached_path:
                        actual_video_path = cached_path
                    else:
                        logging.warning(f"⚠️ 緩存超時，直接使用原始路徑: {os.path.basename(video_path)}")
            else:
                logging.info(f"🔧 緩存已禁用，直接處理: {os.path.basename(video_path)}")
            
            # 🔄 预加载下一个视频 - 🔧 緊急修復
            if cache_manager and ENABLE_CACHE and PRELOAD_COUNT > 0:
                cache_manager.preload_videos(video_paths, video_idx)
            
            # 🔗 获取视频信息用于数据库记录
            video_info = get_video_info(actual_video_path)
            
            # 🔍 缓存后查重检查（避免浪费处理资源）- 🔧 修复：使用原始路径计算哈希
            if progress_manager.video_record_manager and progress_manager.video_record_manager.db_manager.is_available():
                # 🔧 关键修复：使用原始视频路径计算SHA256，确保数据库一致性
                hash_value = progress_manager.video_record_manager.calculate_video_sha256(video_path, quick_mode=False)
                if hash_value:
                    is_processed, db_record = progress_manager.video_record_manager.is_video_processed(video_path, hash_value)
                    if is_processed:
                        logging.info(f"🚀 缓存后发现重复，跳过处理: {os.path.basename(video_path)} "
                                   f"(处理者: {db_record.get('computer_name', 'unknown')})")
                        
                        # 清理缓存文件（节省磁盘空间）
                        if cache_manager and actual_video_path != video_path:
                            try:
                                cache_manager.remove_from_cache(video_path)
                                logging.debug(f"🗑️ 已清理重复文件缓存: {os.path.basename(video_path)}")
                            except Exception as e:
                                logging.debug(f"清理缓存失败: {e}")
                        
                        # 标记为已完成（本地记录）
                        progress_manager.mark_completed(video_path, db_record.get('output_path', ''), 0)
                        return True
                    
                    # 检查是否正在被其他电脑处理
                    is_processing, processing_info = progress_manager.video_record_manager.is_video_processing(video_path, hash_value)
                    if is_processing:
                        logging.info(f"🚀 缓存后发现正在处理，跳过: {os.path.basename(video_path)} "
                                   f"(处理者: {processing_info.get('computer_name', 'unknown')})")
                        
                        # 清理缓存文件
                        if cache_manager and actual_video_path != video_path:
                            try:
                                cache_manager.remove_from_cache(video_path)
                            except Exception as e:
                                logging.debug(f"清理缓存失败: {e}")
                        
                        return False  # 跳过处理
            
            # 标记为处理中（传递视频信息）
            progress_manager.mark_processing(video_path, video_info)
            
            # 处理视频
            success, processing_time, error_msg = process_single_video(
                actual_video_path, output_path, roi, hardware_info, video_idx, len(video_paths)
            )
            
            if success:
                progress_manager.mark_completed(video_path, output_path, processing_time)
                return True
            else:
                progress_manager.mark_failed(video_path, error_msg or "处理失败")
                return False
                
        except Exception as e:
            logging.error(f"视频处理异常 {video_path}: {e}")
            try:
                progress_manager.mark_failed(video_path, str(e))
            except:
                pass
            return False
        finally:
            # 🧹 强制垃圾回收，释放内存
            try:
                import gc
                gc.collect()
                
                # 清理局部变量
                locals().clear()
                
                # 如果是进程池，额外清理
                if hasattr(os, 'getpid'):
                    import psutil
                    process = psutil.Process()
                    # 限制内存使用
                    if process.memory_info().rss > 2 * 1024 * 1024 * 1024:  # 超过2GB
                        logging.warning(f"工作进程内存使用过高: {process.memory_info().rss / 1024 / 1024:.1f}MB")
                        gc.collect()
            except Exception as cleanup_error:
                logging.debug(f"内存清理异常: {cleanup_error}")
                pass

    # 🚀 2025年极限并行优化
    if i9_optimizer:
        max_workers = i9_optimizer.get_optimal_worker_count('encoding')
    else:
        max_workers = hardware_info.get("max_parallel", 4)
        
    # 硬件编码器限制调整
    if hardware_info["encoder_type"] != "software":
        max_workers = min(max_workers, 8)  # 2025年硬件编码器更强
    else:
        cpu_cores = hardware_info.get("cpu_cores", 8)
        if hardware_info.get('is_i9', False):
            max_workers = min(max_workers, min(cpu_cores - 2, 24))  # i9极限配置
        else:
            max_workers = min(max_workers, cpu_cores // 2)
    
    logging.info(f"🚀 2025年极限配置: {hardware_info['encoder_type']}, 并行数: {max_workers}, "
                f"i9优化: {hardware_info.get('is_i9', False)}")
    
    # 选择执行器类型
    executor_class = concurrent.futures.ThreadPoolExecutor if hardware_info["encoder_type"] != "software" else concurrent.futures.ProcessPoolExecutor
    
    with executor_class(max_workers=max_workers) as executor:
        # 清屏并开始处理
        print("\033[2J\033[H", end="")
        
        video_info_list = [(path, idx) for idx, path in enumerate(video_paths)]
        futures = [executor.submit(process_single_wrapper, info) for info in video_info_list]
        
        # 等待完成并更新进度
        processed_count = 0
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                logging.error(f"任务异常: {e}")
            
            processed_count += 1
            update_main_progress()
            
            # 🧹 定期内存清理 - 每处理10个视频清理一次
            if processed_count % 10 == 0:
                try:
                    import gc
                    gc.collect()
                    
                    # 获取当前内存使用情况
                    import psutil
                    process = psutil.Process()
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    logging.info(f"🧹 内存清理完成，当前使用: {memory_mb:.1f}MB")
                    
                    # 如果内存使用过高，强制清理
                    if memory_mb > 4096:  # 超过4GB
                        logging.warning(f"⚠️ 内存使用过高: {memory_mb:.1f}MB，执行强制清理")
                        gc.collect()
                        gc.collect()  # 双重清理
                        
                except Exception as cleanup_error:
                    logging.debug(f"内存清理异常: {cleanup_error}")
                    pass
    
    total_pbar.close()
    
    return {
        'success_count': success_count,
        'failed_count': failed_count,
        'total_count': len(video_paths)
    }

def check_database_table_with_retry(max_retries=None, retry_interval=5):
    """检查数据库连接（带重试机制）"""
    if not ENABLE_MULTI_COMPUTER_SYNC:
        return True
    
    retry_count = 0
    while True:
        try:
            print(f"🔗 正在尝试连接数据库... (第{retry_count + 1}次)")
            db_manager = DatabaseManager()
            
            if not db_manager.is_available():
                raise Exception("数据库管理器初始化失败")
                
            with db_manager.get_connection() as connection:
                if not connection or not connection.is_connected():
                    raise Exception("数据库连接失败")
                
                logger.info("✅ 数据库连接成功")
                print("✅ 数据库连接成功")
                return True
                    
        except Exception as e:
            retry_count += 1
            logger.warning(f"❌ 数据库连接失败 (第{retry_count}次): {e}")
            print(f"❌ 数据库连接失败 (第{retry_count}次): {e}")
            
            if max_retries and retry_count >= max_retries:
                logger.error(f"数据库连接失败，已达到最大重试次数 ({max_retries})")
                print(f"❌ 数据库连接失败，已达到最大重试次数 ({max_retries})")
                return False
            
            print(f"⏳ {retry_interval}秒后重试...")
            time.sleep(retry_interval)

def check_database_table():
    """检查数据库连接（假设表已存在）"""
    if not ENABLE_MULTI_COMPUTER_SYNC:
        return True
        
    try:
        db_manager = DatabaseManager()
        if not db_manager.is_available():
            logger.warning("数据库不可用，将以单机模式运行")
            return False
            
        with db_manager.get_connection() as connection:
            if not connection or not connection.is_connected():
                logger.warning("数据库连接失败，将以单机模式运行")
                return False
            
            logger.info("✅ 数据库连接正常")
            return True
                
    except Exception as e:
        logger.error(f"数据库连接检查失败: {e}")
        print(f"❌ 数据库连接检查失败: {e}")
        return False

def setup_logging():
    """设置日志并返回日志文件路径"""
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    log_filename = f"unified_video_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file_path = os.path.join(log_dir, log_filename)
    
    # 获取日志文件的绝对路径
    abs_log_path = os.path.abspath(log_file_path)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    # 减少第三方库日志
    logging.getLogger('PIL').setLevel(logging.WARNING)
    logging.getLogger('matplotlib').setLevel(logging.WARNING)
    
    # 记录日志文件地址
    logger = logging.getLogger(__name__)
    logger.info(f"📝 日志文件地址: {abs_log_path}")
    print(f"📝 日志文件地址: {abs_log_path}")
    
    return abs_log_path

def main():
    """🚀 主函数 - 2025年NAS极限优化版"""
    print("🎬 MC_L的终极战斗仪视频处理器 - NAS极限优化版")
    print("🚀 2025年MC_L的优化策略:")
    print("   💾 智能本地缓存系统 (500TB+ NAS优化)")
    print("   ⚡ i9处理器极致性能调优")
    print("   🌐 异步预读取和断点续传")
    print("   🧠 内存和存储智能管理")
    print("=" * 60)
    
    # 设置日志并获取日志文件路径
    log_file_path = setup_logging()
    
    # 🔗 检查数据库表（多电脑协作）
    if ENABLE_MULTI_COMPUTER_SYNC:
        print("🔗 检查数据库连接和表结构...")
        print("💡 数据库连接失败时将自动重试，直到连接成功...")
        
        # 使用带重试的数据库连接检查
        if not check_database_table_with_retry():
            print("⚠️ 数据库连接重试失败，将以单机模式继续运行")
            # 可以选择继续或退出
            choice = input("是否继续以单机模式运行? (y/n，回车默认是): ").strip().lower()
            if choice in ['n', 'no']:
                print("👋 退出程序")
                return
    
    # 验证配置
    print("🔍 验证配置...")
    if not os.path.exists(FFMPEG_PATH):
        print(f"❌ FFmpeg路径不存在: {FFMPEG_PATH}")
        return
    else:
        print(f"✅ FFmpeg已找到: {FFMPEG_PATH}")
    
    if not os.path.exists(INPUT_DIR):
        print(f"❌ 输入目录不存在: {INPUT_DIR}")
        return
    else:
        print(f"✅ 输入目录: {INPUT_DIR}")
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"✅ 创建输出目录: {OUTPUT_DIR}")
    else:
        print(f"✅ 输出目录: {OUTPUT_DIR}")
    
    if LOCAL_CACHE_DIR and not os.path.exists(LOCAL_CACHE_DIR):
        os.makedirs(LOCAL_CACHE_DIR)
        print(f"✅ 创建缓存目录: {LOCAL_CACHE_DIR}")
    elif LOCAL_CACHE_DIR:
        print(f"✅ 缓存目录: {LOCAL_CACHE_DIR}")
    
    # 创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(PROGRESS_FOLDER, exist_ok=True)
    
    print("✅ 配置验证通过")
    
    # 显示当前配置
    print("\n📋 当前配置:")
    print(f"  输入目录: {INPUT_DIR}")
    print(f"  输出目录: {OUTPUT_DIR}")
    print(f"  切头尾功能: {'启用' if ENABLE_HEAD_TAIL_CUT else '禁用'}")
    if ENABLE_HEAD_TAIL_CUT:
        print(f"    片头时间: {HEAD_CUT_TIME}秒")
        print(f"    片尾时间: {TAIL_CUT_TIME}秒")
    print(f"  裁剪功能: {'启用' if ENABLE_CROPPING else '禁用'}")
    if ENABLE_CROPPING:
        print(f"    目标分辨率: {TARGET_RESOLUTION[0]}x{TARGET_RESOLUTION[1]}")
    
    print("\n🚀 NAS极限优化配置:")
    print(f"  本地缓存目录: {LOCAL_CACHE_DIR}")
    print(f"  最大缓存大小: {MAX_CACHE_SIZE_GB}GB")
    print(f"  预加载视频数: {PRELOAD_COUNT}个")
    print(f"  异步下载线程: {ASYNC_DOWNLOAD_THREADS}个")
    print(f"  i9睿频优化: {'启用' if ENABLE_I9_TURBO else '禁用'}")
    print(f"  CPU亲和性优化: {'启用' if CPU_AFFINITY_OPTIMIZATION else '禁用'}")
    print(f"  内存池大小: {MEMORY_POOL_SIZE_GB}GB")
    print(f"  临时处理目录: {TEMP_PROCESSING_DIR}")
    
    # 初始化硬件检测器 - 防死锁加固版
    print("\n🔧 检测硬件配置...")
    try:
        # 使用超时保护来避免硬件检测死锁
        import threading
        hardware_detector = None
        hardware_info = None
        
        def detect_hardware_thread():
            nonlocal hardware_detector, hardware_info
            try:
                hardware_detector = HardwareDetector(log_file_path)
                hardware_info = hardware_detector.detect_hardware_capabilities()
                logging.info("✅ 硬件检测完成")
            except Exception as e:
                logging.error(f"硬件检测线程异常: {e}")
        
        # 启动检测线程，设置60秒超时
        detect_thread = threading.Thread(target=detect_hardware_thread, daemon=True)
        detect_thread.start()
        detect_thread.join(timeout=60)
        
        if detect_thread.is_alive():
            print("⚠️  硬件检测超时，使用基础配置继续运行")
            logging.warning("硬件检测超时，回退到基础硬件检测")
            # 创建基础配置，避免卡在HardwareDetector初始化
            hardware_detector = None
            hardware_info = {
                'encoder_type': 'software',
                'encoder': 'libx264', 
                'cpu_cores': multiprocessing.cpu_count(),
                'memory_gb': psutil.virtual_memory().total / (1024**3),
                'is_i9': multiprocessing.cpu_count() >= 16,
                'max_parallel': min(multiprocessing.cpu_count() // 2, 8),
                'numa_nodes': 1,
                'options': {},
                'computer_name': f"fallback_{socket.gethostname()}"
            }
        elif hardware_info is None:
            print("⚠️  硬件检测异常，使用基础配置继续运行")
            logging.warning("硬件检测异常，回退到基础硬件检测")
            hardware_detector = None
            hardware_info = {
                'encoder_type': 'software',
                'encoder': 'libx264',
                'cpu_cores': multiprocessing.cpu_count(),
                'memory_gb': psutil.virtual_memory().total / (1024**3),
                'is_i9': multiprocessing.cpu_count() >= 16,
                'max_parallel': min(multiprocessing.cpu_count() // 2, 8),
                'numa_nodes': 1,
                'options': {},
                'computer_name': f"fallback_{socket.gethostname()}"
            }
            
    except Exception as e:
        print(f"⚠️  硬件检测失败: {e}")
        logging.error(f"硬件检测失败，使用默认配置: {e}")
        # 确保程序能继续运行的最小配置
        hardware_detector = None
        hardware_info = {
            'encoder_type': 'software',
            'encoder': 'libx264',
            'cpu_cores': multiprocessing.cpu_count(),
            'memory_gb': 8.0,
            'is_i9': False,
            'max_parallel': 4,
            'numa_nodes': 1,
            'options': {},
                'computer_name': f"error_{socket.gethostname()}"
        }
    
    print(f"✅ 2025年极限硬件检测完成:")
    print(f"  编码器类型: {hardware_info['encoder_type']}")
    print(f"  编码器: {hardware_info['encoder']}")
    print(f"  CPU核心数: {hardware_info.get('cpu_cores', 'N/A')} (i9: {hardware_info.get('is_i9', False)})")
    print(f"  内存: {hardware_info.get('memory_gb', 0):.1f}GB (池: {hardware_info.get('memory_pool_gb', 0)}GB)")
    print(f"  最大并行数: {hardware_info.get('max_parallel', 'N/A')}")
    print(f"  NUMA节点: {hardware_info.get('numa_nodes', 'N/A')}")
    
    # 显示缓存状态
    cache_stats = hardware_info.get('cache_stats', {})
    print(f"  缓存状态: {cache_stats.get('total_entries', 0)}条目 ({cache_stats.get('total_size_gb', 0):.1f}GB)")
    print(f"  网络性能: 平均{cache_stats.get('avg_network_speed_mbps', 0):.1f}MB/s, 峰值{cache_stats.get('peak_network_speed_mbps', 0):.1f}MB/s")
    
    # 获取计算机名称（优先使用用户配置，处理硬件检测器为None的情况）
    computer_name = COMPUTER_NAME if COMPUTER_NAME else hardware_info.get('computer_name', f"fallback_{socket.gethostname()}")
    
    # 初始化增强的数据库系统管理器
    db_system_manager = None
    if ENABLE_MULTI_COMPUTER_SYNC:
        print("\n🔧 初始化数据库系统...")
        db_system_manager = DatabaseSystemManager(computer_name, log_file_path)
        if db_system_manager.initialize_system():
            print("✅ 数据库系统初始化成功")
            # 替换硬件信息中的video_record_manager
            hardware_info['video_record_manager'] = db_system_manager.record_manager
        else:
            print("⚠️ 数据库系统初始化失败，将以单机模式运行")
            db_system_manager = None
    
    # 初始化进度管理器（集成多电脑协作）
    video_record_manager = hardware_info.get('video_record_manager')
    progress_manager = ProgressManager(computer_name, video_record_manager)
    
    # 🔧 配置智能查重系统
    if video_record_manager:
        # 设置重复视频输出路径（可配置）
        duplicate_output_path = os.path.join(os.path.dirname(OUTPUT_DIR), "重复视频")
        video_record_manager.set_duplicate_output_path(duplicate_output_path)
        print(f"✅ 重复视频将移动到: {duplicate_output_path}")
        
        # 创建重复视频输出目录
        if not os.path.exists(duplicate_output_path):
            os.makedirs(duplicate_output_path, exist_ok=True)
    
    # 🔗 显示多电脑协作状态
    if db_system_manager and db_system_manager.db_manager.is_available():
        print(f"🔗 多电脑协作: 已启用 (增强版)")
        print(f"   数据库: {MYSQL_CONFIG['host']}:{MYSQL_CONFIG.get('port', 3306)}")
        print(f"   电脑名称: {hardware_info.get('computer_name', 'unknown')}")
        print(f"   处理人: {PROCESSOR_NAME}")
        
        # 显示系统状态报告
        try:
            status_report = db_system_manager.get_system_status_report()
            basic_health = status_report.get('basic_health', False)
            performance_metrics = status_report.get('performance_metrics', {})
            
            print(f"   系统健康: {'✅ 正常' if basic_health else '⚠️ 异常'}")
            if performance_metrics:
                print(f"   数据库连接: {performance_metrics.get('active_connections', 'N/A')}")
                print(f"   缓冲池使用率: {performance_metrics.get('buffer_pool_usage_percent', 'N/A')}%")
            
            # 显示协作统计
            table_stats = status_report.get('table_statistics', {})
            if table_stats:
                total_records = table_stats.get('total_records', 0)
                success_count = table_stats.get('successful_processes', 0)
                today_count = table_stats.get('today_processes', 0)
                print(f"📊 处理统计: 总计{total_records}, 成功{success_count}, 今日{today_count}")
                
                # 显示各电脑统计
                computer_stats = table_stats.get('computer_statistics', [])
                if computer_stats:
                    print(f"   协作电脑（前5台）:")
                    for computer in computer_stats[:5]:
                        print(f"     {computer['computer_name']}: {computer['success_count']}/{computer['total_count']} "
                              f"({computer['success_rate']}%)")
                        
            # 显示建议
            recommendations = status_report.get('recommendations', [])
            if recommendations:
                print(f"💡 系统建议:")
                for rec in recommendations[:3]:  # 显示前3个建议
                    print(f"   - {rec}")
                    
        except Exception as e:
            logger.debug(f"获取系统状态报告失败: {e}")
            # 回退到基本统计显示
            try:
                stats = video_record_manager.get_processing_statistics()
                if stats.get('computer_stats'):
                    print(f"📊 协作统计（近7天）:")
                    for computer in stats['computer_stats'][:5]:
                        success_rate = computer['completed'] / computer['total'] * 100 if computer['total'] > 0 else 0
                        print(f"   {computer['computer_name']}: {computer['completed']}/{computer['total']} "
                              f"({success_rate:.1f}%)")
            except Exception as e2:
                logger.debug(f"获取基本统计也失败: {e2}")
    else:
        print(f"⚠️  多电脑协作: 已禁用（单机模式）")
    
    # 检查崩溃恢复
    print("🔄 检查是否需要崩溃恢复...")
    recovery_data = progress_manager.check_crash_recovery()
    
    should_resume = False
    resume_roi = None
    
    if recovery_data:
        print(f"🔥 检测到上次会话异常终止 ({recovery_data.get('last_session_time', 'unknown')})")
        
        # 检测部分输出文件
        partial_files = progress_manager.detect_partial_outputs()
        if partial_files:
            print(f"⚠️  发现 {len(partial_files)} 个可能的部分处理文件:")
            for pf in partial_files[:5]:  # 只显示前5个
                size_mb = pf['size'] / (1024*1024)
                print(f"   - {pf['name']} ({size_mb:.1f}MB)")
            if len(partial_files) > 5:
                print(f"   ... 还有 {len(partial_files)-5} 个文件")
                
        # 询问是否继续处理
        print("\n🤔 发现上次处理可能被中断，您希望:")
        print("   [1] 继续断点续传 (推荐)")
        print("   [2] 重新开始处理 (清理部分文件)")
        print("   [3] 退出程序")
        
        while True:
            try:
                choice = input("\n请选择 (1-3，回车默认选1): ").strip()
                if choice == '' or choice == '1':
                    should_resume = True
                    resume_roi = recovery_data.get('roi_settings')
                    if resume_roi:
                        resume_roi = tuple(resume_roi)
                    print("✅ 选择断点续传模式")
                    break
                elif choice == '2':
                    print("🧹 清理部分文件并重新开始...")
                    for pf in partial_files:
                        try:
                            os.remove(pf['path'])
                            print(f"   删除: {pf['name']}")
                        except Exception as e:
                            print(f"   删除失败 {pf['name']}: {e}")
                    progress_manager.clear_crash_recovery()
                    should_resume = False
                    print("✅ 清理完成，重新开始处理")
                    break
                elif choice == '3':
                    print("👋 退出程序")
                    return
                else:
                    print("❌ 请输入 1、2 或 3")
            except KeyboardInterrupt:
                print("\n👋 用户取消操作")
                return
    else:
        progress_manager.clear_crash_recovery()
    
    # 查找视频文件（快速扫描，不做深度检查）
    print(f"\n📂 扫描视频文件: {INPUT_DIR}")
    video_paths = find_video_files(INPUT_DIR)
    
    if not video_paths:
        print("❌ 未找到支持的视频文件")
        return
    else:
        print(f"✅ 找到 {len(video_paths)} 个视频文件")
    
    # ROI选择（仅在启用裁剪功能时）
    roi = None
    if ENABLE_CROPPING:
        print(f"\n🎯 ROI区域选择...")
        
        # 优先使用断点续传的ROI设置
        if should_resume and resume_roi:
            roi = resume_roi
            print(f"✅ 使用断点续传的ROI设置: {roi}")
        else:
            # 检查是否有保存的ROI设置
            saved_roi = progress_manager.progress_data.get('roi_settings')
            if saved_roi:
                print(f"发现保存的ROI设置: {saved_roi}")
                use_saved = input("使用保存的ROI设置? (y/n，回车默认是): ").strip().lower()
                if use_saved in ['y', 'yes', '']:
                    roi = tuple(saved_roi)
                    print(f"✅ 使用保存的ROI设置: {roi}")
            
            if roi is None:
                print("🎯 开始16:9智能ROI选择...")
                roi_selector = ROISelector()
                
                # 选择第一个视频作为预览
                preview_video = video_paths[0]
                print(f"📺 使用视频进行ROI预览: {os.path.basename(preview_video)}")
                
                roi = roi_selector.select_roi_for_video(preview_video)
                if roi is None:
                    print("❌ ROI选择失败")
                    return
                
                # 保存ROI设置（包含基准尺寸）
                progress_manager.progress_data['roi_settings'] = roi
                progress_manager.save_progress()
                
                if len(roi) == 6:
                    x, y, w, h, base_w, base_h = roi
                    print(f"✅ ROI选择完成: 裁剪区域({x},{y},{w},{h}) 基准尺寸({base_w}x{base_h})")
                else:
                    print(f"✅ ROI选择完成: {roi}")
        
        print(f"📐 使用ROI区域: {roi}")
    else:
        print("ℹ️  裁剪功能已禁用，跳过ROI选择")
    
    print(f"\n💡 简化过滤功能已取消，因为缓存后有数据库查重操作")
    print(f"📝 共找到 {len(video_paths)} 个视频文件，将直接进入缓存阶段")
    
    if not video_paths:
        print("✅ 没有找到任何视频文件！")
        return
    
    # 📋 ROI选择完成后，直接读取进度文件跳过已完成视频
    print(f"\n📋 正在读取进度文件检查已完成的视频...")
    filtered_video_paths = []
    skipped_count = 0
    
    # 遍历所有视频，使用进度管理器的快速检查模式
    for video_path in video_paths:
        video_name = os.path.basename(video_path)
        
        # 使用进度管理器的快速检查模式（检查输出文件、本地记录、旧版本记录）
        if progress_manager.is_completed(video_path, quick_check=True):
            print(f"⏭️  跳过已完成: {video_name} (进度文件记录)")
            skipped_count += 1
        else:
            filtered_video_paths.append(video_path)
    
    print(f"📊 进度文件过滤结果:")
    print(f"   原始视频数量: {len(video_paths)}")
    print(f"   已完成跳过: {skipped_count}")
    print(f"   待处理数量: {len(filtered_video_paths)}")
    
    # 使用过滤后的视频列表
    video_paths = filtered_video_paths
    
    if not video_paths:
        print("✅ 所有视频都已完成处理！")
        return
    
    # 开始批量处理
    print(f"\n🚀 开始批量处理...")
    print(f"处理模式: ", end="")
    if ENABLE_HEAD_TAIL_CUT and ENABLE_CROPPING:
        print("完整处理 (切头尾 + 裁剪)")
    elif ENABLE_HEAD_TAIL_CUT:
        print("仅切头尾")
    elif ENABLE_CROPPING:
        print("仅裁剪")
    else:
        print("❌ 未启用任何处理功能")
        return
    
    start_time = time.time()
    
    # 保存崩溃恢复信息
    if video_paths:
        progress_manager.save_crash_recovery(video_paths[0], roi)
    
    try:
        # 🚀 使用新的流水线管理器处理
        results = process_video_batch_with_pipeline(video_paths, roi, hardware_info, progress_manager)
        
        # 🔄 检查是否有数据库插入失败的视频需要重试
        if results['failed_count'] > 0 and ENABLE_MULTI_COMPUTER_SYNC:
            print(f"\n🔄 检测到 {results['failed_count']} 个处理失败的视频，开始重试数据库插入...")
            
            max_retry_rounds = 3  # 最多重试3轮
            retry_delay = 60  # 每轮重试间隔60秒
            
            for retry_round in range(max_retry_rounds):
                print(f"\n🔄 第 {retry_round + 1}/{max_retry_rounds} 轮重试...")
                
                # 重新扫描失败的视频，尝试插入数据库
                failed_videos = []
                for video_path in video_paths:
                    if progress_manager.video_record_manager:
                        # 检查视频是否在数据库中
                        video_name = os.path.basename(video_path)
                        try:
                            video_info = progress_manager.video_record_manager.get_video_info_fast(video_path)
                            if video_info:
                                hash_value = progress_manager.video_record_manager.calculate_video_sha256(video_path, quick_mode=True)
                                success = progress_manager.video_record_manager.insert_video_record(
                                    video_path, video_name, hash_value, video_info
                                )
                                if not success:
                                    failed_videos.append(video_path)
                                else:
                                    print(f"✅ 重试成功插入: {video_name}")
                        except Exception as e:
                            logger.warning(f"重试检查视频失败 {video_name}: {e}")
                            failed_videos.append(video_path)
                
                if not failed_videos:
                    print("✅ 所有视频数据库插入重试成功！")
                    break
                elif retry_round < max_retry_rounds - 1:
                    print(f"⏳ 仍有 {len(failed_videos)} 个视频插入失败，{retry_delay}秒后进行下一轮重试...")
                    time.sleep(retry_delay)
                else:
                    print(f"❌ 经过 {max_retry_rounds} 轮重试，仍有 {len(failed_videos)} 个视频无法插入数据库")
        
        elapsed_time = time.time() - start_time
        
        # 🚀 显示2025年极限优化最终统计
        cache_manager = hardware_info.get('cache_manager')
        final_cache_stats = cache_manager.get_cache_stats() if cache_manager else {}
        
        print(f"\n🎬 NAS极限优化处理完成! 🚀📊")
        print("=" * 50)
        print(f"📈 处理统计:")
        print(f"   ✅ 成功处理:    {results['success_count']} 个 ({results['success_count']/results['total_count']*100:.1f}%)")
        print(f"   ❌ 处理失败:     {results['failed_count']} 个")
        print(f"   📁 总计文件:   {results['total_count']} 个")
        print("-" * 50)
        print(f"⚡ 性能统计:")
        print(f"   🕐 批量总耗时: {elapsed_time:.1f} 秒")
        if results['success_count'] > 0:
            print(f"   📊 平均处理速度: {elapsed_time/results['success_count']:.1f} 秒/个")
        print("-" * 50)
        print(f"🚀 2025年NAS极限优化统计:")
        print(f"   💾 缓存命中率: {final_cache_stats.get('cache_hit_ratio', 0):.1f}%")
        print(f"   📥 缓存总大小: {final_cache_stats.get('total_size_gb', 0):.1f}GB")
        print(f"   🌐 平均网速: {final_cache_stats.get('avg_network_speed_mbps', 0):.1f}MB/s")
        print(f"   ⚡ 峰值网速: {final_cache_stats.get('peak_network_speed_mbps', 0):.1f}MB/s")
        print(f"   💨 i9优化: {'已启用' if hardware_info.get('is_i9', False) else '未启用'}")
        print("-" * 50)
        
        # 显示数据库插入统计
        video_record_manager = hardware_info.get('video_record_manager')
        if video_record_manager and ENABLE_MULTI_COMPUTER_SYNC:
            db_stats = video_record_manager.get_db_insert_statistics()
            print(f"🗄️ 数据库操作统计:")
            print(f"   ✅ 成功插入数据库: {db_stats['success_count']} 个视频 ({db_stats['success_rate']:.1f}%)")
            print(f"   ❌ 插入失败:       {db_stats['failed_count']} 个视频")
            print(f"   📊 总计尝试:       {db_stats['total_count']} 次操作")
        else:
            print(f"🗄️ 数据库操作统计: 数据库功能未启用或不可用")
        print("=" * 50)
        
        # 显示失败文件详情
        failed_stats = progress_manager.get_statistics()
        if failed_stats['failed'] > 0:
            print(f"\n❌ 失败文件详情:")
            for fail_info in progress_manager.progress_data['failed']:
                if isinstance(fail_info, dict):
                    print(f"  - {fail_info.get('name', 'Unknown')}: {fail_info.get('error', 'Unknown error')}")
        
        # 清理崩溃恢复文件（正常完成）
        progress_manager.clear_crash_recovery()
        print("🔄 已清理崩溃恢复数据")
        
    except KeyboardInterrupt:
        print(f"\n⏸️  用户中断处理")
        progress_manager.save_crash_recovery("", roi)  # 保存中断状态
    except Exception as e:
        print(f"\n❌ 处理过程中发生错误: {e}")
        logging.error(f"批量处理异常: {e}", exc_info=True)
        progress_manager.save_crash_recovery("", roi)  # 保存错误状态
    finally:
        # 清理数据库系统资源
        if db_system_manager:
            print("🔧 正在清理数据库系统资源...")
            db_system_manager.cleanup_resources()

if __name__ == '__main__':
    main()