#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AcFun视频批量下载器
优先使用yt-dlp进行视频下载，支持多线程和批量处理
支持自动回退到you-get作为备选方案
"""

import os
import sys
import subprocess
import threading
import time
import json
import re
import glob
import socket
import shutil
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from typing import List, Optional
from pathlib import Path
import argparse
from tqdm import tqdm
import queue
import io
import hashlib
import psutil
import tempfile
try:
    import fcntl  # Unix/Linux文件锁
    HAS_FCNTL = True
except ImportError:
    HAS_FCNTL = False
try:
    import msvcrt  # Windows文件锁
    HAS_MSVCRT = True
except ImportError:
    HAS_MSVCRT = False

# ====================================================================
# 📋 配置区域 - 所有可配置项都在这里
# ====================================================================

# 系统路径配置
SYSTEM_CONFIG = {
    # 系统工具路径
    'ffmpeg_path': r'D:\ffmpeg\bin\ffmpeg.exe',  # FFmpeg路径
    
    # 文件路径配置
    'default_urls_file': r'C:\Users\89498\Desktop\acfan.txt',  # 默认URL文件路径
    
    # 下载路径配置
    'download_dir': r'Z:\personal_folder\L\acfun下载',  # 默认下载目录
    
    # 下载状态记录配置
    'status_record_dir': r'Z:\personal_folder\L\acfun下载记录',  # 状态记录目录
    'status_file_name': 'download_status.json',  # 状态文件名
    'enable_status_tracking': True,  # 启用状态跟踪
    
    # 网络配置
    'network_timeout': 10,       # 网络连接检查超时（秒）
    'dns_timeout': 3,           # DNS解析超时（秒）
    'http_timeout': 5,          # HTTP请求超时（秒）
    'info_timeout': 20,         # 获取视频信息超时（秒）
    
    # 系统配置
    'encoding': 'utf-8',        # 文件编码
    'windows_compatible': True,  # Windows兼容模式
}

# 默认下载配置
DEFAULT_CONFIG = {
    # 视频质量配置  
    'video_quality': 'best',    # 视频质量: best, worst, 或具体格式如 720p
    'video_format': 'mp4',      # 优先视频格式: mp4, mkv, webm 等
    'audio_quality': 'best',    # 音频质量: best, worst, 或比特率如 128k
    
    # 下载器配置
    'preferred_downloader': 'yt-dlp',  # 优先使用的下载器: yt-dlp, you-get
    'retry_times': 3,           # 重试次数
    'timeout': 300,             # 下载超时时间(秒)，增加到5分钟
    'concurrent_downloads': 2,   # 并发下载数，降低以减少服务器压力
    'internal_retries': 2,      # 下载器内部重试次数，减少重复重试
    'fragment_retries': 2,      # 分片重试次数
    
    # 文件名配置
    'filename_template': '%(title)s.%(ext)s',  # 文件名模板
    'sanitize_filename': True,   # 是否清理文件名中的特殊字符
    'restrict_filenames': True,  # 限制文件名字符
    
    # 文件清理配置
    'cleanup_temp_files': True,  # 是否自动清理临时文件和其他非视频文件
    'keep_fragments': False,     # 是否保留分片文件
    'only_video_files': True,    # 只保留视频文件，删除其他所有文件
    'no_part_files': True,      # 禁用.part临时文件
    
    # 显示配置
    'show_resolution': True,     # 是否显示视频分辨率信息
    'show_progress': True,       # 是否显示下载进度
    'show_individual_progress': True,  # 是否显示单个视频下载进度
    'progress_bar_width': 120,   # 进度条宽度 (增加到120字符)
    'progress_update_interval': 0.5,  # 进度更新间隔（秒）
    'progress_smoothing': True,  # 启用进度平滑显示
    'terminal_friendly': True,   # 终端友好模式
    'verbose_mode': False,       # 是否启用详细模式
    
    # 多线程下载配置 - 优化以减少临时文件
    'enable_multithreading': False,  # 禁用多线程下载，减少临时文件和错误
    'max_fragments': 1,           # 单线程下载，避免分片文件
    'http_chunk_size': 1048576,   # HTTP分块大小（1MB）
    
    # 高级下载优化
    'buffer_size': 65536,        # 缓冲区大小（64KB）
    'socket_timeout': 30,        # 套接字超时
    'max_sleep_interval': 10,    # 最大等待间隔
    'embed_metadata': True,      # 嵌入元数据
    'merge_output_format': True, # 强制输出格式
    
    # 元数据配置
    'write_description': False,  # 不写入描述文件
    'write_info_json': False,   # 不写入信息JSON
    'write_thumbnail': False,   # 不下载缩略图
    'write_subtitles': False,   # 不下载字幕
    'write_annotations': False, # 不写入注释
    'write_comments': False,    # 不写入评论
    'embed_subs': False,        # 不嵌入字幕
    'embed_thumbnail': False,   # 不嵌入缩略图
}


# ====================================================================
# 📊 下载状态管理器
# ====================================================================

class DownloadStatusManager:
    """下载状态管理器，支持多台电脑并发访问"""
    
    def __init__(self, status_dir: str, status_file: str = 'download_status.json'):
        """
        初始化状态管理器
        
        Args:
            status_dir: 状态文件存储目录
            status_file: 状态文件名
        """
        self.status_dir = Path(status_dir)
        self.status_file_path = self.status_dir / status_file
        self.lock_file_path = self.status_dir / f"{status_file}.lock"
        
        # 确保目录存在
        self._ensure_directory()
        
        # 初始化状态文件
        self._ensure_status_file()
    
    def _ensure_directory(self):
        """确保状态目录存在"""
        try:
            self.status_dir.mkdir(parents=True, exist_ok=True)
            print(f"📁 状态记录目录已准备就绪: {self.status_dir}")
        except Exception as e:
            print(f"❌ 无法创建状态目录 {self.status_dir}: {e}")
            raise
    
    def _ensure_status_file(self):
        """确保状态文件存在且格式正确"""
        if not self.status_file_path.exists():
            try:
                self._write_status_file({})
                print(f"📄 已创建状态文件: {self.status_file_path}")
            except Exception as e:
                print(f"❌ 无法创建状态文件: {e}")
                raise
    
    def _generate_url_hash(self, url: str) -> str:
        """为URL生成唯一哈希标识"""
        return hashlib.md5(url.encode('utf-8')).hexdigest()
    
    def _acquire_lock(self, file_handle):
        """获取文件锁（跨平台）"""
        if HAS_MSVCRT:  # Windows
            while True:
                try:
                    msvcrt.locking(file_handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except IOError:
                    time.sleep(0.1)
        elif HAS_FCNTL:  # Unix/Linux
            fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX)
        else:
            # 如果都不支持，使用简单的重试机制
            pass
    
    def _release_lock(self, file_handle):
        """释放文件锁（跨平台）"""
        if HAS_MSVCRT:  # Windows
            try:
                msvcrt.locking(file_handle.fileno(), msvcrt.LK_UNLCK, 1)
            except IOError:
                pass
        elif HAS_FCNTL:  # Unix/Linux
            fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)
        else:
            # 如果都不支持，什么都不做
            pass
    
    def _read_status_file(self) -> dict:
        """安全读取状态文件"""
        try:
            with open(self.status_file_path, 'r', encoding=SYSTEM_CONFIG['encoding']) as f:
                self._acquire_lock(f)
                try:
                    content = f.read().strip()
                    if not content:
                        return {}
                    return json.loads(content)
                finally:
                    self._release_lock(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        except Exception as e:
            print(f"⚠️ 读取状态文件时出错: {e}")
            return {}
    
    def _write_status_file(self, status_data: dict):
        """安全写入状态文件"""
        try:
            with open(self.status_file_path, 'w', encoding=SYSTEM_CONFIG['encoding']) as f:
                self._acquire_lock(f)
                try:
                    json.dump(status_data, f, ensure_ascii=False, indent=2)
                finally:
                    self._release_lock(f)
        except Exception as e:
            print(f"❌ 写入状态文件时出错: {e}")
            raise
    
    def get_url_status(self, url: str) -> int:
        """
        获取URL的下载状态
        
        Args:
            url: 视频URL
            
        Returns:
            int: 0=未下载, 1=已下载
        """
        url_hash = self._generate_url_hash(url)
        status_data = self._read_status_file()
        return status_data.get(url_hash, {}).get('status', 0)
    
    def set_url_status(self, url: str, status: int, title: str = "", download_time: str = ""):
        """
        设置URL的下载状态
        
        Args:
            url: 视频URL
            status: 状态值 (0=未下载, 1=已下载)
            title: 视频标题
            download_time: 下载时间
        """
        url_hash = self._generate_url_hash(url)
        status_data = self._read_status_file()
        
        if url_hash not in status_data:
            status_data[url_hash] = {}
        
        status_data[url_hash].update({
            'url': url,
            'status': status,
            'title': title,
            'download_time': download_time or time.strftime('%Y-%m-%d %H:%M:%S'),
            'last_updated': time.strftime('%Y-%m-%d %H:%M:%S')
        })
        
        self._write_status_file(status_data)
    
    def get_downloaded_urls(self) -> List[str]:
        """获取所有已下载的URL列表"""
        status_data = self._read_status_file()
        return [info['url'] for info in status_data.values() if info.get('status') == 1]
    
    def get_pending_urls(self, all_urls: List[str]) -> List[str]:
        """从URL列表中筛选出未下载的URL"""
        pending = []
        for url in all_urls:
            if self.get_url_status(url) == 0:
                pending.append(url)
        return pending
    
    def get_status_summary(self) -> dict:
        """获取下载状态统计摘要"""
        status_data = self._read_status_file()
        total = len(status_data)
        downloaded = sum(1 for info in status_data.values() if info.get('status') == 1)
        pending = total - downloaded
        
        return {
            'total': total,
            'downloaded': downloaded,
            'pending': pending,
            'download_rate': (downloaded / total * 100) if total > 0 else 0
        }


# ====================================================================
# 🎬 AcFun下载器类
# ====================================================================

class AcFunDownloader:
    def __init__(self, 
                 output_dir: str = None,
                 max_workers: int = None,
                 retry_times: int = None,
                 timeout: int = None,
                 prefer_ytdlp: bool = None,
                 config: dict = None):
        """
        初始化AcFun下载器
        
        Args:
            output_dir: 下载目录
            max_workers: 最大并发下载数
            retry_times: 重试次数
            timeout: 超时时间（秒）
            prefer_ytdlp: 是否优先使用yt-dlp
            config: 配置字典，会覆盖默认配置
        """
        # 合并配置
        self.config = DEFAULT_CONFIG.copy()
        self.system_config = SYSTEM_CONFIG.copy()
        if config:
            self.config.update(config)
            # 允许config覆盖system_config
            for key, value in config.items():
                if key in self.system_config:
                    self.system_config[key] = value
            
        # 参数优先级：传入参数 > 配置文件 > 默认值
        self.output_dir = Path(output_dir or self.system_config['download_dir'])
        self.max_workers = max_workers or self.config['concurrent_downloads']
        self.retry_times = retry_times or self.config['retry_times']
        self.timeout = timeout or self.config['timeout']
        self.prefer_ytdlp = prefer_ytdlp if prefer_ytdlp is not None else (self.config['preferred_downloader'] == 'yt-dlp')
        
        # 创建下载目录
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化状态管理器
        if self.system_config.get('enable_status_tracking', True):
            try:
                self.status_manager = DownloadStatusManager(
                    status_dir=self.system_config['status_record_dir'],
                    status_file=self.system_config['status_file_name']
                )
                print("📊 状态跟踪系统已启用")
            except Exception as e:
                print(f"⚠️ 状态跟踪系统初始化失败，将跳过状态记录: {e}")
                self.status_manager = None
        else:
            self.status_manager = None
            print("📊 状态跟踪系统已禁用")
        
        # 统计信息
        self.download_stats = {
            'success': 0,
            'failed': 0,
            'total': 0,
            'failed_urls': []
        }
        
        # 线程锁
        self.stats_lock = threading.Lock()
        
        # 进度条相关
        self.overall_progress = None
        self.current_video_progress = {}
        self.progress_lock = threading.Lock()
        
        # 检查下载器可用性
        self.available_downloaders = self._check_downloaders()
        
        # 检查磁盘空间
        self._check_disk_space()
    
    def _check_disk_space(self):
        """检查磁盘空间"""
        try:
            # 获取下载目录所在磁盘的可用空间
            disk_usage = psutil.disk_usage(str(self.output_dir))
            free_space_gb = disk_usage.free / (1024**3)  # 转换为GB
            
            # 如果可用空间小于5GB，发出警告
            if free_space_gb < 5:
                print(f"⚠️ 警告: 磁盘可用空间较少 ({free_space_gb:.1f} GB)")
                print("   建议清理磁盘空间或更换下载目录")
            elif free_space_gb < 1:
                print(f"❌ 错误: 磁盘空间不足 ({free_space_gb:.1f} GB)")
                print("   请清理磁盘空间后重试")
                sys.exit(1)
            else:
                print(f"💾 磁盘可用空间: {free_space_gb:.1f} GB")
                
        except Exception as e:
            print(f"⚠️ 无法检查磁盘空间: {e}")
    
    def _get_safe_filename(self, title: str, max_length: int = 100) -> str:
        """生成安全的文件名，避免过长导致的问题"""
        # 确保title是有效字符串
        if not title or not isinstance(title, str):
            return "Unknown"
        
        title = str(title).strip()
        if not title:
            return "Unknown"
        
        # 移除或替换不安全的字符
        safe_chars = re.sub(r'[<>:"/\\|?*]', '_', title)
        safe_chars = re.sub(r'\s+', ' ', safe_chars).strip()
        
        # 如果清理后为空，返回默认值
        if not safe_chars:
            return "Unknown"
        
        # 限制长度
        if len(safe_chars) > max_length:
            safe_chars = safe_chars[:max_length].rstrip()
        
        return safe_chars or "Unknown"
    
    def _get_terminal_width(self) -> int:
        """获取终端宽度 - 改进版本，更好的兼容性"""
        try:
            # 尝试多种方法获取终端宽度
            width = 80  # 默认宽度
            
            # 方法1: 使用shutil
            try:
                width = shutil.get_terminal_size().columns
            except:
                pass
            
            # 方法2: 使用os.environ（对某些终端更可靠）
            if width == 80:
                try:
                    width = int(os.environ.get('COLUMNS', 80))
                except:
                    pass
            
            # 方法3: Windows特殊处理
            if width == 80 and os.name == 'nt':
                try:
                    result = subprocess.run(['mode', 'con'], 
                                          capture_output=True, text=True, timeout=2)
                    if result.returncode == 0:
                        for line in result.stdout.split('\n'):
                            if 'Columns:' in line:
                                width = int(line.split(':')[1].strip())
                                break
                except:
                    pass
            
            # 确保宽度在合理范围内，并为不同终端调整
            min_width = 60  # 最小宽度
            max_width = min(150, self.config['progress_bar_width'])  # 最大宽度
            
            return max(min_width, min(width - 10, max_width))  # 预留10个字符边距
            
        except Exception:
            return 80  # 安全的默认值
    
    def _get_optimal_workers(self) -> int:
        """根据系统资源动态确定最优并发数"""
        try:
            # 获取系统信息
            disk_usage = psutil.disk_usage(str(self.output_dir))
            free_space_gb = disk_usage.free / (1024**3)
            
            # 获取系统内存信息
            memory = psutil.virtual_memory()
            available_memory_gb = memory.available / (1024**3)
            
            # 基础并发数调整
            optimal = self.max_workers
            
            # 根据可用磁盘空间调整并发数
            if free_space_gb < 2:
                optimal = max(1, optimal // 4)  # 更保守
                print(f"⚠️ 磁盘空间严重不足({free_space_gb:.1f}GB)，并发数降至 {optimal}")
            elif free_space_gb < 5:
                optimal = max(1, optimal // 2)
                print(f"💾 磁盘空间较少({free_space_gb:.1f}GB)，并发数调整至 {optimal}")
            elif free_space_gb < 10:
                optimal = max(1, optimal * 2 // 3)
                print(f"💾 磁盘空间一般({free_space_gb:.1f}GB)，并发数调整至 {optimal}")
            
            # 根据可用内存调整并发数（避免内存不足导致系统卡顿）
            if available_memory_gb < 2:
                optimal = max(1, min(optimal, 1))
                print(f"⚠️ 可用内存不足({available_memory_gb:.1f}GB)，限制并发数至 {optimal}")
            elif available_memory_gb < 4:
                optimal = max(1, min(optimal, 2))
                print(f"💾 可用内存较少({available_memory_gb:.1f}GB)，限制并发数至 {optimal}")
            
            # 根据分片数调整：如果分片数较高，应该降低视频并发数
            if self.config['max_fragments'] > 2:
                optimal = max(1, min(optimal, 2))
                print(f"🧵 考虑到分片并发({self.config['max_fragments']})，视频并发数限制至 {optimal}")
            
            # 最终安全检查：确保不超过合理范围
            optimal = max(1, min(optimal, 3))  # 最多3个并发，避免对服务器造成过大压力
            
            if optimal != self.max_workers:
                print(f"🔧 智能并发调整: {self.max_workers} → {optimal}")
            
            return optimal
            
        except Exception as e:
            print(f"⚠️ 无法获取系统信息，使用保守并发数: {e}")
            return max(1, min(self.max_workers, 2))  # 出错时使用保守设置
    
    def _check_downloaders(self):
        """检查可用的下载器"""
        available = []
        
        # 检查yt-dlp
        try:
            result = subprocess.run(['yt-dlp', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                available.append('yt-dlp')
                print(f"✓ yt-dlp版本: {result.stdout.strip()}")
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        
        # 检查you-get
        try:
            result = subprocess.run(['you-get', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                available.append('you-get')
                print(f"✓ you-get版本: {result.stdout.strip()}")
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        
        if not available:
            print("❌ 未找到可用的下载器")
            print("请安装以下任一下载器:")
            print("  pip install yt-dlp     (推荐，支持AcFun)")
            print("  pip install you-get")
            sys.exit(1)
        
        # 根据偏好和可用性选择主要下载器
        if self.prefer_ytdlp and 'yt-dlp' in available:
            primary = 'yt-dlp'
        elif 'yt-dlp' in available:
            primary = 'yt-dlp'
        else:
            primary = available[0]
        
        print(f"🚀 使用下载器: {primary}")
        if len(available) > 1:
            backup = [d for d in available if d != primary]
            print(f"🔄 备用下载器: {', '.join(backup)}")
        
        return {'primary': primary, 'available': available}
    
    def _is_valid_acfun_url(self, url: str) -> bool:
        """验证AcFun URL格式"""
        acfun_patterns = [
            r'https?://www\.acfun\.cn/v/ac\d+',
            r'https?://m\.acfun\.cn/v/\?ac=\d+',
        ]
        return any(re.match(pattern, url.strip()) for pattern in acfun_patterns)
    
    def _clean_url(self, url: str) -> str:
        """清理和标准化URL格式"""
        url = url.strip()
        
        # 移除URL末尾的下划线（常见的格式问题）
        if url.endswith('_'):
            url = url[:-1]
            
        # 移除URL末尾的多余字符
        url = re.sub(r'[_\s]+$', '', url)
        
        # 标准化URL格式
        if 'acfun.cn/v/ac' in url and not url.startswith('http'):
            url = 'https://www.' + url
            
        return url
    
    def _check_server_status(self) -> bool:
        """检查AcFun服务器状态，避免在服务器繁忙时开始大量下载"""
        try:
            response = requests.get("https://www.acfun.cn", timeout=10)
            if response.status_code == 502:
                print("⚠️ 检测到AcFun服务器繁忙(502)，建议稍后重试")
                return False
            elif response.status_code >= 500:
                print(f"⚠️ AcFun服务器状态异常({response.status_code})，建议稍后重试")
                return False
            return True
        except Exception as e:
            print(f"⚠️ 无法检查服务器状态，继续下载: {e}")
            return True  # 检查失败时继续下载
    
    def _check_network_connection(self, url: str = "https://www.acfun.cn") -> bool:
        """检查网络连接是否正常，采用宽松策略"""
        # 尝试多种方式检查网络连接
        connection_methods = [
            self._check_dns_resolution,
            self._check_http_connection,
            self._check_basic_connectivity
        ]
        
        for method in connection_methods:
            try:
                if method(url):
                    return True
            except Exception:
                continue
        
        # 即使所有检查都失败，也返回True让下载器自己处理网络问题
        # 因为下载器可能有更好的网络处理能力
        return True
    
    def _check_dns_resolution(self, url: str) -> bool:
        """检查DNS解析"""
        try:
            socket.gethostbyname("www.acfun.cn")
            return True
        except socket.gaierror:
            return False
    
    def _check_http_connection(self, url: str) -> bool:
        """检查HTTP连接"""
        try:
            response = requests.head(url, timeout=self.system_config['http_timeout'])
            return response.status_code < 500  # 放宽条件，只要不是服务器错误就认为连接正常
        except requests.RequestException:
            return False
    
    def _check_basic_connectivity(self, url: str) -> bool:
        """检查基本网络连接"""
        try:
            # 尝试连接到常用的DNS服务器
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.system_config['dns_timeout'])
            result = sock.connect_ex(("8.8.8.8", 53))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    def _get_detailed_error_info(self, result, cmd) -> str:
        """获取详细的错误信息并提供解决建议"""
        error_info = []
        
        if result.returncode != 0:
            error_info.append(f"返回码: {result.returncode}")
        
        # 合并所有输出进行分析
        all_output = (result.stderr or "") + (result.stdout or "")
        
        # 打印完整的错误输出用于调试
        if self.config.get('verbose_mode', False):
            print(f"   🔍 完整错误输出:")
            print(f"   stdout: {result.stdout[:500] if result.stdout else 'None'}")
            print(f"   stderr: {result.stderr[:500] if result.stderr else 'None'}")
        
        # 常见错误模式匹配 - 增加更多模式
        error_patterns = [
            (r"AttributeError.*'NoneType'.*'lower'", "视频信息获取失败，可能是临时网络问题或视频已删除"),
            (r"No such file or directory.*Frag\d+", "片段文件丢失，建议减少并发数"),
            (r"HTTP Error 502|Bad Gateway", "服务器临时不可用，稍后重试"),
            (r"HTTP Error 404|Not Found", "视频不存在或已删除"),
            (r"HTTP Error 403|Forbidden", "访问被拒绝，可能需要登录或视频有访问限制"),
            (r"No space left on device", "磁盘空间不足，请清理磁盘"),
            (r"Conversion failed", "视频转码失败，可能是格式问题"),
            (r"Unable to download webpage", "网页无法访问，检查网络连接"),
            (r"Connection.*timeout|timed out", "网络连接超时，检查网络状态"),
            (r"Permission denied", "权限不足，检查文件夹权限"),
            (r"Postprocessing.*failed", "后处理失败，可能是FFmpeg问题"),
            (r"Rate limit|Too many requests", "请求频率过高，被服务器限制"),
            (r"Private video|Access denied", "私有视频或无权访问"),
            (r"Unable to extract.*info", "无法提取视频信息，可能是网站更新或网络问题"),
            (r"Video unavailable", "视频不可用"),
            (r"This video is not available", "该视频不可用"),
            (r"Sign in to confirm", "需要登录确认"),
            (r"Requested format is not available", "请求的格式不可用"),
        ]
        
        matched_error = False
        for pattern, suggestion in error_patterns:
            if re.search(pattern, all_output, re.IGNORECASE):
                error_info.append(suggestion)
                matched_error = True
                break
        
        # 如果没有匹配到常见错误，提取关键错误信息
        if not matched_error:
            # 提取ERROR行
            error_lines = []
            for line in all_output.split('\n'):
                line = line.strip()
                if any(keyword in line.upper() for keyword in ['ERROR:', 'FAILED:', 'EXCEPTION:', 'WARNING:']):
                    # 清理行内容，移除ANSI颜色代码
                    clean_line = re.sub(r'\x1b\[[0-9;]*m', '', line)
                    if len(clean_line) > 10:  # 过滤掉太短的行
                        error_lines.append(clean_line)
            
            if error_lines:
                # 选择最相关的错误信息
                best_error = error_lines[0]  # 通常第一个ERROR最重要
                error_info.append(best_error[:200])  # 增加长度限制
            else:
                # 如果没有ERROR行，查找其他有用信息
                for line in all_output.split('\n')[-10:]:  # 查看最后10行
                    line = line.strip()
                    if line and len(line) > 20 and not line.startswith('['):
                        error_info.append(line[:150])
                        break
        
        if len(error_info) <= 1:
            error_info.append("未知错误，请检查网络连接和URL有效性")
        
        return "; ".join(error_info)[:400]  # 增加长度限制以显示更多信息
    
    def _should_retry_error(self, result, attempt: int) -> tuple[bool, int]:
        """根据错误类型判断是否应该重试，返回(是否重试, 等待时间)"""
        if not result:
            return True, 2 ** attempt  # 默认重试策略
        
        all_output = (result.stderr or "") + (result.stdout or "")
        
        # 不应该重试的错误类型
        no_retry_patterns = [
            r"HTTP Error 404|Not Found",           # 视频不存在
            r"Private video|Access denied",        # 私有视频
            r"No space left on device",            # 磁盘空间不足
            r"Permission denied",                  # 权限不足
            r"Video unavailable",                  # 视频不可用
            r"This video is not available",        # 该视频不可用
            r"Sign in to confirm",                 # 需要登录
            r"HTTP Error 403.*Forbidden",          # 访问被禁止
        ]
        
        for pattern in no_retry_patterns:
            if re.search(pattern, all_output, re.IGNORECASE):
                return False, 0  # 不重试
        
        # 需要长等待时间的错误
        if re.search(r"HTTP Error 502|Bad Gateway|Rate limit|Too many requests", all_output, re.IGNORECASE):
            wait_time = min(30, 5 * (2 ** attempt))  # 最多等待30秒
            return True, wait_time
        
        # AttributeError通常是临时问题，可以重试，但等待时间稍长
        if re.search(r"AttributeError.*'NoneType'", all_output, re.IGNORECASE):
            wait_time = min(15, 3 * (2 ** attempt))  # 最多等待15秒
            return True, wait_time
        
        # 网络相关错误，使用标准重试策略
        if re.search(r"Connection.*timeout|timed out|Unable to download", all_output, re.IGNORECASE):
            return True, 2 ** attempt
        
        # 默认重试策略
        return True, 2 ** attempt
    
    def _get_video_info(self, url: str) -> Optional[dict]:
        """获取视频信息，采用容错策略"""
        print(f"🔍 正在获取视频信息: {url}")
        
        try:
            downloader = self.available_downloaders['primary']
            
            if downloader == 'yt-dlp':
                cmd = ['yt-dlp', '--dump-json', '--no-download', '--no-warnings', url]
            else:  # you-get
                cmd = ['you-get', '--json', url]
            
            print(f"   📝 执行命令: {' '.join(cmd)}")
                
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=self.system_config['info_timeout'])
            
            print(f"   返回码: {result.returncode}")
            
            if result.returncode == 0 and result.stdout:
                try:
                    # 尝试解析JSON，可能有多行输出
                    output_lines = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
                    json_line = None
                    
                    for line in output_lines:
                        if line.startswith('{') and line.endswith('}'):
                            json_line = line
                            break
                    
                    if not json_line:
                        print(f"   ⚠️ 未找到有效的JSON输出")
                        print(f"   原始输出: {result.stdout[:200]}")
                        return None
                    
                    data = json.loads(json_line)
                    
                    # 统一返回格式
                    if downloader == 'yt-dlp':
                        # 安全获取字段，避免NoneType错误
                        title = data.get('title') or 'Unknown'
                        width = data.get('width') or 0
                        height = data.get('height') or 0
                        resolution = f"{width}x{height}" if width and height else "Unknown"
                        duration = data.get('duration') or 0
                        duration_str = f"{int(duration//60):02d}:{int(duration%60):02d}" if duration else "Unknown"
                        uploader = data.get('uploader') or 'Unknown'
                        
                        # 确保title是字符串类型
                        if not isinstance(title, str):
                            title = str(title) if title else 'Unknown'
                        
                        print(f"   ✓ 视频标题: {title[:50]}")
                        if self.config['show_resolution']:
                            print(f"   📺 分辨率: {resolution}")
                            print(f"   ⏱️ 时长: {duration_str}")
                            print(f"   👤 UP主: {uploader}")
                        
                        return {
                            'title': title,
                            'resolution': resolution,
                            'width': width,
                            'height': height,
                            'duration': duration,
                            'uploader': uploader,
                            'view_count': data.get('view_count') or 0
                        }
                    else:
                        print(f"   ✓ you-get返回数据")
                        return data
                        
                except json.JSONDecodeError as e:
                    print(f"   ⚠️ JSON解析失败: {str(e)}")
                    print(f"   原始输出: {result.stdout[:200]}")
            else:
                print(f"   ⚠️ 命令失败，错误: {result.stderr[:100] if result.stderr else '无错误信息'}")
                    
        except subprocess.TimeoutExpired:
            print(f"   ⚠️ 获取视频信息超时 (20秒)")
        except Exception as e:
            print(f"   ⚠️ 获取视频信息时出错: {str(e)}")
        
        return None
    
    def _cleanup_temp_files(self):
        """清理下载目录中的临时文件和其他非视频文件"""
        try:
            # 临时文件模式 - 增强分片文件清理
            temp_patterns = [
                '*.part',           # 下载临时文件
                '*.ytdl',           # yt-dlp临时文件
                '*.temp',           # 通用临时文件
                '*.tmp',            # 临时文件
                '*-Frag*',          # 所有分片文件
                '*.part-Frag*',     # 分片文件
                '*.f*-Frag*',       # 分片文件
                '*_t[0-9]*-Frag*',  # 带线程ID的分片文件
            ]
            
            # 其他不需要的文件模式
            unwanted_patterns = [
                '*.description',    # 描述文件
                '*.info.json',      # 信息JSON文件
                '*.annotations.xml', # 注释文件
                '*.live_chat.json', # 聊天记录
                '*.webp',           # 缩略图
                '*.jpg',            # 缩略图
                '*.png',            # 缩略图
                '*.srt',            # 字幕文件
                '*.vtt',            # 字幕文件
                '*.ass',            # 字幕文件
                '*.ssa',            # 字幕文件
                '*.xml',            # 弹幕文件
                '*.json',           # 各种JSON文件
            ]
            
            if not self.config['keep_fragments']:
                temp_patterns.extend([
                    '*.f[0-9]*',    # 分片文件
                    '*-Frag[0-9]*', # 分片文件
                ])
            
            all_patterns = temp_patterns + unwanted_patterns
            cleaned_count = 0
            
            for pattern in all_patterns:
                for unwanted_file in self.output_dir.glob(pattern):
                    try:
                        unwanted_file.unlink()
                        cleaned_count += 1
                        if self.config['verbose_mode']:
                            print(f"   🧹 清理文件: {unwanted_file.name}")
                    except Exception as e:
                        if self.config['verbose_mode']:
                            print(f"   ⚠️ 无法删除文件 {unwanted_file.name}: {e}")
            
            if cleaned_count > 0 and not self.config['verbose_mode']:
                print(f"   🧹 清理了 {cleaned_count} 个临时/不需要的文件")
                
        except Exception as e:
            if self.config['verbose_mode']:
                print(f"   ⚠️ 清理文件时出错: {e}")
    
    def _rename_downloaded_files(self, video_title):
        """重命名下载的文件，移除线程ID后缀，并返回重命名后的文件路径"""
        renamed_files = []
        try:
            import threading
            thread_id = threading.current_thread().ident
            
            # 查找包含线程ID的文件
            pattern = f"*_t{thread_id}.*"
            for file_path in self.output_dir.glob(pattern):
                try:
                    # 生成新文件名（移除线程ID）
                    old_name = file_path.name
                    new_name = old_name.replace(f'_t{thread_id}', '')
                    new_path = file_path.parent / new_name
                    
                    # 如果目标文件已存在，添加数字后缀
                    counter = 1
                    while new_path.exists():
                        name_parts = new_name.rsplit('.', 1)
                        if len(name_parts) == 2:
                            new_name = f"{name_parts[0]}_{counter}.{name_parts[1]}"
                        else:
                            new_name = f"{new_name}_{counter}"
                        new_path = file_path.parent / new_name
                        counter += 1
                    
                    # 重命名文件
                    file_path.rename(new_path)
                    renamed_files.append(new_path)
                    if self.config['verbose_mode']:
                        print(f"   📝 文件重命名: {old_name} -> {new_name}")
                    
                except Exception as e:
                    if self.config['verbose_mode']:
                        print(f"   ⚠️ 文件重命名失败 {file_path.name}: {e}")
                    
        except Exception as e:
            if self.config['verbose_mode']:
                print(f"   ⚠️ 重命名文件时发生错误: {e}")
        
        return renamed_files
    
    def _show_downloaded_file_location(self, video_title):
        """显示下载文件的具体位置"""
        try:
            # 查找最近下载的视频文件
            video_extensions = ['.mp4', '.mkv', '.webm', '.avi', '.mov', '.flv']
            recent_files = []
            
            # 获取最近5分钟内创建或修改的视频文件
            current_time = time.time()
            for ext in video_extensions:
                for file_path in self.output_dir.glob(f"*{ext}"):
                    # 检查文件修改时间
                    if current_time - file_path.stat().st_mtime < 300:  # 5分钟内
                        recent_files.append(file_path)
            
            if recent_files:
                # 按修改时间排序，最新的在前
                recent_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                latest_file = recent_files[0]
                file_size = latest_file.stat().st_size / (1024 * 1024)  # 转换为MB
                
                print(f"   📍 文件已保存到: {latest_file.absolute()}")
                print(f"   📦 文件大小: {file_size:.1f} MB")
            else:
                # 如果没找到最近的文件，显示目录信息
                print(f"   📁 文件已保存到目录: {self.output_dir.absolute()}")
                
        except Exception as e:
            if self.config['verbose_mode']:
                print(f"   ⚠️ 无法显示文件位置: {e}")
            print(f"   📁 请在此目录查看: {self.output_dir.absolute()}")
    
    def _create_progress_hook(self, video_id: str, video_title: str):
        """创建进度回调函数 - 优化版本"""
        last_update_time = 0
        last_percentage = -1
        
        def progress_hook(d):
            nonlocal last_update_time, last_percentage
            current_time = time.time()
            
            if d['status'] == 'downloading':
                if 'total_bytes' in d or 'total_bytes_estimate' in d:
                    total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
                    downloaded = d.get('downloaded_bytes', 0)
                    if total > 0:
                        percentage = (downloaded / total) * 100
                        speed = d.get('speed', 0)
                        eta = d.get('eta', 0)
                        
                        # 限制更新频率 - 根据配置动态调整
                        update_interval = self.config.get('progress_update_interval', 0.5)
                        min_change = 1.0 if not self.config.get('progress_smoothing', True) else 0.5
                        
                        if (current_time - last_update_time < update_interval and 
                            abs(percentage - last_percentage) < min_change and 
                            percentage < 99):
                            return
                        
                        last_update_time = current_time
                        last_percentage = percentage
                        
                        with self.progress_lock:
                            if video_id not in self.current_video_progress:
                                if self.config['show_individual_progress']:
                                    # 清理标题，确保显示效果
                                    clean_title = video_title.replace('\n', ' ').replace('\r', ' ').strip()
                                    desc = f"📥 {clean_title[:30]}..."
                                    terminal_width = self._get_terminal_width()
                                    
                                    # 根据终端友好模式调整显示
                                    if self.config.get('terminal_friendly', True):
                                        # 使用标准的tqdm格式，确保显示纯色进度条
                                        bar_format = '{l_bar}{bar}| {rate_fmt}'
                                        ascii_bar = False  # 使用Unicode字符显示更美观的进度条
                                    else:
                                        bar_format = '{l_bar}{bar}| {rate_fmt}'
                                        ascii_bar = False
                                    
                                    # 计算合适的位置，避免重叠
                                    position = len(self.current_video_progress) + 2  # 从2开始，为总体进度条留空间
                                    
                                    self.current_video_progress[video_id] = tqdm(
                                        total=100,
                                        desc=desc,
                                        unit="%",
                                        position=position,
                                        leave=False,
                                        ncols=terminal_width,
                                        bar_format=bar_format,
                                        ascii=ascii_bar,
                                        dynamic_ncols=True,  # 动态调整宽度
                                        miniters=2,  # 最小更新间隔（百分比）
                                        mininterval=self.config.get('progress_update_interval', 0.5),
                                        file=sys.stderr  # 输出到stderr，避免与其他输出混合
                                    )
                            
                            if video_id in self.current_video_progress:
                                pbar = self.current_video_progress[video_id]
                                current_n = int(percentage)
                                
                                # 只有当进度真正变化时才更新
                                if pbar.n != current_n:
                                    pbar.n = current_n
                                    
                                    # 更新速度信息（简化显示）
                                    speed_str = ""
                                    if speed:
                                        if speed > 1024 * 1024:
                                            speed_str = f"{speed / (1024 * 1024):.1f}MB/s"
                                        elif speed > 1024:
                                            speed_str = f"{speed / 1024:.1f}KB/s"
                                        else:
                                            speed_str = f"{speed:.0f}B/s"
                                    
                                    # ETA信息（简化显示）
                                    eta_str = ""
                                    if eta and eta > 0:
                                        if eta > 3600:  # 超过1小时
                                            eta_str = f"ETA: {eta//3600}h{(eta%3600)//60}m"
                                        elif eta > 60:
                                            eta_str = f"ETA: {eta//60}m{eta%60}s"
                                        else:
                                            eta_str = f"ETA: {eta}s"
                                    
                                    # 清理标题并组合描述
                                    clean_title = video_title.replace('\n', ' ').replace('\r', ' ').strip()
                                    desc_parts = [f"📥 {clean_title[:25]}..."]
                                    if speed_str:
                                        desc_parts.append(speed_str)
                                    if eta_str:
                                        desc_parts.append(eta_str)
                                    
                                    desc = " ".join(desc_parts)
                                    pbar.set_description(desc)
                                    
                                    # 使用更温和的刷新方式
                                    try:
                                        pbar.refresh()
                                    except:
                                        pass  # 忽略刷新错误，避免影响下载
            
            elif d['status'] == 'finished':
                with self.progress_lock:
                    if video_id in self.current_video_progress:
                        pbar = self.current_video_progress[video_id]
                        # 只有在真正达到100%时才设置为完成
                        if pbar.n >= 99:  # 允许一些误差
                            pbar.n = 100
                            clean_title = video_title.replace('\n', ' ').replace('\r', ' ').strip()
                            pbar.set_description(f"✅ {clean_title[:28]}... 完成")
                            try:
                                pbar.refresh()
                                time.sleep(0.1)  # 短暂显示完成状态
                                pbar.close()
                            except:
                                pass
                            del self.current_video_progress[video_id]
                        else:
                            # 如果进度还很低就收到finished信号，可能是误报，不关闭进度条
                            clean_title = video_title.replace('\n', ' ').replace('\r', ' ').strip()
                            pbar.set_description(f"🔄 {clean_title[:25]}... 处理中")
                            try:
                                pbar.refresh()
                            except:
                                pass
                        
            elif d['status'] == 'processing':
                # 处理后处理阶段（格式转换、合并等）
                with self.progress_lock:
                    if video_id in self.current_video_progress:
                        pbar = self.current_video_progress[video_id]
                        clean_title = video_title.replace('\n', ' ').replace('\r', ' ').strip()
                        pbar.set_description(f"🔄 {clean_title[:25]}... 后处理中")
                        try:
                            pbar.refresh()
                        except:
                            pass
        
        return progress_hook
    
    def _build_ytdlp_command(self, url: str, is_backup: bool = False) -> List[str]:
        """构建yt-dlp命令"""
        cmd = ['yt-dlp']
        
        # FFmpeg路径配置
        if self.system_config['ffmpeg_path']:
            cmd.extend(['--ffmpeg-location', self.system_config['ffmpeg_path']])
        
        # 基本下载选项 - 移除 -P 参数，因为我们在文件名模板中使用完整路径
        cmd.extend([
            '--no-overwrites',  # 避免覆盖
        ])
        
        # Windows兼容性
        if self.system_config['windows_compatible']:
            cmd.extend(['--restrict-filenames', '--windows-filenames'])
        
        # 文件选项
        if self.config['no_part_files']:
            cmd.append('--no-part')
        
        # 元数据选项
        metadata_options = [
            ('write_description', '--write-description', '--no-write-description'),
            ('write_info_json', '--write-info-json', '--no-write-info-json'),
            ('write_thumbnail', '--write-thumbnail', '--no-write-thumbnail'),
            ('write_subtitles', '--write-subs', '--no-write-sub'),
            ('write_annotations', '--write-annotations', '--no-write-annotations'),
            ('write_comments', '--write-comments', '--no-write-comments'),
        ]
        
        for config_key, enable_flag, disable_flag in metadata_options:
            cmd.append(enable_flag if self.config[config_key] else disable_flag)
        
        cmd.extend([
            '--no-write-playlist-metafiles',
            '--no-write-auto-sub',
            '--no-post-overwrites',
        ])
        
        # 嵌入选项
        if self.config['embed_metadata']:
            cmd.append('--embed-metadata')
        
        if not self.config['embed_subs']:
            cmd.append('--no-embed-subs')
        
        if not self.config['embed_thumbnail']:
            cmd.append('--no-embed-thumbnail')
        
        cmd.append('--no-embed-info-json')
        
        # 输出格式
        if self.config['merge_output_format']:
            cmd.extend(['--merge-output-format', self.config['video_format']])
        
        # 重试配置和错误恢复 - 优化以减少临时文件
        retry_count = str(self.config['internal_retries'] if not is_backup else 1)
        fragment_retry_count = str(self.config['fragment_retries'] if not is_backup else 1)
        cmd.extend([
            '--retries', retry_count,
            '--fragment-retries', fragment_retry_count,
            '--retry-sleep', 'linear=1:3:8',   # 减少重试延迟，避免过长等待
            '--file-access-retries', '3',      # 减少文件访问重试次数
            '--abort-on-error',                # 遇到错误时中止，避免无限重试
            '--no-keep-fragments',             # 不保留片段，减少文件系统负担
            '--no-part',                       # 不使用.part临时文件
        ])
        
        # 质量选择
        format_selector = f'{self.config["video_quality"]}[ext={self.config["video_format"]}]/{self.config["video_quality"]}'
        cmd.extend(['-f', format_selector])
        
        # 文件名模板 - 简化命名，减少临时文件复杂性
        # 使用完整路径确保文件下载到正确目录，不添加线程ID避免文件名过于复杂
        safe_template = str(self.output_dir / self.config['filename_template'])
        cmd.extend(['-o', safe_template])
        
        # 单线程下载配置 - 避免分片文件
        fragments = '1'  # 强制单线程下载，避免产生临时分片文件
        cmd.extend(['--concurrent-fragments', fragments])
        
        # 进度显示
        if self.config['show_individual_progress']:
            cmd.extend(['--newline', '--progress'])
        else:
            cmd.append('--no-progress')
        
        # 网络配置
        if self.config['socket_timeout']:
            cmd.extend(['--socket-timeout', str(self.config['socket_timeout'])])
        
        # 最后添加URL
        cmd.append(url)
        return cmd
    
    def _build_youget_command(self, url: str) -> List[str]:
        """构建you-get命令"""
        cmd = [
            'you-get',
            '--output-dir', str(self.output_dir),
            '--format', self.config['video_format'],
            url
        ]
        return cmd

    def _parse_ytdlp_progress(self, line: str, progress_hook):
        """解析yt-dlp的进度输出 - 优化版本"""
        try:
            line = line.strip()
            if not line:
                return
            
            # 查找下载进度行
            if '[download]' in line:
                # 解析百分比
                percent_match = re.search(r'(\d+(?:\.\d+)?)%', line)
                if percent_match:
                    percentage = float(percent_match.group(1))
                    
                    # 解析下载速度 - 改进正则表达式以支持更多格式
                    speed = 0
                    speed_match = re.search(r'(\d+(?:\.\d+)?)\s*(KiB|MiB|GiB|kB|MB|GB|B)/s', line)
                    if speed_match:
                        speed_val = float(speed_match.group(1))
                        speed_unit = speed_match.group(2)
                        
                        # 统一转换为字节每秒
                        if speed_unit in ['GiB', 'GB']:
                            speed = speed_val * 1024 * 1024 * 1024
                        elif speed_unit in ['MiB', 'MB']:
                            speed = speed_val * 1024 * 1024
                        elif speed_unit in ['KiB', 'kB']:
                            speed = speed_val * 1024
                        else:
                            speed = speed_val
                    
                    # 解析ETA - 支持更多时间格式
                    eta = 0
                    eta_match = re.search(r'ETA\s+(\d+):(\d+)', line)
                    if eta_match:
                        eta = int(eta_match.group(1)) * 60 + int(eta_match.group(2))
                    else:
                        # 尝试解析秒格式
                        eta_sec_match = re.search(r'ETA\s+(\d+)s', line)
                        if eta_sec_match:
                            eta = int(eta_sec_match.group(1))
                    
                    # 调用进度回调
                    progress_data = {
                        'status': 'downloading',
                        'downloaded_bytes': percentage,  # 这里用百分比代替字节数
                        'total_bytes': 100,
                        'speed': speed,
                        'eta': eta
                    }
                    progress_hook(progress_data)
            
            # 检查后处理状态 - 新增处理状态识别
            elif any(pattern in line for pattern in [
                '[Merger] Merging formats',
                '[ExtractAudio]',
                '[FixupM4a]',
                '[FixupM3u8]',
                '[FixupWebm]',
                '[FixupMp4]',
                'Post-processing',
                'Converting'
            ]):
                progress_data = {'status': 'processing'}
                progress_hook(progress_data)
            
            # 检查完成状态 - 更严格的判断
            elif any(pattern in line for pattern in [
                'has already been downloaded',
                'Deleting original file'
            ]):
                # 只有在明确表示已下载或删除原文件时才认为完成
                progress_data = {'status': 'finished'}
                progress_hook(progress_data)
            elif '[download] 100%' in line:
                # 只有看到明确的100%下载完成才认为完成
                progress_data = {'status': 'finished'}
                progress_hook(progress_data)
                
        except Exception as e:
            if self.config['verbose_mode']:
                print(f"   ⚠️ 解析进度时出错: {e}")
    
    def _is_download_successful(self, result, output_lines: List[str]) -> bool:
        """
        智能判断下载是否成功
        不仅仅依赖返回码，还要检查输出内容
        
        Args:
            result: subprocess结果对象
            output_lines: 输出行列表
            
        Returns:
            bool: 是否下载成功
        """
        # 如果返回码为0，通常表示成功
        if result.returncode == 0:
            return True
        
        # 即使返回码非0，也要检查是否实际下载成功
        all_output = ''.join(output_lines)
        
        # 成功的标志
        success_indicators = [
            'has already been downloaded',  # 文件已存在
            '[download] 100%',              # 下载100%完成
            'download completed',           # 下载完成
            '[Merger] Merging formats',     # 格式合并（后处理）
            'Deleting original file',       # 删除原始文件（后处理完成）
        ]
        
        # 检查是否有成功标志（但要排除有严重错误的情况）
        for indicator in success_indicators:
            if indicator in all_output:
                # 即使有成功标志，也要检查是否有严重错误
                serious_errors = [
                    'ERROR:',
                    'FAILED:',
                    'No space left on device',
                    'Permission denied',
                    'Connection refused',
                    'Unable to download webpage',
                    'Video unavailable'
                ]
                
                has_serious_error = any(error in all_output for error in serious_errors)
                if not has_serious_error:
                    if self.config.get('verbose_mode', False):
                        print(f"   🔍 检测到成功标志: {indicator}")
                    return True
                else:
                    if self.config.get('verbose_mode', False):
                        print(f"   ⚠️ 虽然有成功标志 {indicator}，但检测到严重错误，判定为失败")
                    break
        
        # 检查是否有100%进度且没有严重错误
        if '100%' in all_output:
            # 严重错误标志
            serious_errors = [
                'ERROR:',
                'FAILED:',
                'No space left on device',
                'Permission denied',
                'Connection refused',
                'Unable to download webpage',
                'Video unavailable'
            ]
            
            # 如果有100%进度但没有严重错误，可能是轻微的后处理问题
            has_serious_error = any(error in all_output for error in serious_errors)
            if not has_serious_error:
                if self.config.get('verbose_mode', False):
                    print(f"   🔍 检测到100%进度且无严重错误，判定为成功")
                return True
        
        # 检查是否实际有文件生成
        # 这里可以检查输出目录中是否有新文件生成
        # 但为了避免复杂性，暂时不实现这个检查
        
        return False
    
    def _download_single_video(self, url: str, index: int = 0, total: int = 0) -> bool:
        """
        下载单个视频
        
        Args:
            url: 视频URL
            index: 当前视频索引
            total: 总视频数
            
        Returns:
            bool: 下载是否成功
        """
        # 清理和标准化URL
        url = self._clean_url(url)
        if not url:
            return False
            
        if not self._is_valid_acfun_url(url):
            print(f"❌ 无效的AcFun URL: {url}")
            return False
        
        prefix = f"[{index+1}/{total}]" if total > 0 else ""
        print(f"{prefix} 开始下载: {url}")
        
        # 检查网络连接（宽松检查，主要用于提示）
        network_ok = self._check_network_connection()
        if not network_ok:
            print(f"⚠️ {prefix} 网络连接检查失败，但仍将尝试下载")
        
        # 获取视频信息（不阻塞下载流程）
        video_info = None
        video_title = "Unknown"
        try:
            video_info = self._get_video_info(url)
            if video_info and 'title' in video_info:
                video_title = video_info['title'][:50] + "..." if len(video_info['title']) > 50 else video_info['title']
                print(f"📺 {prefix} 视频标题: {video_title}")
        except Exception as e:
            print(f"⚠️ {prefix} 获取视频信息失败，但继续下载: {str(e)[:50]}")
        
        # 首先尝试主要下载器（通常是yt-dlp）
        success = False
        for attempt in range(self.retry_times):
            result = None  # 初始化result变量
            try:
                downloader = self.available_downloaders['primary']
                
                if downloader == 'yt-dlp':
                    cmd = self._build_ytdlp_command(url)
                else:  # you-get
                    cmd = self._build_youget_command(url)
                
                # 执行下载 - 优化版本，改善进度显示和减少卡顿
                video_id = url.split('/')[-1]  # 从URL提取视频ID
                progress_hook = self._create_progress_hook(video_id, video_title)
                
                with subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding='utf-8',
                    errors='ignore',
                    bufsize=0,  # 无缓冲，立即获取输出
                    universal_newlines=True
                ) as process:
                    
                    output_lines = []
                    start_time = time.time()
                    last_progress_time = 0
                    processing_detected = False
                    
                    # 实时读取输出并解析进度
                    while True:
                        try:
                            # 使用非阻塞读取，避免卡顿
                            line = process.stdout.readline()
                            if not line:
                                # 检查进程是否结束
                                if process.poll() is not None:
                                    break
                                # 短暂等待避免CPU占用过高
                                time.sleep(0.01)
                                continue
                                
                            output_lines.append(line)
                            current_time = time.time()
                            
                            # 检查超时
                            if current_time - start_time > self.timeout:
                                print(f"   ⏰ 下载超时，正在终止进程...")
                                process.terminate()
                                # 给进程一些时间优雅退出
                                try:
                                    process.wait(timeout=5)
                                except subprocess.TimeoutExpired:
                                    process.kill()
                                raise subprocess.TimeoutExpired(cmd, self.timeout)
                            
                            # 检测后处理阶段
                            if any(keyword in line for keyword in [
                                '[Merger]', '[ExtractAudio]', '[Fixup', 'Post-processing', 'Converting'
                            ]) and not processing_detected:
                                processing_detected = True
                                print(f"   🔄 {prefix} 正在进行后处理，请稍候...")
                            
                            # 解析yt-dlp的进度输出 - 限制更新频率
                            if (downloader == 'yt-dlp' and 
                                self.config['show_individual_progress'] and
                                current_time - last_progress_time > 0.1):  # 限制100ms更新一次
                                self._parse_ytdlp_progress(line, progress_hook)
                                last_progress_time = current_time
                                
                        except UnicodeDecodeError:
                            # 忽略编码错误，继续处理
                            continue
                        except Exception as e:
                            if self.config['verbose_mode']:
                                print(f"   ⚠️ 读取输出时出错: {e}")
                            break
                    
                    # 等待进程结束，但设置超时避免无限等待
                    try:
                        process.wait(timeout=30)  # 最多等待30秒
                    except subprocess.TimeoutExpired:
                        print(f"   ⚠️ 进程结束超时，强制终止")
                        process.kill()
                        process.wait()
                    
                    # 只有在真正成功时才关闭进度条
                    # 不要在这里强制设置为finished，让后续的成功判断来处理
                    
                    result = type('Result', (), {
                        'returncode': process.returncode,
                        'stdout': ''.join(output_lines),
                        'stderr': ''
                    })()
                
                    # 智能判断下载是否成功 - 不仅仅依赖返回码
                    download_success = self._is_download_successful(result, output_lines)
                    
                    if download_success:
                        print(f"✓ {prefix} 下载成功: {video_title} (使用 {downloader})")
                        
                        # 确保进度条正确关闭
                        video_id = url.split('/')[-1]
                        with self.progress_lock:
                            if video_id in self.current_video_progress:
                                pbar = self.current_video_progress[video_id]
                                pbar.n = 100
                                clean_title = video_title.replace('\n', ' ').replace('\r', ' ').strip()
                                pbar.set_description(f"✅ {clean_title[:28]}... 完成")
                                try:
                                    pbar.refresh()
                                    time.sleep(0.1)
                                    pbar.close()
                                except:
                                    pass
                                del self.current_video_progress[video_id]
                        
                        # 重命名文件，移除线程ID后缀
                        self._rename_downloaded_files(video_title)
                        # 显示下载的文件位置
                        self._show_downloaded_file_location(video_title)
                        # 清理临时文件
                        if self.config['cleanup_temp_files']:
                            self._cleanup_temp_files()
                        # 更新下载状态
                        if self.status_manager:
                            try:
                                self.status_manager.set_url_status(url, 1, video_title)
                            except Exception as e:
                                print(f"⚠️ {prefix} 状态更新失败: {e}")
                        success = True
                        break
                    else:
                        error_msg = self._get_detailed_error_info(result, cmd)
                        print(f"⚠️ {prefix} {downloader}下载失败 (尝试 {attempt+1}/{self.retry_times}): {error_msg}")
                        
                        # 如果是最后一次尝试，清理进度条
                        if attempt == self.retry_times - 1:
                            video_id = url.split('/')[-1]
                            with self.progress_lock:
                                if video_id in self.current_video_progress:
                                    pbar = self.current_video_progress[video_id]
                                    clean_title = video_title.replace('\n', ' ').replace('\r', ' ').strip()
                                    pbar.set_description(f"❌ {clean_title[:28]}... 失败")
                                    try:
                                        pbar.refresh()
                                        time.sleep(0.5)
                                        pbar.close()
                                    except:
                                        pass
                                    del self.current_video_progress[video_id]
                    
            except subprocess.TimeoutExpired:
                print(f"⚠️ {prefix} {downloader}下载超时 (尝试 {attempt+1}/{self.retry_times})")
                # 超时时也需要清理进度条
                if attempt == self.retry_times - 1:
                    video_id = url.split('/')[-1]
                    with self.progress_lock:
                        if video_id in self.current_video_progress:
                            pbar = self.current_video_progress[video_id]
                            clean_title = video_title.replace('\n', ' ').replace('\r', ' ').strip()
                            pbar.set_description(f"⏰ {clean_title[:28]}... 超时")
                            try:
                                pbar.refresh()
                                time.sleep(0.5)
                                pbar.close()
                            except:
                                pass
                            del self.current_video_progress[video_id]
            except Exception as e:
                print(f"⚠️ {prefix} {downloader}下载出错 (尝试 {attempt+1}/{self.retry_times}): {str(e)}")
                # 异常时也需要清理进度条
                if attempt == self.retry_times - 1:
                    video_id = url.split('/')[-1]
                    with self.progress_lock:
                        if video_id in self.current_video_progress:
                            pbar = self.current_video_progress[video_id]
                            clean_title = video_title.replace('\n', ' ').replace('\r', ' ').strip()
                            pbar.set_description(f"💥 {clean_title[:28]}... 错误")
                            try:
                                pbar.refresh()
                                time.sleep(0.5)
                                pbar.close()
                            except:
                                pass
                            del self.current_video_progress[video_id]
            
            # 如果不是最后一次尝试，智能判断是否重试
            if not success and attempt < self.retry_times - 1:
                should_retry, wait_time = self._should_retry_error(result, attempt)
                if should_retry:
                    if wait_time > 10:
                        print(f"   检测到服务器繁忙或临时问题，等待 {wait_time} 秒后重试...")
                    else:
                        print(f"   等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    print(f"   检测到不可恢复的错误，跳过重试")
                    break  # 不再重试，直接退出循环
        
        # 如果主要下载器失败，尝试备用下载器
        if not success and len(self.available_downloaders['available']) > 1:
            for backup_downloader in self.available_downloaders['available']:
                if backup_downloader != self.available_downloaders['primary']:
                    print(f"🔄 {prefix} 尝试使用备用下载器: {backup_downloader}")
                    try:
                        if backup_downloader == 'yt-dlp':
                            cmd = self._build_ytdlp_command(url, is_backup=True)
                        else:  # you-get
                            cmd = self._build_youget_command(url)
                        
                        result = subprocess.run(
                            cmd,
                            capture_output=True,
                            text=True,
                            timeout=self.timeout,
                            encoding='utf-8',
                            errors='ignore'
                        )
                        
                        if result.returncode == 0:
                            print(f"✓ {prefix} 下载成功: {video_title} (使用备用 {backup_downloader})")
                            # 显示下载的文件位置
                            self._show_downloaded_file_location(video_title)
                            # 清理临时文件
                            if self.config['cleanup_temp_files']:
                                self._cleanup_temp_files()
                            # 更新下载状态
                            if self.status_manager:
                                try:
                                    self.status_manager.set_url_status(url, 1, video_title)
                                except Exception as e:
                                    print(f"⚠️ {prefix} 状态更新失败: {e}")
                            success = True
                            break
                        else:
                            error_msg = self._get_detailed_error_info(result, cmd)
                            print(f"⚠️ {prefix} 备用{backup_downloader}也失败: {error_msg}")
                            
                    except Exception as e:
                        print(f"⚠️ {prefix} 备用{backup_downloader}出错: {str(e)}")
        
        if success:
            return True
        
        print(f"❌ {prefix} 下载失败: {video_title}")
        return False
    
    def download_urls(self, urls: List[str]) -> dict:
        """
        批量下载视频
        
        Args:
            urls: 视频URL列表
            
        Returns:
            dict: 下载统计信息
        """
        if not urls:
            print("❌ 没有提供下载URL")
            return self.download_stats
        
        # 过滤和清理有效URL
        valid_urls = []
        for url in urls:
            cleaned_url = self._clean_url(url)
            if cleaned_url:
                valid_urls.append(cleaned_url)
        
        # 状态检查：过滤已下载的URL
        if self.status_manager:
            original_count = len(valid_urls)
            pending_urls = self.status_manager.get_pending_urls(valid_urls)
            downloaded_count = original_count - len(pending_urls)
            
            if downloaded_count > 0:
                print(f"📊 状态检查完成: 共 {original_count} 个URL，已下载 {downloaded_count} 个，待下载 {len(pending_urls)} 个")
                # 显示下载进度统计
                if original_count > 0:
                    summary = self.status_manager.get_status_summary()
                    print(f"📈 总体进度: {summary['downloaded']}/{summary['total']} ({summary['download_rate']:.1f}%)")
            
            valid_urls = pending_urls
        
        total_urls = len(valid_urls)
        
        if total_urls == 0:
            if self.status_manager and original_count > 0:
                print("✅ 所有视频都已下载完成！")
            else:
                print("❌ 没有有效的URL")
            return self.download_stats
        
        print(f"🚀 开始批量下载，共 {total_urls} 个视频")
        print("=" * 80)
        print(f"📁 ✨ 下载目录: {self.output_dir.absolute()}")
        print(f"📂 💾 确保该目录有足够空间存储视频文件")
        print("=" * 80)
        print(f"🔄 并发数: {self.max_workers}")
        if self.config['enable_multithreading']:
            print(f"🧵 多线程分片: {self.config['max_fragments']} 个并发分片")
        print("-" * 60)
        
        self.download_stats['total'] = total_urls
        start_time = time.time()
        
        # 创建总体进度条（只创建一次）
        if self.config['show_progress'] and self.overall_progress is None:
            terminal_width = self._get_terminal_width()
            
            # 根据终端友好模式调整总体进度条格式
            if self.config.get('terminal_friendly', True):
                # 使用标准的tqdm格式，确保显示纯色进度条
                bar_format = '{l_bar}{bar}| {n}/{total} [{elapsed}<{remaining}]'
                ascii_bar = False  # 使用Unicode字符显示更美观的进度条
            else:
                bar_format = '{l_bar}{bar}| {n}/{total} [{elapsed}<{remaining}]'
                ascii_bar = False
            
            self.overall_progress = tqdm(
                total=total_urls,
                desc="🎬 总体进度",
                unit="视频",
                position=0,
                ncols=terminal_width,
                leave=True,
                bar_format=bar_format,
                ascii=ascii_bar,
                dynamic_ncols=True,  # 动态调整宽度
                mininterval=0.1,     # 更频繁的更新
                maxinterval=1.0,     # 更短的最大更新间隔
                file=sys.stderr      # 输出到stderr，避免与其他输出混合
            )
        elif self.config['show_progress'] and self.overall_progress is not None:
            # 如果进度条已存在，只更新总数
            self.overall_progress.total = total_urls
            self.overall_progress.refresh()
        
        # 动态调整并发数量
        optimal_workers = self._get_optimal_workers()
        
        # 使用线程池进行并发下载
        with ThreadPoolExecutor(max_workers=optimal_workers) as executor:
            # 提交所有下载任务
            future_to_url = {
                executor.submit(self._download_single_video, url, i, total_urls): url 
                for i, url in enumerate(valid_urls)
            }
            
            # 等待任务完成
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    success = future.result()
                    with self.stats_lock:
                        if success:
                            self.download_stats['success'] += 1
                        else:
                            self.download_stats['failed'] += 1
                            self.download_stats['failed_urls'].append(url)
                        
                        # 更新总进度条
                        if self.overall_progress:
                            # 更新进度
                            self.overall_progress.update(1)
                            
                            # 更新描述信息，包含成功和失败统计
                            success_count = self.download_stats['success']
                            failed_count = self.download_stats['failed']
                            if failed_count > 0:
                                desc = f"🎬 总体进度 (✅{success_count} ❌{failed_count})"
                            else:
                                desc = f"🎬 总体进度 (✅{success_count})"
                            self.overall_progress.set_description(desc)
                            
                            # 强制刷新显示
                            try:
                                self.overall_progress.refresh()
                            except:
                                pass
                            
                except Exception as e:
                    print(f"❌ 任务执行异常: {str(e)}")
                    with self.stats_lock:
                        self.download_stats['failed'] += 1
                        self.download_stats['failed_urls'].append(url)
                        
                        # 更新总进度条
                        if self.overall_progress:
                            # 更新进度
                            self.overall_progress.update(1)
                            
                            # 更新描述信息
                            success_count = self.download_stats['success']
                            failed_count = self.download_stats['failed']
                            if failed_count > 0:
                                desc = f"🎬 总体进度 (✅{success_count} ❌{failed_count})"
                            else:
                                desc = f"🎬 总体进度 (✅{success_count})"
                            self.overall_progress.set_description(desc)
                            
                            # 强制刷新显示
                            try:
                                self.overall_progress.refresh()
                            except:
                                pass
        
        # 关闭总体进度条
        if self.overall_progress:
            self.overall_progress.close()
            
        # 清理所有剩余的单个视频进度条
        with self.progress_lock:
            for video_id, pbar in list(self.current_video_progress.items()):
                try:
                    pbar.close()
                except:
                    pass
            self.current_video_progress.clear()
            
        # 确保终端输出清洁
        if self.config['show_individual_progress']:
            try:
                # 清空stderr缓冲区，确保进度条完全清除
                sys.stderr.flush()
                # 输出一个空行，分隔进度条和后续内容
                print("", file=sys.stderr)
            except:
                pass
        
        end_time = time.time()
        duration = end_time - start_time
        
        # 打印统计信息
        print("\n" + "=" * 80)
        print("📊 下载完成统计")
        print("=" * 80)
        print(f"✓ 成功: {self.download_stats['success']}")
        print(f"❌ 失败: {self.download_stats['failed']}")
        print(f"📝 总计: {self.download_stats['total']}")
        print(f"⏱️  耗时: {duration:.1f} 秒")
        print(f"⚡ 平均速度: {self.download_stats['total']/duration:.2f} 个/秒")
        
        # 显示下载目录信息
        if self.download_stats['success'] > 0:
            print(f"\n📁 所有下载的视频文件都保存在:")
            print(f"   🎯 {self.output_dir.absolute()}")
            try:
                # 统计下载目录中的视频文件
                video_extensions = ['.mp4', '.mkv', '.webm', '.avi', '.mov', '.flv']
                video_files = []
                for ext in video_extensions:
                    video_files.extend(list(self.output_dir.glob(f"*{ext}")))
                
                if video_files:
                    total_size = sum(f.stat().st_size for f in video_files) / (1024 * 1024 * 1024)  # GB
                    print(f"   📊 目录共有 {len(video_files)} 个视频文件，总大小约 {total_size:.2f} GB")
            except Exception:
                pass
        
        # 显示失败的URL
        if self.download_stats['failed_urls']:
            print(f"\n❌ 失败的URL ({len(self.download_stats['failed_urls'])}):")
            for url in self.download_stats['failed_urls']:
                print(f"   {url}")
        
        print("=" * 80)
        return self.download_stats
    
    def download_from_file(self, file_path: str) -> dict:
        """
        从文件读取URL并批量下载
        
        Args:
            file_path: 包含URL的文件路径
            
        Returns:
            dict: 下载统计信息
        """
        try:
            with open(file_path, 'r', encoding=SYSTEM_CONFIG['encoding']) as f:
                raw_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                # 清理和标准化URL
                urls = []
                for url in raw_urls:
                    cleaned_url = self._clean_url(url)
                    if cleaned_url:
                        urls.append(cleaned_url)
            
            print(f"📄 从文件读取到 {len(urls)} 个URL: {file_path}")
            return self.download_urls(urls)
            
        except FileNotFoundError:
            print(f"❌ 文件不存在: {file_path}")
            return self.download_stats
        except Exception as e:
            print(f"❌ 读取文件失败: {str(e)}")
            return self.download_stats


def create_sample_urls_file():
    """创建示例URL文件到默认路径"""
    sample_content = """# AcFun视频URL列表
# 每行一个URL，以#开头的行为注释
# 示例：
https://www.acfun.cn/v/ac47796285
https://www.acfun.cn/v/ac12345678
https://www.acfun.cn/v/ac87654321
"""
    default_file_path = SYSTEM_CONFIG['default_urls_file']
    try:
        with open(default_file_path, 'w', encoding=SYSTEM_CONFIG['encoding']) as f:
            f.write(sample_content)
        print(f"✓ 已创建示例URL文件: {default_file_path}")
        print("现在可以编辑该文件，然后直接运行: python acfun_downloader.py")
    except Exception as e:
        print(f"❌ 创建文件失败: {e}")
        # 备选方案：在当前目录创建
        with open('acfun_urls.txt', 'w', encoding=SYSTEM_CONFIG['encoding']) as f:
            f.write(sample_content)
        print("✓ 已在当前目录创建示例文件: acfun_urls.txt")


def handle_status_commands(args):
    """处理状态管理相关命令"""
    try:
        # 初始化状态管理器
        status_manager = DownloadStatusManager(
            status_dir=SYSTEM_CONFIG['status_record_dir'],
            status_file=SYSTEM_CONFIG['status_file_name']
        )
        
        if args.status_summary:
            # 显示状态统计
            summary = status_manager.get_status_summary()
            print("📊 下载状态统计:")
            print(f"   总URL数量: {summary['total']}")
            print(f"   已下载: {summary['downloaded']}")
            print(f"   未下载: {summary['pending']}")
            print(f"   完成率: {summary['download_rate']:.1f}%")
        
        elif args.list_downloaded:
            # 列出已下载的URL
            downloaded_urls = status_manager.get_downloaded_urls()
            if downloaded_urls:
                print(f"📋 已下载的URL列表 (共 {len(downloaded_urls)} 个):")
                for i, url in enumerate(downloaded_urls, 1):
                    print(f"   {i:3d}. {url}")
            else:
                print("📋 暂无已下载的URL")
        
        elif args.reset_url:
            # 重置指定URL的状态
            url = args.reset_url.strip()
            current_status = status_manager.get_url_status(url)
            if current_status == 1:
                status_manager.set_url_status(url, 0, "", "")
                print(f"✅ 已重置URL状态: {url}")
            else:
                print(f"ℹ️ URL状态已经是未下载: {url}")
        
        elif args.reset_all:
            # 重置所有URL状态
            print("⚠️ 此操作将重置所有URL的下载状态，请确认...")
            response = input("输入 'yes' 确认重置所有状态: ")
            if response.lower() == 'yes':
                # 读取所有状态并重置为0
                status_data = status_manager._read_status_file()
                reset_count = 0
                for url_hash, info in status_data.items():
                    if info.get('status') == 1:
                        info['status'] = 0
                        info['last_updated'] = time.strftime('%Y-%m-%d %H:%M:%S')
                        reset_count += 1
                
                status_manager._write_status_file(status_data)
                print(f"✅ 已重置 {reset_count} 个URL的下载状态")
            else:
                print("❌ 操作已取消")
                
    except Exception as e:
        print(f"❌ 状态管理操作失败: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='AcFun视频批量下载器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        """
    )
    
    parser.add_argument('-u', '--urls', nargs='+', help='要下载的视频URL')
    parser.add_argument('-f', '--file', default=SYSTEM_CONFIG['default_urls_file'], help=f'包含URL的文件路径 (默认: {SYSTEM_CONFIG["default_urls_file"]})')
    parser.add_argument('-o', '--output', default=SYSTEM_CONFIG['download_dir'], help=f'下载目录 (默认: {SYSTEM_CONFIG["download_dir"]})')
    parser.add_argument('-w', '--workers', type=int, default=2, help='并发下载数 (默认: 2)')
    parser.add_argument('-r', '--retry', type=int, default=3, help='重试次数 (默认: 3)')
    parser.add_argument('-t', '--timeout', type=int, default=300, help='超时时间/秒 (默认: 300)')
    parser.add_argument('--downloader', choices=['yt-dlp', 'you-get', 'auto'], default='auto', help='指定下载器 (默认: auto)')
    parser.add_argument('--create-sample', action='store_true', help='创建示例URL文件')
    
    # 状态管理相关参数
    parser.add_argument('--status-summary', action='store_true', help='显示下载状态统计')
    parser.add_argument('--list-downloaded', action='store_true', help='列出所有已下载的URL')
    parser.add_argument('--reset-url', help='重置指定URL的下载状态')
    parser.add_argument('--reset-all', action='store_true', help='重置所有URL的下载状态')
    parser.add_argument('--force-redownload', action='store_true', help='强制重新下载所有URL（忽略状态）')
    
    args = parser.parse_args()
    
    if args.create_sample:
        create_sample_urls_file()
        return
    
    # 处理状态管理命令
    if any([args.status_summary, args.list_downloaded, args.reset_url, args.reset_all]):
        handle_status_commands(args)
        return
    
    # 如果没有提供URLs且文件不存在，则提示错误
    if not args.urls and not os.path.exists(args.file):
        parser.print_help()
        print(f"\n❌ 默认文件不存在: {args.file}")
        print("请提供URL参数 (-u) 或确保默认文件存在")
        return
    
    # 确定下载器偏好
    prefer_ytdlp = args.downloader != 'you-get'
    if args.downloader == 'you-get':
        prefer_ytdlp = False
    elif args.downloader == 'yt-dlp':
        prefer_ytdlp = True
    else:  # auto
        prefer_ytdlp = True
    
    # 创建下载器配置
    config = {}
    if args.force_redownload:
        config['enable_status_tracking'] = False  # 禁用状态跟踪以强制重新下载
        print("🔄 强制重新下载模式已启用，将忽略所有下载状态")
    
    downloader = AcFunDownloader(
        output_dir=args.output,
        max_workers=args.workers,
        retry_times=args.retry,
        timeout=args.timeout,
        prefer_ytdlp=prefer_ytdlp,
        config=config
    )
    
    try:
        if args.urls:
            # 从命令行参数下载
            downloader.download_urls(args.urls)
        else:
            # 从文件下载（使用默认文件或指定文件）
            downloader.download_from_file(args.file)
    
    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断下载")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 程序执行出错: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
