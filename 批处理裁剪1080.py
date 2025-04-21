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

# 设置日志处理器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("video_process.log"),
        logging.StreamHandler()
    ]
)

# 获取系统信息
system_info = {
    "os": platform.system(),
    "processor": platform.processor(),
    "python": platform.python_version(),
}
logging.info(f"系统信息: {json.dumps(system_info)}")

# 系统编码
system_encoding = locale.getpreferredencoding()
logging.info(f"系统编码: {system_encoding}")

# 设置环境变量
os.environ["PYTHONIOENCODING"] = "utf-8"

# 配置临时目录
temp_dir = Path("./temp")
temp_dir.mkdir(exist_ok=True)

# 线程锁，用于进度条更新
progress_lock = threading.Lock()

# 记录开始时间
start_time = time.time()

# 硬件加速器配置
HW_CONFIGS = {
    "nvidia": {
        "encoders": ["h264_nvenc", "hevc_nvenc"],
        "options": {
            "preset": "p1",      # p1(最快) p2 p3 p4 p5 p6 p7(最高质量)
            "tune": "ll",        # ll(低延迟) ull(超低延迟)
            "b:v": "5M",
            "maxrate": "10M",
            "bufsize": "10M",
            "spatial_aq": "1",   # 空间自适应量化
            "temporal_aq": "1",  # 时间自适应量化
            "rc": "vbr_hq",      # 码率控制模式
        }
    },
    "amd": {
        "encoders": ["h264_amf", "hevc_amf"],
        "options": {
            "quality": "speed",
            "usage": "lowlatency",
            "b:v": "5M",
            "rc": "vbr_peak_constrained",
        }
    },
    "intel": {
        "encoders": ["h264_qsv", "hevc_qsv"],
        "options": {
            "preset": "veryfast",
            "look_ahead": "0",   # 禁用前向分析，减少延迟
            "b:v": "5M",
            "global_quality": "23",
        }
    },
    "software": {
        "encoders": ["libx264", "libx265"],
        "options": {
            "preset": "ultrafast",
            "tune": "fastdecode",
            "crf": "28",         # 更高的CRF加快速度
            "threads": "0",      # 使用所有核心
            "aq-mode": "3",      # 自适应量化模式
        }
    }
}

# 检测可用的硬件加速
def detect_hardware():
    """检测系统硬件配置并返回最佳编码器设置"""
    # 检测CPU核心数
    cpu_count = os.cpu_count() or 4
    
    # 初始化结果
    hw_info = {
        "cpu_cores": cpu_count,
        "encoder_type": "software",
        "encoder": "libx264",
        "options": {},
        "max_parallel": min(cpu_count // 2, 4)  # 默认并行数
    }
    
    try:
        # 检查可用编码器
        result = subprocess.run(
            ['ffmpeg', '-hide_banner', '-encoders'], 
            capture_output=True, text=True, encoding='utf-8'
        )
        
        # 检测NVIDIA GPU
        if any(encoder in result.stdout for encoder in HW_CONFIGS["nvidia"]["encoders"]):
            hw_info["encoder_type"] = "nvidia"
            hw_info["encoder"] = next(e for e in HW_CONFIGS["nvidia"]["encoders"] if e in result.stdout)
            hw_info["options"] = HW_CONFIGS["nvidia"]["options"].copy()
            hw_info["max_parallel"] = 4  # NVIDIA通常支持4个并行编码
            
            # 获取GPU信息
            try:
                gpu_info = subprocess.run(['nvidia-smi', '--query-gpu=name,memory.total,driver_version', '--format=csv'],
                                        capture_output=True, text=True)
                hw_info["gpu_info"] = gpu_info.stdout.strip() if gpu_info.returncode == 0 else "Unknown"
            except:
                hw_info["gpu_info"] = "Error getting GPU info"
                
        # 检测AMD GPU
        elif any(encoder in result.stdout for encoder in HW_CONFIGS["amd"]["encoders"]):
            hw_info["encoder_type"] = "amd"
            hw_info["encoder"] = next(e for e in HW_CONFIGS["amd"]["encoders"] if e in result.stdout)
            hw_info["options"] = HW_CONFIGS["amd"]["options"].copy()
            hw_info["max_parallel"] = 3  # AMD通常支持3个并行编码
            
        # 检测Intel GPU
        elif any(encoder in result.stdout for encoder in HW_CONFIGS["intel"]["encoders"]):
            hw_info["encoder_type"] = "intel"
            hw_info["encoder"] = next(e for e in HW_CONFIGS["intel"]["encoders"] if e in result.stdout)
            hw_info["options"] = HW_CONFIGS["intel"]["options"].copy()
            hw_info["max_parallel"] = 3  # Intel通常支持3个并行编码
                
        # 使用软件编码
        else:
            hw_info["encoder_type"] = "software"
            hw_info["encoder"] = HW_CONFIGS["software"]["encoders"][0]
            hw_info["options"] = HW_CONFIGS["software"]["options"].copy()
            hw_info["max_parallel"] = max(1, min(cpu_count // 2, 4))  # 软件编码限制更多并行数
            
        # 记录检测到的硬件
        logging.info(f"检测到的硬件: {hw_info}")
        return hw_info
        
    except Exception as e:
        logging.error(f"硬件检测失败: {e}")
        return hw_info

# 从ffmpeg输出解析进度
def parse_progress(line):
    """从ffmpeg输出行解析进度信息"""
    info = {}
    
    # 使用更强的正则表达式一次提取所有信息
    patterns = {
        'frame': r'frame=\s*(\d+)',
        'fps': r'fps=\s*(\d+\.?\d*)',
        'time': r'time=\s*(\d+):(\d+):(\d+\.\d+)',
        'speed': r'speed=\s*(\d+\.?\d*)x',
        'size': r'size=\s*(\d+)kB'
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, line)
        if match:
            if key == 'time':
                h, m, s = match.groups()
                info[key] = float(h) * 3600 + float(m) * 60 + float(s)
            elif key == 'frame' or key == 'size':
                info[key] = int(match.group(1))
            else:
                info[key] = float(match.group(1))
    
    return info

# 构建高效的FFmpeg命令
def build_ffmpeg_command(input_file, output_file, filter_complex, hw_info):
    """根据硬件情况构建最优的FFmpeg命令"""
    cmd = [
        'ffmpeg', '-y',
        '-i', input_file,
        # 添加输入分析参数，提高稳定性
        '-analyzeduration', '100M',  # 增加分析时长
        '-probesize', '100M',        # 增加探测大小
        '-vf', filter_complex
    ]
    
    # 添加硬件加速编码器和选项
    cmd.extend(['-c:v', hw_info['encoder']])
    
    # 添加关键帧控制参数，解决进度条拖动问题
    if hw_info['encoder_type'] == "nvidia":
        # NVIDIA GPU编码器参数
        cmd.extend([
            '-g', '30',              # 每秒1个关键帧(30帧率下)
            '-keyint_min', '15',     # 最小关键帧间隔
        ])
    elif hw_info['encoder_type'] == "amd":
        # AMD GPU编码器参数
        cmd.extend([
            '-g', '30',              # GOP大小，关键帧间隔
            '-keyint_min', '15'      # 最小关键帧间隔
        ])
    elif hw_info['encoder_type'] == "intel":
        # Intel GPU编码器参数
        cmd.extend([
            '-g', '30',              # GOP大小
        ])
    else:
        # 软件编码器参数
        cmd.extend([
            '-g', '30',              # 每30帧一个关键帧
            '-keyint_min', '15',     # 最小关键帧间隔
            '-sc_threshold', '40',   # 场景变化阈值
            '-bf', '2'               # B帧数量
        ])
    
    # 添加所有编码选项
    for key, value in hw_info['options'].items():
        # 避免重复添加已经设置的参数
        if key not in ['g', 'keyint_min', 'sc_threshold', 'bf']:
            cmd.extend([f'-{key}', f'{value}'])
    
    # 音频设置
    cmd.extend([
        '-c:a', 'aac',
        '-b:a', '192k',
    ])
    
    # 额外的通用优化
    cmd.extend([
        '-movflags', '+faststart',  # Web优化：将元数据移到文件前部
        '-map_metadata', '-1',      # 删除所有元数据，减小文件大小
        '-vsync', 'cfr',            # 恒定帧率，提高播放器兼容性
    ])
    
    # 输出文件
    cmd.append(output_file)
    return cmd

# 高效裁剪函数 - 直接使用ffmpeg，带有独立进度条
def process_video(video_path, output_video_path, roi, hardware_info, video_idx=0, total_videos=1, target_resolution=(1920, 1080)):
    """使用ffmpeg直接裁剪视频，效率更高，带有独立进度条"""
    x, y, w, h = roi
    
    # 创建文件名用于显示（避免路径过长）
    filename = os.path.basename(video_path)
    short_name = filename[:30] + "..." if len(filename) > 30 else filename
    
    # 获取文件大小（用于判断是否为大文件）
    try:
        file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
        is_large_file = file_size_mb > 1000  # 大于1GB认为是大文件
    except:
        is_large_file = False
    
    # 创建此视频的进度条
    pbar = tqdm(
        total=100, 
        desc=f"视频 {video_idx+1}/{total_videos}: {short_name}", 
        position=video_idx+1,  # 位置从1开始，0是总进度条
        leave=True,
        bar_format='{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'
    )
    
    # 尝试3次处理，增加容错能力
    max_retries = 3
    
    for retry_count in range(max_retries):
        try:
            if retry_count > 0:
                logging.info(f"第{retry_count+1}次尝试处理视频: {video_path}")
                pbar.set_postfix_str(f"重试 {retry_count+1}/{max_retries}...")
            else:
                logging.info(f"开始处理视频: {video_path}")
            
            # 获取视频信息
            probe_cmd = [
                'ffprobe',
                '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=width,height,codec_name,duration,pix_fmt',
                '-show_entries', 'format=duration',
                '-of', 'json',
                video_path
            ]
            result = subprocess.run(probe_cmd, capture_output=True, text=True, encoding='utf-8')
            if result.returncode != 0:
                raise Exception(f"获取视频信息失败: {result.stderr}")
            
            # 解析JSON结果
            video_info = json.loads(result.stdout)
            width = int(video_info['streams'][0]['width'])
            height = int(video_info['streams'][0]['height'])
            codec = video_info['streams'][0].get('codec_name', 'unknown')
            pix_fmt = video_info['streams'][0].get('pix_fmt', 'unknown')
            
            # 检测是否为10位色深视频
            is_10bit = ('p10' in pix_fmt) or ('p12' in pix_fmt) or ('p16' in pix_fmt) or ('yuv420p10' in pix_fmt)
            
            # 获取持续时间（首选stream，其次format）
            duration = float(
                video_info['streams'][0].get('duration') or 
                video_info['format'].get('duration', 0)
            )
            
            logging.info(f"视频尺寸: {width}x{height}, 编码器: {codec}, 时长: {duration:.2f}秒, 格式: {pix_fmt}")
            pbar.set_postfix_str(f"尺寸: {width}x{height}, 时长: {duration:.2f}秒")
            
            # 更新进度条到5%表示视频分析完成
            pbar.update(5)
            
            # 基于视频高度选择处理方式
            target_width, target_height = target_resolution
            if height > target_height:
                # 裁剪后缩放
                filter_complex = f"crop={w}:{h}:{x}:{y},scale={target_width}:{target_height}"
            else:
                # 如果视频高度小于等于目标高度，裁剪后添加黑边
                black_top = (target_height - h) // 2
                black_bottom = target_height - h - black_top
                black_left = (target_width - w) // 2
                black_right = target_width - w - black_left

                filter_complex = f"crop={w}:{h}:{x}:{y},pad={target_width}:{target_height}:{black_left}:{black_top}:black"
            
            # 调整编码器设置，处理10位色深视频
            current_hw_info = hardware_info.copy()
            
            # 如果是10位色深视频，且硬件不支持10位处理，切换到合适的编码器
            if is_10bit:
                # 检查分辨率是否超过1080p
                is_4k = (target_width > 1920 or target_height > 1080)
                
                if current_hw_info["encoder_type"] == "nvidia":
                    if current_hw_info["encoder"] == "h264_nvenc":
                        if is_4k:
                            logging.info("检测到10位4K视频，切换到高质量CPU编码")
                            # 对于4K 10位视频，使用高质量CPU编码
                            current_hw_info["encoder_type"] = "software"
                            current_hw_info["encoder"] = "libx264"
                            current_hw_info["options"] = {
                                "preset": "slow",
                                "crf": "18",
                                "pix_fmt": "yuv420p",  # 强制输出为8位色深
                            }
                        else:
                            # 尝试使用HEVC编码器(支持10位)
                            logging.info("检测到10位视频，尝试使用HEVC-NVENC编码器")
                            current_hw_info["encoder"] = "hevc_nvenc"
                            # 添加10位相关选项
                            current_hw_info["options"]["spatial-aq"] = "1"
                            current_hw_info["options"]["temporal-aq"] = "1"
                # 其他编码器处理类似...
                
            # 添加像素格式指定
            if is_10bit and current_hw_info["encoder_type"] == "software":
                # 软件编码强制输出为8位
                filter_complex += ",format=yuv420p"
            
            # 构建最优的FFmpeg命令
            cmd = build_ffmpeg_command(video_path, output_video_path, filter_complex, current_hw_info)
            
            logging.info(f"执行命令: {' '.join(cmd)}")
            pbar.update(5)  # 更新进度条到10%表示准备开始编码
            
            # 使用subprocess运行ffmpeg命令并实时监控输出
            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    encoding='utf-8',
                    bufsize=1
                )
                
                # 监控进度
                last_time = 0
                last_percentage = 10  # 从10%开始，因为前面已经更新了10%
                
                # 超时计数器
                last_update_time = time.time()
                
                while process.poll() is None:  # 进程仍在运行
                    # 读取一行stderr（ffmpeg的进度信息）
                    line = process.stderr.readline()
                    
                    if line:
                        progress_info = parse_progress(line)
                        if 'time' in progress_info and progress_info['time'] > last_time:
                            last_time = progress_info['time']
                            last_update_time = time.time()  # 更新最后进度更新时间
                            
                            # 计算进度百分比 (视频编码占总进度的85%，从10%到95%)
                            percentage = min(95, 10 + last_time * 85 / duration)
                            delta = percentage - last_percentage
                            
                            if delta > 0:
                                pbar.update(delta)
                                last_percentage = percentage
                                
                                # 添加进度信息
                                extra_info = {}
                                if 'fps' in progress_info:
                                    extra_info['FPS'] = f"{progress_info['fps']:.1f}"
                                if 'speed' in progress_info:
                                    extra_info['速度'] = f"{progress_info['speed']}x"
                                if 'size' in progress_info:
                                    extra_info['大小'] = f"{progress_info['size']/1024:.1f}MB"
                                
                                # 同步锁保护进度条更新
                                with progress_lock:
                                    pbar.set_postfix(**extra_info)
                    
                    # 检查超时（大文件300秒，普通文件180秒内无进度更新）
                    timeout_threshold = 300 if is_large_file else 180
                    if time.time() - last_update_time > timeout_threshold:
                        logging.warning(f"处理视频超过{timeout_threshold}秒无进度更新: {video_path}")
                        # 不直接终止，尝试发送SIGTERM而不是强制终止
                        try:
                            # 在Windows上使用taskkill更温和地终止进程
                            if platform.system() == "Windows":
                                subprocess.run(['taskkill', '/PID', str(process.pid), '/F'], capture_output=True)
                            else:
                                # 在Linux/Mac上发送SIGTERM
                                import signal
                                os.kill(process.pid, signal.SIGTERM)
                            # 给进程5秒钟来清理
                            process.wait(timeout=5)
                        except:
                            # 强制终止
                            process.terminate()
                        
                        # 不立即退出，而是尝试下一次重试
                        raise Exception(f"处理超时，{timeout_threshold}秒内没有进度更新")
                    
                    # 避免CPU占用过高，同时改善响应性
                    time.sleep(0.05)
                
                # 检查最终返回码
                returncode = process.returncode
                if returncode != 0:
                    stderr = process.stderr.read()
                    raise Exception(f"ffmpeg处理失败 (代码 {returncode}): {stderr}")
                
                # 更新到100%完成
                pbar.update(100 - last_percentage)
                pbar.set_postfix_str("完成✓")
                logging.info(f"视频处理完成: {video_path}")
                pbar.close()
                return True
                
            except Exception as subprocess_err:
                logging.error(f"执行过程异常: {str(subprocess_err)}")
                # 继续外层异常处理
                raise
        except Exception as e:
            if retry_count < max_retries - 1:
                logging.warning(f"处理失败，尝试第{retry_count+2}次: {str(e)}")
                pbar.set_postfix_str(f"失败✗ - 将重试 {retry_count+2}/{max_retries}")
                time.sleep(2)  # 等待2秒后重试
                continue
            
            # 最后一次尝试，使用备用方案
            logging.error(f"处理视频 {video_path} 失败: {str(e)}")
            pbar.set_postfix_str("失败✗ - 尝试备用方案")
            
            # 失败时尝试使用最基本的命令
            try:
                logging.info("尝试使用基本命令...")
                # 检查是否为4K视频
                is_4k = (target_width > 1920 or target_height > 1080)
                
                # 根据分辨率和视频特性调整参数
                preset = "medium" if not is_4k else "slow"
                crf = "23" if not is_4k else "18"
                
                basic_cmd = [
                    'ffmpeg', '-y',
                    '-i', video_path,
                    '-vf', f"crop={w}:{h}:{x}:{y},scale={target_width}:{target_height},format=yuv420p",
                    '-c:v', 'libx264',           # 使用最兼容的H.264编码器
                    '-preset', preset,           # 根据分辨率调整预设
                    '-crf', crf,                 # 根据分辨率调整质量
                    '-profile:v', 'high',        # 高规格，提高兼容性
                    '-level', '4.1',             # 兼容大多数播放器
                    '-g', '30',                  # 每30帧一个关键帧
                    '-keyint_min', '15',         # 最小关键帧间隔
                    '-sc_threshold', '40',       # 场景变化阈值
                    '-bf', '2',                  # B帧数量
                    '-movflags', '+faststart',   # 优化Web播放
                    '-pix_fmt', 'yuv420p',       # 最广泛支持的像素格式(8位)
                    '-c:a', 'aac',               # 音频编码
                    '-b:a', '192k',              # 音频比特率
                    '-vsync', 'cfr',             # 恒定帧率
                    output_video_path
                ]
                logging.info(f"执行命令: {' '.join(basic_cmd)}")
                
                pbar.set_postfix_str("尝试备用方案...")
                
                # 直接执行
                subprocess.Popen(
                    basic_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    encoding='utf-8'
                ).wait()
                
                # 检查文件是否创建成功
                if os.path.exists(output_video_path) and os.path.getsize(output_video_path) > 1000000:  # 文件大于1MB
                    logging.info("基本命令处理完成")
                    pbar.update(100 - pbar.n)  # 更新到100%
                    pbar.set_postfix_str("完成(备用)✓")
                    pbar.close()
                    return True
                else:
                    pbar.set_postfix_str("失败✗")
                    pbar.close()
                    return False
                    
            except Exception as e2:
                logging.error(f"基本命令也失败: {str(e2)}")
                pbar.close()
                return False
    
    pbar.close()
    return False

def process_videos_in_parallel(video_paths, output_paths, rois, hardware_info, target_resolution=(1920, 1080)):
    """并行处理视频，带有总进度条和每个视频的单独进度条"""
    # 确保输出目录存在
    if output_paths and len(output_paths) > 0:
        os.makedirs(os.path.dirname(output_paths[0]), exist_ok=True)

    # 创建一个总进度条
    total_pbar = tqdm(
        total=len(video_paths), 
        desc="总进度", 
        position=0,
        leave=True,
        bar_format='{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
    )
    
    # 用于追踪已完成的视频数量
    completed_videos = 0
    completed_lock = threading.Lock()
    
    # 回调函数，处理完成后更新总进度条
    def task_done_callback(future):
        nonlocal completed_videos
        try:
            result = future.result()
            with completed_lock:
                completed_videos += 1
                total_pbar.update(1)
        except Exception as e:
            logging.error(f"任务完成回调异常: {e}")
    
    # 使用检测到的最佳并行数
    max_workers = hardware_info["max_parallel"]
    
    # 降低并行数以提高稳定性
    if hardware_info["encoder_type"] != "software":
        # 硬件编码器最多2个并行，以减少资源争用
        max_workers = min(max_workers, 2)
        logging.info(f"为提高稳定性，硬件编码器并行数调整为: {max_workers}")
    else:
        # 软件编码也限制并行数，以避免CPU过载
        max_workers = min(max_workers, 3)
        logging.info(f"为提高稳定性，软件编码器并行数调整为: {max_workers}")
    
    # 根据硬件类型调整线程池类型
    if hardware_info["encoder_type"] == "software":
        # 软件编码使用进程池以利用多核心CPU
        executor_class = concurrent.futures.ProcessPoolExecutor
    else:
        # 硬件编码使用线程池以共享GPU资源
        executor_class = concurrent.futures.ThreadPoolExecutor
    
    with executor_class(max_workers=max_workers) as executor:
        # 清空终端，为进度条腾出空间
        print("\033[2J\033[H", end="")  # 清屏命令
        
        # 提交所有任务
        futures = []
        for i, (video_path, output_path, roi) in enumerate(zip(video_paths, output_paths, rois)):
            future = executor.submit(
                process_video, 
                video_path, 
                output_path, 
                roi,
                hardware_info,
                i % max_workers,  # 视频在屏幕上的索引位置
                len(video_paths),  # 总视频数量
                target_resolution
            )
            future.add_done_callback(task_done_callback)
            futures.append(future)
        
        # 等待所有任务完成
        concurrent.futures.wait(futures)
    
    # 关闭总进度条
    total_pbar.close()
    
    # 返回处理结果
    success_count = sum(1 for future in futures if future.result())
    logging.info(f"成功处理 {success_count}/{len(video_paths)} 个视频")
    return success_count

if __name__ == '__main__':
    input_dir = r"F:\0411\内地\边水往事"  # 替换为你的视频文件输入目录路径
    output_dir = r"F:\0411\内地out\边水往事"  # 替换为你的视频文件输出目录路径
    target_resolution = (3840, 1608)
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 清理临时目录
    try:
        shutil.rmtree(temp_dir)
        temp_dir.mkdir(exist_ok=True)
    except:
        logging.warning("临时目录清理失败，继续执行...")
    
    # 定义ROI区域（根据实际需求调整）
    roi = (2, 101, 3837, 1472)  # x, y, w, h
    
    # 获取所有视频文件
    video_paths = glob.glob(os.path.join(input_dir, '*.mp4'))
    output_paths = []
    rois = []
    
    for i, video_path in enumerate(video_paths):
        output_path = os.path.join(output_dir, os.path.basename(video_path))
        output_paths.append(output_path)
        rois.append(roi)

    # 检查是否有视频需要处理
    if not video_paths:
        logging.warning(f"在 {input_dir} 中没有找到MP4视频文件")
    else:
        logging.info(f"找到 {len(video_paths)} 个视频文件，开始处理...")
        try:
            # 测试FFmpeg是否可用
            test_cmd = ['ffmpeg', '-version']
            result = subprocess.run(test_cmd, capture_output=True, text=True, encoding='utf-8')
            if result.returncode != 0:
                logging.error("FFmpeg不可用，请确保已正确安装")
                exit(1)
            logging.info("FFmpeg版本: " + result.stdout.split('\n')[0])
            
            # 检测硬件并获取最佳配置
            hardware_info = detect_hardware()
            logging.info(f"将使用编码器: {hardware_info['encoder']} ({hardware_info['encoder_type']})")
            logging.info(f"并行处理数量: {hardware_info['max_parallel']}")
            
            # 处理视频
            success_count = process_videos_in_parallel(
                video_paths, output_paths, rois, hardware_info, target_resolution
            )
            print(f"\n成功处理 {success_count}/{len(video_paths)} 个视频")
        except Exception as e:
            logging.error(f"主程序异常: {e}")

    end_time = time.time()
    # 计算运行时间
    elapsed_time = end_time - start_time
    logging.info(f'处理完成！总耗时: {elapsed_time:.2f}秒')