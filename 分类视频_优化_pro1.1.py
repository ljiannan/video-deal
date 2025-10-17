import os
import shutil
import logging
import json
import subprocess
import queue
import threading
import time
import signal
from pathlib import Path
from dataclasses import dataclass
from collections import Counter
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# --- 日志设置 ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("video_processing_log.log", mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()


def find_ffprobe(configured_path: str) -> str | None:
    """
    智能查找 ffprobe 可执行文件。
    1. 优先使用用户配置的绝对路径。
    2. 如果路径无效或未配置，则在系统 PATH 中搜索。
    3. 如果都找不到，返回 None。
    """
    if configured_path and os.path.isfile(configured_path):
        logger.info(f"成功找到指定的 ffprobe 路径: {configured_path}")
        return configured_path

    logger.warning("配置的 ffprobe 路径无效或未提供，正在系统环境变量 (PATH) 中搜索...")
    ffprobe_in_path = shutil.which("ffprobe")
    if ffprobe_in_path:
        logger.info(f"在系统 PATH 中找到 ffprobe: {ffprobe_in_path}")
        return ffprobe_in_path

    logger.error("错误: 未能找到 ffprobe。")
    logger.error("解决方案:")
    logger.error("  1. 在下方的 Config 类中正确设置 FFPROBE_PATH 的绝对路径。")
    logger.error("  2. 或者，确保 FFmpeg 已安装并将其 bin 目录添加到了系统环境变量 PATH 中。")
    return None


def check_resolution(video_path: Path, ffprobe_executable: str):
    """
    使用 ffprobe 检查视频分辨率，返回宽度、高度以及朝向（横屏/竖屏）。
    """
    try:
        command = [
            ffprobe_executable,
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            str(video_path)
        ]
        result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
        metadata = json.loads(result.stdout)

        video_stream = next((s for s in metadata.get("streams", []) if s.get("codec_type") == "video"), None)

        if not video_stream:
            logger.warning(f"在 '{video_path.name}' 中未找到视频流。")
            return None, None, None

        width = int(video_stream.get("width", 0))
        height = int(video_stream.get("height", 0))

        if width == 0 or height == 0:
            logger.warning(f"视频分辨率为零: {video_path.name}")
            return None, None, None

        orientation = "vertical" if width < height else "horizontal"
        return width, height, orientation

    except subprocess.CalledProcessError as e:
        logger.error(f"ffprobe 在处理 '{video_path.name}' 时出错 (返回码 {e.returncode}): {e.stderr.strip()}")
        return None, None, None
    except json.JSONDecodeError:
        logger.error(f"解析 ffprobe 输出时出错: {video_path.name}")
        return None, None, None
    except Exception as e:
        logger.error(f"读取 '{video_path.name}' 时发生未知错误: {e}")
        return None, None, None


def move_and_update_stats(video_path: Path, dest_folder: Path, category_key: str, stats: Counter, lock: threading.Lock):
    """
    一个原子操作：移动文件并线程安全地更新统计计数器。
    新增：如果目标文件已存在，则重命名。
    """
    try:
        dest_folder.mkdir(parents=True, exist_ok=True)
        dest_path = dest_folder / video_path.name

        # 如果目标文件已存在，则查找新的唯一文件名
        counter = 1
        while dest_path.exists():
            dest_path = dest_folder / f"{video_path.stem} ({counter}){video_path.suffix}"
            counter += 1

        shutil.move(str(video_path), str(dest_path))
        with lock:
            stats[category_key] += 1
        logger.info(f"已移动: {video_path.name} -> {dest_path.name}")
    except Exception as e:
        logger.error(f"移动文件 '{video_path.name}' 失败: {e}")
        with lock:
            stats['failed_to_move'] += 1


def process_video(video_path: Path, output_dirs: dict, stats: Counter, lock: threading.Lock, ffprobe_executable: str):
    """
    处理单个视频文件，并根据新的分辨率标准进行分类。
    """
    width, height, orientation = check_resolution(video_path, ffprobe_executable)

    if width is None:
        with lock:
            stats['unreadable_files'] += 1
        return

    if min(width, height) < 1080:
        move_and_update_stats(video_path, Path(output_dirs["invalid"]), "invalid", stats, lock)
        return

    category = None
    if orientation == "horizontal":
        if width == 7680 and height == 4320: category = "horizontal_8k"
        elif width == 3840 and height == 2160: category = "horizontal_4k"
        elif (width == 2560 and height == 1440) or (width == 2160 and height == 1440): category = "horizontal_2k"
        elif width == 1920 and height == 1080: category = "horizontal_1k"
        else: category = "horizontal_other"
    elif orientation == "vertical":
        if width == 4320 and height == 7680: category = "vertical_8k"
        elif width == 2160 and height == 3840: category = "vertical_4k"
        elif (width == 1440 and height == 2560) or (width == 1440 and height == 2160): category = "vertical_2k"
        elif width == 1080 and height == 1920: category = "vertical_1k"
        else: category = "vertical_other"

    if category:
        move_and_update_stats(video_path, Path(output_dirs[category]), category, stats, lock)


def print_summary(stats: Counter):
    """
    打印一份清晰的处理结果摘要。
    """
    logger.info("=" * 60)
    logger.info("最终处理结果摘要:")
    logger.info("=" * 60)

    total_processed = sum(stats.values()) - stats['failed_to_move'] - stats['unreadable_files'] - stats['skipped_unstable']

    summary_lines = [
        "\n--- 分类结果统计 ---\n",
        "合格视频 (横屏):",
        f"  - 7680x4320 (8K):      {stats['horizontal_8k']} 个",
        f"  - 3840x2160 (4K):      {stats['horizontal_4k']} 个",
        f"  - 2560x1440/2160x1440 (2K): {stats['horizontal_2k']} 个",
        f"  - 1920x1080 (1K):      {stats['horizontal_1k']} 个",
        f"  - 其他分辨率:           {stats['horizontal_other']} 个",
        "\n合格视频 (竖屏):",
        f"  - 4320x7680 (8K):      {stats['vertical_8k']} 个",
        f"  - 2160x3840 (4K):      {stats['vertical_4k']} 个",
        f"  - 1440x2560/1440x2160 (2K): {stats['vertical_2k']} 个",
        f"  - 1080x1920 (1K):      {stats['vertical_1k']} 个",
        f"  - 其他分辨率:           {stats['vertical_other']} 个",
        "\n" + "-" * 23 + "\n",
        f"不合格视频 (短边<1080p): {stats['invalid']} 个",
        "\n--- 处理状态 ---\n",
        f"成功分类文件: {total_processed} 个",
        f"无法读取文件: {stats['unreadable_files']} 个 (可能已损坏或无视频流)",
        f"移动失败文件: {stats['failed_to_move']} 个",
        f"因不稳定跳过: {stats['skipped_unstable']} 个 (文件可能仍在写入中)",
    ]
    print("\n".join(summary_lines))
    logger.info("=" * 60)


def is_file_stable(file_path: Path, stability_period_secs: int = 5) -> bool:
    """
    通过检查文件大小在一段时间内是否保持不变来判断文件是否稳定（即已停止写入）。
    """
    if not file_path.is_file():
        return False

    try:
        last_size = file_path.stat().st_size
        time.sleep(stability_period_secs)
        current_size = file_path.stat().st_size

        if last_size == current_size and last_size > 0:
            logger.debug(f"文件 '{file_path.name}' 大小稳定，判定为已完成。")
            return True
        else:
            logger.debug(f"文件 '{file_path.name}' 仍在写入中或为空。")
            return False
    except FileNotFoundError:
        logger.warning(f"检查稳定性时文件 '{file_path.name}' 已消失。")
        return False
    except PermissionError:
        logger.debug(f"文件 '{file_path.name}' 暂时无法访问（可能被占用），判定为不稳定。")
        return False
    except Exception as e:
        logger.error(f"检查文件 '{file_path.name}' 稳定性时出错: {e}")
        return False


class VideoHandler(PatternMatchingEventHandler):
    """
    监视文件系统事件，并将符合条件的视频文件路径添加到处理队列中。
    """
    def __init__(self, path_queue: queue.Queue, processed_files: set):
        video_extensions = ["*.mp4", "*.mov", "*.avi", "*.mkv", "*.m4v", "*.webm"]
        super().__init__(
            patterns=video_extensions,
            ignore_patterns=[],
            ignore_directories=True,
            case_sensitive=False,
        )
        self.queue = path_queue
        self.processed_files = processed_files

    def _add_to_queue(self, event_path: str):
        """检查文件是否已在处理，如果没有则添加到队列。"""
        file_path = Path(event_path)
        if file_path not in self.processed_files:
            logger.info(f"事件触发: 检测到文件 '{file_path.name}'，已加入处理队列。")
            self.processed_files.add(file_path)  # 立即添加以防重复
            self.queue.put(file_path)

    def on_created(self, event):
        if event.is_directory: return
        self._add_to_queue(event.src_path)

    def on_modified(self, event):
        if event.is_directory: return
        self._add_to_queue(event.src_path)


class VideoProcessor:
    """
    主处理器，管理文件扫描、监控、处理队列和工作线程。
    """
    def __init__(self, config):
        self.config = config
        self.input_dir = Path(config.INPUT_DIR)
        self.output_dirs = self._generate_output_dirs()
        self.ffprobe_executable = find_ffprobe(config.FFPROBE_PATH)

        self.queue = queue.Queue()
        self.processed_files = set()
        self.stats = Counter()
        self.stats_lock = threading.Lock()
        self.shutdown_event = threading.Event()

    def _generate_output_dirs(self) -> dict:
        """根据基础输出目录自动生成所有分类子目录"""
        base = self.config.BASE_OUTPUT_DIR
        dirs = {
            "invalid": os.path.join(base, "分辨率不合格"),
            "horizontal_1k": os.path.join(base, "分辨率合格", "横屏", "1K"),
            "horizontal_2k": os.path.join(base, "分辨率合格", "横屏", "2K"),
            "horizontal_4k": os.path.join(base, "分辨率合格", "横屏", "4K"),
            "horizontal_8k": os.path.join(base, "分辨率合格", "横屏", "8K"),
            "horizontal_other": os.path.join(base, "分辨率合格", "横屏", "其他分辨率"),
            "vertical_1k": os.path.join(base, "分辨率合格", "竖屏", "1K"),
            "vertical_2k": os.path.join(base, "分辨率合格", "竖屏", "2K"),
            "vertical_4k": os.path.join(base, "分辨率合格", "竖屏", "4K"),
            "vertical_8k": os.path.join(base, "分辨率合格", "竖屏", "8K"),
            "vertical_other": os.path.join(base, "分辨率合格", "竖屏", "其他分辨率"),
        }
        for path in dirs.values():
            os.makedirs(path, exist_ok=True)
        return dirs

    def initial_scan(self):
        """对目录进行初次扫描，将已存在的文件加入队列。"""
        logger.info(f"开始对目录进行初始扫描: {self.input_dir}")
        video_extensions = ["*.mp4", "*.mov", "*.avi", "*.mkv", "*.m4v", "*.webm"]
        found_count = 0
        for ext in video_extensions:
            for file_path in self.input_dir.rglob(ext):
                if file_path.is_file() and file_path not in self.processed_files:
                    self.processed_files.add(file_path)
                    self.queue.put(file_path)
                    found_count += 1
        logger.info(f"初始扫描完成，{found_count} 个文件已加入队列。")

    def worker(self, worker_id: int):
        """工作线程，从队列中获取文件并处理。"""
        logger.info(f"工作线程 #{worker_id} 已启动。")
        while not self.shutdown_event.is_set():
            try:
                video_path = self.queue.get(timeout=1)  # 从队列获取任务
                try:
                    logger.info(f"[线程 #{worker_id}] 开始处理: {video_path.name}")

                    if not is_file_stable(video_path, self.config.STABILITY_PERIOD_SECS):
                        logger.warning(f"文件 '{video_path.name}' 大小不稳定，将等待下一次事件触发重试。")
                        self.processed_files.remove(video_path)
                        with self.stats_lock:
                            self.stats['skipped_unstable'] += 1
                        continue

                    process_video(video_path, self.output_dirs, self.stats, self.stats_lock, self.ffprobe_executable)
                finally:
                    self.queue.task_done()  # 确保任务完成信号总是被发送
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"[线程 #{worker_id}] 工作循环中发生未知错误: {e}", exc_info=True)

        logger.info(f"工作线程 #{worker_id} 已关闭。")

    def start(self):
        """启动所有处理流程。"""
        if not self.ffprobe_executable:
            return

        threads = []
        for i in range(self.config.NUM_WORKERS):
            thread = threading.Thread(target=self.worker, args=(i + 1,))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        self.initial_scan()

        observer = None
        if self.config.ENABLE_MONITOR:
            logger.info("=" * 60)
            logger.info(f"开始实时监控目录: {self.input_dir}")
            logger.info("按 Ctrl+C 停止脚本...")
            logger.info("=" * 60)
            event_handler = VideoHandler(self.queue, self.processed_files)
            observer = Observer()
            observer.schedule(event_handler, str(self.input_dir), recursive=True)
            observer.start()

        # 注册信号处理器
        def signal_handler(signum, frame):
            if not self.shutdown_event.is_set():
                logger.warning("\n接收到中断信号，正在准备关闭...")
                self.shutdown(observer, threads)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # 主线程阻塞于此，直到shutdown_event被设置
        self.shutdown_event.wait()

    def shutdown(self, observer, threads):
        """执行优雅的关闭流程。"""
        if self.shutdown_event.is_set():
            return # 防止重复关闭

        if observer and observer.is_alive():
            logger.info("正在停止文件监控...")
            observer.stop()
            observer.join()

        logger.info("正在等待队列中的剩余任务处理完成...")
        self.queue.join()

        logger.info("正在关闭工作线程...")
        self.shutdown_event.set() # 确保所有线程都能收到关闭信号
        for thread in threads:
            thread.join()

        logger.info("=" * 60)
        logger.info("所有任务处理完毕，脚本已安全退出。")
        print_summary(self.stats)


@dataclass(frozen=True)
class Config:
    """脚本的所有配置项"""
    # --- 路径配置 ---
    INPUT_DIR: str = r"D:\航拍原始数据"
    BASE_OUTPUT_DIR: str = r"D:\一体化原始数据分视频out\已分完"
    FFPROBE_PATH: str = r"C:\ffmpeg-master-latest-win64-gpl\bin\ffprobe.exe"

    # --- 功能开关 ---
    ENABLE_MONITOR: bool = True  # 是否在初次扫描后启用实时监控

    # --- 性能配置 ---
    NUM_WORKERS: int = 4  # 并行处理视频的工作线程数
    STABILITY_PERIOD_SECS: int = 5  # 文件稳定期（秒），用于判断文件是否写入完成


def main():
    """主函数，初始化并运行处理器"""
    config = Config()
    logger.info("视频分类脚本启动...")
    processor = VideoProcessor(config)
    processor.start()


if __name__ == "__main__":
    main()