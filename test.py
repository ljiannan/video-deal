import os
import shutil
import subprocess
import time
import logging
from datetime import datetime, time as dt_time
from pathlib import Path
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import signal


# ==============================================================================
# 配置类 - 请根据您的环境修改此区域的所有值
# ==============================================================================
@dataclass(frozen=True)
class Config:
    """脚本的所有配置项"""
    # --- 路径配置 ---
    SOURCE_DIR: Path = Path(r"E:\自然")  # 源文件夹：存放待移动视频的文件夹
    DEST_DIR: Path = Path(r"Z:\a项目\航拍特写\李建楠\9.22\自然")  # 目标文件夹：视频最终移动到的文件夹
    FFMPEG_PATH: Path = Path(r"D:\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe")
    FFPROBE_PATH: Path = Path(r"D:\ffmpeg-master-latest-win64-gpl\bin\ffprobe.exe")  # ffprobe路径，通常与ffmpeg在一起
    LOG_FILE: Path = Path("video_move_log.log")

    # --- 规则配置 ---
    VIDEO_EXTENSIONS: frozenset = frozenset({'.mp4', '.mov', '.avi', '.mkv', '.flv', '.wmv'})  # 允许的视频文件扩展名（不区分大小写）
    TRIGGER_SIZE_GB: float = 12.0  # 触发移动任务的源文件夹最小体积（单位：GB）

    # --- 时间窗口配置 (小时, 分钟) ---
    # 晚上8点
    TIME_WINDOW_START: tuple[int, int] = (15, 0)  # 在此时间之后，忽略文件夹大小限制
    # 上午9点
    TIME_WINDOW_END: tuple[int, int] = (10, 0)  # 在此时间之前，忽略文件夹大小限制

    # --- 重试与延迟配置 ---
    MAX_RETRIES: int = 3  # 文件验证或移动失败时的最大重试次数
    RETRY_DELAY_SECONDS: int = 5  # 每次重试之间的等待时间（秒）

    # --- 性能配置 ---
    MAX_WORKERS: int = 12  # 并行处理文件的最大线程数，可根据CPU和磁盘性能调整


# ==============================================================================
# 全局变量 - 请勿修改
# ==============================================================================
# 用于优雅地处理中断信号 (Ctrl+C)
shutdown_requested = False


# ==============================================================================
# 脚本核心功能
# ==============================================================================

def setup_logging(log_file: Path):
    """配置日志记录器，同时输出到控制台和文件。"""
    # 获取根日志记录器
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # 如果已经有处理器，先移除，防止重复添加
    if logger.hasHandlers():
        logger.handlers.clear()

    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # 文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 控制台处理器
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


def handle_shutdown_signal(signum, frame):
    """处理Ctrl+C等中断信号，实现优雅退出。"""
    global shutdown_requested
    if not shutdown_requested:
        logging.warning("接收到中断信号！将取消未开始的任务并等待当前任务完成...")
        shutdown_requested = True
    else:
        logging.error("强制退出！")
        exit(1)


def get_folder_size_gb(folder_path: Path) -> float:
    """计算文件夹的总大小并返回GB为单位的数值。"""
    total_size = 0
    try:
        for fp in folder_path.rglob('*'):
            if fp.is_file():
                try:
                    total_size += fp.stat().st_size
                except FileNotFoundError:
                    # 文件在遍历时被删除，记录警告并继续
                    logging.warning(f"计算大小时文件已不存在，跳过: {fp}")
                    continue
    except FileNotFoundError:
        logging.error(f"路径不存在，无法计算大小: {folder_path}")
        return 0
    return total_size / (1024 ** 3)


def is_file_stable(file_path: Path, check_delay_seconds: int = 2) -> bool:
    """
    检查文件大小在一段时间内是否稳定，以判断是否已停止写入。
    :param file_path: 要检查的文件路径。
    :param check_delay_seconds: 检查总时长（秒）。
    :return: 如果文件大小稳定则返回 True，否则返回 False。
    """
    # 增加一个更短的、多次的检查机制，以更快地判断文件是否在写入
    # 总共等待 check_delay_seconds，但每0.5秒检查一次
    checks = max(1, int(check_delay_seconds / 0.5))
    last_size = -1

    try:
        last_size = file_path.stat().st_size
        if last_size == 0:
            return False  # 0字节文件直接认为不稳定

        for _ in range(checks):
            time.sleep(0.5)
            current_size = file_path.stat().st_size
            if current_size != last_size:
                return False  # 文件大小发生变化，不稳定
            last_size = current_size
        return True  # 在检查周期内，文件大小保持不变
    except FileNotFoundError:
        # 如果在检查期间文件被删除，则认为它不稳定
        return False


def is_time_in_unrestricted_window(start_time, end_time) -> bool:
    """检查当前时间是否在不受大小限制的时间窗口内。"""
    now = datetime.now().time()

    # 跨午夜的情况 (例如: 20:00 - 09:00)
    if start_time > end_time:
        return now >= start_time or now < end_time
    # 当天内的情况
    return start_time <= now < end_time


def is_valid_video(file_path: Path, config: Config) -> bool:
    """使用ffprobe快速检测文件是否为可读的视频文件，并加入重试机制。"""
    command = [
        str(config.FFPROBE_PATH),
        '-v', 'error',          # 只在发生错误时输出
        '-show_format',        # 请求显示格式信息
        '-show_streams',       # 请求显示流信息
        str(file_path)
    ]

    for attempt in range(config.MAX_RETRIES):
        try:
            result = subprocess.run(command, check=False, capture_output=True, text=True, encoding='utf-8')
            if result.returncode == 0:
                return True
            else:
                logging.warning(f"ffprobe验证失败 (尝试 {attempt + 1}/{config.MAX_RETRIES}) for {file_path.name}. "
                                f"错误: {result.stderr.strip()}")
        except Exception as e:
            logging.error(f"执行ffprobe时发生异常 (尝试 {attempt + 1}/{config.MAX_RETRIES}) for {file_path.name}: {e}")

        if attempt < config.MAX_RETRIES - 1:
            time.sleep(config.RETRY_DELAY_SECONDS)

    logging.error(f"ffprobe在 {config.MAX_RETRIES} 次尝试后仍无法验证文件: {file_path.name}")
    return False


def move_with_progress(src: Path, dst: Path, pbar_manager):
    """
    通过“先复制再删除”的方式移动文件，并显示进度条。
    此方法用于跨驱动器移动。
    """
    file_size = src.stat().st_size
    file_name = src.name

    # 使用主进度条管理器创建用于单个文件传输的嵌套进度条
    with pbar_manager.sub_bar(
            total=file_size,
            desc=f"移动中 {file_name[:15]}..",
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            leave=False  # 完成后隐藏此进度条
    ) as file_pbar:
        with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
            while chunk := fsrc.read(4 * 1024 * 1024):  # 使用4MB的块大小
                fdst.write(chunk)
                file_pbar.update(len(chunk))

    # 最终验证：确保目标文件大小与源文件一致
    dst_size = dst.stat().st_size
    if file_size != dst_size:
        raise IOError(f"移动后文件大小不一致！源: {file_size}, 目标: {dst_size}")

    shutil.copystat(src, dst)  # 复制元数据（如创建/修改时间）
    os.remove(src)  # 安全删除源文件


def process_single_file_for_move(src_path: Path, config: Config, pbar_manager) -> str | None:
    """
    仅执行移动操作。设计为在验证阶段之后被调用。
    :param src_path: 源文件路径。
    :param config: 配置对象。
    :param pbar_manager: 进度条管理器。
    :return: 一个描述处理结果的字符串，或在成功时返回None。
    """
    dst_path = config.DEST_DIR / src_path.name
    if dst_path.exists():
        return f"跳过: {src_path.name} (目标已存在同名文件)"

    # 移动操作的重试逻辑
    for attempt in range(config.MAX_RETRIES):
        try:
            # 检查是否为跨驱动器移动，以决定是否需要显示进度
            if os.stat(src_path).st_dev != os.stat(dst_path.parent).st_dev:
                move_with_progress(src_path, dst_path, pbar_manager)
            else:
                # 同驱动器内移动是瞬间完成的
                shutil.move(src_path, dst_path)

            logging.info(f"成功移动: {src_path.name} -> {dst_path}")
            return None  # 成功移动，返回None
        except Exception as e:
            logging.error(f"移动文件失败 (尝试 {attempt + 1}/{config.MAX_RETRIES}) for {src_path.name}: {e}")
            if attempt < config.MAX_RETRIES - 1:
                time.sleep(config.RETRY_DELAY_SECONDS)

    return f"失败: {src_path.name} (移动操作在 {config.MAX_RETRIES} 次尝试后彻底失败)"


def main():
    """主函数，执行所有逻辑。"""
    config = Config()
    setup_logging(config.LOG_FILE)
    logging.info("视频文件安全移动脚本已启动。")

    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    start_time = dt_time(config.TIME_WINDOW_START[0], config.TIME_WINDOW_START[1])
    end_time = dt_time(config.TIME_WINDOW_END[0], config.TIME_WINDOW_END[1])

    if not config.SOURCE_DIR.is_dir():
        logging.error(f"源文件夹不存在: {config.SOURCE_DIR}")
        return
    config.DEST_DIR.mkdir(parents=True, exist_ok=True)
    if not config.FFMPEG_PATH.is_file():
        logging.error(f"FFmpeg未在指定路径找到: {config.FFMPEG_PATH}")
        return
    if not config.FFPROBE_PATH.is_file():
        logging.error(f"ffprobe未在指定路径找到: {config.FFPROBE_PATH}")
        return

    while not shutdown_requested:
        try:
            folder_size = get_folder_size_gb(config.SOURCE_DIR)
            unrestricted_time = is_time_in_unrestricted_window(start_time, end_time)
            logging.info(
                f"当前源文件夹大小: {folder_size:.2f} GB. 触发阈值: {config.TRIGGER_SIZE_GB} GB. 是否在无限制时间段: {unrestricted_time}.")

            should_move = folder_size >= config.TRIGGER_SIZE_GB or unrestricted_time
            if not should_move:
                logging.info("条件未满足，将在60秒后重新检查...")
                time.sleep(60)
                continue

            files_to_move = [
                f for f in config.SOURCE_DIR.rglob('*')
                if f.is_file() and f.suffix.lower() in config.VIDEO_EXTENSIONS
            ]

            if not files_to_move:
                logging.info("源文件夹中没有找到视频文件。将在60秒后重新检查...")
                time.sleep(60)
                continue

            logging.info(f"条件满足，发现 {len(files_to_move)} 个视频文件待处理。")

            # --- 阶段一：并行验证文件 ---
            logging.info("阶段一：开始并行验证所有文件...")
            valid_files_to_move = []
            with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
                # 提交验证任务
                future_to_path = {executor.submit(is_file_stable, path): path for path in files_to_move}

                with tqdm(as_completed(future_to_path), total=len(files_to_move), desc="验证稳定性", unit="个") as pbar:
                    for future in pbar:
                        path = future_to_path[future]
                        try:
                            if future.result():
                                # 稳定后，再验证视频格式
                                if is_valid_video(path, config):
                                    valid_files_to_move.append(path)
                                else:
                                    logging.info(f"跳过: {path.name} (ffprobe验证失败)")
                            else:
                                logging.info(f"跳过: {path.name} (文件大小不稳定或为0)")
                        except Exception as e:
                            logging.error(f"验证 {path.name} 时发生错误: {e}")

            if shutdown_requested:
                logging.warning("操作被中断，跳过移动阶段。")
                continue

            if not valid_files_to_move:
                logging.info("没有找到验证通过且可移动的文件。")
            else:
                logging.info(f"阶段二：验证通过 {len(valid_files_to_move)} 个文件，开始并行移动...")

                # --- 阶段二：并行移动已验证的文件 ---
                class TqdmManager:
                    def __init__(self, main_pbar):
                        self.main_pbar = main_pbar
                    def sub_bar(self, *args, **kwargs):
                        kwargs['position'] = self.main_pbar.pos + 1
                        return tqdm(*args, **kwargs)
                    def update_main(self, n=1):
                        self.main_pbar.update(n)

                with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
                    with tqdm(total=len(valid_files_to_move), desc="总体移动进度", unit="个文件", position=0) as main_pbar:
                        pbar_manager = TqdmManager(main_pbar)

                        future_to_path = {
                            executor.submit(process_single_file_for_move, path, config, pbar_manager): path
                            for path in valid_files_to_move
                        }

                        for future in as_completed(future_to_path):
                            if shutdown_requested:
                                for f in future_to_path:
                                    if not f.done(): f.cancel()
                                break

                            try:
                                result = future.result()
                                if result:
                                    logging.info(result)
                            except Exception as e:
                                path = future_to_path[future]
                                logging.error(f"处理移动任务 {path.name} 时发生严重错误: {e}")
                            pbar_manager.update_main()

            if not shutdown_requested:
                logging.info("本轮处理完成，将在60秒后开始新一轮检查...")
                time.sleep(60)

        except Exception as e:
            logging.critical(f"主循环发生严重错误: {e}", exc_info=True)
            if not shutdown_requested:
                logging.info("发生错误，将在5分钟后重试...")
                time.sleep(300)

    logging.info("脚本已安全停止。")


if __name__ == "__main__":
    main()