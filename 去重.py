import os
import hashlib
from logging.handlers import RotatingFileHandler
import imagehash
import cv2
from PIL import Image
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager, Lock
from tqdm import tqdm
import logging
import shutil
import numpy as np

# --- 优化配置参数 ---
VIDEO_EXTENSIONS = ('.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv')
HASH_ALGORITHM = 'sha256'
CHUNK_SIZE = 1024 * 1024  # 1MB
FRAME_INTERVAL = 90
MAX_FRAMES = 3
BATCH_SIZE = 20  # 减小批次大小
NUM_PROCESSES = max(os.cpu_count() - 1, 1)  # 保留一个核心给系统
LOG_DIR = 'logs'
DUPLICATE_DIR = r"E:\5\重复"

# --- 日志配置 ---
LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'duplicate_video_finder.log')

# 创建日志目录
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# 配置日志记录器
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 创建文件处理器
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=10 * 1024 * 1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)

# 创建控制台处理器
console_handler = logging.StreamHandler()

# 创建格式化器
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# 添加处理器到日志记录器
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 确保处理器不会重复
logger.propagate = False


def quick_hash(filepath, sample_size=1024 * 1024):
    """快速计算文件头部哈希"""
    try:
        with open(filepath, 'rb') as f:
            return hashlib.md5(f.read(sample_size)).hexdigest()
    except Exception:
        return None


def calculate_video_signature(filepath):
    """优化的视频特征计算"""
    try:
        cap = cv2.VideoCapture(filepath)
        if not cap.isOpened():
            return None

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total_frames <= 0:
            cap.release()
            return None

        # 只取视频的开始、中间和结尾的帧
        frame_positions = [
            0,
            total_frames // 2,
            max(0, total_frames - 1)
        ]

        signatures = []
        for pos in frame_positions:
            cap.set(cv2.CAP_PROP_POS_FRAMES, pos)
            ret, frame = cap.read()
            if ret:
                # 进一步降低分辨率
                frame = cv2.resize(frame, (16, 16))
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                img = Image.fromarray(gray)
                hash_value = imagehash.average_hash(img)
                signatures.append(str(hash_value))

        cap.release()
        return signatures
    except Exception as e:
        return None


def move_duplicate(file_to_move):
    """移动重复文件"""
    try:
        if not os.path.exists(DUPLICATE_DIR):
            os.makedirs(DUPLICATE_DIR)

        new_filepath = os.path.join(DUPLICATE_DIR, os.path.basename(file_to_move))
        count = 1
        while os.path.exists(new_filepath):
            base, ext = os.path.splitext(os.path.basename(file_to_move))
            new_filepath = os.path.join(DUPLICATE_DIR, f"{base}_{count}{ext}")
            count += 1

        shutil.move(file_to_move, new_filepath)
        logger.info(f"Moved {file_to_move} to {new_filepath}")
        return True
    except Exception as e:
        logger.error(f"Error moving {file_to_move}: {e}")
        return False


def process_file(args):
    """单文件处理函数"""
    filepath, shared_dict = args
    try:
        # 快速检查文件是否存在
        if not os.path.exists(filepath):
            return None

        # 获取文件大小
        file_size = os.path.getsize(filepath)

        # 快速哈希检查
        quick_hash_value = quick_hash(filepath)
        if not quick_hash_value:
            return None

        # 检查是否已经处理过
        if quick_hash_value in shared_dict:
            existing_file = shared_dict[quick_hash_value]

            # 检查文件是否仍然存在
            if not os.path.exists(existing_file):
                shared_dict[quick_hash_value] = filepath
                return None

            # 比较文件大小
            existing_size = os.path.getsize(existing_file)
            if abs(file_size - existing_size) / max(file_size, existing_size) > 0.01:
                return None

            # 计算视频特征
            sig1 = calculate_video_signature(filepath)
            sig2 = calculate_video_signature(existing_file)

            if sig1 and sig2 and len(sig1) == len(sig2):
                differences = sum(s1 != s2 for s1, s2 in zip(sig1, sig2))
                if differences <= 1:  # 允许最多1帧的差异
                    return (filepath, existing_file)
        else:
            shared_dict[quick_hash_value] = filepath

        return None
    except Exception:
        return None


def find_duplicate_videos(directory):
    """优化的主函数"""
    # 使用Manager来共享数据
    with Manager() as manager:
        shared_dict = manager.dict()

        # 收集所有视频文件
        all_files = []
        for root, _, files in os.walk(directory):
            for filename in files:
                if filename.lower().endswith(VIDEO_EXTENSIONS):
                    filepath = os.path.join(root, filename)
                    all_files.append(filepath)

        logger.info(f"Found {len(all_files)} video files.")

        # 创建进程池
        with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
            # 准备任务参数
            tasks = [(f, shared_dict) for f in all_files]

            # 使用tqdm显示进度
            duplicates = []
            with tqdm(total=len(all_files), desc="Processing videos") as pbar:
                # 分批提交任务
                for i in range(0, len(tasks), BATCH_SIZE):
                    batch = tasks[i:i + BATCH_SIZE]
                    futures = [executor.submit(process_file, args) for args in batch]

                    # 处理每个批次的结果
                    for future in futures:
                        try:
                            result = future.result()
                            if result:
                                duplicates.append(result)
                            pbar.update(1)
                        except Exception as e:
                            logger.error(f"Error processing file: {e}")
                            pbar.update(1)

        # 移动重复文件
        moved_count = 0
        for filepath, existing_filepath in duplicates:
            try:
                if move_duplicate(existing_filepath):
                    moved_count += 1
            except Exception as e:
                logger.error(f"Error moving file {existing_filepath}: {e}")

        logger.info(f"Total files processed: {len(all_files)}")
        logger.info(f"Duplicates found and moved: {moved_count}")


if __name__ == "__main__":
    directory_to_search = r"F:\Artlist\10s+Food+slow\合格"

    if not os.path.exists(DUPLICATE_DIR):
        os.makedirs(DUPLICATE_DIR)

    find_duplicate_videos(directory_to_search)
    logger.info("Finished processing all videos.")