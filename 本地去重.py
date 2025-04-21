# -*- coding: utf-8 -*-
"""
Time:     2025/3/12 13:31
Author:   ZhaoQi Cao(czq)
Version:  V 1.0
File:     remove_repeate_video.py
date:     2025/3/12.py
Describe: Write during the python at Tianjin
GitHub link: https://github.com/caozhaoqi
Blog link: https://caozhaoqi.github.io
WeChat Official Account: 码间拾遗（Code Snippets）
Power by macOS on Mac mini m4(2024)
"""
import os
import hashlib
import shutil
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from dbutils.pooled_db import PooledDB  # 使用小写开头的 pooled_db
from tqdm import tqdm
import pymysql  # 使用 pymysql 连接 MySQL
from pymysql.err import IntegrityError, InternalError, OperationalError

# from src.sql.cons import disk_id

# --- 配置参数 ---
VIDEO_EXTENSIONS = ('.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv')
SOURCE_DIR = r"E:\3.11重复\macro"
TARGET_DIR = r"E:\3.11重复\数据库重复"
NUM_THREADS = 8
LOG_FILE = './logs/remove_repeat.log'
LOG_LEVEL = logging.INFO  # 可以根据需要调整为 logging.DEBUG

os.makedirs('./logs',exist_ok=True)
# --- MySQL 数据库配置 ---
DB_CONFIG = {
    "host": "192.168.10.70",
    "user": "root",
    "password": "zq828079",
    "database": "yunjing",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}
# ---连接池配置---
POOL_CONFIG = {
    "mincached": 2,  # 最小空闲连接数
    "maxcached": 5,  # 最大空闲连接数
    "maxconnections": 10,  # 最大连接数
    "blocking": True  # 如果连接池已满, 是否阻塞等待
}

DB_TABLE = "videos"


# --- 日志配置 ---
# 创建一个 logger
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

# 创建一个 handler，用于写入日志文件
fh = logging.FileHandler(LOG_FILE)
fh.setLevel(LOG_LEVEL)

# 再创建一个 handler，用于输出到控制台
ch = logging.StreamHandler()
ch.setLevel(LOG_LEVEL)  # 控制台的日志级别也可以单独设置

# 定义 handler 的输出格式
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# 给 logger 添加 handler
logger.addHandler(fh)
logger.addHandler(ch)



# 使用全局变量pool
pool = None

# --- 函数定义 ---

def calculate_md5(filepath):
    hasher = hashlib.md5()
    try:
        with open(filepath, 'rb') as file:
            while True:
                chunk = file.read(4096)
                if not chunk:
                    break
                hasher.update(chunk)
    except OSError as e:
        logger.error(f"Error reading file {filepath}: {e}")
        return None
    return hasher.hexdigest()

def database_connect():
    """创建数据库连接池"""
    global pool  # 声明使用全局变量
    try:
        pool = PooledDB(pymysql, **DB_CONFIG, **POOL_CONFIG)
        logger.info("Database connection pool created.")
        return pool  # 返回连接池对象 (虽然这里实际上并未使用返回值)
    except pymysql.MySQLError as e:
        logger.error(f"Database connection error: {e}")
        exit(1)

def get_connection():
    """从连接池获取连接"""
    if pool is None:
        database_connect()  # 如果连接池未初始化，则初始化
    return pool.connection()


def file_exists_in_database(cursor, filename, md5):
    """使用参数化查询防止 SQL 注入"""
    sql = f"SELECT 1 FROM {DB_TABLE} WHERE name = %s or md5 = %s"
    cursor.execute(sql, (filename, md5))
    return cursor.fetchone() is not None


def insert_into_database(cursor, filename, md5):
    """使用参数化查询防止 SQL 注入"""
    # 假设 disk_id 在这里是一个已定义的变量或常量
    # disk_id = "your_disk_id"  # 替换为实际的 disk_id
    sql = f"INSERT INTO {DB_TABLE} (name, link, created_at, md5, keywords, datal_id, save_path) VALUES (%s, %s, NOW(), %s, %s, %s, %s)"
    try:
        cursor.execute(sql, (filename, '', md5, '', 83, filename))
        logger.info(f"Inserted into database: {filename}, {md5}")
        return True
    except IntegrityError:  # 文件名已存在 (主键冲突)
        logger.warning(f"Filename already exists in database (skipping): {filename}")
        return False
    except Exception as e:
        logger.error(f"Database insertion error: {e}")
        return False



def process_video_file(filepath):
    """处理单个视频文件，包含重试机制"""
    filename = os.path.basename(filepath)
    md5 = calculate_md5(filepath)
    if md5 is None:
        return 0

    max_retries = 3
    for attempt in range(max_retries):
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                target_filepath = os.path.join(TARGET_DIR, filename)
                if not file_exists_in_database(cursor, filename, md5):
                    if os.path.exists(target_filepath):
                        logger.warning(f"Target file already exists (MD5 collision?): {target_filepath}")
                        return 0
                    try:
                        if insert_into_database(cursor, filename, md5):
                            logger.info(f"insert data: {filename} success!!!")
                            return 1
                        else:
                            return 0
                    except OSError as e:
                        logger.error(f"Error moving file {filepath}: {e}")
                        return 0
                else:
                    shutil.move(filepath, target_filepath)
                    logger.info(f"Moved: {filepath} -> {target_filepath}")
                    logger.info(f"Skipped (already exists): {filename}")
                    return 0

        except (InternalError, OperationalError) as e:
            logger.warning(f"Database error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 指数退避 (等待 1, 2, 4 秒...)
                continue
            else:
                logger.error(f"Max retries reached. Giving up on {filepath}.")
                return 0
        except Exception as e:
            logger.exception(f"An unexpected error: {e}")
            return 0
        finally:
            conn.close()  # 将连接返回到连接池

    return 0  # 如果所有重试都失败，返回 0



def traverse_directory(directory):
    """递归遍历目录，返回所有视频文件路径"""
    video_files = []
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)
        if os.path.isfile(item_path) and item.lower().endswith(VIDEO_EXTENSIONS):
            video_files.append(item_path)
        elif os.path.isdir(item_path):
            video_files.extend(traverse_directory(item_path))
    return video_files


# --- 主程序 ---

def main():
    os.makedirs(TARGET_DIR, exist_ok=True)

    # database_connect()  # 不需要在这里调用，get_connection() 会自动处理
    video_files = traverse_directory(SOURCE_DIR)
    num_files = len(video_files)
    logger.info(f"Found {num_files} video files to process.")

    processed_count = 0

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor, \
            tqdm(total=num_files, desc="Processing", unit="file") as pbar:

        futures = {executor.submit(process_video_file, filepath): filepath
                   for filepath in video_files}

        for future in as_completed(futures):
            filepath = futures[future]
            try:
                result = future.result()
                processed_count += result
                pbar.update(1)
            except Exception as e:
                logger.exception(f"Error processing {filepath}: {e}")
                pbar.update(1)  # 即使出错，也要更新进度条

    logger.info(f"Processed {processed_count} of {num_files} files.")


if __name__ == "__main__":
    main()