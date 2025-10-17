#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/9/11 17:46
# @Author  : CUI liuliu (Refactored by Gemini)
# @File    : douyu_videoDown_CLI_ytdlp_db_cn_aria2c.py

import os
import sys
import time
import socket
import shutil

# ==============================================================================
# --- 全局配置区 ---
# 您所有的配置都在这里修改
# ==============================================================================

# 1. 数据库与路径配置
# ------------------------------------------------------------------------------
MYSQL_CONFIG = {
    'host': '192.168.10.70',
    'user': 'root',
    'password': 'zq828079',
    'database': 'huya_video_tasks'
}
DB_TABLE_NAME = 'douyu_video_download'
DOWNLOAD_DIRECTORY = r"C:\douyu_400t"
FFMPEG_PATH = r"C:\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe"

# 2. 下载身份标识
# ------------------------------------------------------------------------------
DOWNLOADER_NAME = "ALL"
#MACHINE_NAME = "d138"
# 提示: 如果想自动获取当前电脑名，可以使用下面这行代码替换上一行
MACHINE_NAME = socket.gethostname()

# 3. yt-dlp 内置下载器性能配置 (当不使用aria2c时生效)
# ------------------------------------------------------------------------------
YT_DLP_CONCURRENT_FRAGMENTS = 16
YT_DLP_FRAGMENT_RETRIES = 10
YT_DLP_NO_OVERWRITES = True

# 4. aria2c 外部下载器配置 (推荐，速度更快)
# ------------------------------------------------------------------------------
# 填入 aria2c.exe 的完整路径。如果留空 ("")，则会使用上面的 yt-dlp 内置下载器。
ARIA2C_PATH = r"aria2-1.37.0-win-64bit-build1\aria2c.exe"

# aria2c 的命令行参数。通常无需修改。
# -c: 断点续传, -x 16: 单服务器最大连接数, -s 16: 单任务连接数, -k 1M: 最小分片大小
ARIA2C_ARGS = "-c -x 16 -s 16 -k 1M --console-log-level=warn --summary-interval=0"

# 5. 存储空间检测配置
# ------------------------------------------------------------------------------
# 当可用空间低于此值 (GB) 时，程序将自动停止，以防止磁盘写满。
# 设置为 0 可禁用此功能。
# 【代码优化】根据您的要求，将阈值从 5GB 修改为 10GB。
MIN_FREE_SPACE_GB = 200

# ==============================================================================
# --- 程序核心代码 ---
# (通常无需修改以下内容)
# ==============================================================================

# 尝试导入 yt-dlp，如果失败则提示用户安装
try:
    import yt_dlp
    from yt_dlp.utils import DownloadError
except ImportError:
    print(
        "错误：未找到 yt-dlp 库。\n"
        "请在命令行中运行 'pip install yt-dlp' 进行安装。"
    )
    sys.exit(1)

# 尝试导入 mysql.connector，如果失败则提示用户安装
try:
    import mysql.connector
except ImportError:
    print(
        "错误：未找到 mysql-connector-python 库。\n"
        "请在命令行中运行 'pip install mysql-connector-python' 进行安装。"
    )
    sys.exit(1)


class YtDlpLogger:
    """
    自定义日志记录器，用于格式化 yt-dlp 的控制台输出。
    """

    def debug(self, msg):
        if msg.startswith('[debug] '):
            pass
        elif msg.startswith('[Merger]'):
            self.info(msg)

    def info(self, msg):
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] [信息] {msg}")

    def warning(self, msg):
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] [警告] {msg}")

    def error(self, msg):
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] [错误] {msg}")


def progress_hook(d):
    """
    yt-dlp 的进度回调函数，用于在单行显示下载状态。
    """
    if d['status'] == 'downloading':
        percent = d.get('_percent_str', 'N/A').strip()
        speed = d.get('_speed_str', 'N/A').strip()
        eta = d.get('_eta_str', 'N/A').strip()
        sys.stdout.write(f"\r\t下载中: {percent} | 速度: {speed} | 剩余时间: {eta}   ")
        sys.stdout.flush()
    elif d['status'] == 'finished':
        print(f"\n\t[成功] 文件 '{os.path.basename(d['filename'])}' 下载完成。")


def check_disk_space(path, min_gb):
    """
    检查指定路径的可用磁盘空间。
    返回一个元组 (bool, float)，表示是否有足够空间以及剩余空间(GB)。
    """
    if min_gb <= 0:
        return True, float('inf')

    try:
        total, used, free = shutil.disk_usage(path)
        free_gb = free / (1024 ** 3)

        if free_gb < min_gb:
            # 【代码优化】调整了日志输出，使其更清晰
            print(f"\n[磁盘空间不足] 目录 '{path}' 剩余空间 {free_gb:.2f} GB，低于设定的阈值 {min_gb} GB。")
            return False, free_gb
        else:
            print(f"[磁盘空间检查] 目录 '{path}' 剩余 {free_gb:.2f} GB，空间充足。")
            return True, free_gb
    except FileNotFoundError:
        print(f"\n[错误] 检查磁盘空间失败：目录 '{path}' 不存在。")
        return False, 0
    except Exception as e:
        print(f"\n[错误] 检查磁盘空间时发生未知错误: {e}")
        return False, 0


def download_videos_from_db(db_config, table_name, download_path, ffmpeg_location, downloader_name, machine_name):
    """
    从 MySQL 数据库中获取 URL 并使用 yt-dlp 进行下载。
    """
    os.makedirs(download_path, exist_ok=True)
    print(f"下载路径已设置为: {os.path.abspath(download_path)}")

    ydl_opts = {
        'outtmpl': os.path.join(download_path, '%(title)s [%(id)s].%(ext)s'),
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'merge_output_format': 'mp4',
        'ffmpeg_location': ffmpeg_location,
        'progress_hooks': [progress_hook],
        'logger': YtDlpLogger(),
        'noprogress': True,
        'noplaylist': True,
        'no_overwrites': YT_DLP_NO_OVERWRITES,
    }

    if ARIA2C_PATH and os.path.exists(ARIA2C_PATH):
        print("检测到 aria2c 配置，将使用 aria2c 进行高速下载。")
        ydl_opts['downloader'] = ARIA2C_PATH
        ydl_opts['downloader_args'] = ARIA2C_ARGS.split()
    else:
        print("未配置或未找到 aria2c，将使用 yt-dlp 内置下载器。")
        ydl_opts['concurrent_fragments'] = YT_DLP_CONCURRENT_FRAGMENTS
        ydl_opts['fragment_retries'] = YT_DLP_FRAGMENT_RETRIES

    while True:
        conn = None
        cursor = None
        task = None

        try:
            # --- 下载前检查磁盘空间 (保留此项，防止启动时空间已不足) ---
            has_enough_space, _ = check_disk_space(DOWNLOAD_DIRECTORY, MIN_FREE_SPACE_GB)
            if not has_enough_space:
                sys.exit("程序启动时磁盘空间已不足，请清理磁盘后重启。")
            # --- 检查结束 ---

            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor(dictionary=True)

            cursor.execute(f"START TRANSACTION;")
            cursor.execute(f"SELECT id, url FROM `{table_name}` WHERE status = 0 LIMIT 1 FOR UPDATE;")
            task = cursor.fetchone()

            if task:
                task_id = task['id']
                url_or_id = task['url']

                cursor.execute(
                    f"UPDATE `{table_name}` SET status = 2, downloader = %s, machine_name = %s WHERE id = %s;",
                    (downloader_name, machine_name, task_id)
                )
                conn.commit()

                if not url_or_id.startswith(('http://', 'https://')):
                    url = f"https://v.douyu.com/show/{url_or_id}"
                else:
                    url = url_or_id

                print(f"\n[任务ID: {task_id}] 开始下载: {url}")

                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        ydl.download([url])

                    # 标记任务为成功
                    cursor.execute(f"UPDATE `{table_name}` SET status = 1 WHERE id = %s;", (task_id,))
                    conn.commit()
                    print(f"\t[成功] 任务 {task_id} 已完成。")

                    # --- 【代码新增】下载完成后，立即检查磁盘空间 ---
                    has_enough_space_after, free_gb_after = check_disk_space(DOWNLOAD_DIRECTORY, MIN_FREE_SPACE_GB)
                    if not has_enough_space_after:
                        print(f"\n[停止] 视频下载后剩余空间 ({free_gb_after:.2f} GB) 已低于阈值 ({MIN_FREE_SPACE_GB} GB)。")
                        sys.exit("程序因磁盘空间不足而自动停止，请清理磁盘后重启。")
                    # --- 检查结束 ---

                except DownloadError as net_err:
                    print(f"\n\t[网络错误] 下载 {url} 时发生网络或源问题: {net_err}")
                    cursor.execute(f"UPDATE `{table_name}` SET status = 4 WHERE id = %s;", (task_id,))
                    conn.commit()
                except Exception as e:
                    print(f"\n\t[致命错误] 下载 {url} 时发生意外错误: {e}")
                    cursor.execute(f"UPDATE `{table_name}` SET status = 3 WHERE id = %s;", (task_id,))
                    conn.commit()
            else:
                print("\n没有待处理的下载任务。等待 60 秒后重试...")
                conn.commit()
                time.sleep(60)

        except mysql.connector.Error as err:
            print(f"\n[数据库错误] {err}")
            if conn and conn.is_connected():
                conn.rollback()
            time.sleep(30)
        finally:
            if cursor:
                cursor.close()
            if conn and conn.is_connected():
                conn.close()


if __name__ == "__main__":
    download_videos_from_db(
        db_config=MYSQL_CONFIG,
        table_name=DB_TABLE_NAME,
        download_path=DOWNLOAD_DIRECTORY,
        ffmpeg_location=FFMPEG_PATH,
        downloader_name=DOWNLOADER_NAME,
        machine_name=MACHINE_NAME
    )