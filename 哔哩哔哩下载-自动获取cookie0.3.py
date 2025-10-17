#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/10/10 18:55
# @Author  : CUI liuliu
# @File    : 哔哩哔哩下载-自动获取cookie0.3.py
# 文件名: bilibili_downloader_mysql_v5.2.3_dynamic_cookie.py
# 版本: v5.2.4 (支持collector字段)

# --- 核心依赖 ---
import requests
import re
import json
import os
import sys
import time
import subprocess
from datetime import datetime
from loguru import logger
import threading
import queue
import socket
import platform
import random
from typing import Dict, Any, Optional, List
# from concurrent.futures import ThreadPoolExecutor, as_completed # 已移除

# --- 新增数据库依赖 ---
import mysql.connector
from mysql.connector import errorcode

# --- GUI 依赖 ---
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext

# --- 配置文件 ---
CONFIG_FILE = "config.json"

# ==============================================================================
# 0. 数据库配置与连接模块 (修改：添加全局表名和collector变量)
# ==============================================================================
MYSQL_CONFIG = {
    'host': '192.168.10.70',
    'user': 'root',
    'password': 'zq828079',
    'database': 'cml_data'
}

# 全局表名变量
TABLE_NAME = 'bilibili_link_sound'
COLLECTOR = 'collector'  # 修改为您的收集者名称，与写入脚本保持一致

# 自动获取电脑名
def get_computer_name():
    """自动获取电脑名"""
    try:
        return socket.gethostname()
    except:
        return "Unknown_Computer"

def _check_and_add_column(cursor, table_name, column_name, column_definition):
    """检查列是否存在，如果不存在则添加"""
    try:
        # 检查列是否存在
        cursor.execute(f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{column_name}'
        """)
        if cursor.fetchone()[0] == 0:
            # 列不存在，添加它
            cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN {column_name} {column_definition}")
    except mysql.connector.Error as err:
        logger.warning(f"处理列 '{column_name}' 时出错: {err}")

COMPUTER_NAME = get_computer_name()

def get_db_connection():
    """获取一个新的数据库连接"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        return conn
    except mysql.connector.Error as err:
        logger.error(f"数据库连接失败: {err}")
        return None


def initialize_database():
    """初始化数据库，如果表不存在则创建，并添加分类字段和collector字段"""
    conn = get_db_connection()
    if not conn:
        logger.error("无法连接到数据库，初始化失败。")
        return
    try:
        cursor = conn.cursor()
        # 使用全局表名变量
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
          `id` INT AUTO_INCREMENT PRIMARY KEY,
          `url` TEXT NOT NULL,
          `status` INT DEFAULT 0 COMMENT '0:未下载, 1:已下载, 2:正在下载, 3:下载失败, 4:已跳过',
          `computer_name` VARCHAR(255) NULL,
          `video_title` TEXT NULL,
          `primary_category` VARCHAR(255) NULL COMMENT '主分类',
          `secondary_category` VARCHAR(255) NULL COMMENT '子分类',
          `collector` VARCHAR(255) NULL COMMENT '收集者',
          `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          INDEX `status_idx` (`status`),
          INDEX `collector_idx` (`collector`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(create_table_query)

        # 检查并添加字段，兼容旧版MySQL
        _check_and_add_column(cursor, TABLE_NAME, 'primary_category', "VARCHAR(255) NULL COMMENT '主分类'")
        _check_and_add_column(cursor, TABLE_NAME, 'secondary_category', "VARCHAR(255) NULL COMMENT '子分类'")
        _check_and_add_column(cursor, TABLE_NAME, 'collector', "VARCHAR(255) NULL COMMENT '收集者'")
        
        try:
            # 唯一索引保持不变
            cursor.execute(f"CREATE UNIQUE INDEX url_unique_idx ON {TABLE_NAME} (url(255));")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_DUP_KEYNAME or err.errno == 1061:
                pass
            else:
                raise
        conn.commit()
        logger.info(f"数据库表 '{TABLE_NAME}' 初始化成功。")
    except mysql.connector.Error as err:
        logger.error(f"创建数据库表失败: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def get_task_to_resume(computer_name: str) -> Optional[Dict[str, Any]]:
    """查找本机之前未完成的任务，增加collector条件"""
    conn = get_db_connection()
    if not conn: return None
    task = None
    try:
        cursor = conn.cursor(dictionary=True)
        # 修改：使用全局表名变量，增加collector条件
        query = f"SELECT id, url, primary_category, secondary_category FROM {TABLE_NAME} WHERE status = 2 AND computer_name = %s AND collector = %s LIMIT 1"
        cursor.execute(query, (computer_name, COLLECTOR))
        task = cursor.fetchone()
    except Exception as e:
        logger.error(f"查询待恢复任务失败: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
    return task


def get_new_task(computer_name: str) -> Optional[Dict[str, Any]]:
    """原子性地获取一个新任务并标记为正在下载，增加collector条件"""
    conn = get_db_connection()
    if not conn: return None
    task = None
    try:
        cursor = conn.cursor(dictionary=True)
        conn.start_transaction()
        # 修改：使用全局表名变量，增加collector条件
        query = f"SELECT id, url, primary_category, secondary_category FROM {TABLE_NAME} WHERE status = 0 AND collector = %s LIMIT 1 FOR UPDATE"
        cursor.execute(query, (COLLECTOR,))
        row = cursor.fetchone()
        if row:
            task_id = row['id']
            # 修改：使用全局表名变量
            update_query = f"UPDATE {TABLE_NAME} SET status = 2, computer_name = %s WHERE id = %s"
            cursor.execute(update_query, (computer_name, task_id))
            task = row
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"获取新任务失败: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
    return task


def update_task_status(task_id: int, status: int, title: Optional[str] = None):
    """更新任务状态"""
    conn = get_db_connection()
    if not conn: return
    try:
        cursor = conn.cursor()
        if status == 1:
            # 修改：使用全局表名变量
            query = f"UPDATE {TABLE_NAME} SET status = %s, video_title = %s WHERE id = %s"
            cursor.execute(query, (status, title, task_id))
        # 将状态4（跳过）和状态3（失败）、0（重置）归为一类处理逻辑
        elif status in [0, 3, 4]:
            # 修改：使用全局表名变量
            query = f"UPDATE {TABLE_NAME} SET status = %s, computer_name = NULL WHERE id = %s"
            cursor.execute(query, (status, task_id))
        conn.commit()
    except Exception as e:
        logger.error(f"更新任务状态失败 (ID: {task_id}): {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


# ==============================================================================
# 1. 企业微信通知模块 (无改动)
# ==============================================================================
class WeChatWorkNotifier:
    _instance = None;
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None: cls._instance = super(WeChatWorkNotifier, cls).__new__(cls)
        return cls._instance

    def __init__(self, config: Dict[str, Any], system_info: Optional[Dict[str, Any]] = None):
        if not hasattr(self, '_initialized'):
            self.enabled = config.get('wechat_enabled', False);
            self.webhook_url = config.get('wechat_webhook_url', '').strip()
            if not self.enabled: logger.info("企业微信通知功能未启用。");self._initialized = True;return
            if not self.webhook_url: logger.warning(
                "企业微信通知已启用，但 Webhook URL 为空，无法发送消息。");self.enabled = False;self._initialized = True;return
            self.system_info = system_info if system_info is not None else {};
            self.system_info_summary = self._format_system_info(self.system_info);
            self._initialized = True;
            logger.info(f"企业微信通知功能已初始化。")

    def _send_message_sync(self, payload: Dict[str, Any]) -> bool:
        if not self.enabled or not self.webhook_url: return False
        try:
            response = requests.post(self.webhook_url, json=payload, timeout=5);
            response.raise_for_status();
            data = response.json()
            if data.get("errcode") == 0:
                logger.info("成功发送企业微信消息。");
                return True
            else:
                logger.error(f"发送企业微信消息失败: {data.get('errmsg')}");
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"发送企业微信消息时网络错误: {e}");
            return False
        except Exception as e:
            logger.error(f"发送企业微信消息时发生未知错误: {e}", exc_info=True);
            return False

    def send_message_async(self, message_content: str):
        if not self.enabled: return
        full_markdown_content = f"**【B站下载器通知】**\n> {message_content}"
        if self.system_info_summary: full_markdown_content += f"\n\n--- \n{self.system_info_summary}"
        payload = {"msgtype": "markdown", "markdown": {"content": full_markdown_content}};
        thread = threading.Thread(target=self._send_message_sync, args=(payload,), daemon=True);
        thread.start()

    def _format_system_info(self, system_info: Dict[str, Any]) -> str:
        parts = []
        if system_info: parts.append(f"**设备名:** {system_info.get('hostname', 'N/A')}");ip = system_info.get(
            'ip_address', 'N/A');_ = ip and parts.append(f"**IP地址:** {ip}")
        return "\n".join(parts) if parts else ""


wechat_work_notifier_instance: Optional[WeChatWorkNotifier] = None


def initialize_wechat_work_notifier(config: Dict[str, Any], system_info: Optional[Dict[str, Any]] = None):
    global wechat_work_notifier_instance
    if wechat_work_notifier_instance is None: wechat_work_notifier_instance = WeChatWorkNotifier(config, system_info)
    return wechat_work_notifier_instance


def get_system_info():
    try:
        hostname = socket.gethostname();
        ip_address = socket.gethostbyname(hostname);
        return {"hostname": hostname,
                "ip_address": ip_address}
    except Exception:
        return {"hostname": "未知", "ip_address": "未知"}


# ==============================================================================
# 2. 日志与GUI解耦 (无改动)
# ==============================================================================
class QueueHandler:
    def __init__(self, log_queue): self.log_queue = log_queue

    def write(self, message): self.log_queue.put(message)


# ==============================================================================
# 3. GUI 应用部分 (修改：移除电脑名配置项)
# ==============================================================================
class DownloaderApp:
    def __init__(self, root):
        self.root = root;
        self.root.title("Bilibili 视频下载器 v5.2.4 (支持Collector字段)");
        self.root.geometry("800x750")
        self.downloader_thread = None;
        self.stop_event = threading.Event();
        self.log_queue = queue.Queue();
        self.queue_handler = QueueHandler(self.log_queue)
        self.create_widgets();
        self.setup_logger();
        self.process_log_queue();
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def create_widgets(self):
        main_frame = tk.Frame(self.root, padx=10, pady=10);
        main_frame.pack(fill=tk.BOTH, expand=True);
        main_frame.grid_rowconfigure(5, weight=1);
        main_frame.grid_columnconfigure(1, weight=1);
        initial_config = load_config()
        base_frame = tk.LabelFrame(main_frame, text="基础配置", padx=5, pady=5);
        base_frame.grid(row=0, column=0, columnspan=3, sticky='ew', pady=(0, 10));
        base_frame.grid_columnconfigure(1, weight=1)
        tk.Label(base_frame, text="保存目录:").grid(row=0, column=0, sticky='w', pady=2);
        self.save_dir_var = tk.StringVar(value=initial_config.get('save_dir', ''));
        tk.Entry(base_frame, textvariable=self.save_dir_var).grid(row=0, column=1, sticky='ew', padx=5);
        tk.Button(base_frame, text="浏览...", command=self.select_save_dir).grid(row=0, column=2)
        tk.Label(base_frame, text="FFmpeg 路径:").grid(row=1, column=0, sticky='w', pady=2);
        self.ffmpeg_path_var = tk.StringVar(value=initial_config.get('ffmpeg_path', ''));
        tk.Entry(base_frame, textvariable=self.ffmpeg_path_var).grid(row=1, column=1, sticky='ew', padx=5);
        tk.Button(base_frame, text="浏览...", command=self.select_ffmpeg_path).grid(row=1, column=2)

        # 显示电脑名（只读）
        tk.Label(base_frame, text="本机名称:").grid(row=2, column=0, sticky='w', pady=2);
        self.computer_name_var = tk.StringVar(value=COMPUTER_NAME)
        computer_name_entry = tk.Entry(base_frame, textvariable=self.computer_name_var, state='readonly')
        computer_name_entry.grid(row=2, column=1, columnspan=2, sticky='ew', padx=5)

        # Cookie文件路径选择
        tk.Label(base_frame, text="Cookie 文件夹路径:").grid(row=3, column=0, sticky='w', pady=2);
        self.cookie_dir_var = tk.StringVar(value=initial_config.get('cookie_dir', ''))
        tk.Entry(base_frame, textvariable=self.cookie_dir_var).grid(row=3, column=1, sticky='ew', padx=5)
        tk.Button(base_frame, text="浏览...", command=self.select_cookie_dir).grid(row=3, column=2)

        wechat_frame = tk.LabelFrame(main_frame, text="企业微信通知", padx=5, pady=5);
        wechat_frame.grid(row=1, column=0, columnspan=3, sticky='ew', pady=(0, 10));
        wechat_frame.grid_columnconfigure(1, weight=1)
        self.wechat_enabled_var = tk.BooleanVar(value=initial_config.get('wechat_enabled', False));
        tk.Checkbutton(wechat_frame, text="启用通知", variable=self.wechat_enabled_var).grid(row=0, column=0)
        tk.Label(wechat_frame, text="Webhook 地址:").grid(row=1, column=0, sticky='w', pady=2);
        self.wechat_webhook_url_var = tk.StringVar(value=initial_config.get('wechat_webhook_url', ''));
        tk.Entry(wechat_frame, textvariable=self.wechat_webhook_url_var).grid(row=1, column=1, columnspan=2,
                                                                              sticky='ew', padx=5)
        log_frame = tk.LabelFrame(main_frame, text="实时日志", padx=5, pady=5);
        log_frame.grid(row=2, column=0, columnspan=3, sticky='nsew', pady=10);
        log_frame.grid_rowconfigure(0, weight=1);
        log_frame.grid_columnconfigure(0, weight=1)
        self.log_display = scrolledtext.ScrolledText(log_frame, state='disabled', wrap=tk.WORD, bg="#2b2b2b",
                                                     fg="#bbbbbb");
        self.log_display.pack(fill=tk.BOTH, expand=True)
        button_frame = tk.Frame(main_frame);
        button_frame.grid(row=3, column=0, columnspan=3, pady=5, sticky='ew');
        button_frame.columnconfigure(0, weight=1);
        button_frame.columnconfigure(1, weight=1)
        self.start_button = tk.Button(button_frame, text="开始监控下载", height=2, command=self.start_downloader);
        self.start_button.grid(row=0, column=0, padx=5, sticky='ew')
        self.stop_button = tk.Button(button_frame, text="停止", height=2, command=self.stop_downloader,
                                     state='disabled');
        self.stop_button.grid(row=0, column=1, padx=5, sticky='ew')

    def setup_logger(self):
        logger.remove();
        logger.add(sys.stdout, colorize=True, level="INFO",
                   format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>");
        logger.add(self.queue_handler, level="INFO", colorize=True,
                   format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")

    def process_log_queue(self):
        try:
            while not self.log_queue.empty(): self.log_display.config(state='normal');self.log_display.insert(tk.END,
                                                                                                              self.log_queue.get_nowait());self.log_display.see(
                tk.END);self.log_display.config(state='disabled')
        finally:
            self.root.after(100, self.process_log_queue)

    def select_save_dir(self):
        path = filedialog.askdirectory(title="请选择视频保存的根目录");
        _ = path and self.save_dir_var.set(path)

    def select_ffmpeg_path(self):
        path = filedialog.askopenfilename(title="请选择 ffmpeg.exe 文件",
                                          filetypes=[("可执行文件", "*.exe"), ("所有文件", "*.*")]);
        _ = path and self.ffmpeg_path_var.set(path)

    def select_cookie_dir(self):
        """打开文件夹对话框选择存放Cookie文件的目录"""
        path = filedialog.askdirectory(title="请选择存放Cookie文件的文件夹")
        if path:
            self.cookie_dir_var.set(path)

    def start_downloader(self):
        save_dir = self.save_dir_var.get().strip();
        ffmpeg_path = self.ffmpeg_path_var.get().strip();
        # 不再从GUI获取电脑名，使用自动获取的电脑名
        computer_name = COMPUTER_NAME
        # 从GUI获取Cookie文件夹路径
        cookie_dir = self.cookie_dir_var.get().strip()
        wechat_enabled = self.wechat_enabled_var.get();
        wechat_webhook_url = self.wechat_webhook_url_var.get().strip()

        # 更新验证逻辑
        if not all([save_dir, ffmpeg_path, cookie_dir]):
            messagebox.showerror("错误", "基础配置中的所有项均不能为空！")
            return

        if not os.path.isdir(cookie_dir):
            messagebox.showerror("错误", "指定的Cookie文件夹路径无效或不是一个文件夹！")
            return

        if wechat_enabled and not wechat_webhook_url:
            messagebox.showerror("错误", "已启用企业微信通知，但 Webhook 地址不能为空！")
            return

        # 更新要保存的配置项（不再保存computer_name）
        config = {
            'save_dir': save_dir,
            'ffmpeg_path': ffmpeg_path,
            'cookie_dir': cookie_dir,
            'wechat_enabled': wechat_enabled,
            'wechat_webhook_url': wechat_webhook_url
        }
        save_config(config)

        if wechat_enabled:
            system_info = get_system_info();
            initialize_wechat_work_notifier(config, system_info);
            _ = wechat_work_notifier_instance and wechat_work_notifier_instance.send_message_async(
            "下载监控任务已启动。")
        self.start_button.config(state='disabled');
        self.stop_button.config(state='normal');
        self.stop_event.clear();
        self.downloader_thread = threading.Thread(target=main_downloader_loop, args=(config, self.stop_event),
                                                  daemon=True);
        self.downloader_thread.start()

    def stop_downloader(self):
        if self.downloader_thread and self.downloader_thread.is_alive():
            logger.warning("正在请求停止，请等待当前任务完成或轮询周期结束...")
            self.stop_event.set()
            self.stop_button.config(state='disabled')
            self.root.after(100, self.check_thread_stopped)

    def check_thread_stopped(self):
        if not self.downloader_thread.is_alive():
            logger.info("监控线程已安全停止。");
            self.start_button.config(state='normal');
            self.stop_button.config(state='disabled')
        else:
            self.root.after(100, self.check_thread_stopped)

    def on_closing(self):
        if self.downloader_thread and self.downloader_thread.is_alive():
            if messagebox.askokcancel("退出",
                                      "下载监控仍在进行中，确定要退出吗？"): self.stop_event.set();self.root.destroy()
        else:
            self.root.destroy()


# ==============================================================================
# 4. 核心下载器部分 (修改：支持分类目录和collector条件)
# ==============================================================================
BASE_SAVE_DIR, FFMPEG_PATH = '', ''
session = requests.Session()

def get_random_cookie_from_dir(dir_path: str) -> Optional[str]:
    """从指定文件夹中随机选择一个.txt文件并读取其Cookie内容"""
    if not os.path.isdir(dir_path):
        logger.error(f"Cookie目录不存在或不是一个文件夹: {dir_path}")
        return None
    
    try:
        txt_files = [f for f in os.listdir(dir_path) if f.endswith('.txt') and os.path.isfile(os.path.join(dir_path, f))]
        if not txt_files:
            logger.error(f"在目录 '{dir_path}' 中没有找到任何 .txt 格式的Cookie文件。")
            return None
        
        random_cookie_file = random.choice(txt_files)
        cookie_file_path = os.path.join(dir_path, random_cookie_file)
        logger.info(f"本次任务随机选用Cookie文件: {random_cookie_file}")
        return get_cookie_from_file(cookie_file_path)
    except Exception as e:
        logger.error(f"从目录 '{dir_path}' 获取随机Cookie时发生错误: {e}")
        return None


def get_cookie_from_file(file_path: str) -> Optional[str]:
    """从指定文件中安全地读取Cookie字符串"""
    if not os.path.exists(file_path):
        logger.error(f"Cookie文件不存在: {file_path}")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            cookie = f.read().strip()
        if not cookie:
            logger.warning(f"Cookie文件为空: {file_path}")
            return None
        return cookie
    except Exception as e:
        logger.error(f"读取Cookie文件时发生错误 ({file_path}): {e}")
        return None


def get_playinfo_from_api(bvid: str, page_number: int = 1) -> Optional[Dict[str, Any]]:
    """
    通过BVID和分P号获取信息，并自动选择最高画质。
    """
    view_api_url = "https://api.bilibili.com/x/web-interface/view"
    params = {'bvid': bvid}
    try:
        response = session.get(view_api_url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()

        if data.get('code') != 0:
            logger.error(f"主信息API请求失败 (bvid: {bvid}): {data.get('message')}")
            return None

        video_data = data.get('data', {})
        main_title = video_data.get('title', '未知标题')
        pages: List[Dict[str, Any]] = video_data.get('pages', [])

        target_page_info = None
        if pages:
            for page in pages:
                if page.get('page') == page_number:
                    target_page_info = page
                    break
            if not target_page_info:
                target_page_info = pages[0]
                logger.warning(f"指定的P号 {page_number} 不存在，将下载第一个分P。")

        if target_page_info:
            part_title = target_page_info.get('part', '')
            cid = target_page_info.get('cid')
            if part_title and part_title != main_title:
                final_title = f"{main_title} P{page_number} {part_title}"
            else:
                final_title = main_title
        else:
            cid = video_data.get('cid')
            final_title = main_title

        if not cid:
            logger.error(f"无法找到有效的CID (bvid: {bvid}, p: {page_number})")
            return None

        play_api_url = "https://api.bilibili.com/x/player/playurl"
        play_params = {'bvid': bvid, 'cid': cid, 'fnval': 4048}
        play_response = session.get(play_api_url, params=play_params, timeout=20)
        play_response.raise_for_status()
        play_data = play_response.json()

        if play_data.get('code') != 0:
            logger.error(f"播放信息API请求失败 (cid: {cid}): {play_data.get('message')}")
            return None

        dash = play_data.get('data', {}).get('dash', {})
        video_streams = dash.get('video', [])
        audio_streams = dash.get('audio', [])

        if not video_streams or not audio_streams:
            logger.error("API响应中未找到有效的音视频流。")
            return None

        best_video_stream = max(video_streams, key=lambda x: x.get('id', 0))
        best_audio_stream = max(audio_streams, key=lambda x: x.get('bandwidth', 0))

        logger.info(
            f"已选择最高画质: {best_video_stream.get('codecs')} at {best_video_stream.get('width')}x{best_video_stream.get('height')}")

        # 增加返回分辨率
        return {
            'videoTitle': final_title,
            'videoUrl': best_video_stream['baseUrl'],
            'audioUrl': best_audio_stream['baseUrl'],
            'width': best_video_stream.get('width', 0),
            'height': best_video_stream.get('height', 0)
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"API请求网络错误 (bvid: {bvid}): {e}")
        return None
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        logger.error(f"解析API响应时出错 (bvid: {bvid}): {e}")
        return None


def saveMedia_single_thread(base_download_dir: str, fileName: str, url: str, desc: str) -> Optional[str]:
    """
    使用单线程流式下载媒体文件，支持断点续传并显示进度条。(修改：接收base_download_dir参数)
    """
    os.makedirs(name=base_download_dir, exist_ok=True)
    final_file_path = os.path.join(base_download_dir, f"{fileName}.{desc}")
    temp_file_path = final_file_path + ".tmp"

    if os.path.exists(final_file_path):
        logger.info(f"{desc} 最终文件已存在，跳过下载。")
        return final_file_path

    try:
        downloaded_size = 0
        if os.path.exists(temp_file_path):
            downloaded_size = os.path.getsize(temp_file_path)

        headers = session.headers.copy()
        if downloaded_size > 0:
            headers['Range'] = f'bytes={downloaded_size}-'
            logger.info(f"发现未完成的 {desc} 文件，从 {downloaded_size / 1024 / 1024:.2f} MB 处继续下载。")

        with session.get(url, headers=headers, stream=True, timeout=(30, 3600)) as r:
            if r.status_code not in [200, 206]:
                r.raise_for_status()

            total_size_str = r.headers.get('Content-Range', r.headers.get('content-length'))
            total_size = 0
            if total_size_str:
                if '/' in total_size_str:
                    total_size = int(total_size_str.split('/')[-1])
                else:
                    total_size = downloaded_size + int(total_size_str)
            else:
                total_size = downloaded_size

            with tqdm(total=total_size, initial=downloaded_size, desc=desc, unit='B', unit_scale=True,
                      unit_divisor=1024, leave=False) as pbar:
                with open(temp_file_path, 'ab') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

        os.rename(temp_file_path, final_file_path)
        logger.success(f"{desc}: 文件下载完成 -> {os.path.basename(final_file_path)}")
        return final_file_path

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP错误导致 {desc} 下载失败: {http_err}. 请检查Cookie是否已过期或不正确。")
        return None
    except Exception as e:
        logger.error(f"{desc} 下载过程中发生错误: {e}")
        return None


def AvMerge(mp3Path: str, mp4Path: str, savePath: str) -> bool:
    """合并音视频 (无改动)"""
    try:
        command = [FFMPEG_PATH, '-y', '-i', mp4Path, '-i', mp3Path, '-c:v', 'copy', '-c:a', 'aac', '-strict',
                   'experimental', savePath]
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL);
        logger.success(f"合并成功: {os.path.basename(savePath)}");
        os.remove(mp3Path);
        os.remove(mp4Path);
        return True
    except Exception as e:
        logger.error(f"FFmpeg合并失败: {e}");
        return False


def processVideoUrl(url: str, primary_category: Optional[str], secondary_category: Optional[str]) -> Dict[str, Any]:
    """
    处理单个视频链接，调用单线程下载函数。(修改：接收分类参数)
    """
    MAX_RETRIES = 5

    bvid_match = re.search(r'BV([a-zA-Z0-9]{10})', url)
    if not bvid_match:
        return {'success': False, 'title': None, 'error_msg': f"无法从URL中提取BVID: {url}", 'reason': 'parse_failed'}
    bvid = bvid_match.group(0)

    page_match = re.search(r'[?&]p=(\d+)', url)
    page_number = int(page_match.group(1)) if page_match else 1

    logger.info(f"识别到 BVID: {bvid}, 分P: {page_number}")

    # 修改：构建分类目录路径
    category_dir = BASE_SAVE_DIR
    if primary_category:
        category_dir = os.path.join(category_dir, primary_category)
    if secondary_category:
        category_dir = os.path.join(category_dir, secondary_category)

    logger.info(f"视频将保存到分类目录: {category_dir}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            videoInfo = get_playinfo_from_api(bvid, page_number)
            if not videoInfo:
                raise Exception("通过API获取视频信息失败。")

            # 判断分辨率，如果宽和高中的较小值小于1000，则跳过
            width = videoInfo.get('width', 0)
            height = videoInfo.get('height', 0)
            if min(width, height) < 1000:
                logger.warning(f"视频 '{videoInfo['videoTitle']}' 分辨率 ({width}x{height}) 低于要求，将跳过。")
                return {'success': False, 'title': videoInfo['videoTitle'], 'reason': 'low_resolution'}

            safe_title = re.sub(r'[\\/*?:"<>|]', '_', videoInfo['videoTitle'])
            final_output = os.path.join(category_dir, f"{safe_title}.mp4")

            if os.path.exists(final_output):
                logger.info(f"文件已存在，跳过: {safe_title}.mp4")
                return {'success': True, 'title': safe_title, 'reason': 'already_exists'}

            # 修改：传递分类目录路径
            mp3_path = saveMedia_single_thread(category_dir, safe_title, videoInfo['audioUrl'], 'audio')
            if not mp3_path: raise Exception("下载音频文件失败。请检查Cookie是否已过期或不正确。")

            mp4_path = saveMedia_single_thread(category_dir, safe_title, videoInfo['videoUrl'], 'video')
            if not mp4_path: raise Exception("下载视频文件失败。请检查Cookie是否已过期或不正确。")

            if not AvMerge(mp3_path, mp4_path, final_output):
                raise Exception("合并音视频文件失败")

            return {'success': True, 'title': safe_title, 'reason': 'downloaded'}
        except Exception as e:
            logger.warning(f"链接处理第 {attempt}/{MAX_RETRIES} 次尝试失败: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(5 * attempt)

    error_msg = f"链接在所有 {MAX_RETRIES} 次尝试后均告失败: {url}"
    logger.error(f"{error_msg}. 最常见原因为Cookie失效，请更新后重试。")
    return {'success': False, 'title': None, 'error_msg': error_msg, 'reason': 'download_failed'}


def main_downloader_loop(config, stop_event):
    """
    主循环 (修改：支持分类目录和collector条件)
    """
    global BASE_SAVE_DIR, FFMPEG_PATH
    BASE_SAVE_DIR = config['save_dir']
    FFMPEG_PATH = config['ffmpeg_path']
    # 不再从config获取电脑名，使用自动获取的电脑名
    COMPUTER_NAME = get_computer_name()

    # 初始化Session时不再设置固定的Cookie
    session.headers.update({
        'Referer': 'https://www.bilibili.com/',
        'Origin': 'https://www.bilibili.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
    })
    cookie_dir = config.get('cookie_dir')
    if not cookie_dir:
        logger.critical("配置文件中未指定Cookie文件夹路径，监控线程无法启动。")
        return

    logger.info(f"\n{'=' * 20}\n下载器核心已启动 (本机: {COMPUTER_NAME})，进入数据库监控模式...\n{'=' * 20}")
    logger.info(f"当前收集者: {COLLECTOR}")
    initialize_database()

    while not stop_event.is_set():
        task = None
        try:
            task = get_task_to_resume(COMPUTER_NAME)
            if task:
                logger.info(f"发现本机未完成的任务，继续处理: {task['url']}")
            else:
                task = get_new_task(COMPUTER_NAME)

            if task:
                # 在处理每个任务前，都从文件重新加载Cookie
                fresh_cookie = get_random_cookie_from_dir(cookie_dir)
                if not fresh_cookie:
                    logger.error(f"无法从目录 '{cookie_dir}' 中获取有效Cookie，本次任务将跳过，稍后重试。")
                    # 不重置任务状态，等待下个循环自动重试
                    stop_event.wait(timeout=30)
                    continue  # 跳过本次循环

                session.headers['Cookie'] = fresh_cookie
                logger.info("已从文件动态更新Cookie。")

                task_id, url = task['id'], task['url']
                # 修改：获取分类信息
                primary_category = task.get('primary_category')
                secondary_category = task.get('secondary_category')

                logger.info(f"开始处理任务 (ID: {task_id}): {url}")
                logger.info(
                    f"分类信息 - 主分类: {primary_category or '未分类'}, 子分类: {secondary_category or '未分类'}")

                # 修改：传递分类信息
                result = processVideoUrl(url, primary_category, secondary_category)

                # 根据 processVideoUrl 返回的结果更新任务状态
                if result.get('success'):
                    # 成功下载或文件已存在，都标记为已下载
                    update_task_status(task_id, 1, result['title'])
                    logger.success(f"任务 (ID: {task_id}) 处理成功: {result['title']}")
                else:
                    # 处理失败的情况
                    if result.get('reason') == 'low_resolution':
                        # 如果是分辨率低，根据要求将状态重置为4
                        update_task_status(task_id, 4)
                        logger.warning(f"任务 (ID: {task_id}) 因分辨率过低已跳过，状态已重置为4 (分辨率不足，跳过下载)。")
                    else:
                        # 其他所有失败情况，标记为失败状态3
                        update_task_status(task_id, 3)
                        logger.error(f"任务 (ID: {task_id}) 处理失败，已标记为失败状态: {url}。")
            else:
                logger.info("数据库中无待处理链接，等待下一次检查...")
                stop_event.wait(timeout=30)

            if not task:
                time.sleep(1)

        except Exception as e:
            logger.error(f"监控循环发生未知错误: {e}", exc_info=True)
            if task and 'id' in task:
                logger.warning(f"将任务 {task['id']} 状态重置为0")
                update_task_status(task['id'], 0)
            stop_event.wait(timeout=60)

    if wechat_work_notifier_instance and wechat_work_notifier_instance.enabled:
        wechat_work_notifier_instance.send_message_async("下载监控任务已停止。")
    logger.info("监控循环已停止。")


# ==============================================================================
# 5. 配置文件处理与程序主入口 (修改：移除computer_name相关配置)
# ==============================================================================
def load_config():
    # 更新默认配置项，移除computer_name
    defaults = {
        "save_dir": "",
        "ffmpeg_path": "",
        "cookie_dir": "",
        "wechat_enabled": False,
        "wechat_webhook_url": ""
    }
    if not os.path.exists(CONFIG_FILE):
        return defaults
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            return {**defaults, **config}
    except (json.JSONDecodeError, TypeError):
        return defaults


def save_config(config_data):
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"保存配置文件失败: {e}")


if __name__ == '__main__':
    try:
        from tqdm import tqdm
    except ImportError:
        root = tk.Tk();
        root.withdraw();
        messagebox.showerror("依赖缺失", "错误：tqdm 库未安装。\n请在命令行运行 'pip install tqdm' 进行安装。");
        sys.exit(1)

    try:
        import mysql.connector
    except ImportError:
        root = tk.Tk();
        root.withdraw();
        messagebox.showerror("依赖缺失",
                             "错误：mysql-connector-python 库未安装。\n请在命令行运行 'pip install mysql-connector-python' 进行安装。");
        sys.exit(1)

    root = tk.Tk();
    app = DownloaderApp(root);
    root.mainloop()