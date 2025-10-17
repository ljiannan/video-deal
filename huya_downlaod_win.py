#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/10/11 11:08
# @Author  : Guo yuhang
# @File    : huya_downlaod_win.py
# @Software: PyCharm
# @Description:""" """
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/10/9 11:14
# @Author  : Guo yuhang
# @File    : huya_downloader_mac.py
# @Software: PyCharm
# @Description:""" 虎牙视频下载器（健壮版）：增强事务管理、文件名安全、下载重试和配置灵活性 """
import requests
import re
import json
import time
import sys
import shutil
import threading
from typing import Dict, Any, Optional
from pathlib import Path

import mysql.connector
from loguru import logger
from tqdm import tqdm


# ———————————————— 配置区域 ————————————————
class Config:
    # 视频保存路径
    SAVE_DIRECTORY = r"\\192.168.10.30\share\a项目\huya_400t"
    # MySQL数据库配置
    MYSQL_CONFIG = {
        'host': '192.168.10.70',
        'user': 'root',
        'password': 'zq828079',
        'database': 'huya_video_tasks'
    }
    DB_TABLE_NAME = 'video_tasks'
    # 最低允许下载的分辨率
    MIN_ALLOWED_RESOLUTION = 480  # 单位：像素（如720表示720P）
    # 最低剩余空间要求（5GB，单位：字节）
    MIN_FREE_SPACE = 5 * 1024 ** 3  # 1GB=1024^3字节
    # 主机名，用于区分不同的下载节点。请务必修改为唯一的、有意义的名称！
    HOSTNAME = "da01"

    # --- 企业微信通知配置 ---
    # 是否启用企业微信通知
    WECHAT_WORK_ENABLED = False
    # 企业微信机器人的 Webhook URL (推荐，配置简单)
    # 如果使用 Webhook，则无需配置下面的 corpid, corpsecret, agentid
    WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=8c1b67be-c53f-4788-8a25-8ce271294498"  # <-- 修改为你的key
    # 仅在不使用 Webhook 时配置以下三项
    CORP_ID = ""
    CORP_SECRET = ""
    AGENT_ID = ""


# —————————————— 日志配置 ——————————————
logger.remove()
# 控制台输出
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
)
# 文件日志
log_file_path = r"./log/huya_downloader.log"
logger.add(
    log_file_path,
    level="DEBUG",
    rotation="100 MB",
    retention="7 days",
    encoding="utf-8"
)


# —————————————— 企业微信通知模块 ——————————————

class WeChatWorkNotifier:
    """
    企业微信通知器，用于发送消息到企业微信。
    支持 Webhook 和 App 两种模式。
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(WeChatWorkNotifier, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized'):
            return

        self.enabled = Config.WECHAT_WORK_ENABLED
        if not self.enabled:
            logger.info("企业微信通知功能已禁用。")
            self._initialized = True
            return

        self.webhook_url = Config.WEBHOOK_URL
        self.corpid = Config.CORP_ID
        self.corpsecret = Config.CORP_SECRET
        self.agentid = Config.AGENT_ID

        self._access_token = None
        self._token_expires_at = 0
        self._token_lock = threading.Lock()
        self._initialized = True

        if "YOUR_WEBHOOK_KEY" in self.webhook_url and not (self.corpid and self.corpsecret and self.agentid):
            logger.warning("企业微信通知已启用，但 Webhook URL 未配置或 App 配置不完整，功能将无法使用。")
            self.enabled = False
        else:
            logger.info("企业微信通知功能已初始化。")

    def _get_access_token(self) -> Optional[str]:
        with self._token_lock:
            if self._access_token and time.time() < self._token_expires_at:
                return self._access_token

            url = f"https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={self.corpid}&corpsecret={self.corpsecret}"
            try:
                response = requests.get(url, timeout=5)
                response.raise_for_status()
                data = response.json()
                if data.get("errcode") == 0:
                    self._access_token = data["access_token"]
                    self._token_expires_at = time.time() + data["expires_in"] - 120
                    logger.info("成功获取企业微信 access token。")
                    return self._access_token
                else:
                    logger.error(f"获取企业微信 access token 失败: {data.get('errmsg')}")
                    return None
            except requests.exceptions.RequestException as e:
                logger.error(f"网络错误，无法获取企业微信 access token: {e}")
                return None

    def _send_message_sync(self, payload: Dict[str, Any]):
        if not self.enabled:
            return

        # 优先使用 Webhook
        if "YOUR_WEBHOOK_KEY" not in self.webhook_url:
            url = self.webhook_url
        else:
            access_token = self._get_access_token()
            if not access_token:
                logger.warning("无法发送企业微信消息：缺少 access token。")
                return
            url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={access_token}"
            # 为 App 模式补充默认参数
            payload['agentid'] = self.agentid
            payload['touser'] = "@all"

        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get("errcode") == 0:
                logger.debug("企业微信消息发送成功。")
            else:
                logger.error(f"发送企业微信消息失败: {data.get('errmsg')}")
        except requests.exceptions.RequestException as e:
            logger.error(f"网络错误，无法发送企业微信消息: {e}")
        except Exception as e:
            logger.error(f"发送企业微信消息时发生未知错误: {e}")

    def send_markdown_async(self, content: str):
        """异步发送 Markdown 消息"""
        if not self.enabled:
            return

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        }
        thread = threading.Thread(target=self._send_message_sync, args=(payload,), daemon=True)
        thread.start()


# 全局通知器实例
wechat_notifier = WeChatWorkNotifier()


def wechat_error_sink(message):
    """
    Loguru 的 sink 函数，用于捕获 ERROR 级别日志并发送企业微信通知。
    """
    record = message.record
    error_message = (
        f"**【错误警报】**\n"
        f"> 主机名: <font color=\"warning\">{Config.HOSTNAME}</font>\n"
        f"> 脚本名: `{Path(__file__).name}`\n"
        f"> 时  间: `{record['time'].strftime('%Y-%m-%d %H:%M:%S')}`\n"
        f"> **错误简报:** <font color=\"comment\">{record['message']}</font>"
    )
    wechat_notifier.send_markdown_async(error_message)


def send_status_notification(status: str):
    """发送程序启动/结束状态通知"""
    color = "info" if status == "启动" else "warning"
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    message = (
        f"**【程序状态通知】**\n"
        f"> 主机名: <font color=\"{color}\">{Config.HOSTNAME}</font>\n"
        f"> 脚本名: `{Path(__file__).name}`\n"
        f"> 时  间: `{current_time}`\n"
        f"> 状  态: **程序已{status}**"
    )
    wechat_notifier.send_markdown_async(message)


# —————————————— 数据库管理类 ——————————————
class DatabaseManager:
    """管理MySQL数据库连接和操作。"""

    def __init__(self, db_config, table_name):
        self.db_config = db_config
        self.table_name = table_name
        self.conn = None
        self.cursor = None

    def connect(self):
        """连接到MySQL数据库"""
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info(f"成功连接到数据库 '{self.db_config['database']}'。")
        except mysql.connector.Error as err:
            logger.error(f"数据库连接失败: {err}")
            raise

    def get_interrupted_task(self, hostname: str) -> dict | None:
        """获取本机中断的下载任务。"""
        # 使用try...finally确保autocommit状态被重置
        original_autocommit = self.conn.autocommit
        try:
            self.conn.autocommit = True  # 查询操作，使用自动提交
            sql = f"SELECT id, url, filename FROM {self.table_name} WHERE status = 'downloading' AND hostname = %s LIMIT 1"
            self.cursor.execute(sql, (hostname,))
            task = self.cursor.fetchone()
            if task:
                return {'id': task[0], 'url': task[1], 'filename': task[2]}
            return None
        finally:
            self.conn.autocommit = original_autocommit  # 恢复原始状态

    def get_new_task(self, hostname: str) -> dict | None:
        """获取新任务并标记为下载中"""
        try:
            self.conn.start_transaction()
            # 添加SKIP LOCKED，跳过已被其他线程锁定的任务，避免锁等待
            query_sql = f"SELECT id, url FROM {self.table_name} WHERE status = 'pending' LIMIT 1 FOR UPDATE SKIP LOCKED"
            self.cursor.execute(query_sql)
            task = self.cursor.fetchone()

            if task:
                task_id, url = task
                update_sql = f"UPDATE {self.table_name} SET status = 'downloading', hostname = %s WHERE id = %s"
                self.cursor.execute(update_sql, (hostname, task_id))
                self.conn.commit()
                logger.info(f"领取新任务 (ID: {task_id})，状态更新为 'downloading'。")
                return {'id': task_id, 'url': url}
            else:
                self.conn.commit()
                return None
        except mysql.connector.Error as err:
            self.conn.rollback()
            logger.error(f"获取新任务时发生数据库错误: {err}", exc_info=True)
            raise
        except Exception as e:
            self.conn.rollback()
            logger.error(f"获取新任务时发生未知错误: {e}", exc_info=True)
            raise

    def update_task_on_success(self, task_id: int, video_title: str, filename: str):
        """下载成功：更新状态、标题和文件名"""
        try:
            sql = f"""
                UPDATE {self.table_name} 
                SET status = 'completed', video_title = %s, filename = %s
                WHERE id = %s
            """
            self.cursor.execute(sql, (video_title, filename, task_id))
            self.conn.commit()
            logger.success(f"任务 (ID: {task_id}) 状态更新为 'completed'。")
        except mysql.connector.Error as err:
            self.conn.rollback()
            logger.error(f"更新任务(ID: {task_id})成功状态时发生数据库错误: {err}", exc_info=True)
            raise

    def update_task_on_failure(self, task_id: int):
        """下载失败：仅更新状态（删除fail_reason字段）"""
        try:
            sql = f"UPDATE {self.table_name} SET status = 'failed' WHERE id = %s"
            self.cursor.execute(sql, (task_id,))
            self.conn.commit()
            logger.error(f"任务 (ID: {task_id}) 状态更新为 'failed'。")
        except mysql.connector.Error as err:
            self.conn.rollback()
            logger.error(f"更新任务(ID: {task_id})失败状态时发生数据库错误: {err}", exc_info=True)
            raise

    def close(self):
        """关闭数据库连接。"""
        if self.conn and self.conn.is_connected():
            self.cursor.close()
            self.conn.close()
            logger.info("数据库连接已关闭。")


# —————————————— 工具函数 ——————————————
def sanitize_filename(filename: str) -> str:
    """移除文件名中的非法字符"""
    return re.sub(r'[\\/*?:"<>|\n\r\t]', "_", filename).strip()


def check_free_space(directory: str) -> bool:
    """检查输出目录所在磁盘的剩余空间是否满足最低要求（5GB）"""
    try:
        disk_usage = shutil.disk_usage(directory)
        free_space_gb = disk_usage.free / (1024 ** 3)
        min_space_gb = Config.MIN_FREE_SPACE / (1024 ** 3)

        logger.info(f"当前存储目录剩余空间：{free_space_gb:.2f} GB（最低要求：{min_space_gb:.0f} GB）")
        if disk_usage.free < Config.MIN_FREE_SPACE:
            logger.error(f"剩余空间不足！当前 {free_space_gb:.2f} GB < 最低要求 {min_space_gb:.0f} GB，将停止后续下载")
            return False
        return True
    except Exception as e:
        logger.error(f"检查剩余空间时发生错误：{str(e)}，将继续下载（建议手动确认空间）")
        return True


def get_huya_video_info(video_page_url: str) -> dict | None:
    """提取虎牙视频信息（含分辨率）"""
    id_match = re.search(r'play/(\d+)\.html', video_page_url)
    if not id_match:
        logger.error(f"URL格式错误：{video_page_url}（需符合 https://www.huya.com/video/play/xxxx.html）")
        return None
    video_id = id_match.group(1)
    logger.info(f"提取视频ID成功：{video_id}")

    api_url = f"https://liveapi.huya.com/moment/getMomentContent?videoId={video_id}&_={int(time.time() * 1000)}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'Referer': video_page_url
    }

    try:
        response = requests.get(api_url, headers=headers, timeout=15)
        response.raise_for_status()

        resp_text = response.text
        if resp_text.startswith('jQuery'):
            json_str = re.search(r'^\w+\((.*)\)$', resp_text).group(1)
        else:
            json_str = resp_text
        data = json.loads(json_str)

        video_info = data.get('data', {}).get('moment', {}).get('videoInfo', {})
        if not video_info:
            logger.error(f"API未返回视频信息（videoId: {video_id}）")
            return None

        quality_resolution_map = {
            '1080P': 1080, '720P': 720, '480P': 480, '360P': 360, '240P': 240,
            '1080p': 1080, '720p': 720, '480p': 480, '360p': 360, '240p': 240,
            '蓝光10M': 1080, '蓝光8M': 720, '蓝光4M': 480, '流畅': 360, '极速': 240
        }

        definitions = video_info.get('definitions', [])
        if not definitions:
            logger.error(f"未找到任何清晰度选项（videoId: {video_id}）。")
            return None
        highest_quality = definitions[0]

        quality_name = highest_quality.get('defName', '未知清晰度')
        resolution = quality_resolution_map.get(quality_name, None)
        if resolution is None:
            logger.warning(f"未识别的清晰度名称：{quality_name}")
            return None

        return {
            "title": video_info.get('videoTitle', f'未知标题_{video_id}'),
            "quality": quality_name,
            "resolution": resolution,
            "mp4_url": highest_quality.get('url'),
            "m3u8_url": highest_quality.get('m3u8')
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"API请求失败：{str(e)}")
        return None
    except (json.JSONDecodeError, AttributeError) as e:
        logger.error(f"API响应解析失败：{str(e)}")
        logger.debug(f"原始响应内容：{resp_text[:500]}...")
        return None
    except Exception as e:
        logger.error(f"提取视频信息时发生错误：{str(e)}")
        return None


def download_video(url: str, filename: str, save_dir: str, resume: bool = False, max_retries: int = 3) -> bool:
    """下载视频，增加重试机制"""
    save_path = save_dir
    save_path.mkdir(parents=True, exist_ok=True)
    file_path = save_path / filename
    attempt = 0

    if file_path.exists():
        try:
            head_response = requests.head(url, allow_redirects=True, timeout=10)
            remote_size = int(head_response.headers.get('content-length', 0))
            local_size = file_path.stat().st_size

            if remote_size > 0 and local_size == remote_size:
                logger.info(f"文件 '{filename}' 已完整存在，无需下载")
                return True
        except Exception as e:
            logger.warning(f"检查文件完整性失败：{str(e)}，将继续下载流程")

    while attempt < max_retries:
        headers = {}
        mode = 'wb'
        downloaded_size = 0

        if resume and file_path.exists():
            downloaded_size = file_path.stat().st_size
            headers['Range'] = f'bytes={downloaded_size}-'
            mode = 'ab'
            if attempt == 0:
                logger.info(f"检测到未完成文件：{filename}，将从 {downloaded_size / 1024 / 1024:.2f} MB 续传。")

        try:
            with requests.get(url, headers=headers, stream=True, timeout=(15, 60)) as resp:
                if resp.status_code == 416:
                    logger.info(f"文件 '{filename}' 已完整下载，无需继续")
                    return True

                resp.raise_for_status()

                if resume and resp.status_code != 206:
                    logger.warning("服务器不支持断点续传，将重新从头下载。")
                    downloaded_size = 0
                    mode = 'wb'
                    resp = requests.get(url, stream=True, timeout=(15, 60))
                    resp.raise_for_status()

                total_size = int(resp.headers.get('content-length', 0))
                if downloaded_size > 0:
                    total_size += downloaded_size

                with tqdm(
                        total=total_size, unit='B', unit_scale=True, desc=filename,
                        ncols=100, initial=downloaded_size, colour='red'
                ) as pbar:
                    with open(file_path, mode) as f:
                        for chunk in resp.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))

            if total_size != 0 and file_path.stat().st_size != total_size:
                raise IOError(f"文件下载不完整：预期 {total_size}，实际 {file_path.stat().st_size}")

            logger.success(f"视频下载完成：{file_path}（大小：{file_path.stat().st_size / 1024 / 1024:.2f} MB）")
            return True

        except (requests.exceptions.RequestException, IOError) as e:
            attempt += 1
            logger.warning(f"下载失败 (第 {attempt}/{max_retries} 次尝试): {e}")
            if attempt < max_retries:
                time.sleep(5 * attempt)
            else:
                logger.error(f"下载失败，已达最大重试次数: {filename}")
                return False
        except Exception as e:
            logger.error(f"下载过程中发生未知错误: {e}")
            return False

    return False


# —————————————— 任务处理函数 ——————————————
def process_task(db_manager: DatabaseManager, task: dict, resume: bool = False) -> bool:
    """处理单个下载任务，返回是否需要继续后续下载（用于空间监测）"""
    task_id = task['id']
    task_url = task['url']
    logger.info(f"\n=== 开始处理任务（ID: {task_id}）：{task_url} ===")

    video_data = get_huya_video_info(task_url)
    if not video_data:
        db_manager.update_task_on_failure(task_id)  # 移除reason参数
        return True

    title = video_data['title']
    quality = video_data['quality']
    resolution = video_data['resolution']
    mp4_url = video_data['mp4_url']
    logger.info(f"任务基础信息：")
    logger.info(f"  视频标题：{title}")
    logger.info(f"  最高清晰度：{quality}")
    logger.info(f"  对应分辨率：{resolution}P")
    logger.info(f"  下载链接：{mp4_url[:80]}...")

    if resolution < Config.MIN_ALLOWED_RESOLUTION:
        db_manager.update_task_on_failure(task_id)  # 移除reason参数
        return True

    if not mp4_url:
        db_manager.update_task_on_failure(task_id)  # 移除reason参数
        return True

    safe_title = sanitize_filename(title)
    filename = task.get('filename') or f"{safe_title}_{quality}.mp4"

    download_success = download_video(
        url=mp4_url,
        filename=filename,
        save_dir=Config.SAVE_DIRECTORY,
        resume=resume
    )

    if download_success:
        db_manager.update_task_on_success(task_id, title, filename)
        return check_free_space(Config.SAVE_DIRECTORY)
    else:
        db_manager.update_task_on_failure(task_id)  # 移除reason参数
        return True


# —————————————— 主程序入口 ——————————————
if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("          虎牙视频下载器（修复版） - 启动          ")
    logger.info("=" * 50)

    # 添加企业微信错误通知 sink
    if wechat_notifier.enabled:
        logger.add(wechat_error_sink, level="ERROR")
        send_status_notification("启动")

    db_manager = DatabaseManager(Config.MYSQL_CONFIG, Config.DB_TABLE_NAME)
    try:
        db_manager.connect()
    except Exception:
        logger.error("数据库连接失败，程序无法启动。")
        sys.exit(1)

    hostname = Config.HOSTNAME
    if hostname == "Your-Hostname-Here" or not hostname:
        logger.error("启动失败：请在文件顶部的配置区（Config类）中设置您的主机名（HOSTNAME）。")
        sys.exit(1)
    logger.info(f"当前运行主机（来自配置）：{hostname}")

    try:
        continue_download = True
        try:
            interrupted_task = db_manager.get_interrupted_task(hostname)
            if interrupted_task:
                logger.warning(f"检测到本机未完成的中断任务（ID: {interrupted_task['id']}），将尝试续传。")
                continue_download = process_task(db_manager, interrupted_task, resume=True)
                if not continue_download:
                    logger.critical("处理中断任务后剩余空间不足，不再领取新任务")
        except Exception as e:
            task_id = interrupted_task.get('id') if interrupted_task else '未知'
            logger.error(f"处理中断任务(ID: {task_id})时发生致命错误，跳过此任务: {e}", exc_info=True)
            continue_download = True

        while continue_download:
            new_task = None
            while True:
                try:
                    new_task = db_manager.get_new_task(hostname)
                    break
                except Exception as e:
                    logger.error(f"获取新任务时发生异常，将在5秒后无限重试: {e}")
                    time.sleep(5)

            if new_task:
                try:
                    continue_download = process_task(db_manager, new_task, resume=False)
                    if not continue_download:
                        logger.critical("处理新任务后剩余空间不足，停止所有下载")
                        break
                except Exception as e:
                    task_id = new_task.get('id', '未知')
                    logger.error(f"处理任务(ID: {task_id})时发生不可恢复的错误，跳过此任务: {e}", exc_info=True)
                    try:
                        db_manager.update_task_on_failure(task_id)  # 移除reason参数
                    except Exception as db_err:
                        logger.critical(f"为失败任务(ID: {task_id})更新状态时再次发生错误，程序可能需要干预: {db_err}")
                time.sleep(2)
            else:
                logger.info("\n=== 数据库中无待处理任务，程序将退出 ===")
                break

    except Exception as e:
        logger.critical(f"主循环发生致命错误，程序被迫终止: {e}", exc_info=True)
    finally:
        if wechat_notifier.enabled:
            send_status_notification("结束")
        db_manager.close()
        logger.info("=" * 50)
        logger.info("          虎牙视频下载器（修复版） - 退出          ")
        logger.info("=" * 50)