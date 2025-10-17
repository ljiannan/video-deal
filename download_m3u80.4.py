import os
import requests
import hashlib
import subprocess
import time
import json
import pymysql
from urllib.parse import urljoin, urlparse
from datetime import datetime
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
from tqdm import tqdm
import glob
import socket


class M3U8Downloader:
    def __init__(self, db_config, download_dir="downloads", max_workers=5):
        self.db_config = db_config
        self.download_dir = download_dir
        self.max_workers = max_workers

        # 获取当前电脑名
        self.computer_name = socket.gethostname()
        self.logger = None

        # 先设置日志
        self.setup_logging()

        self.ffmpeg_path = r"C:\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe"
        self.ffprobe_path = r"C:\ffmpeg-master-latest-win64-gpl\bin\ffprobe.exe"

        # 创建下载目录
        os.makedirs(download_dir, exist_ok=True)

        # 测试数据库连接并检查表结构
        if self.test_database_connection():
            self.check_and_update_table_structure()

    def setup_logging(self):
        """设置完整的日志系统"""
        # 创建logs目录
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # 设置日志文件名（包含日期和电脑名）
        log_filename = f"{log_dir}/m3u8_downloader_{self.computer_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger('M3U8Downloader')
        self.logger.info(f"M3U8下载器日志系统初始化完成 - 电脑名: {self.computer_name}")

    def check_and_update_table_structure(self):
        """检查并更新数据库表结构"""
        connection = self.get_database_connection()
        if not connection:
            return False

        try:
            with connection.cursor() as cursor:
                # 检查computer字段是否存在
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = 'taobao_live' AND column_name = 'computer'
                """, (self.db_config['database'],))
                result = cursor.fetchone()

                if result[0] == 0:
                    # 添加computer字段
                    self.log_info("检测到缺少computer字段，正在添加...")
                    cursor.execute("ALTER TABLE taobao_live ADD COLUMN computer VARCHAR(255) NULL")
                    connection.commit()
                    self.log_info("成功添加computer字段到taobao_live表")

            return True
        except Exception as e:
            self.log_error(f"检查更新表结构失败: {e}")
            return False
        finally:
            connection.close()

    def log_info(self, message):
        """记录信息日志"""
        self.logger.info(message)

    def log_warning(self, message):
        """记录警告日志"""
        self.logger.warning(message)

    def log_error(self, message):
        """记录错误日志"""
        self.logger.error(message)

    def log_debug(self, message):
        """记录调试日志"""
        self.logger.debug(message)

    def save_operation_log(self, operation, status, details="", live_id=""):
        """保存操作日志到文件"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'status': status,
            'live_id': live_id,
            'details': details,
            'computer': self.computer_name
        }

        # 保存到操作日志文件
        op_log_file = f"logs/m3u8_operation_log_{self.computer_name}.json"
        log_data = []

        # 如果文件存在，读取现有日志
        if os.path.exists(op_log_file):
            try:
                with open(op_log_file, 'r', encoding='utf-8') as f:
                    log_data = json.load(f)
            except:
                pass

        # 添加新日志条目
        log_data.append(log_entry)

        # 保存回文件
        try:
            with open(op_log_file, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.log_error(f"保存操作日志失败: {e}")

    def test_database_connection(self):
        """测试数据库连接"""
        try:
            connection = pymysql.connect(**self.db_config)
            connection.close()
            self.log_info("数据库连接测试成功")
            self.save_operation_log("database_connection", "success", "数据库连接测试成功")
            return True
        except Exception as e:
            error_msg = f"数据库连接测试失败: {e}"
            self.log_error(error_msg)
            self.save_operation_log("database_connection", "failed", error_msg)
            return False

    def wait_for_database_connection(self):
        """等待数据库连接恢复正常"""
        self.log_info("等待数据库连接恢复...")
        while True:
            if self.test_database_connection():
                self.log_info("数据库连接已恢复")
                return True
            self.log_info("数据库连接仍未恢复，30秒后重试...")
            time.sleep(30)

    def get_database_connection(self):
        """获取新的数据库连接（线程安全）"""
        try:
            return pymysql.connect(**self.db_config)
        except Exception as e:
            self.log_error(f"创建数据库连接失败: {e}")
            return None

    def get_pending_downloads(self):
        """获取待下载的视频列表 - 每次获取3个任务"""
        connection = self.get_database_connection()
        if not connection:
            return []

        try:
            with connection.cursor() as cursor:
                # 先获取当前电脑的未完成任务（状态4）
                sql = "SELECT liveId, replayUrl FROM taobao_live WHERE status = 4 AND computer = %s LIMIT 3"
                cursor.execute(sql, (self.computer_name,))
                current_tasks = cursor.fetchall()

                if current_tasks:
                    count_msg = f"找到 {len(current_tasks)} 个当前电脑的未完成任务"
                    self.log_info(count_msg)
                    return [{"liveId": row[0], "replayUrl": row[1]} for row in current_tasks]

                # 如果没有当前电脑的未完成任务，获取新的任务
                sql = """
                SELECT liveId, replayUrl FROM taobao_live 
                WHERE (status = 0 OR status = 3) 
                ORDER BY update_time ASC 
                LIMIT 3
                """
                cursor.execute(sql)
                results = cursor.fetchall()

                if results:
                    # 更新这些任务的状态为4（下载队列中）并记录电脑名
                    live_ids = [row[0] for row in results]

                    # 构建IN条件的占位符
                    placeholders = ','.join(['%s'] * len(live_ids))

                    update_sql = f"""
                    UPDATE taobao_live 
                    SET status = 4, computer = %s, update_time = CURRENT_TIMESTAMP 
                    WHERE liveId IN ({placeholders})
                    """

                    cursor.execute(update_sql, [self.computer_name] + live_ids)
                    connection.commit()

                count_msg = f"获取到 {len(results)} 个新的待下载视频"
                self.log_info(count_msg)
                self.save_operation_log("get_pending_downloads", "success", count_msg)

                return [{"liveId": row[0], "replayUrl": row[1]} for row in results]
        except Exception as e:
            error_msg = f"获取待下载列表失败: {e}"
            self.log_error(error_msg)
            self.save_operation_log("get_pending_downloads", "failed", error_msg)
            return []
        finally:
            connection.close()

    def check_task_status(self, live_id):
        """检查任务状态，确保可以下载"""
        connection = self.get_database_connection()
        if not connection:
            return False

        try:
            with connection.cursor() as cursor:
                sql = "SELECT status, computer FROM taobao_live WHERE liveId = %s"
                cursor.execute(sql, (live_id,))
                result = cursor.fetchone()

                if not result:
                    return False

                status, computer = result
                # 只有状态为4且电脑名为当前电脑，或者状态为0或3的任务才可以下载
                return (status == 4 and computer == self.computer_name) or status in [0, 3]
        except Exception as e:
            self.log_error(f"检查任务状态失败 {live_id}: {e}")
            return False
        finally:
            connection.close()

    def update_video_status(self, live_id, status, filename=None, filesize=None):
        """更新视频状态和信息"""
        connection = self.get_database_connection()
        if not connection:
            return False

        try:
            with connection.cursor() as cursor:
                if filename or filesize:
                    sql = """
                    UPDATE taobao_live 
                    SET status = %s, filename = %s, filesize = %s, computer = %s, update_time = CURRENT_TIMESTAMP 
                    WHERE liveId = %s
                    """
                    cursor.execute(sql, (status, filename, filesize, self.computer_name, live_id))
                else:
                    sql = """
                    UPDATE taobao_live 
                    SET status = %s, computer = %s, update_time = CURRENT_TIMESTAMP 
                    WHERE liveId = %s
                    """
                    cursor.execute(sql, (status, self.computer_name, live_id))

                connection.commit()

                status_msg = f"更新视频 {live_id} 状态为 {status}"
                self.log_info(status_msg)
                self.save_operation_log("update_video_status", "success", status_msg, live_id)

                return True
        except Exception as e:
            error_msg = f"更新视频状态失败 {live_id}: {e}"
            self.log_error(error_msg)
            self.save_operation_log("update_video_status", "failed", error_msg, live_id)
            connection.rollback()
            return False
        finally:
            connection.close()

    def download_m3u8_file(self, m3u8_url, live_id):
        """下载m3u8文件"""
        headers = {
            'accept': '*/*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'referer': 'https://pages-fast.m.taobao.com/wow/z/app/tbpc/tbzb-anchor/index?spm=a21bo.29164217.0.0.3a8cbt02bt02e5&x-ssr=true&id=1714128138',
            'sec-ch-ua': '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'script',
            'sec-fetch-mode': 'no-cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
            'origin': 'https://pages-fast.m.taobao.com',
        }

        try:
            self.log_info(f"开始下载M3U8文件: {m3u8_url}")
            response = requests.get(m3u8_url, timeout=30, headers=headers)
            response.raise_for_status()

            m3u8_content = response.text
            m3u8_file_path = os.path.join(self.download_dir, f"{live_id}.m3u8")

            with open(m3u8_file_path, 'w', encoding='utf-8') as f:
                f.write(m3u8_content)

            success_msg = f"下载m3u8文件成功: {m3u8_file_path}"
            self.log_info(success_msg)
            self.save_operation_log("download_m3u8", "success", success_msg, live_id)

            return m3u8_content, m3u8_file_path
        except Exception as e:
            error_msg = f"下载m3u8文件失败 {m3u8_url}: {e}"
            self.log_error(error_msg)
            self.save_operation_log("download_m3u8", "failed", error_msg, live_id)
            return None, None

    def parse_m3u8_content(self, m3u8_content, base_url):
        """解析m3u8内容，提取ts片段URL"""
        ts_urls = []
        lines = m3u8_content.split('\n')

        for line in lines:
            line = line.strip()
            if line and not line.startswith('#') and line.endswith('.ts'):
                # 构建完整的ts URL
                if line.startswith('http'):
                    ts_url = line
                else:
                    ts_url = urljoin(base_url, line)
                ts_urls.append(ts_url)

        self.log_info(f"解析到 {len(ts_urls)} 个ts片段")
        return ts_urls

    def download_ts_file(self, ts_url, ts_filename, live_id_folder, progress_callback=None, retry_count=3):
        """下载单个ts文件"""
        for attempt in range(retry_count):
            try:
                response = requests.get(ts_url, timeout=30)
                response.raise_for_status()

                ts_file_path = os.path.join(live_id_folder, ts_filename)
                with open(ts_file_path, 'wb') as f:
                    f.write(response.content)

                if progress_callback:
                    progress_callback(1)  # 通知进度更新

                self.log_debug(f"下载ts片段成功: {ts_filename}")
                return True
            except Exception as e:
                warning_msg = f"下载ts片段失败 {ts_filename} (尝试 {attempt + 1}/{retry_count}): {e}"
                self.log_warning(warning_msg)
                if attempt < retry_count - 1:
                    time.sleep(2)  # 等待2秒后重试

        error_msg = f"下载ts片段失败，已重试{retry_count}次: {ts_filename}"
        self.log_error(error_msg)
        return False

    def download_all_ts_files(self, ts_urls, live_id):
        """下载所有ts文件（添加tqdm进度显示）"""
        live_id_folder = os.path.join(self.download_dir, live_id)
        os.makedirs(live_id_folder, exist_ok=True)

        downloaded_count = 0
        failed_count = 0
        total_count = len(ts_urls)

        self.log_info(f"开始下载 {total_count} 个TS片段...")

        # 创建tqdm进度条
        pbar = tqdm(total=total_count, desc=f"下载TS {live_id}", unit="file")

        def update_progress(increment=1):
            nonlocal downloaded_count
            downloaded_count += increment
            pbar.update(increment)

        # 使用线程池并发下载
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_ts = {}

            for i, ts_url in enumerate(ts_urls):
                # TS文件命名前缀加上liveid
                ts_filename = f"{live_id}_{i + 1:05d}.ts"
                future = executor.submit(self.download_ts_file, ts_url, ts_filename, live_id_folder, update_progress)
                future_to_ts[future] = (ts_url, ts_filename)

            # 处理下载结果
            for future in as_completed(future_to_ts):
                ts_url, ts_filename = future_to_ts[future]
                try:
                    if not future.result():
                        failed_count += 1
                except Exception as e:
                    self.log_error(f"下载ts片段异常 {ts_filename}: {e}")
                    failed_count += 1

        pbar.close()

        # 最终结果
        result_msg = f"TS下载完成: {downloaded_count}/{total_count}, 失败: {failed_count}"
        self.log_info(result_msg)
        self.save_operation_log("download_ts_files", "success", result_msg, live_id)

        return downloaded_count, failed_count, live_id_folder

    def calculate_folder_size(self, folder_path):
        """计算文件夹总大小"""
        total_size = 0
        try:
            # 使用glob匹配所有文件，包括子目录
            for file_path in glob.glob(os.path.join(folder_path, "**"), recursive=True):
                if os.path.isfile(file_path):
                    total_size += os.path.getsize(file_path)
            return total_size
        except Exception as e:
            self.log_error(f"计算文件夹大小失败 {folder_path}: {e}")
            return 0

    def download_and_convert_video(self, video_info):
        """下载并转换单个视频"""
        live_id = video_info["liveId"]
        replay_url = video_info["replayUrl"]

        self.log_info(f"开始处理视频: {live_id}")
        self.save_operation_log("start_video_processing", "started", f"开始处理视频: {live_id}", live_id)

        try:
            # 检查任务状态，确保可以下载
            if not self.check_task_status(live_id):
                self.log_warning(f"任务状态检查失败，跳过下载: {live_id}")
                return False

            # 更新状态为下载中
            if not self.update_video_status(live_id, 2):  # 2=下载中
                self.log_error(f"无法更新视频状态为下载中: {live_id}")
                return False

            # 1. 下载m3u8文件
            self.log_info(f"步骤1/5: 下载M3U8文件...")
            m3u8_content, m3u8_file_path = self.download_m3u8_file(replay_url, live_id)
            if not m3u8_content:
                self.update_video_status(live_id, 3)  # 3=下载失败
                return False

            # 2. 解析m3u8内容，获取ts片段URL
            self.log_info(f"步骤2/5: 解析M3U8内容...")
            base_url = replay_url.rsplit('/', 1)[0] + '/'  # 获取基础URL
            ts_urls = self.parse_m3u8_content(m3u8_content, base_url)

            if not ts_urls:
                self.log_error(f"未找到ts片段: {live_id}")
                self.update_video_status(live_id, 3)
                return False

            # 3. 下载所有ts片段
            self.log_info(f"步骤3/5: 下载TS片段...")
            downloaded_count, failed_count, live_id_folder = self.download_all_ts_files(ts_urls, live_id)

            if failed_count > len(ts_urls) * 0.1:  # 如果失败率超过10%，认为下载失败
                error_msg = f"ts片段下载失败率过高: {live_id}"
                self.log_error(error_msg)
                self.update_video_status(live_id, 3)
                return False

            # 4. 计算整个文件夹的大小
            self.log_info(f"步骤4/5: 计算文件夹大小...")
            total_filesize = self.calculate_folder_size(live_id_folder)

            # 5. 更新数据库状态
            self.log_info(f"步骤5/5: 更新数据库状态...")
            self.update_video_status(
                live_id,
                1,  # 1=下载成功
                filename=live_id,  # filename写入liveid
                filesize=total_filesize  # filesize写入整个文件夹的大小
            )

            success_msg = f"视频TS片段下载完成: {live_id}, 总大小: {total_filesize} 字节, TS文件数: {downloaded_count}"
            self.log_info(success_msg)
            self.save_operation_log("video_processing", "success", success_msg, live_id)

            # 6. 清理临时文件（删除m3u8文件）
            self.cleanup_temp_files(live_id_folder, m3u8_file_path, live_id)

            return True

        except Exception as e:
            error_msg = f"处理视频异常 {live_id}: {e}"
            self.log_error(error_msg)
            self.save_operation_log("video_processing", "failed", error_msg, live_id)
            self.update_video_status(live_id, 3)
            return False

    def cleanup_temp_files(self, live_id_folder, m3u8_file_path, live_id):
        """清理临时文件，包括m3u8文件"""
        try:
            # 删除m3u8文件
            if m3u8_file_path and os.path.exists(m3u8_file_path):
                os.remove(m3u8_file_path)
                self.log_info(f"已删除M3U8文件: {m3u8_file_path}")

            cleanup_msg = f"已清理临时文件，保留TS片段"
            self.log_info(cleanup_msg)
            self.save_operation_log("cleanup_files", "success", cleanup_msg, live_id)

        except Exception as e:
            warning_msg = f"清理临时文件失败: {e}"
            self.log_warning(warning_msg)
            self.save_operation_log("cleanup_files", "warning", warning_msg, live_id)

    def run(self):
        """主运行函数"""
        self.log_info("M3U8下载器启动")
        self.save_operation_log("program_start", "success", "M3U8下载器开始运行")

        last_db_check = time.time()
        db_check_interval = 1800  # 30分钟

        while True:
            try:
                # 检查数据库连接（每30分钟一次）
                current_time = time.time()
                if current_time - last_db_check > db_check_interval:
                    if not self.test_database_connection():
                        self.wait_for_database_connection()
                    last_db_check = current_time

                # 获取待下载的视频
                pending_videos = self.get_pending_downloads()

                if not pending_videos:
                    self.log_info("没有待下载的视频，等待1分钟后重试")
                    time.sleep(60)  # 等待1分钟
                    continue

                # 串行处理视频下载，避免数据库连接冲突
                for i, video in enumerate(pending_videos):
                    self.log_info(f"处理视频 {i + 1}/{len(pending_videos)}: {video['liveId']}")
                    try:
                        success = self.download_and_convert_video(video)
                        if success:
                            success_msg = f"视频下载成功: {video['liveId']}"
                            self.log_info(success_msg)
                            self.save_operation_log("video_download", "success", success_msg, video['liveId'])
                        else:
                            error_msg = f"视频下载失败: {video['liveId']}"
                            self.log_error(error_msg)
                            self.save_operation_log("video_download", "failed", error_msg, video['liveId'])
                    except Exception as e:
                        error_msg = f"处理视频异常 {video['liveId']}: {e}"
                        self.log_error(error_msg)
                        self.save_operation_log("video_download", "error", error_msg, video['liveId'])

                self.log_info("本轮下载完成，等待10秒后继续")
                time.sleep(10)  # 等待10秒

            except KeyboardInterrupt:
                self.log_info("用户中断下载")
                self.save_operation_log("program_end", "interrupted", "用户手动中断")
                break
            except Exception as e:
                error_msg = f"运行异常: {e}"
                self.log_error(error_msg)
                self.save_operation_log("program_error", "error", error_msg)
                time.sleep(30)  # 出错后等待30秒


if __name__ == "__main__":
    # 数据库配置
    db_config = {
        'host': '192.168.10.70',  # 数据库主机
        'user': 'root',  # 数据库用户名
        'password': 'zq828079',  # 数据库密码
        'database': 'cml_data',  # 数据库名
        'charset': 'utf8mb4'
    }
    # D:\400tb淘宝 Z:\a项目\400t\淘宝
    downloader = M3U8Downloader(db_config, download_dir=r"Z:\a项目\400t\淘宝", max_workers=10)

    try:
        downloader.run()
    except KeyboardInterrupt:
        print("程序被用户中断")