import os
import shutil
import subprocess
import mysql.connector
import re
import uuid  # 用于创建唯一的临时文件夹名称
import time
import tempfile  # [新增] 用于安全地创建临时文件
from tqdm import tqdm
from mysql.connector import Error
from loguru import logger  # 导入loguru

# ==============================================================================
# --- 配置区 (保持不变) ---
# ==============================================================================
# 1. 设置包含视频链接的文本文件路径
URLS_FILE_PATH = r"Z:\personal_folder\dzy\YouTube\gyh\d08.txt"

# 2. 设置视频下载后保存的文件夹路径
SAVE_PATH = r"Z:\a项目\航拍特写\郭宇航\8.29"

# 3. 设置 FFMPEG 的绝对路径
FFMPEG_PATH = r"D:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe"

# 4. 设置包含多个Cookie的文件的绝对路径 (请确保用 '## NEW COOKIE ##' 分隔)
COOKIE_FILE_PATH = r"C:\Users\Dell\Desktop\cookie.txt"
DB_CONFIG = {
    'host': '192.168.10.70',
    'user': 'root',
    'password': 'zq828079',
    'database': 'YouTube_downloader',
}
TEMP_DOWNLOAD_PATH = r"E:\temp_youtube_downloads"
USE_ARIA2C = True
ARIA2C_PATH = r"D:\aria2-1.37.0-win-64bit-build1\aria2-1.37.0-win-64bit-build1\aria2c.exe"
MOVE_RETRY_CONFIG = {
    'attempts': 16,
    'delay_seconds': 16
}

# 配置loguru日志
logger.add(
    "youtube_downloader.log",  # 日志文件名
    rotation="10 MB",  # 日志文件大小达到10MB时轮转
    retention="30 days",  # 保留30天的日志
    level="INFO",  # 日志级别
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}"  # 日志格式
)


# ==============================================================================
# --- 功能实现区 (新增Cookie切换逻辑) ---
# ==============================================================================

def create_db_connection():
    """创建数据库连接"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG, autocommit=True, connect_timeout=10)
        if connection.is_connected():
            logger.info("成功连接到数据库。")
            return connection
    except Error as e:
        logger.error(f"连接数据库时出错: {e}")
        return None


def ensure_db_connection(connection):
    """确保数据库连接是活动的，如果不是则尝试重连"""
    try:
        connection.ping(reconnect=True, attempts=3, delay=5)
    except mysql.connector.Error as err:
        logger.error(f"数据库连接丢失且无法重连: {err}")
        logger.info("正在尝试创建一个新的数据库连接...")
        return create_db_connection()
    return connection


def create_database_if_not_exists(connection):
    """检查并创建数据库（如果不存在）"""
    cursor = connection.cursor()
    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS youtube_downloader")
        logger.info("数据库 'youtube_downloader' 已检查/创建。")
    except Error as e:
        logger.error(f"检查或创建数据库时出错: {e}")
    finally:
        cursor.close()


def create_video_table(connection):
    """创建视频表"""
    cursor = connection.cursor()
    try:
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS downloaded_videos
                       (
                           id INT AUTO_INCREMENT PRIMARY KEY,
                           video_url VARCHAR(255) NOT NULL UNIQUE,
                           download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                       )
                       """)
        logger.info("视频表创建或已存在。")
    except Error as e:
        logger.error(f"创建视频表时出错: {e}")
    finally:
        cursor.close()


def check_video_exists(connection, video_url):
    """检查视频是否已下载"""
    connection = ensure_db_connection(connection)
    if not connection or not connection.is_connected():
        logger.error("无法执行数据库查询：连接不可用。")
        return False

    cursor = connection.cursor()
    try:
        cursor.execute("SELECT id FROM downloaded_videos WHERE video_url = %s", (video_url,))
        return cursor.fetchone() is not None
    except Error as e:
        logger.error(f"查询数据库时出错: {e}")
        return False
    finally:
        cursor.close()


def insert_video_url(connection, video_url):
    """插入已下载视频的URL"""
    connection = ensure_db_connection(connection)
    if not connection or not connection.is_connected():
        logger.error("无法执行数据库插入：连接不可用。")
        return False

    cursor = connection.cursor()
    try:
        cursor.execute("INSERT INTO downloaded_videos (video_url) VALUES (%s)", (video_url,))
        return True
    except Error as e:
        logger.error(f"插入数据库时出错: {e}")
        return False
    finally:
        cursor.close()


def read_urls_from_file(file_path):
    """从指定的文本文件中读取URL列表。"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        logger.info(f"成功从 '{file_path}' 文件中加载了 {len(urls)} 个URL。")
        return urls
    except FileNotFoundError:
        logger.error(f"URL文件未找到: '{file_path}'")
        return None
    except Exception as e:
        logger.error(f"读取URL文件时发生未知错误: {e}")
        return None


# -------------------------- 新增：读取多个Cookie的函数 --------------------------
def read_cookies_from_file(file_path):
    """从指定的文本文件中读取多个Cookie，以 '## NEW COOKIE ##' 分隔。"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 使用 '## NEW COOKIE ##' 作为分隔符来切分cookie
        cookies = [cookie.strip() for cookie in content.split('## NEW COOKIE ##') if cookie.strip()]

        if not cookies:
            logger.error(f"Cookie文件 '{file_path}' 为空或格式不正确。请确保cookie以 '## NEW COOKIE ##' 分隔。")
            return None

        logger.info(f"成功从 '{file_path}' 文件中加载了 {len(cookies)} 个Cookie。")
        return cookies
    except FileNotFoundError:
        logger.error(f"Cookie文件未找到: '{file_path}'")
        return None
    except Exception as e:
        logger.error(f"读取Cookie文件时发生未知错误: {e}")
        return None
# ----------------------------------------------------------------------------


def find_executable(name, custom_path=""):
    """查找可执行文件的路径。"""
    if custom_path and os.path.exists(custom_path):
        logger.info(f"使用用户配置的 {name} 路径: {custom_path}")
        return custom_path
    executable_path = shutil.which(name)
    if executable_path:
        logger.info(f"自动找到 {name}: {executable_path}")
        return executable_path
    logger.warning(f"未在配置或系统PATH中找到 {name}。")
    return None


# -------------------------- 修改：获取视频分辨率函数，增加Cookie切换 --------------------------
def get_video_resolution(video_url, cookies_list, ffmpeg_location):
    """通过yt-dlp获取视频分辨率，轮流尝试多个Cookie，优先返回1080p及以上分辨率"""
    if not cookies_list:
        logger.error("无可用Cookie，无法获取分辨率。")
        return "获取失败", 0

    for i, cookie_data in enumerate(cookies_list):
        logger.info(f"正在使用第 {i + 1}/{len(cookies_list)} 个Cookie获取视频 '{video_url}' 的分辨率...")
        temp_cookie_file = None
        try:
            # 创建一个临时的cookie文件
            with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8', suffix='.txt') as f:
                f.write(cookie_data)
                temp_cookie_file = f.name

            command = [
                'yt-dlp', '--cookies', temp_cookie_file, '--ffmpeg-location', ffmpeg_location,
                '-F', video_url  # -F: 列出所有格式
            ]
            result = subprocess.run(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                encoding='utf-8', errors='replace', check=True
            )
            format_output = result.stdout

            # 正则匹配分辨率
            resolution_regex = re.compile(r'(\d+)x(\d+)')
            resolutions = set()
            for line in format_output.split('\n'):
                match = resolution_regex.search(line)
                if match:
                    width, height = match.groups()
                    resolutions.add((int(width), int(height)))

            if not resolutions:
                logger.warning(f"使用当前Cookie未从视频 {video_url} 中提取到分辨率信息")
                continue # 尝试下一个Cookie

            # 筛选分辨率
            target_resolutions = [(f"{w}x{h}", h) for w, h in resolutions if h >= 1080]
            if target_resolutions:
                target_resolutions.sort(key=lambda x: x[1], reverse=True)
                return target_resolutions[0][0], target_resolutions[0][1]
            else:
                all_resolutions = [(f"{w}x{h}", h) for w, h in resolutions]
                all_resolutions.sort(key=lambda x: x[1], reverse=True)
                return all_resolutions[0][0], all_resolutions[0][1]

        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.strip()
            logger.warning(f"使用第 {i + 1} 个Cookie获取分辨率失败: {error_msg.splitlines()[-1]}")
            # 如果是可识别的cookie错误，则继续循环；否则可能是视频本身的问题
            if "http error 403" in error_msg.lower() or "http error 429" in error_msg.lower():
                continue
            else:
                logger.error("获取分辨率时遇到未知错误，停止尝试。")
                break
        except Exception as e:
            logger.error(f"使用第 {i + 1} 个Cookie获取分辨率时发生未知错误: {e}")
        finally:
            if temp_cookie_file and os.path.exists(temp_cookie_file):
                os.remove(temp_cookie_file)

    logger.error(f"所有Cookie都无法获取视频 {video_url} 的分辨率。")
    return "获取失败", 0
# ----------------------------------------------------------------------------


# -------------------------- 修改：下载函数，增加Cookie切换和重试逻辑 --------------------------
def download_video_with_ytdlp(video_url, final_save_path, ffmpeg_location, cookies_list, aria2c_location,
                              connection=None):
    """
    轮流尝试多个Cookie，使用临时暂存区和aria2c高速下载并合并视频，然后移动到最终位置。
    """
    logger.info(f"开始处理视频: {video_url}")

    # 1. 使用支持多Cookie的函数获取分辨率
    logger.info("正在获取视频分辨率...")
    video_resolution_str, video_height = get_video_resolution(video_url, cookies_list, ffmpeg_location)
    logger.info(f"视频 {video_url} 的目标下载分辨率: {video_resolution_str}")

    if video_height == 0:
        logger.error(f"无法获取视频 {video_url} 的分辨率，跳过下载。")
        return
    if video_height < 1080:
        logger.warning(f"视频 {video_url} 的最高可用分辨率 ({video_resolution_str}) 低于 1080p，跳过下载。")
        return

    # 2. 准备临时工作目录
    temp_work_dir = os.path.join(TEMP_DOWNLOAD_PATH, str(uuid.uuid4()))
    os.makedirs(temp_work_dir, exist_ok=True)
    logger.info(f"使用临时目录: {temp_work_dir}")

    download_successful = False
    move_successful = False
    final_filename = ""

    # 3. 循环尝试所有Cookie进行下载
    for i, cookie_data in enumerate(cookies_list):
        logger.info(f"--> 正在尝试使用第 {i + 1}/{len(cookies_list)} 个Cookie进行下载...")
        temp_cookie_file_path = None

        try:
            # 为当前Cookie创建临时文件
            fd, temp_cookie_file_path = tempfile.mkstemp(suffix='.txt', text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as temp_f:
                temp_f.write(cookie_data)

            command = [
                'yt-dlp', '--cookies', temp_cookie_file_path, '--ffmpeg-location', ffmpeg_location,
                '--windows-filenames', '--no-mtime',
                '-o', os.path.join(temp_work_dir, '%(title)s - %(id)s.%(ext)s'),
                '-f', "bestvideo+bestaudio/best[height>=1080]",
                '--merge-output-format', 'mp4',
            ]
            if aria2c_location:
                command.extend(['--downloader', 'aria2c'])
            command.append(video_url)

            logger.info(f"执行命令: {' '.join(command)}")
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       bufsize=1, universal_newlines=True, encoding='utf-8', errors='replace')

            progress_regex = re.compile(r'\[download\]\s+(?P<percent>[\d\.]+)%')
            speed_regex = re.compile(r'\bat\s+(?P<speed>[\d\.]+[KMG]iB/s)\b')
            
            with tqdm(total=100, unit='%', ncols=120, desc=f"下载进度 (Cookie #{i+1})", ascii="->") as pbar:
                current_speed = "--"
                for line in process.stdout:
                    percent_match = progress_regex.search(line)
                    if percent_match: pbar.update(float(percent_match.group('percent')) - pbar.n)
                    speed_match = speed_regex.search(line)
                    if speed_match: current_speed = speed_match.group('speed')
                    pbar.set_postfix({"网速": current_speed})

                    if '[Merger] Merging formats into' in line:
                        try: final_filename = os.path.basename(line.split('"')[1])
                        except IndexError: pass
                    elif '[download] Destination:' in line:
                        try:
                            temp_fn = os.path.basename(line.strip().split(': ')[1])
                            if temp_fn.endswith(('.mp4', '.mkv', '.webm')): final_filename = temp_fn
                        except IndexError: pass

            return_code = process.wait()
            stderr_output = process.stderr.read()

            if return_code == 0:
                logger.success(f"使用第 {i + 1} 个Cookie下载成功。")
                download_successful = True
                break  # 下载成功，跳出Cookie循环
            else:
                logger.warning(f"使用第 {i + 1} 个Cookie下载失败 (返回码: {return_code})。")
                error_str = stderr_output.lower()
                if "http error 429" in error_str or "http error 403" in error_str or "unavailable" in error_str:
                    logger.info("此Cookie可能已失效或被限制，清理临时文件并尝试下一个。")
                    # 清理当前尝试产生的垃圾文件
                    for item in os.listdir(temp_work_dir): os.remove(os.path.join(temp_work_dir, item))
                    continue  # 尝试下一个Cookie
                else:
                    logger.error(f"发生未知或不可恢复的错误，停止尝试此视频。STDERR:\n{stderr_output}")
                    break  # 错误与Cookie无关，停止重试

        finally:
            if temp_cookie_file_path and os.path.exists(temp_cookie_file_path):
                os.remove(temp_cookie_file_path)

    # 4. 下载后的处理逻辑
    try:
        if download_successful:
            if not final_filename:
                # 如果从日志中未解析出文件名，则在目录中查找
                found_files = [f for f in os.listdir(temp_work_dir) if f.endswith('.mp4')]
                if found_files:
                    final_filename = found_files[0]
                    logger.info(f"从目录中找到最终文件: {final_filename}")
                else:
                    logger.error("下载成功，但在临时目录中未找到 .mp4 文件。")
                    return

            temp_file_path = os.path.join(temp_work_dir, final_filename)
            final_file_path = os.path.join(final_save_path, final_filename)

            # 移动文件，带重试
            logger.info(f"正在移动文件到最终位置: {final_file_path}")
            for attempt in range(MOVE_RETRY_CONFIG['attempts']):
                try:
                    shutil.move(temp_file_path, final_file_path)
                    move_successful = True
                    logger.success("文件移动完成。")
                    break
                except (IOError, OSError) as move_error:
                    logger.warning(f"移动文件失败 (尝试 {attempt + 1}/{MOVE_RETRY_CONFIG['attempts']})。错误: {move_error}")
                    if attempt < MOVE_RETRY_CONFIG['attempts'] - 1:
                        time.sleep(MOVE_RETRY_CONFIG['delay_seconds'])
                    else:
                        logger.error("达到最大重试次数，移动文件最终失败。")

            # 如果文件移动成功，则更新数据库
            if move_successful and connection:
                if insert_video_url(connection, video_url):
                    logger.info(f"已将 {video_url} 标记为已下载。")
                else:
                    logger.warning(f"文件已移动，但数据库记录失败。URL: {video_url}")

        else:
            logger.error(f"尝试所有 {len(cookies_list)} 个Cookie后，下载 {video_url} 仍失败。")

    except Exception as e:
        logger.error(f"下载后处理过程中发生未知错误: {e}")
    finally:
        # 清理逻辑：移动成功 或 整个下载过程失败时，都清理临时目录
        if (move_successful or not download_successful) and os.path.exists(temp_work_dir):
            try:
                shutil.rmtree(temp_work_dir)
                logger.info(f"已清理临时目录: {temp_work_dir}")
            except OSError as e:
                logger.error(f"清理临时目录 {temp_work_dir} 失败: {e}")

# --- 主执行区 (修改了Cookie加载和函数调用) ---
if __name__ == "__main__":
    logger.info("YouTube下载器启动")

    ffmpeg_exec_path = find_executable("ffmpeg", FFMPEG_PATH)
    if not ffmpeg_exec_path:
        logger.error("FFmpeg 未找到，请配置或安装。")
        exit(1)

    ytdlp_exec_path = find_executable("yt-dlp")
    if not ytdlp_exec_path:
        logger.error("'yt-dlp' 未找到，请安装。")
        exit(1)

    aria2c_exec_path = None
    if USE_ARIA2C:
        aria2c_exec_path = find_executable("aria2c", ARIA2C_PATH)

    # [修改] 从文件加载多个Cookie
    all_cookies = read_cookies_from_file(COOKIE_FILE_PATH)
    if not all_cookies:
        logger.error("未能加载任何Cookie，程序终止。")
        exit(1)

    video_urls_to_download = read_urls_from_file(URLS_FILE_PATH)
    if video_urls_to_download is None:
        exit(1)

    db_connection = create_db_connection()
    if not db_connection:
        exit(1)

    # 数据库初始化
    conn_for_setup = ensure_db_connection(db_connection)
    if conn_for_setup and conn_for_setup.is_connected():
        create_database_if_not_exists(conn_for_setup)
        try:
            conn_for_setup.database = DB_CONFIG['database']
        except mysql.connector.Error as err:
            logger.error(f"无法切换到数据库 '{DB_CONFIG['database']}': {err}")
            exit(1)
        create_video_table(conn_for_setup)
    else:
        logger.error("无法建立初始数据库连接进行设置。")
        exit(1)

    try:
        os.makedirs(os.path.abspath(SAVE_PATH), exist_ok=True)
        os.makedirs(os.path.abspath(TEMP_DOWNLOAD_PATH), exist_ok=True)
        logger.info(f"最终视频将保存到: {os.path.abspath(SAVE_PATH)}")
        logger.info(f"临时文件将存于: {os.path.abspath(TEMP_DOWNLOAD_PATH)}")
    except OSError as e:
        logger.error(f"创建文件夹时出错: {e}")
        exit(1)

    if not video_urls_to_download:
        logger.warning("URL文件为空。")
    else:
        logger.info(f"准备下载 {len(video_urls_to_download)} 个视频...")
        for i, url in enumerate(video_urls_to_download):
            logger.info(f"======== 开始处理第 {i + 1} / {len(video_urls_to_download)} 个视频 ========")
            if not check_video_exists(db_connection, url):
                # [修改] 将Cookie列表传递给下载函数
                download_video_with_ytdlp(url, os.path.abspath(SAVE_PATH), ffmpeg_exec_path,
                                          all_cookies, aria2c_exec_path, connection=db_connection)
            else:
                logger.info(f"视频 {url} 已经下载过，跳过。")

        logger.info("======== 所有下载任务已处理完毕。 ========")

    if db_connection and db_connection.is_connected():
        db_connection.close()
        logger.info("数据库连接已关闭。")