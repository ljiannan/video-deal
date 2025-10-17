import os
import shutil
import subprocess
import time
import logging
import pymysql
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


class FileLockManager:
    """
    文件锁管理器 - 防止文件被多个设备同时读取
    """

    def __init__(self, db_config, timeout_minutes=30):
        self.db_config = db_config
        self.computer_name = socket.gethostname()
        self.timeout = timeout_minutes * 60

        # 初始化时创建表
        self._create_table()

    def _get_connection(self):
        """获取数据库连接"""
        try:
            return pymysql.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4',
                autocommit=False
            )
        except Exception as e:
            logging.error(f"数据库连接失败: {e}")
            raise

    def _create_table(self):
        """创建文件锁表"""
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                sql = """
                CREATE TABLE IF NOT EXISTS file_lock (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    computer_name VARCHAR(255) NOT NULL,
                    file_name VARCHAR(255) NOT NULL,
                    lock_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    is_locked TINYINT(1) DEFAULT 1,
                    UNIQUE KEY unique_file (file_name, is_locked)
                )
                """
                cursor.execute(sql)

                # 创建索引提高查询性能
                index_sql = [
                    "CREATE INDEX IF NOT EXISTS idx_file_name ON file_lock(file_name)",
                    "CREATE INDEX IF NOT EXISTS idx_computer_name ON file_lock(computer_name)",
                    "CREATE INDEX IF NOT EXISTS idx_lock_time ON file_lock(lock_time)"
                ]

                for sql_stmt in index_sql:
                    try:
                        cursor.execute(sql_stmt)
                    except Exception:
                        pass  # 索引可能已存在

            connection.commit()
            logging.info("文件锁表创建/检查完成")
        except Exception as e:
            logging.error(f"创建表失败: {e}")
            connection.rollback()
            raise
        finally:
            connection.close()

    def _cleanup_expired_locks(self, file_name=None):
        """清理过期的锁"""
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                if file_name:
                    sql = """
                    UPDATE file_lock SET is_locked = 0 
                    WHERE file_name = %s AND is_locked = 1 
                    AND lock_time < DATE_SUB(NOW(), INTERVAL %s SECOND)
                    """
                    cursor.execute(sql, (file_name, self.timeout))
                else:
                    sql = """
                    UPDATE file_lock SET is_locked = 0 
                    WHERE is_locked = 1 
                    AND lock_time < DATE_SUB(NOW(), INTERVAL %s SECOND)
                    """
                    cursor.execute(sql, (self.timeout,))

                affected_rows = cursor.rowcount
                if affected_rows > 0:
                    logging.debug(f"清理了 {affected_rows} 个过期的锁")

            connection.commit()
        except Exception as e:
            logging.error(f"清理过期锁失败: {e}")
            connection.rollback()
        finally:
            connection.close()

    def acquire_lock(self, file_name):
        """
        尝试获取文件锁

        Args:
            file_name (str): 文件名

        Returns:
            bool: 是否成功获取锁
        """
        # 先清理过期的锁
        self._cleanup_expired_locks(file_name)

        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                # 检查文件是否已被其他电脑锁定
                check_sql = """
                SELECT computer_name FROM file_lock 
                WHERE file_name = %s AND is_locked = 1
                """
                cursor.execute(check_sql, (file_name,))
                result = cursor.fetchone()

                if result:
                    if result[0] == self.computer_name:
                        # 自己已经锁定，更新锁定时间
                        update_sql = """
                        UPDATE file_lock SET lock_time = NOW() 
                        WHERE file_name = %s AND computer_name = %s AND is_locked = 1
                        """
                        cursor.execute(update_sql, (file_name, self.computer_name))
                        connection.commit()
                        return True
                    else:
                        # 被其他电脑锁定
                        logging.warning(f"文件 {file_name} 正被电脑 {result[0]} 使用")
                        return False
                else:
                    # 没有锁定，尝试获取锁
                    try:
                        insert_sql = """
                        INSERT INTO file_lock (computer_name, file_name, lock_time, is_locked) 
                        VALUES (%s, %s, NOW(), 1)
                        """
                        cursor.execute(insert_sql, (self.computer_name, file_name))
                        connection.commit()
                        logging.info(f"成功锁定文件: {file_name}")
                        return True
                    except pymysql.IntegrityError:
                        # 插入失败，说明有其他进程刚刚锁定了文件
                        connection.rollback()
                        logging.warning(f"文件 {file_name} 已被其他进程锁定")
                        return False
        except Exception as e:
            logging.error(f"获取文件锁失败 {file_name}: {e}")
            connection.rollback()
            return False
        finally:
            connection.close()

    def wait_for_lock(self, file_name, max_wait_seconds=10, check_interval=5):
        """
        等待文件锁释放

        Args:
            file_name (str): 文件名
            max_wait_seconds (int): 最大等待时间（秒）
            check_interval (int): 检查间隔（秒）

        Returns:
            bool: 是否成功获取锁
        """
        start_time = time.time()
        attempt = 0

        while time.time() - start_time < max_wait_seconds:
            attempt += 1
            if self.acquire_lock(file_name):
                logging.info(f"成功获取文件锁 {file_name} (尝试 {attempt} 次)")
                return True

            remaining_time = max_wait_seconds - (time.time() - start_time)
            logging.info(f"等待文件 {file_name} 解锁... 剩余时间: {remaining_time:.1f}秒")
            time.sleep(check_interval)

        logging.error(f"等待文件锁超时: {file_name}")
        return False

    def release_lock(self, file_name):
        """
        释放文件锁

        Args:
            file_name (str): 文件名
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                sql = """
                UPDATE file_lock SET is_locked = 0 
                WHERE file_name = %s AND computer_name = %s AND is_locked = 1
                """
                cursor.execute(sql, (file_name, self.computer_name))

                if cursor.rowcount > 0:
                    connection.commit()
                    logging.debug(f"释放文件锁: {file_name}")
                else:
                    logging.warning(f"未找到活跃的文件锁: {file_name}")
        except Exception as e:
            logging.error(f"释放文件锁失败 {file_name}: {e}")
            connection.rollback()
        finally:
            connection.close()


def setup_logger():
    """配置日志记录器，使其同时输出到控制台和文件"""
    logger = logging.getLogger('video_mover')
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler('video_mover.log', mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def check_disk_space(destination_path, required_space_tb=1):
    """检查目标路径所在磁盘的可用空间是否大于等于指定TB"""
    logger = logging.getLogger('video_mover')
    required_space = required_space_tb * 1024 ** 4  # 转换为字节 (1TB = 1024^4 bytes)
    try:
        # 获取磁盘根路径
        if os.name == 'nt':  # Windows系统
            drive, _ = os.path.splitdrive(destination_path)
            if not drive:
                # 如果没有驱动器（如相对路径），使用当前目录的驱动器
                drive = os.path.splitdrive(os.getcwd())[0]
            disk_path = drive + '\\'  # 确保路径格式正确，如'F:\\'
        else:  # 类Unix系统
            disk_path = '/'  # 根目录

        # 获取磁盘使用情况
        du = shutil.disk_usage(disk_path)
        available_space = du.free
        available_tb = available_space / (1024 ** 4)  # 转换为TB

        logger.info(f"目标磁盘 '{disk_path}' 可用空间: {available_tb:.2f} TB")
        return available_space >= required_space
    except Exception as e:
        logger.error(f"检查磁盘空间时出错: {e}")
        return False  # 出错时视为空间不足


def get_unique_destination_path(destination_path):
    """
    (Used only when the destination file is valid)
    在文件名后附加一个计数器，例如 ' (1)', ' (2)'，直到找到一个唯一的文件名。
    """
    directory, full_filename = os.path.split(destination_path)
    base_name, extension = os.path.splitext(full_filename)
    counter = 1
    while True:
        new_filename = f"{base_name} ({counter}){extension}"
        new_destination_path = os.path.join(directory, new_filename)
        if not os.path.exists(new_destination_path):
            return new_destination_path
        counter += 1


def check_video_integrity(file_path, ffprobe_executable_path, logger):
    """使用指定路径的 ffprobe 检查视频文件的完整性"""
    try:
        command = [ffprobe_executable_path, '-v', 'error', '-i', file_path]
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError:
        # 检查失败，非错误
        return False
    except Exception as e:
        logger.error(f"用 ffprobe 检查 '{os.path.basename(file_path)}' 时发生未知错误: {e}")
        return False


def copy_with_progress(source_path, destination_path, file_description):
    """使用 tqdm 显示进度条来复制文件"""
    file_size = os.path.getsize(source_path)
    chunk_size = 1024 * 1024  # 1MB

    with open(source_path, 'rb') as src_file, open(destination_path, 'wb') as dst_file:
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=file_description, leave=False) as pbar:
            while True:
                chunk = src_file.read(chunk_size)
                if not chunk:
                    break
                dst_file.write(chunk)
                pbar.update(len(chunk))


def process_single_video(source_path, destination_folder, source_folder, ffprobe_path, logger):
    """
    处理单个视频的完整流程（无文件锁版本）：
    1. 检查源文件完整性
    2. 保留原路径结构，构建目标路径并创建目录
    3. 检查目标文件是否存在，根据完整性决定替换或重命名
    4. 带进度复制
    5. 删除源文件
    """
    # 计算源文件相对于源文件夹的相对路径（保留目录结构）
    rel_path = os.path.relpath(source_path, source_folder)
    file_name = os.path.basename(source_path)
    full_relative_path = rel_path  # 包含完整相对路径的文件名

    try:
        logger.info(f"开始处理: {full_relative_path}")

        # 步骤 1: 检查源文件完整性
        if not check_video_integrity(source_path, ffprobe_path, logger):
            logger.warning(f"源文件 '{full_relative_path}' 检查未通过，跳过。")
            return f"跳过 (源文件检查未通过): {full_relative_path}"

        # 步骤 2: 构建带目录结构的目标路径并创建目录
        destination_path = os.path.join(destination_folder, rel_path)
        # 创建目标路径的父目录（确保目录结构存在）
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)

        # 步骤 3: 处理目标文件存在的情况
        if os.path.exists(destination_path):
            logger.info(f"目标文件 '{full_relative_path}' 已存在。正在检查其完整性...")
            is_destination_ok = check_video_integrity(destination_path, ffprobe_path, logger)

            if not is_destination_ok:
                logger.warning(f"目标文件 '{full_relative_path}' 已损坏或不完整。将被新文件替换。")
                # 尝试删除旧的损坏文件
                try:
                    os.remove(destination_path)
                except OSError as e:
                    logger.error(f"无法删除损坏的目标文件 '{destination_path}': {e}。跳过此文件。")
                    return f"失败 (无法删除损坏的目标): {full_relative_path}"
            else:
                logger.info(f"目标文件 '{full_relative_path}' 完好。将为新文件添加后缀。")
                destination_path = get_unique_destination_path(destination_path)
                new_full_path = os.path.relpath(destination_path, destination_folder)
                logger.info(f"新文件将被保存为 '{new_full_path}'")

        # 步骤 4: 带进度条地复制文件
        final_relative_path = os.path.relpath(destination_path, destination_folder)
        copy_with_progress(source_path, destination_path, final_relative_path)

        # 验证复制完整性
        if os.path.getsize(source_path) != os.path.getsize(destination_path):
            raise IOError("复制失败：源文件和目标文件大小不匹配。")

        # 步骤 5: 复制成功后删除源文件
        os.remove(source_path)
        logger.info(f"成功移动: {final_relative_path}")
        return f"成功: {final_relative_path}"

    except Exception as e:
        logger.error(f"处理 '{full_relative_path}' 时发生严重错误: {e}")
        # 清理可能已创建的部分文件
        if 'destination_path' in locals() and os.path.exists(destination_path):
            try:
                os.remove(destination_path)
            except:
                pass
        return f"失败: {full_relative_path}"


def process_single_video_with_lock(source_path, destination_folder, source_folder, ffprobe_path, lock_manager, logger):
    """
    处理单个视频的完整流程（带文件锁版本）：
    1. 获取文件锁
    2. 检查源文件完整性
    3. 保留原路径结构，构建目标路径并创建目录
    4. 检查目标文件是否存在，根据完整性决定替换或重命名
    5. 带进度复制
    6. 删除源文件
    7. 释放文件锁
    """
    # 计算源文件相对于源文件夹的相对路径（保留目录结构）
    rel_path = os.path.relpath(source_path, source_folder)
    file_name = os.path.basename(source_path)
    full_relative_path = rel_path  # 包含完整相对路径的文件名

    try:
        logger.info(f"开始处理: {full_relative_path}")

        # 步骤 1: 获取文件锁
        if not lock_manager.wait_for_lock(source_path, max_wait_seconds=60):
            logger.warning(f"无法获取文件锁，跳过: {full_relative_path}")
            return f"跳过 (无法获取文件锁): {full_relative_path}"

        # 步骤 2: 检查源文件完整性
        if not check_video_integrity(source_path, ffprobe_path, logger):
            logger.warning(f"源文件 '{full_relative_path}' 检查未通过，跳过。")
            lock_manager.release_lock(source_path)  # 释放锁
            return f"跳过 (源文件检查未通过): {full_relative_path}"

        # 步骤 3: 构建带目录结构的目标路径并创建目录
        destination_path = os.path.join(destination_folder, rel_path)
        # 创建目标路径的父目录（确保目录结构存在）
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)

        # 步骤 4: 处理目标文件存在的情况
        if os.path.exists(destination_path):
            logger.info(f"目标文件 '{full_relative_path}' 已存在。正在检查其完整性...")
            is_destination_ok = check_video_integrity(destination_path, ffprobe_path, logger)

            if not is_destination_ok:
                logger.warning(f"目标文件 '{full_relative_path}' 已损坏或不完整。将被新文件替换。")
                # 尝试删除旧的损坏文件
                try:
                    os.remove(destination_path)
                except OSError as e:
                    logger.error(f"无法删除损坏的目标文件 '{destination_path}': {e}。跳过此文件。")
                    lock_manager.release_lock(source_path)  # 释放锁
                    return f"失败 (无法删除损坏的目标): {full_relative_path}"
            else:
                logger.info(f"目标文件 '{full_relative_path}' 完好。将为新文件添加后缀。")
                destination_path = get_unique_destination_path(destination_path)
                new_full_path = os.path.relpath(destination_path, destination_folder)
                logger.info(f"新文件将被保存为 '{new_full_path}'")

        # 步骤 5: 带进度条地复制文件
        final_relative_path = os.path.relpath(destination_path, destination_folder)
        copy_with_progress(source_path, destination_path, final_relative_path)

        # 验证复制完整性
        if os.path.getsize(source_path) != os.path.getsize(destination_path):
            raise IOError("复制失败：源文件和目标文件大小不匹配。")

        # 步骤 6: 复制成功后删除源文件
        os.remove(source_path)
        logger.info(f"成功移动: {final_relative_path}")

        # 步骤 7: 释放文件锁
        lock_manager.release_lock(source_path)
        return f"成功: {final_relative_path}"

    except Exception as e:
        logger.error(f"处理 '{full_relative_path}' 时发生严重错误: {e}")
        # 清理可能已创建的部分文件
        if 'destination_path' in locals() and os.path.exists(destination_path):
            try:
                os.remove(destination_path)
            except:
                pass
        # 确保释放文件锁
        lock_manager.release_lock(source_path)
        return f"失败: {full_relative_path}"


def move_videos_concurrently(source_folder, destination_folder, ffprobe_path, max_workers, logger):
    """
    使用线程池并发地移动视频，保持原路径结构（无文件锁版本）。
    处理完一批后，持续监控源文件夹，每60秒检查一次。
    新增功能：检查目标磁盘空间，不足1TB时暂停移动
    """
    video_extensions = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.mpeg', '.mpg']

    while True:
        tasks_to_process = []
        # 遍历源文件夹下所有视频文件（包括子目录）
        for root, dirs, files in os.walk(source_folder):
            for file in files:
                if any(file.lower().endswith(ext) for ext in video_extensions):
                    file_path = os.path.join(root, file)
                    tasks_to_process.append(file_path)

        if not tasks_to_process:
            logger.info("--- 源文件夹中无视频文件。将在 60 秒后重新扫描... ---")
            time.sleep(60)
            continue

        # 检查目标磁盘空间是否足够（至少1TB）
        if not check_disk_space(destination_folder):
            logger.warning(f"--- 目标磁盘可用空间不足1TB，暂停移动操作。将在60秒后重新检查... ---")
            time.sleep(60)
            continue

        logger.info(f"--- 新一轮扫描发现 {len(tasks_to_process)} 个视频文件，开始处理... ---")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务时传入源文件夹参数，用于计算相对路径
            futures = {
                executor.submit(
                    process_single_video,
                    task,
                    destination_folder,
                    source_folder,
                    ffprobe_path,
                    logger
                ): task for task in tasks_to_process
            }

            for future in as_completed(futures):
                result = future.result()  # 结果处理已在process_single_video中通过日志完成

        logger.info("--- 本轮所有任务已处理完毕。即将开始新一轮扫描... ---")


def move_videos_concurrently_with_lock(source_folder, destination_folder, ffprobe_path, max_workers, lock_manager,
                                       logger):
    """
    使用线程池并发地移动视频，保持原路径结构，并加入文件锁管理。
    处理完一批后，持续监控源文件夹，每60秒检查一次。
    新增功能：检查目标磁盘空间，不足1TB时暂停移动
    """
    video_extensions = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.mpeg', '.mpg']

    while True:
        tasks_to_process = []
        # 遍历源文件夹下所有视频文件（包括子目录）
        for root, dirs, files in os.walk(source_folder):
            for file in files:
                if any(file.lower().endswith(ext) for ext in video_extensions):
                    file_path = os.path.join(root, file)
                    tasks_to_process.append(file_path)

        if not tasks_to_process:
            logger.info("--- 源文件夹中无视频文件。将在 60 秒后重新扫描... ---")
            time.sleep(60)
            continue

        # 检查目标磁盘空间是否足够（至少1TB）
        if not check_disk_space(destination_folder):
            logger.warning(f"--- 目标磁盘可用空间不足1TB，暂停移动操作。将在60秒后重新检查... ---")
            time.sleep(60)
            continue

        logger.info(f"--- 新一轮扫描发现 {len(tasks_to_process)} 个视频文件，开始处理... ---")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务时传入源文件夹参数，用于计算相对路径
            futures = {
                executor.submit(
                    process_single_video_with_lock,
                    task,
                    destination_folder,
                    source_folder,
                    ffprobe_path,
                    lock_manager,
                    logger
                ): task for task in tasks_to_process
            }

            for future in as_completed(futures):
                result = future.result()  # 结果处理已在process_single_video中通过日志完成

        logger.info("--- 本轮所有任务已处理完毕。即将开始新一轮扫描... ---")


if __name__ == "__main__":
    logger = setup_logger()

    # --- 请在这里修改为您自己的路径和配置 ---
    ffprobe_path = r"C:\ffmpeg-master-latest-win64-gpl\bin\ffprobe.exe"
    source_directory = r"Z:\a项目\航拍特写\一体化成品"
    destination_directory = r"D:\航拍特写"
    MAX_CONCURRENT_MOVES = 3

    # 数据库配置 - 文件锁功能
    db_config = {
        'host': '192.168.10.70',  # 数据库主机
        'user': 'root',  # 数据库用户名
        'password': 'zq828079',  # 数据库密码
        'database': 'video_process'  # 数据库名
    }
    # --- 配置结束 ---

    # 初始化文件锁管理器
    lock_manager = None
    try:
        lock_manager = FileLockManager(db_config, timeout_minutes=30)
        logger.info("文件锁管理器初始化成功")
    except Exception as e:
        logger.error(f"文件锁管理器初始化失败: {e}")
        logger.info("将继续运行但不使用文件锁功能")

    if not os.path.isfile(ffprobe_path):
        logger.critical(f"致命错误：在指定路径找不到 ffprobe.exe 文件: {ffprobe_path}")
        exit()

    logger.info("脚本启动，开始持续监控...")
    logger.info(f"使用 ffprobe 路径: {ffprobe_path}")
    logger.info(f"源文件夹: {source_directory}")
    logger.info(f"目标文件夹: {destination_directory}")
    logger.info(f"最大并发数: {MAX_CONCURRENT_MOVES}")
    logger.info(f"使用文件锁: {'是' if lock_manager else '否'}")

    if not os.path.exists(destination_directory):
        logger.info(f"目标文件夹不存在，正在创建: {destination_directory}")
        os.makedirs(destination_directory)

    try:
        if lock_manager:
            move_videos_concurrently_with_lock(source_directory, destination_directory, ffprobe_path,
                                               MAX_CONCURRENT_MOVES, lock_manager, logger)
        else:
            # 使用无锁版本
            move_videos_concurrently(source_directory, destination_directory, ffprobe_path, MAX_CONCURRENT_MOVES,
                                     logger)
    except KeyboardInterrupt:
        logger.info("脚本被用户手动中断。正在退出...")
        exit()