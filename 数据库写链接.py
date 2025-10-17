# 文件名: add_links_to_db.py
# 描述: 用于将TXT文件中的Bilibili链接批量添加到MySQL数据库
# 版本: v1.1 (手动路径版)

import os
import sys
import mysql.connector
from mysql.connector import errorcode

# ==============================================================================
# 1. 请在这里手动输入您的TXT文件路径
# ==============================================================================
# 示例 (Windows): TXT_FILE_PATH = r"C:\Users\YourName\Desktop\links.txt"
# 示例 (Linux/macOS): TXT_FILE_PATH = "/home/user/documents/links.txt"
# ---
TXT_FILE_PATH = r"Z:\personal_folder\L\b站数据库连接\自然.txt"
table_name = 'bilibili_link_ljn'
# ==============================================================================


# --- 数据库配置 ---
# 确保此配置与主下载程序的配置一致
MYSQL_CONFIG = {
    'host': '192.168.10.70',
    'user': 'root',
    'password': 'zq828079',
    'database': 'data_sql'
}


def preprocess_url(url: str) -> str:
    """
    根据要求预处理URL：
    如果链接中含有'?'，检查其前一个字符是否为'/'。如果不是，则在'?'前加上'/'。
    """
    url = url.strip()
    if not url:
        return ""

    q_index = url.find('?')
    if q_index != -1:
        if q_index > 0 and url[q_index - 1] != '/':
            return url[:q_index] + '/' + url[q_index:]
    return url


def initialize_database(conn):
    """确保数据库表存在"""
    try:
        cursor = conn.cursor()

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
          `id` INT AUTO_INCREMENT PRIMARY KEY,
          `url` TEXT NOT NULL,
          `status` INT DEFAULT 0 COMMENT '0:未下载, 1:已下载, 2:正在下载',
          `computer_name` VARCHAR(255) NULL,
          `video_title` TEXT NULL,
          `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          INDEX `status_idx` (`status`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(create_table_query)
        # 为url添加唯一索引以防重复，如果不存在的话
        try:
            cursor.execute(f"CREATE UNIQUE INDEX url_unique_idx ON {table_name} (url(255));")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_DUP_KEYNAME or err.errno == 1061:  # 索引已存在
                pass
            else:
                raise
        conn.commit()
        print(f"数据库表 '{table_name}' 初始化成功。")
    except mysql.connector.Error as err:
        print(f"错误: 创建数据库表失败: {err}", file=sys.stderr)
        sys.exit(1)
    finally:
        cursor.close()


def main():
    file_path = TXT_FILE_PATH

    if "请在这里填入" in file_path:
        print("错误: 请先在脚本顶部修改 'TXT_FILE_PATH' 变量，填入正确的TXT文件路径。", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(file_path):
        print(f"错误: 文件不存在 -> {file_path}", file=sys.stderr)
        sys.exit(1)

    # 读取并预处理链接
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_urls = f.readlines()

        urls_to_add = [preprocess_url(line) for line in raw_urls if preprocess_url(line)]
        if not urls_to_add:
            print("文件中未找到有效链接。")
            sys.exit(0)
    except Exception as e:
        print(f"读取文件时出错: {e}", file=sys.stderr)
        sys.exit(1)

    # 连接数据库并插入数据
    conn = None
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        print("数据库连接成功。")

        initialize_database(conn)  # 确保表和索引存在

        cursor = conn.cursor()

        # 使用 INSERT IGNORE 来忽略已存在的重复链接（基于url_unique_idx唯一索引）
        insert_query = "INSERT IGNORE INTO bilibili_link_ljn (url, status) VALUES (%s, 0)"

        # 将链接列表转换为元组列表以供 executemany 使用
        data_to_insert = [(url,) for url in urls_to_add]

        cursor.executemany(insert_query, data_to_insert)
        conn.commit()

        added_count = cursor.rowcount
        total_count = len(urls_to_add)
        duplicate_count = total_count - added_count

        print("\n--- 导入完成 ---")
        print(f"总共处理链接: {total_count}")
        print(f"成功新增链接: {added_count}")
        print(f"发现重复链接 (已忽略): {duplicate_count}")

    except mysql.connector.Error as err:
        print(f"数据库操作失败: {err}", file=sys.stderr)
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("数据库连接已关闭。")


if __name__ == '__main__':
    main()