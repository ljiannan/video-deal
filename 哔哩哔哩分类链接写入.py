#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/9/29 17:50
# @Author  : CUI liuliu
# @File    : 哔哩哔哩分类链接写入.py
# 文件名: add_links_to_db.py
# 描述: 用于将TXT文件中的Bilibili链接批量添加到MySQL数据库
# 版本: v1.3 (支持collector字段)

import os
import sys
import re
import mysql.connector
from mysql.connector import errorcode

# ==============================================================================
# 1. 请在这里手动输入您的TXT文件路径和分类信息
# ==============================================================================
# 示例 (Windows): TXT_FILE_PATH = r"C:\Users\YourName\Desktop\links.txt"
# 示例 (Linux/macOS): TXT_FILE_PATH = "/home/user/documents/links.txt"
# ---
TXT_FILE_PATH = r"C:\cmd_file\Integrated_Processing\yinxiao.txt"

# 全局变量 - 表名和分类信息
TABLE_NAME = 'bilibili_link_sound'
PRIMARY_CATEGORY = '音效'  # 修改为您需要的主分类
SECONDARY_CATEGORY = '自然场景'  # 修改为您需要的子分类
COLLECTOR = 'collector'  # 修改为您的收集者名称

# 目标链接格式
TARGET_FORMAT = "https://www.bilibili.com/video/{}/?spm_id_from=333.788.videopod.sections&vd_source=70d66cbca3c7b4a209f5d6d69d0fe1b1"
# ==============================================================================


# --- 数据库配置 ---
# 确保此配置与主下载程序的配置一致
MYSQL_CONFIG = {
    'host': '192.168.10.70',
    'user': 'root',
    'password': 'zq828079',
    'database': 'cml_data'
}


def clean_bilibili_url(url: str) -> str:
    """
    清洗Bilibili链接，支持多种格式：
    1. 标准BV链接
    2. 分集链接（带p参数）
    3. list链接（提取bvid和p参数）
    """
    url = url.strip()
    if not url:
        return ""

    # 处理list链接格式
    if url.startswith('https://www.bilibili.com/list/'):
        # 从list链接中提取bvid和p参数
        bvid_match = re.search(r'[?&]bvid=([^&]+)', url)
        p_match = re.search(r'[?&]p=(\d+)', url)

        if bvid_match:
            bvid = bvid_match.group(1)
            # 构建标准链接
            cleaned_url = TARGET_FORMAT.format(bvid)
            # 如果有p参数，添加到链接中
            if p_match:
                p_value = p_match.group(1)
                cleaned_url += f"&p={p_value}"
            return cleaned_url
        else:
            # 如果无法提取bvid，返回原始链接
            return url

    # 处理标准BV链接和分集链接
    pattern = r'(https?://www\.bilibili\.com/video/)?(BV[0-9A-Za-z]{10,12})([^#\s]*)?'
    match = re.search(pattern, url)

    if match:
        bv_id = match.group(2)
        params = match.group(3) or ""

        # 构建标准链接
        cleaned_url = TARGET_FORMAT.format(bv_id)

        # 检查是否有p参数
        p_match = re.search(r'[?&]p=(\d+)', params)
        if p_match:
            p_value = p_match.group(1)
            cleaned_url += f"&p={p_value}"

        return cleaned_url

    # 如果无法匹配任何模式，返回原始链接
    return url


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


def check_column_exists(conn, table_name, column_name):
    """检查表中是否已存在某列"""
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns 
            WHERE table_schema = DATABASE() 
            AND table_name = '{table_name}' 
            AND column_name = '{column_name}'
        """)
        result = cursor.fetchone()
        return result[0] > 0
    except Exception as e:
        print(f"检查列存在性时出错: {e}")
        return False
    finally:
        if cursor:
            cursor.close()


def check_index_exists(conn, table_name, index_name):
    """检查索引是否已存在"""
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) FROM information_schema.statistics 
            WHERE table_schema = DATABASE() 
            AND table_name = '{table_name}' 
            AND index_name = '{index_name}'
        """)
        result = cursor.fetchone()
        return result[0] > 0
    except Exception as e:
        print(f"检查索引存在性时出错: {e}")
        return False
    finally:
        if cursor:
            cursor.close()


def initialize_database(conn):
    """确保数据库表存在，包含分类字段和collector字段"""
    cursor = None
    try:
        cursor = conn.cursor()

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

        # 检查并添加缺失的字段（兼容旧版本MySQL）
        if not check_column_exists(conn, TABLE_NAME, 'primary_category'):
            try:
                cursor.execute(
                    f"ALTER TABLE `{TABLE_NAME}` ADD COLUMN `primary_category` VARCHAR(255) NULL COMMENT '主分类'")
                print("已添加 primary_category 字段")
            except Exception as e:
                print(f"添加primary_category字段失败: {e}")

        if not check_column_exists(conn, TABLE_NAME, 'secondary_category'):
            try:
                cursor.execute(
                    f"ALTER TABLE `{TABLE_NAME}` ADD COLUMN `secondary_category` VARCHAR(255) NULL COMMENT '子分类'")
                print("已添加 secondary_category 字段")
            except Exception as e:
                print(f"添加secondary_category字段失败: {e}")

        if not check_column_exists(conn, TABLE_NAME, 'collector'):
            try:
                cursor.execute(f"ALTER TABLE `{TABLE_NAME}` ADD COLUMN `collector` VARCHAR(255) NULL COMMENT '收集者'")
                print("已添加 collector 字段")
            except Exception as e:
                print(f"添加collector字段失败: {e}")

        # 检查并创建唯一索引（如果不存在）
        if not check_index_exists(conn, TABLE_NAME, 'url_unique_idx'):
            try:
                cursor.execute(f"CREATE UNIQUE INDEX url_unique_idx ON {TABLE_NAME} (url(255));")
                print("已创建 url_unique_idx 索引")
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_DUP_KEYNAME or err.errno == 1061:  # 索引已存在
                    print("索引 url_unique_idx 已存在")
                else:
                    raise
        else:
            print("索引 url_unique_idx 已存在")

        conn.commit()
        print(f"数据库表 '{TABLE_NAME}' 初始化成功。")
    except mysql.connector.Error as err:
        print(f"错误: 创建数据库表失败: {err}", file=sys.stderr)
        raise
    finally:
        if cursor:
            cursor.close()


def batch_insert_data(cursor, insert_query, data_to_insert, batch_size=100):
    """分批插入数据以避免锁表大小超出限制"""
    total_count = len(data_to_insert)
    added_count = 0

    for i in range(0, total_count, batch_size):
        batch = data_to_insert[i:i + batch_size]
        try:
            cursor.executemany(insert_query, batch)
            added_count += cursor.rowcount
            print(f"已处理 {min(i + batch_size, total_count)}/{total_count} 条记录")
        except mysql.connector.Error as err:
            print(f"插入批次 {i // batch_size + 1} 时出错: {err}")
            # 如果批次插入失败，尝试单条插入
            for j, data in enumerate(batch):
                try:
                    cursor.execute(insert_query, data)
                    added_count += 1
                except mysql.connector.Error as err2:
                    print(f"跳过无法插入的记录: {data[0]} - 错误: {err2}")

    return added_count


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

        # 先清洗链接，然后预处理
        cleaned_urls = [clean_bilibili_url(line) for line in raw_urls]
        urls_to_add = [preprocess_url(url) for url in cleaned_urls if preprocess_url(url)]

        if not urls_to_add:
            print("文件中未找到有效链接。")
            sys.exit(0)

        print(f"成功清洗并预处理了 {len(urls_to_add)} 个链接")

        # 显示前几个链接作为示例
        print("\n链接清洗示例:")
        for i, (raw, cleaned) in enumerate(zip(raw_urls[:3], urls_to_add[:3])):
            print(f"原始: {raw.strip()}")
            print(f"清洗后: {cleaned}")
            if i < 2:  # 只显示前3个
                print("---")

    except Exception as e:
        print(f"读取文件时出错: {e}", file=sys.stderr)
        sys.exit(1)

    # 连接数据库并插入数据
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        print("数据库连接成功。")

        initialize_database(conn)  # 确保表和索引存在

        cursor = conn.cursor()

        # 使用 INSERT IGNORE 来忽略已存在的重复链接（基于url_unique_idx唯一索引）
        # 修改：包含分类字段和collector字段
        insert_query = f"INSERT IGNORE INTO {TABLE_NAME} (url, status, primary_category, secondary_category, collector) VALUES (%s, 0, %s, %s, %s)"

        # 将链接列表转换为元组列表以供 executemany 使用，包含分类信息和collector
        data_to_insert = [(url, PRIMARY_CATEGORY, SECONDARY_CATEGORY, COLLECTOR) for url in urls_to_add]

        # 分批插入数据
        added_count = batch_insert_data(cursor, insert_query, data_to_insert)
        conn.commit()

        total_count = len(urls_to_add)
        duplicate_count = total_count - added_count

        print("\n--- 导入完成 ---")
        print(f"总共处理链接: {total_count}")
        print(f"成功新增链接: {added_count}")
        print(f"发现重复链接 (已忽略): {duplicate_count}")
        print(f"使用的分类 - 主分类: {PRIMARY_CATEGORY}, 子分类: {SECONDARY_CATEGORY}")
        print(f"收集者: {COLLECTOR}")

    except mysql.connector.Error as err:
        print(f"数据库操作失败: {err}", file=sys.stderr)
    except Exception as e:
        print(f"发生未知错误: {e}", file=sys.stderr)
    finally:
        if cursor:
            cursor.close()
            print("数据库游标已关闭。")
        if conn and conn.is_connected():
            conn.close()
            print("数据库连接已关闭。")


if __name__ == '__main__':
    main()