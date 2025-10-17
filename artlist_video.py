#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/3/17 9:54
# @Author  : CUI liuliu
# @File    : artlist_video01.py
import os
import time
import requests
import mysql.connector
from mysql.connector import Error
import shutil
import subprocess
import logging
from tqdm import tqdm
from datetime import datetime
import random
import uuid
from concurrent.futures import ThreadPoolExecutor

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("artlist_download.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    'user': 'root',
    'password': 'zq828079',
    'host': '192.168.10.70',
    'database': 'yunjing',
    'raise_on_warnings': True
}

# 文件存储路径配置
SAVE_DIR = r"Z:\数据采集组\sport\器械健身-小器械训练"
os.makedirs(SAVE_DIR, exist_ok=True)

# 自定义ID前缀
DATAL_ID_PREFIX = "大12"
page_start = 7

# 添加关键词组配置
KEYWORDS = [
    "Stability Ball Crunch",
]

# 添加随机User-Agent池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 OPR/120.0.0.0"
]

# 添加请求重试装饰器
def retry_on_failure(max_retries=3, delay=5):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"最终尝试失败: {str(e)}")
                        raise
                    logger.warning(f"第{attempt + 1}次尝试失败，等待{delay}秒后重试: {str(e)}")
                    time.sleep(delay + random.uniform(0, 2))  # 添加随机延迟
            return None
        return wrapper
    return decorator

def get_random_headers():
    """生成随机请求头"""
    return {
        "Content-Type": "application/json",
        "Origin": "https://artlist.io",
        "Referer": "https://artlist.io/",
        "User-Agent": random.choice(USER_AGENTS),
        "X-Anonymous-Id": str(uuid.uuid4()),  # 随机生成
        "X-User-Status": "guest",
        "X-Visitor-Id": str(uuid.uuid4()),  # 随机生成
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Sec-Ch-Ua": '"Chromium";v="134", "Not:A-Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }

def create_table():
    """创建数据表"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS artlist_video_sport (
            id VARCHAR(255) PRIMARY KEY,
            clip_name VARCHAR(255),
            original_url VARCHAR(512) UNIQUE,
            insert_time DATETIME DEFAULT CURRENT_TIMESTAMP,
            download_state BOOLEAN DEFAULT FALSE,
            save_path VARCHAR(512),
            datal_id VARCHAR(255)
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("数据表创建/验证成功")
    except Error as e:
        # 捕获特定的数据库错误
        if e.errno == 1050:  # 表已存在错误码
            logger.warning("数据表已存在，无需创建")
        else:
            logger.error(f"数据库错误: {e}")
    except Exception as e:
        logger.error(f"其他错误: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def check_download_state(video_id):
    """检查是否已下载"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        check_query = """
        SELECT download_state FROM artlist_video_sport 
        WHERE id = %s
        """
        cursor.execute(check_query, (video_id,))
        result = cursor.fetchone()

        return result[0] if result else False

    except Error as e:
        logger.error(f"状态检查失败: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def insert_video_record(video_data):
    """插入视频记录"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_query = """
               INSERT INTO artlist_video_sport
                   (id, clip_name, original_url)
               VALUES
                   (%s, %s, %s)
               AS alias
               ON DUPLICATE KEY UPDATE
                   clip_name = alias.clip_name,
                   original_url = alias.original_url
               """
        cursor.execute(insert_query, (
            video_data['id'],
            video_data['clipName'],
            video_data['clipPath']
        ))
        conn.commit()
        logger.info(f"已插入记录: {video_data['id']}")
        return cursor.lastrowid

    except Error as e:
        logger.error(f"数据库插入错误: {e}")
        return None
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def update_video_record(video_id, save_path):
    """更新下载状态"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 生成自定义ID：前缀 + 时间戳 + 随机后缀
        custom_id = f"{DATAL_ID_PREFIX}"

        update_query = """
        UPDATE artlist_video_sport 
        SET download_state = TRUE,
            save_path = %s,
            datal_id = %s
        WHERE id = %s
        """
        cursor.execute(update_query, (save_path, custom_id, video_id))
        conn.commit()
        logger.info(f"已更新记录: {video_id}")

    except Error as e:
        logger.error(f"数据库更新错误: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def process_m3u8(video_item,keyword):
    """处理m3u8下载和转换"""
    try:
        video_id = video_item['id']
        logger.info(f"开始处理视频: {video_id}")

        # 下载原始m3u8文件
        original_url = video_item['clipPath']
        logger.debug(f"下载原始m3u8: {original_url}")

        response = requests.get(original_url)
        response.raise_for_status()

        # 解析最高画质片段
        lines = response.text.split('\n')
        last_stream = None
        for i, line in enumerate(lines):
            if "#EXT-X-STREAM-INF" in line:
                last_stream = lines[i + 1].strip()

        if not last_stream:
            raise ValueError("未找到有效视频流")

        # 构建新URL
        base_url = '/'.join(original_url.split('/')[:-1])
        new_url = f"{base_url}/{last_stream}"
        logger.debug(f"解析到高清流地址: {new_url}")

        out_path=os.path.join(SAVE_DIR,keyword)
        os.makedirs(out_path,exist_ok=True)

        # 下载实际m3u8
        m3u8_path = os.path.join(out_path, f"{video_id}.m3u8")
        response = requests.get(new_url)
        response.raise_for_status()
        with open(m3u8_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"已下载m3u8文件: {m3u8_path}")

        # 解析m3u8文件，获取所有ts片段
        ts_files = []
        with open(m3u8_path, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if line.strip().startswith('#EXTINF'):
                    # 下一行是ts文件
                    if i + 1 < len(lines):
                        ts_file = lines[i + 1].strip()
                        if ts_file:
                            ts_files.append(ts_file)

        if not ts_files:
            raise ValueError("未找到有效的ts片段")

        # 创建视频文件夹
        video_save_dir = os.path.join(out_path, f"{video_id}")
        os.makedirs(video_save_dir, exist_ok=True)

        # 添加并发下载ts片段
        def download_ts_segment(ts_info):
            ts_url, ts_path = ts_info
            for _ in range(3):  # 重试机制
                try:
                    response = requests.get(ts_url, headers=get_random_headers(), stream=True)
                    response.raise_for_status()
                    with open(ts_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    return True
                except Exception as e:
                    logger.warning(f"下载ts片段失败，重试: {ts_url}, 错误: {e}")
                    time.sleep(random.uniform(1, 3))
            return False

        # 并发下载ts片段
        ts_download_tasks = [(f"{base_url}/{ts_file}", os.path.join(video_save_dir, ts_file)) 
                           for ts_file in ts_files]
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(tqdm(
                executor.map(download_ts_segment, ts_download_tasks),
                total=len(ts_download_tasks),
                desc="下载视频片段"
            ))

        if not all(results):
            raise Exception("部分视频片段下载失败")

        # 生成输入列表文件
        input_list_path = os.path.join(video_save_dir, 'input_list.txt')
        with open(input_list_path, 'w', encoding='utf-8') as f:
            for ts_file in ts_files:
                f.write(f"file '{os.path.join(video_save_dir, ts_file)}'\n")

        logger.info(f"生成输入列表文件: {input_list_path}")

        # 转换MP4
        output_path = os.path.join(out_path, f"{video_id}.mp4")
        logger.info(f"开始视频转换: {output_path}")
        ffmpeg_path = r"D:\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe"
        ffmpeg_cmd = [
            ffmpeg_path,
            '-y',
            '-loglevel', 'debug',
            '-protocol_whitelist', 'file,pipe,concat',
            '-f', 'concat',
            '-safe', '0',
            '-i', input_list_path,
            '-c', 'copy',
            output_path
        ]

        process = subprocess.run(
            ffmpeg_cmd,
            check=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            encoding='utf-8',  # 显式设置编码为 UTF-8
            errors='ignore',  # 忽略无法解码的字符
            text=True
        )

        logger.info(f"视频转换完成: {output_path}")

        # 删除处理过程中生成的中间文件和文件夹
        logger.info("开始清理临时文件...")
        # 删除m3u8文件
        if os.path.exists(m3u8_path):
            os.remove(m3u8_path)
            logger.debug(f"已删除m3u8文件: {m3u8_path}")
        # 删除输入列表文件
        if os.path.exists(input_list_path):
            os.remove(input_list_path)
            logger.debug(f"已删除输入列表文件: {input_list_path}")
        # 删除视频文件夹及其内容
        if os.path.exists(video_save_dir):
            shutil.rmtree(video_save_dir)
            logger.debug(f"已删除视频文件夹: {video_save_dir}")
        logger.info("临时文件清理完成.")

        return output_path

    except Exception as e:
        logger.error(f"视频处理失败: {str(e)}")
        return None


def process_video(video_item,keyword):
    """处理单个视频"""
    video_id = video_item['id']

    # 状态检查
    if check_download_state(video_id):
        logger.info(f"视频已下载，跳过处理: {video_id}")
        return
    # 插入数据库记录
    insert_video_record(video_item)

    # 处理视频下载
    start_time = time.time()
    output_path = process_m3u8(video_item,keyword)
    if not output_path:
        return

    # 更新数据库
    update_video_record(video_id, output_path)

    duration = time.time() - start_time
    logger.info(f"视频处理完成: {video_id} 耗时: {duration:.2f}s")

@retry_on_failure(max_retries=3, delay=5)
def send_artlist_graphql_request(keyword, page):
    """发送GraphQL请求"""
    url = "https://search-api.artlist.io/v1/graphql"
    
    payload = {
        "query": """
        query ClipList(
            $filterCategories: [Int!],
            $searchTerms: [String],
            $sortType: Int,
            $queryType: Int,
            $page: Int,
            $durationMin: Int,
            $durationMax: Int,
            $orientation: ClipOrientation
        ) {
            clipList(
                filterCategories: $filterCategories,
                searchTerms: $searchTerms,
                sortType: $sortType,
                queryType: $queryType,
                page: $page,
                durationMin: $durationMin,
                durationMax: $durationMax,
                orientation: $orientation
            ) {
                querySearchType
                exactResults {
                    id
                    clipName
                    clipNameForUrl
                    thumbnailUrl
                    clipPath
                    isInFavorites
                    isInDownloadHistory
                    isNew
                    isVfx
                    isInFolder
                    filmMakerDisplayName
                    storyId
                    storyNameForURL
                    isLicensed
                    isOriginal
                    storyName
                    duration
                    width
                    height
                    orientation
                    availableFormats {
                        id
                        displayName
                    }
                    filmMakerId
                    filmMakerNameForUrl
                }
                similarResults {
                    id
                    clipName
                    clipNameForUrl
                    thumbnailUrl
                    clipPath
                    isInFavorites
                    isInDownloadHistory
                    isNew
                    isVfx
                    isInFolder
                    filmMakerDisplayName
                    storyId
                    storyNameForURL
                    isLicensed
                    isOriginal
                    storyName
                    duration
                    width
                    height
                    orientation
                }
                totalExact
                totalSimilar
                topDownloadedClipsCount
                totalLexicalResults
                totalSemanticResults
            }
        }
        """,
        "variables": {
            "page": page,
            "queryType": 1,
            # "filterCategories": [224],
            "searchTerms": [keyword],  # 使用关键词
            "sortType": 1,
            "durationMin": 10000
        }
    }

    # 使用会话保持连接
    with requests.Session() as session:
        session.headers.update(get_random_headers())
        response = session.post(url, json=payload)
        response.raise_for_status()
        return response.json()

def main_processing():
    logger.info("=" * 50)
    logger.info("启动Artlist视频下载任务")
    logger.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 创建数据表
    create_table()

    # 遍历关键词
    for keyword in KEYWORDS:
        logger.info(f"开始处理关键词: {keyword}")
        
        # 获取数据
        for page in range(page_start, page_start + 30):
            try:
                logger.info(f"正在获取第 {page} 页的视频列表...")
                result = send_artlist_graphql_request(keyword, page)



                if not result:
                    logger.error(f"获取视频列表失败: {keyword} - 第{page}页")
                    continue

                exact_results = result.get('data', {}).get('clipList', {}).get('exactResults', [])
                total_count = len(exact_results)
                
                if not exact_results:
                    logger.info(f"没有更多视频了: {keyword} - 第{page}页")
                    break

                logger.info(f"发现 {total_count} 个待处理视频")

                # 创建一个元组列表，每个元组包含video_item和keyword
                video_keyword_pairs = [(video_item, keyword) for video_item in exact_results]

                # 使用线程池处理视频，传递元组列表
                with ThreadPoolExecutor(max_workers=3) as executor:
                    list(tqdm(
                        executor.map(lambda args: process_video(*args), video_keyword_pairs),
                        total=total_count,
                        desc=f"处理视频 - {keyword} - 第{page}页"
                    ))

                # 随机延迟，避免请求过快
                time.sleep(random.uniform(5, 9))

            except Exception as e:
                logger.error(f"处理页面失败: {keyword} - 第{page}页, 错误: {e}")
                continue

        logger.info(f"关键词 {keyword} 处理完成")
        time.sleep(random.uniform(10, 15))  # 关键词之间的间隔

    logger.info(f"所有任务完成")
    logger.info(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main_processing()