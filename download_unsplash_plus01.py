#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/7/29 16:36
# @Author  : CUI liuliu
# @File    : download_unsplash_plus.py


import os
import re
import requests
import pymysql
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs

# -------------------
# 配置
# -------------------
MYSQL_CONFIG = {
    'host': '192.168.10.70',
    'user': 'root',
    'password': 'zq828079',
    'database': 'data_sql',
    'charset': 'utf8mb4'
}
TABLE_NAME = 'unsplash_img'
output_path = r'E:\unsplash_downloads\人脸'
os.makedirs(output_path, exist_ok=True)

THREAD_NUM = 5
BATCH_SIZE = 50  # 每次批量取多少条任务

# -------------------
# 日志配置
# -------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("unsplash_download.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# -------------------
# 工具函数
# -------------------
def clean_filename(s):
    return re.sub(r'[\\/:*?"<>|]', '_', s)

def get_extension_from_url(url):
    try:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        fm = query.get('fm', ['jpg'])[0]
        if fm.lower() in ('jpg', 'jpeg', 'png', 'webp'):
            return fm.lower()
        else:
            return 'jpg'
    except Exception as e:
        logger.warning(f"解析格式失败: {e}, 使用默认 jpg")
        return 'jpg'

# -------------------
# 下载单个图片
# -------------------
def download_image(task):
    id_, slug, full_url, keywords = task
    ext = get_extension_from_url(full_url)
    filename = clean_filename(slug) if slug else str(id_)
    subfolder = clean_filename(keywords) if keywords else 'unknown'
    folder_path = os.path.join(output_path, subfolder)
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, f"{filename}.{ext}")

    try:
        resp = requests.get(full_url, timeout=20)
        if resp.status_code == 200:
            with open(file_path, 'wb') as f:
                f.write(resp.content)
            logger.info(f"✅ 下载成功: {file_path} (关键词: {keywords})")
            return (id_, True)
        else:
            logger.error(f"❌ 下载失败: HTTP {resp.status_code} - URL: {full_url}")
            return (id_, False)
    except Exception as e:
        logger.error(f"❌ 请求异常 id={id_}: {e}")
        return (id_, False)

# -------------------
# 主逻辑
# -------------------
def main():
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        while True:
            # 取待下载任务，并立即锁定（download_state=2）
            # cursor.execute(f"""
            #     SELECT id, slug, full_url, keywords
            #     FROM {TABLE_NAME}
            #     WHERE
            #         download_state = 0
            #     LIMIT {BATCH_SIZE}
            # """)
            plant_keywords = [
                # 动物类 (Animals)

                #  "rose", "tulip", "daisy", "sunflower", "orchid", "lily", "lavender", "jasmine", "peony", "marigold",
                # "poppy", "cherry blossom", "magnolia", "hibiscus", "iris", "camellia", "azalea", "hydrangea", "gardenia", "daffodil",
                # "fern", "cactus", "succulent", "bamboo", "ivy", "maple", "oak", "pine", "willow", "birch",
                # "palm", "aloe", "sage", "basil", "mint", "rosemary", "thyme", "parsley", "cilantro", "spinach",
                # "kale", "lettuce", "broccoli", "cauliflower", "pumpkin", "zucchini", "tomato", "pepper", "carrot", "radish"
                "people face","person", "people", "man", "woman", "child", "adult", "boy", "girl", "athlete"
            ]
            # 构建关键词条件：将列表转换为多个 OR 连接的 LIKE 语句
            keyword_conditions = " OR ".join([f"keywords LIKE '%{keyword}%'" for keyword in plant_keywords])

            cursor.execute(f"""
                SELECT id, slug, full_url, keywords
                FROM {TABLE_NAME}
                WHERE
                    download_state = 0
                    AND ({keyword_conditions})  -- 添加关键词筛选条件
                LIMIT {BATCH_SIZE}
            """)
            rows = cursor.fetchall()

            if not rows:
                logger.info("✅ 没有待下载任务了，退出")
                break

            ids = [row[0] for row in rows]
            # 锁定任务
            cursor.executemany(f"UPDATE {TABLE_NAME} SET download_state=2 WHERE id=%s", [(i,) for i in ids])
            conn.commit()
            logger.info(f"🚀 本批次准备下载 {len(rows)} 条记录")

            # 多线程下载
            with ThreadPoolExecutor(max_workers=THREAD_NUM) as executor:
                futures = [executor.submit(download_image, row) for row in rows]
                for future in tqdm(as_completed(futures), total=len(futures), desc='下载中'):
                    id_, success = future.result()
                    if success:
                        cursor.execute(f"UPDATE {TABLE_NAME} SET download_state=1 WHERE id=%s", (id_,))
                    else:
                        # 如果失败，可以选择重置为0，下次重试
                        cursor.execute(f"UPDATE {TABLE_NAME} SET download_state=0 WHERE id=%s", (id_,))
                    conn.commit()

        conn.close()
        logger.info("🎉 所有下载任务完成")
    except Exception as e:
        logger.exception(f"运行出错: {e}")

if __name__ == "__main__":
    main()
