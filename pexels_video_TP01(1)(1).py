import random
import time
from urllib.parse import unquote, urlsplit, parse_qs
import requests
import os
import mysql.connector
import logging
from urllib.parse import unquote
from datetime import datetime


headers = {
    "content-type": "application/json",
    "cookie": "_fbp=fb.1.1740644381892.22830351949064128; pexels_message_banner_creative-rituals-2025=1; _hjSessionUser_171201=eyJpZCI6IjZmZGJkNTI5LTFjNDItNTc2Mi04MzdmLWNmNjllYzc3YTEzZSIsImNyZWF0ZWQiOjE3NDA2NDQzNzA4MDQsImV4aXN0aW5nIjp0cnVlfQ==; __cf_bm=ewMgDctRGw6pWr2okH307HbJibq3y61AqVcfQTNHn_s-1741137444-1.0.1.1-6tyepLmp3ewJ_a0nUfgbInfmopb_BDucb6cfxEBRo8MJgF2f95Hz7xBx0h8ZSYrlYCjWrVg1dc.h7nn9B_1JCxry9raQT5KCUqgrcfBzNj0; _cfuvid=rzefsCE5ovlEZ3y_dERsJCAjqAShDmEE0C4Qze_PfvI-1741137444663-0.0.1.1-604800000; _sp_ses.9ec1=*; country-code-v2=CN; cf_clearance=sGaznpWekBUSTxOxlT60O7ZJJa0tXtyK_nFADF2yT_g-1741137448-1.2.1.1-bi.1.ZKsNj.tpryWpxywNQ3Qb8gT9y1F7S0O4lFdRI9hrNVt1ATTbUeAo49qrU1gHoTubbWxjDH1HMtJ0Iqy5408YZSGGyTtbg9uhEFLkj9.7BW5AwKTgW75iue7cOMrKRYWknBTuNVJ_9g7JZ1OPCXfP6tkBUgwNaYeiBioH729jvFlp0ytiRa3aNWHEovg5l0owOcIbEhBhRZAWpMbdTLtYJ4XKwvXELvvJsgtWzRoZMMhPrOzytFXJR6i3gueLC5gi_HJ8JbqKTZYW50tq1Dwr5rxxJHUAOwd.Smif_BJShYV9K._YgkCMPp5JKXCZFcitvhCNxKssFQx_inpvNJVpH9c4E2qUNJ6HaJyi9Z7pMyq15X7bBUSZvfpHizqjYOwNuDna8DZb9FK6Kbzq.C.OoSVnJ1v8g22boNHUYs; _hjSession_171201=eyJpZCI6ImEyZjRmYjhlLTNmZTUtNGY0NS1iZDdiLTg1ZjRhMzQ2ZWI2ZiIsImMiOjE3NDExMzc0NTE4NTUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; _gid=GA1.2.1332797806.1741137539; OptanonAlertBoxClosed=2025-03-05T01:19:03.120Z; _ga=GA1.1.449538479.1740644369; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Mar+05+2025+09%3A20%3A52+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202301.1.0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=CN%3BTJ; _ga_8JE65Q40S6=GS1.1.1741137450.2.1.1741137659.0.0.0; _sp_id.9ec1=ddaf142a-e7d7-4159-b6ce-c654ef25dfc2.1740644368.2.1741137746.1740646115.d3d1ff66-4bbb-46f2-9a2a-9f9c33e5028b.532fc1cf-76b5-459f-98fc-0c9df506964d.e5f30f40-11d3-4c5e-9600-181ffeb13c97.1741137448194.31; _dd_s=rum=2&id=1b8418c1-dbb0-461e-bf62-7d6acfea0696&created=1741137447696&expire=1741138657628",
    "priority": "u=1, i",
    "referer": "https://www.pexels.com/search/man/",
    "sec-ch-ua": '"Not(A:Brand";v="99", "Microsoft Edge";v="133", "Chromium";v="133"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "secret-key": "H2jk9uKnhRmL6WPwh89zBezWvr",
    "traceparent": "00-0000000000000000b547d24c9268aa63-574e0b40417ecd22-01",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
    "x-client-type": "react",
    "x-forwarded-cf-connecting-ip": "",
    "x-forwarded-cf-ipregioncode": "",
    "x-forwarded-http_cf_ipcountry": ""
}

# ===================== Logo展示 =====================
print(f"{'*' * 40}")
print(f"* Pexels视频采集器启动")
print(f"* 启动时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'*' * 40}\n")
# ===================================================
# ===================== 配置区域 =====================
# keyword="dog" #搜索关键词
KEYWORDS =  [
    "Barbell Squat", "Barbell Bench Press", "Barbell Deadlift", "Barbell Row",

]
output_path = fr"Z:\健身视频样例\视频\1111111111111111111111111111"  # 保存路径
page_start = 1  # 开始页数
page_end = 50
DATAL_ID = 'D25'
mysql_config = {
    'user': 'root',
    'password': 'zq828079',
    'host': '192.168.10.70',
    'database': 'data_sql'
}

# ===================================================
# ===================== 日志配置 =====================
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler(f'pexels_video.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# ===================================================

def create_table(cursor):

    """创建数据库表"""
    try:
        ret = cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS pexels_vid_three (
                id INT AUTO_INCREMENT PRIMARY KEY,
                video_name VARCHAR(255) NOT NULL,
                category VARCHAR(50) ,
                tags TEXT,
                datal_id VARCHAR(50) DEFAULT NULL,
                download_state BOOLEAN DEFAULT FALSE,  -- 新增状态字段
                download_link VARCHAR(255) UNIQUE,
                save_path VARCHAR(255) DEFAULT NULL
            )
        """)
        logger.info(f"数据库表创建/验证成功: {ret}")
    except Exception as e:
        logger.error(f"数据库表创建失败: {str(e)}")
        raise


def download_video(url, save_path):
    """下载视频并更新状态"""
    conn = None
    try:
        # 建立数据库连接
        conn = mysql.connector.connect(**mysql_config)

        # 查询download_state的状态（使用新表pexels_vid_three）
        with conn.cursor() as cursor:
            select_sql = """
                SELECT download_state 
                FROM pexels_vid_three 
                WHERE download_link = %s
            """
            cursor.execute(select_sql, (url,))
            result = cursor.fetchone()

            if result is None:
                logger.error(f"URL {url} 不存在于数据库中")
                return False, url

            download_state = result[0]

            if download_state:
                logger.info(f"URL {url} 已下载，跳过")
                return True, url

        # 开始下载
        response = requests.get(url, stream=True, timeout=30)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f"{save_path}下载成功")

            # 更新数据库状态（使用新表pexels_vid_three）
            with conn.cursor() as cursor:
                update_sql = """
                    UPDATE pexels_vid_three 
                    SET download_state = %s,
                        datal_id = %s, 
                        save_path = %s
                    WHERE download_link = %s 
                """
                cursor.execute(update_sql, (True, DATAL_ID, save_path, url))
                conn.commit()
                logger.info(f"{url}下载状态更新成功")
            return True, url
        else:
            logger.error(f"下载失败，状态码：{response.status_code}")
            return False, url
    except Exception as e:
        logger.error(f"下载异常: {str(e)}")
        return False, url
    finally:
        if conn is not None:
            conn.close()

def get_existing_links():
    """获取数据库中已存在的所有下载链接（使用新表pexels_vid_three）"""
    try:
        with mysql.connector.connect(**mysql_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT download_link FROM pexels_vid_three WHERE download_state = True")
                return {link[0] for link in cursor.fetchall()}
    except Exception as e:
        logger.error(f"获取已存在链接失败: {str(e)}")
        return set()


from concurrent.futures import ThreadPoolExecutor  # 添加导入


def process_page(page_num, existing_links, keyword):
    """处理单个页面（新增existing_links参数）"""
    logger.info(f"{'=' * 30} 开始处理{keyword}第 {page_num} 页 {'=' * 30}")
    url = f"https://www.pexels.com/en-us/api/v3/search/videos?query={keyword}&page={page_num}&per_page=24&orientation=all&size=all&color=all&sort=popular&seo_tags=true"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        time.sleep(5)
    except requests.exceptions.RequestException as e:
        logger.error(f"页面请求失败: {str(e)}")
        return existing_links

    if response.status_code != 404 and response.status_code != 403:  # 修正逻辑判断
        try:
            data = response.json()
            results = []
            new_links = set()
            download_tasks = []  # 新增下载任务列表

            for idx, item in enumerate(data["data"], 1):
                download_link = item["attributes"]["video"]["download_link"];
                id = item["attributes"]["id"];
                filename = f'{id}.mp4'

                if download_link in existing_links:
                    logger.warning(f"链接重复，跳过: {download_link}")
                    continue

                tags = item["attributes"]["tags"]
                tags_name = [tag.get("name") for tag in tags]  # 简化列表推导式
                out_path = os.path.join(output_path, keyword)
                os.makedirs(out_path, exist_ok=True)
                save_path = os.path.join(out_path, filename)

                tags_str = ",".join(tags_name)
                record = (filename, keyword, tags_str, download_link)
                results.append(record)
                new_links.add(download_link)
                download_tasks.append((download_link, save_path))  # 添加下载任务
                logger.debug(f"记录添加成功: {filename}")

            if results:
                try:
                    with mysql.connector.connect(**mysql_config) as conn:
                        with conn.cursor() as cursor:
                            create_table(cursor)
                            sql = """
                                INSERT IGNORE INTO pexels_vid_three 
                                (video_name, category, tags, download_link)
                                VALUES (%s, %s, %s, %s)
                            """
                            cursor.executemany(sql, results)
                            conn.commit()

                    inserted = cursor.rowcount
                    existing_links.update(new_links)
                    logger.info(f"成功插入 {inserted} 条记录")

                    # 新增多线程下载部分
                    if download_tasks:
                        logger.info(f"开始多线程下载，共{len(download_tasks)}个任务")
                        with ThreadPoolExecutor(max_workers=3) as executor:
                            futures = []
                            for link, path in download_tasks:
                                futures.append(executor.submit(download_video, link, path))

                            for future in futures:
                                try:
                                    future.result()  # 等待任务完成
                                except Exception as e:
                                    logger.error(f"视频下载失败: {str(e)}")
                        logger.info("当前页面下载任务全部完成")

                except mysql.connector.Error as e:
                    logger.error(f"数据库操作失败: {str(e)}")
            else:
                logger.warning("当前页面没有有效数据")

        except ValueError:
            logger.error("JSON解析失败")
    else:
        logger.error(f"请求失败 HTTP {response.status_code}")

    return existing_links


# ===================== 主程序 =====================
if __name__ == "__main__":
    os.makedirs(output_path, exist_ok=True)
    logger.info(f"输出目录已准备: {output_path}")

    # 先建表，防止查表时报错
    try:
        with mysql.connector.connect(**mysql_config) as conn:
            with conn.cursor() as cursor:
                create_table(cursor)
    except Exception as e:
        logger.error(f"初始化建表失败: {str(e)}")

    try:
        # 初始化时获取所有已存在的链接
        existing_links = get_existing_links()
        logger.info(f"已加载 {len(existing_links)} 条历史记录")
        for keyword in KEYWORDS:
            for page_num in range(page_start, page_end):
                existing_links = process_page(page_num, existing_links,keyword)

                # 添加适当延迟
                if page_num % 5 == 0:
                    logger.info(f"已完成 {page_num} 页，暂停20秒...")
                    time.sleep(20)
                else:
                    time.sleep(10)

    except KeyboardInterrupt:
        logger.warning("用户中断操作！")
    except Exception as e:
        logger.critical(f"程序异常终止: {str(e)}", exc_info=True)
    finally:
        logger.info("程序运行结束\n")