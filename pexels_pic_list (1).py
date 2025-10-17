import time
from urllib.parse import unquote, urlsplit, parse_qs
import requests
import os
import logging
from urllib.parse import unquote
from datetime import datetime
import threading


headers = {
    # "accept": "*/*",
    # "accept-encoding": "gzip, deflate, br, zstd",
    # "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
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
print(f"* Pexels图片采集器启动")
print(f"* 启动时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'*' * 40}\n")
# ===================================================

# ===================== 配置区域 =====================
# 更新为五大类关键词 (人物/动物/建筑/自然风景/植物) 包含文字的关键词
keywords = [
"protest sign with text"
    ]
out_path = fr"Z:\Ljn\文字\文字"

page_start= 1  # 开始页数
page_add = 50
links_record_file = "downloaded_links.txt"  # 已下载链接记录文件

# ===================================================

# ===================== 已下载链接管理 =====================
def load_downloaded_links():
    """读取已下载链接集合"""
    if not os.path.exists(links_record_file):
        return set()
    with open(links_record_file, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def append_downloaded_link(link, lock=None):
    """将新下载链接追加到文件（线程安全）"""
    if lock:
        with lock:
            with open(links_record_file, 'a', encoding='utf-8') as f:
                f.write(link + '\n')
    else:
        with open(links_record_file, 'a', encoding='utf-8') as f:
            f.write(link + '\n')
# ===================================================

# ===================== 关键词进度管理 =====================
def get_progress_file(keyword):
    """获取关键词对应的进度文件名"""
    return f"progress_{keyword}.txt"

def load_last_page(keyword):
    """读取关键词上次下载到的最大页码"""
    progress_file = get_progress_file(keyword)
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                return int(f.read().strip())
        except Exception:
            return 0
    return 0

def save_last_page(keyword, page_num):
    """保存关键词最新下载到的页码"""
    progress_file = get_progress_file(keyword)
    with open(progress_file, 'w', encoding='utf-8') as f:
        f.write(str(page_num))
# ===================================================

# ===================== 日志配置 =====================
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler(f'pexels_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# ===================================================


def get_filename_from_url(url):
    """从下载链接的dl参数中提取文件名"""
    try:
        # 解码URL
        decoded_url = unquote(url)
        logger.debug(f"原始URL: {decoded_url}")

        # 拆分URL路径和参数
        parsed = urlsplit(decoded_url)
        query_params = parse_qs(parsed.query)

        # 优先从dl参数获取文件名
        if 'dl' in query_params:
            filename = query_params['dl'][0]  # 取第一个dl参数值
            logger.debug(f"从dl参数获取文件名: {filename}")
        else:
            # 回退方案：从路径获取文件名
            filename = parsed.path.split('/')[-1]
            logger.debug(f"从路径获取文件名: {filename}")

        # 安全处理文件名（移除路径分隔符）
        filename = filename.split('/')[-1]
        return filename

    except Exception as e:
        logger.warning(f"文件名解析失败: {str(e)}")
        return f"unknown_{int(datetime.now().timestamp())}.jpg"


def download_image(url, save_path):
    """下载图片到本地"""
    try:
        logger.info(f"开始下载: {url}")
        if os.path.exists(save_path):
            logger.info("链接已下载，跳过")
        else:
            response = requests.get(url, stream=True, timeout=30)

            if response.status_code == 200:
                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"下载成功: {save_path}")
                return True

            logger.error(f"下载失败 HTTP状态码: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"下载异常: {str(e)}")
        return False


def process_page(page_num, keyword, downloaded_links, lock=None):
    """处理单个页面（断点续传+本地去重）"""
    logger.info(f"{'=' * 30} 开始处理{keyword}第 {page_num} 页 {'=' * 30}")
    url = f"https://www.pexels.com/en-us/api/v3/search/photos?query={keyword}&page={page_num}&per_page=24&orientation=all&size=all&color=all&sort=popular&seo_tags=true"
    output_path=os.path.join(out_path,f'{keyword}')
    os.makedirs(output_path,exist_ok=True)
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"页面请求失败: {str(e)}")
        return

    if response.status_code == 200:
        try:
            data = response.json()
            for idx, item in enumerate(data["data"], 1):
                try:
                    download_link = item["attributes"]["image"]["download_link"]
                    if download_link in downloaded_links:
                        logger.info(f"链接已下载过，跳过: {download_link}")
                        continue
                    description = item["attributes"].get("description", "")
                    filename = get_filename_from_url(download_link)
                    save_path = os.path.join(output_path, filename)
                    if not os.path.exists(save_path):
                        download_success = download_image(download_link, save_path)
                        if download_success:
                            append_downloaded_link(download_link, lock)
                            downloaded_links.add(download_link)
                            logger.info(f"已记录下载链接: {download_link}")
                        else:
                            logger.warning(f"跳过未下载文件: {filename}")
                    else:
                        logger.info(f"文件已存在: {filename}")
                        append_downloaded_link(download_link, lock)
                        downloaded_links.add(download_link)
                except KeyError as e:
                    logger.warning(f"数据字段缺失: {str(e)}")
                    continue
        except ValueError:
            logger.error("JSON解析失败")
    else:
        logger.error(f"请求失败 HTTP {response.status_code}")

# ===================== 主程序 =====================
if __name__ == "__main__":
    os.makedirs(out_path, exist_ok=True)
    logger.info(f"输出目录已准备: {out_path}")
    # 加载已下载链接
    downloaded_links = load_downloaded_links()
    logger.info(f"已加载 {len(downloaded_links)} 条已下载链接")
    lock = threading.Lock()
    try:
        for keyword in keywords:
            last_page = load_last_page(keyword)
            logger.info(f"关键词[{keyword}]上次下载到第 {last_page} 页，将从第 {last_page+1} 页开始")
            for page_num in range(max(page_start, last_page+1), page_start+page_add):
                process_page(page_num, keyword, downloaded_links, lock)
                save_last_page(keyword, page_num)
                if page_num % 5 == 0:
                    logger.info(f"已完成 {page_num} 页，暂停5秒...")
                    time.sleep(5)
                else:
                    time.sleep(1)
    except KeyboardInterrupt:
        logger.warning("用户中断操作！")
    except Exception as e:
        logger.critical(f"程序异常终止: {str(e)}", exc_info=True)
    finally:
        logger.info("程序运行结束\n")