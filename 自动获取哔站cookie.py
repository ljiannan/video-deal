import os
import platform
import json
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

# --- 配置项 ---
#无需修改
COOKIE_JSON_FILENAME = 'bilibili_cookies.json'
#cookie路径，一个txt一个cookie
COOKIE_TXT_FILENAME = r'Y:\personal_folder\yyk\cookie\自己.txt'


def find_chrome_executable() -> str | None:
    """
    智能查找系统中Google Chrome浏览器的可执行文件路径。
    """
    system = platform.system()
    if system == "Windows":
        possible_paths = [
            os.path.join(os.environ.get("ProgramFiles", "C:\\Program Files"),
                         "Google\\Chrome\\Application\\chrome.exe"),
            os.path.join(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)"),
                         "Google\\Chrome\\Application\\chrome.exe"),
            os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google\\Chrome\\Application\\chrome.exe")
        ]
    elif system == "Darwin":
        possible_paths = ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"]
    else:
        possible_paths = ["/usr/bin/google-chrome", "/usr/bin/google-chrome-stable", "/opt/google/chrome/google-chrome"]
    for path in possible_paths:
        if os.path.exists(path):
            return path
    return None


def format_cookies_to_header_string(cookies: list) -> str:
    """
    将Selenium的Cookie列表转换成请求头字符串。
    """
    if not cookies:
        return ""
    return "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])


def save_cookie_to_files(cookies: list, stage: str):
    """
    将Cookie保存到json和txt文件，并清晰地打印到控制台。
    """
    print(f"\n[保存Cookie阶段: {stage}]")
    try:
        with open(COOKIE_JSON_FILENAME, 'w', encoding='utf-8') as f:
            json.dump(cookies, f, ensure_ascii=False, indent=4)
        print(f"     -- ✅ 登录状态已更新到 '{COOKIE_JSON_FILENAME}'。")
    except Exception as e:
        print(f"     -- ❌ 保存到 {COOKIE_JSON_FILENAME} 失败: {e}")

    cookie_header_string = format_cookies_to_header_string(cookies)
    try:
        with open(COOKIE_TXT_FILENAME, 'w', encoding='utf-8') as f:
            f.write(cookie_header_string)
        print(f"     -- ✅ 请求头Cookie已覆盖写入到 '{COOKIE_TXT_FILENAME}'。")
    except Exception as e:
        print(f"     -- ❌ 保存到 {COOKIE_TXT_FILENAME} 失败: {e}")

    print("\n" + "=" * 70)
    print(f"🔑【请求头 Cookie 字符串 ({stage})】:")
    print(cookie_header_string)
    print("=" * 70)


def get_bilibili_cookie(login_url: str, target_url: str, is_automated_run: bool):
    """
    在指定登录页完成登录，然后跳转到目标页获取最终Cookie。
    每次都会弹出可见的浏览器窗口。
    """
    print("\n[步骤 1/5] 🚀 Cookie获取流程开始...")
    driver = None
    login_successful = False
    try:
        print("[步骤 2/5] ⚙️  正在配置并启动Chrome浏览器...")
        options = webdriver.ChromeOptions()
        chrome_path = find_chrome_executable()
        if chrome_path:
            options.binary_location = chrome_path

        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        options.add_argument(f'user-agent={user_agent}')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # 【关键修改】移除了所有headless(无头/静默)模式的设置
        # 现在无论是否为自动运行，浏览器都会正常显示出来
        print("     -- 浏览器将以可见模式运行。")

        chromedriver_path = r"C:\Users\DELL\.wdm\drivers\chromedriver\win64\140.0.7339.207\chromedriver-win32\chromedriver.exe"
        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
        print("     -- WebDriver初始化成功！")

        print(f"\n[步骤 3/5] 🏠 正在B站首页 ({login_url}) 进行登录...")
        if os.path.exists(COOKIE_JSON_FILENAME):
            print("     -- ✅ 找到Cookie文件，尝试自动登录...")
            try:
                with open(COOKIE_JSON_FILENAME, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                driver.get("https://www.bilibili.com/blank.html")
                for cookie in cookies:
                    if 'expiry' in cookie:
                        cookie['expiry'] = int(cookie['expiry'])
                    driver.add_cookie(cookie)

                driver.get(login_url)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a.header-entry-avatar")))
                print("     -- 🎉 自动登录成功！")
                login_successful = True
            except Exception:
                print(f"     -- ⚠️ 自动登录失败！可能是Cookie已过期。")
                if is_automated_run:
                    print("     -- 自动化模式下无法手动登录，本轮更新失败。")
                    return None
        else:
            print("     -- ℹ️  未找到登录信息。")
            if is_automated_run:
                print(f"     -- 自动化模式下必须存在 '{COOKIE_JSON_FILENAME}' 文件，本轮更新失败。")
                return None

        if not login_successful:
            print("     -- 请进行首次手动登录。")
            driver.get(login_url)
            print("\n" + "=" * 60)
            print("     -- 👋 【请手动操作】请在弹出的浏览器中完成【扫码登录】。")
            print("     -- 登录成功后，请在本窗口按下 'Enter' 键。")
            print("=" * 60)
            input("     -- 我已登录完成，请按Enter键继续 -> ")
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.header-entry-avatar")))
            print("     -- ✅ 登录状态确认成功！")
            initial_cookies = driver.get_cookies()
            save_cookie_to_files(initial_cookies, stage="首次登录后")
            login_successful = True

        if login_successful:
            print(f"\n[步骤 4/5] 🎯 登录成功，正在跳转到目标链接...")
            print(f"     -- {target_url}")
            driver.get(target_url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "app")))
            print("     -- ✅ 目标页面加载成功。")

            print(f"\n[步骤 5/5] 🍪 正在获取并保存最终的Cookie信息...")
            final_cookies = driver.get_cookies()
            save_cookie_to_files(final_cookies, stage="跳转后最终版")
            return final_cookies

        return None

    except Exception as e:
        print(f"\n[程序异常] ❌ 在执行过程中发生严重错误: {e}")
        return None
    finally:
        print(f"\n[清理阶段] ✨ 本轮流程结束。")
        if driver:
            # 无论是哪一轮，任务完成后都自动关闭浏览器，以使循环继续
            print("     -- 任务完成，浏览器将自动关闭。")
            driver.quit()


def main():
    """
    主函数，包含无限循环，用于定时执行Cookie获取任务。
    """
    print("=" * 60)
    print(" Bilibili Cookie 自动更新工具 ".center(52, "="))
    print("=" * 60)
    print("程序将首先执行一次登录流程，成功后将每两天自动更新一次Cookie。")
    print("每次更新时，都会【弹出浏览器窗口】进行操作。")
    print("首次运行时，如果需要，请按提示手动登录一次。")
    print("之后，您只需保持此程序窗口运行即可。")

    homepage_url = 'https://www.bilibili.com'
    target_url = 'https://www.bilibili.com/video/BV1Aip7zwEKe?spm_id_from=333.788.recommend_more_video.0&vd_source=69d0cac3d731aa88a8ad598f4423673f'
    run_count = 0

    while True:
        try:
            run_count += 1
            is_automated = (run_count > 1)

            print("\n" + "#" * 70)
            print(f"### 开始执行第 {run_count} 轮Cookie获取任务 ###")
            print("#" * 70)

            cookies = get_bilibili_cookie(homepage_url, target_url, is_automated_run=is_automated)

            print("\n" + "-" * 60)
            if cookies:
                print(f"✅ 第 {run_count} 轮操作成功完成！最终Cookie已更新。")
            else:
                print(f"😥 第 {run_count} 轮操作失败，未能成功获取Cookie。")

            if not cookies and run_count == 1:
                print("❌ 首次设置失败，请检查错误信息后重新运行程序。")
                input("按Enter键退出。")
                break

            print("-" * 60)
            wait_hours = 48
            print(f"\n⏳ 程序将进入休眠状态，将在约 {wait_hours} 小时后自动进行下一次更新。")
            print("   您可以最小化此窗口，但请【不要关闭】它。")
            print("   要强制停止程序，请直接关闭此窗口或在窗口内按 Ctrl+C。")

            for i in range(1,wait_hours+1):
                print(f"暂停计数：第{i}个小时，还有{wait_hours-i}个小时开始")
                time.sleep(60 * 60)

        except KeyboardInterrupt:
            print("\n\n程序被用户手动中断。正在退出...")
            break
        except Exception as e:
            print(f"\n❌ 循环中发生未知严重错误: {e}")
            print("   为避免频繁出错，程序将在1小时后尝试重启流程...")
            time.sleep(3600)


if __name__ == "__main__":
    main()