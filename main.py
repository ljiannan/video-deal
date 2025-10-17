import sys
import time
import subprocess
import signal
from pathlib import Path

# --- 配置区 ---
# 需要并发运行的脚本列表。
# 路径是相对于这个 main.py 脚本的。
SCRIPTS_TO_RUN = [
    "sql_download_pro.py",
    "video_moving.py",
]

# 用于优雅关闭的全局标志
shutdown_requested = False


def main():
    """
    主函数，用于启动、监控并自动重启并发脚本。
    """
    print("--- 守护进程：开始启动并监控子脚本 ---")

    # 获取当前运行的 Python 解释器路径
    python_executable = sys.executable
    # 获取 main.py 脚本所在的目录
    base_path = Path(__file__).parent

    # 使用字典来管理子进程，方便按名称查找和重启
    managed_processes = {script: None for script in SCRIPTS_TO_RUN}

    # --- 优雅关闭处理器 ---
    def shutdown(signum, frame):
        global shutdown_requested
        if shutdown_requested:  # 防止重复执行
            return
        print("\n--- 守护进程：收到关闭信号，正在终止所有子进程... ---")
        shutdown_requested = True
        for script_name, p in managed_processes.items():
            if p and p.poll() is None:  # 检查进程是否存在且仍在运行
                print(f"--- 守护进程：正在终止 '{script_name}' (PID: {p.pid})...")
                p.terminate()  # 发送 SIGTERM 信号，请求终止
        # 等待一段时间让进程自行终止
        time.sleep(2)
        for script_name, p in managed_processes.items():
            if p and p.poll() is None:  # 如果进程还未终止
                print(f"--- 守护进程：强制终止 '{script_name}' (PID: {p.pid})...")
                p.kill()  # 发送 SIGKILL 信号，强制杀死

    signal.signal(signal.SIGINT, shutdown)  # 处理 Ctrl+C
    signal.signal(signal.SIGTERM, shutdown)  # 处理终止信号

    # --- 启动脚本的辅助函数 ---
    def start_script(script_name):
        try:
            script_path = base_path / script_name
            print(f"--- 守护进程：正在启动脚本: {script_path} ---")
            process = subprocess.Popen([python_executable, str(script_path)])
            managed_processes[script_name] = process
            print(f"--- 守护进程：脚本 '{script_name}' 已启动，进程ID为 {process.pid}")
        except Exception as e:
            print(f"--- 守护进程错误：启动 '{script_name}' 失败: {e}。稍后将重试。")
            managed_processes[script_name] = None

    # --- 首次启动所有脚本 ---
    for script in SCRIPTS_TO_RUN:
        start_script(script)

    print("\n--- 守护进程：所有脚本已启动，进入监控循环... ---")

    # --- 监控与重启循环 ---
    while not shutdown_requested:
        for script_name, process in managed_processes.items():
            # 如果进程不存在或已终止
            if process is None or process.poll() is not None:
                exit_code = "未知" if process is None else process.returncode
                print(f"--- 守护进程警告：脚本 '{script_name}' 已终止 (退出码: {exit_code})。正在重启...")
                start_script(script_name)
        time.sleep(10)  # 每 10 秒检查一次

    print("--- 守护进程：关闭完成。程序退出。 ---")


if __name__ == "__main__":
    main()