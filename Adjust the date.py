import os
import time
import random
from datetime import datetime, timedelta

# 定义视频文件的扩展名列表
VIDEO_EXTENSIONS = ('.mp4', '.avi', '.mkv', '.mov', '.flv', '.wmv')


def random_time_in_range(start_time, end_time):
    """生成在给定时间段内的随机时间"""
    # 将时间字符串转换为 datetime 对象
    start_dt = datetime.strptime(start_time, "%H:%M:%S")
    end_dt = datetime.strptime(end_time, "%H:%M:%S")

    # 计算时间段的总秒数
    delta_seconds = int((end_dt - start_dt).total_seconds())

    # 随机生成秒数
    random_seconds = random.randint(0, delta_seconds)

    # 获取随机的时间
    random_time = start_dt + timedelta(seconds=random_seconds)

    # 返回随机时间的字符串格式
    return random_time.strftime("%H:%M:%S")


def change_video_dates(folder_path, start_time, end_time):
    # 获取当天的日期
    today_date = datetime.today().strftime('%Y-%m-%d')

    # 遍历文件夹中的所有文件
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            # 如果文件是视频文件
            if file.lower().endswith(VIDEO_EXTENSIONS):
                # 在指定时间段内随机生成时间
                random_time = random_time_in_range(start_time, end_time)

                # 将日期和随机时间组合
                new_datetime_str = f"{today_date} {random_time}"

                # 将新的日期时间字符串转换为 Unix 时间戳
                new_timestamp = time.mktime(time.strptime(new_datetime_str, "%Y-%m-%d %H:%M:%S"))

                # 获取文件的完整路径
                file_path = os.path.join(root, file)

                # 修改文件的时间戳
                os.utime(file_path, (new_timestamp, new_timestamp))
                print(f"文件 {file} 的日期已修改为 {new_datetime_str}")


# 输入文件夹路径
folder_path = r"Z:\项目\航拍特写\董子瑶\7.29\美食"  # 替换成实际的文件夹路径

# 设置时间段（例如从 09:00:00 到 18:00:00）
start_time = "09:10:00"
end_time = "18:30:00"

# 调用函数修改视频文件日期
change_video_dates(folder_path, start_time, end_time)

print("所有视频文件的日期已修改完毕")
