import cv2
import os
import shutil
import concurrent.futures


def is_1920x1080_and_50fps(video_path):
    # 打开视频文件
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"无法打开视频文件: {video_path}")
        return False

    # 读取视频的分辨率和帧率
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    cap.release()  # 释放视频资源

    # return width == 1920 and height == 1080 and fps >= 25
    return min(height,width) >=1080


def process_video(video_path, input_directory, output_directory):
    if is_1920x1080_and_50fps(video_path):
        # 计算目标路径，保持原文件夹结构
        relative_path = os.path.relpath(os.path.dirname(video_path), input_directory)
        target_dir = os.path.join(output_directory, relative_path)
        os.makedirs(target_dir, exist_ok=True)

        # 移动文件到目标目录
        output_path = os.path.join(target_dir, os.path.basename(video_path))
        shutil.move(video_path, output_path)
        print(f"视频已移动到: {output_path}")
        return video_path
    return None


def find_and_move_1920x1080_videos(input_directory, output_directory, max_workers=4):
    # 创建输出文件夹（如果不存在）
    os.makedirs(output_directory, exist_ok=True)

    video_list = []
    video_paths = []

    # 遍历目录下的所有文件
    for root, dirs, files in os.walk(input_directory):
        for file in files:
            # 检查文件扩展名
            if file.endswith(('.mp4', '.mov', '.avi', '.mkv', '.wmv')):
                video_path = os.path.join(root, file)
                video_paths.append(video_path)

    # 使用多线程处理视频文件
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_video, video_path, input_directory, output_directory) for video_path in video_paths]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                video_list.append(result)

    return video_list


if __name__ == "__main__":
    input_directory_to_search = r"F:\待质检"  # 修改为你的视频文件夹路径
    output_directory = r"F:\待质检\分辨率合格"  # 修改为你想移动到的文件夹路径

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    results = find_and_move_1920x1080_videos(input_directory_to_search, output_directory)

    print(f"筛选完成，共找到并移动了 {len(results)} 个 1920x1080 且帧率在 25 及以上的视频文件到 {output_directory}。")