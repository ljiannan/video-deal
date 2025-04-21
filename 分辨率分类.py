import cv2
import os
import shutil

def get_video_resolution(video_path):
    """获取视频的分辨率"""
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return None, None
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()
    return width, height

def classify_resolution(width, height):
    """根据分辨率分类视频"""
    # 先判断是否为横屏（宽大于高）
    if width >= height:
        # 横屏时，判断宽是否小于2560或者高是否小于1440
        if width < 1080 or height < 1080:
            return '分辨率不合格'
        else:
            return '分辨率合格'
    else:
        # 竖屏时，此时高度相当于横屏的宽度，宽度相当于横屏的高度
        if height < 1080 or width < 1080:
            return '分辨率不合格'
        else:
            return '分辨率合格'


def move_video(src_path, root_dir, resolution_class):
    """移动视频到指定分类文件夹，并保持原有的目录结构"""
    # 分解源路径以获取子文件夹结构
    src_root, src_relpath = os.path.split(os.path.abspath(src_path))
    rel_dirs = os.path.relpath(src_root, root_dir).split(os.sep)
    subdir = os.path.join(*rel_dirs) if rel_dirs else '.'

    # 构建目标路径
    dest_dir = os.path.join(root_dir, resolution_class, subdir)
    # dir = os.path.dirname(dest_dir)
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, os.path.basename(src_path))

    # 移动文件
    shutil.move(src_path, dest_path)
    print(f"Moved {src_path} to {dest_path}")

def process_videos(root_dir):
    resolution_classes = {
        '分辨率合格':'分辨率合格',
        '分辨率不合格': '分辨率不合格'
    }

    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.lower().endswith(('.mp4', '.avi', '.mov', '.mkv','.m4v','.WMV','.rmvb','.DAT','.VOB','WEBM')):  # 添加其他视频格式
                video_path = os.path.join(root, file)
                width, height = get_video_resolution(video_path)
                if width is None or height is None:
                    print(f"Unable to open video: {video_path}")
                    continue

                resolution_class = classify_resolution(width, height)
                move_video(video_path, root_dir, resolution_classes[resolution_class])

if __name__ == "__main__":
    root_dir = r"E:\artlist\4.18\航拍"# 替换为你的视频文件根目录路径
    process_videos(root_dir)
