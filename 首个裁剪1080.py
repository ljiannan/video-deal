import os
import time
import cv2
import subprocess
import sys
import re
from pathlib import Path
import locale
from tqdm import tqdm
import threading

# 获取系统编码
system_encoding = locale.getpreferredencoding()
print(f"系统编码: {system_encoding}")

# 设置环境变量，处理中文路径问题
os.environ["PYTHONIOENCODING"] = "utf-8"

# 记录开始时间
start_time = time.time()

# 直接指定文件路径，确保与实际路径匹配
input_file = Path(r"F:\0411\内地\边水往事\04 4K_no_head_tail.mp4")
output_dir = Path(r"F:\0411\内地out\边水往事")
output_dir.mkdir(parents=True, exist_ok=True)

# 临时文件放在当前目录
temp_dir = Path("./temp")
temp_dir.mkdir(exist_ok=True)
temp_frame_path = temp_dir / "frame.jpg"

# 检测系统支持的硬件加速
def check_hw_acceleration():
    """检测系统支持的硬件加速选项"""
    encoders = []
    try:
        # 检测NVIDIA GPU加速
        result = subprocess.run(
            ['ffmpeg', '-hide_banner', '-encoders'], 
            capture_output=True, text=True, encoding='utf-8'
        )
        if 'hevc_nvenc' in result.stdout:
            encoders.append(('hevc_nvenc', 'NVIDIA HEVC'))
        if 'h264_nvenc' in result.stdout:
            encoders.append(('h264_nvenc', 'NVIDIA H264'))
        
        # 检测AMD GPU加速
        if 'hevc_amf' in result.stdout:
            encoders.append(('hevc_amf', 'AMD HEVC'))
        if 'h264_amf' in result.stdout:
            encoders.append(('h264_amf', 'AMD H264'))
            
        # 检测Intel GPU加速
        if 'hevc_qsv' in result.stdout:
            encoders.append(('hevc_qsv', 'Intel HEVC'))
        if 'h264_qsv' in result.stdout:
            encoders.append(('h264_qsv', 'Intel H264'))
            
        # 如果没有硬件加速，使用软件编码
        if not encoders:
            encoders.append(('libx264', 'Software H264'))
    except Exception as e:
        print(f"检测硬件加速失败: {e}")
        encoders.append(('libx264', 'Software H264'))
    
    print(f"可用编码器: {encoders}")
    return encoders[0][0]  # 返回第一个可用的编码器

# 从ffmpeg输出解析进度
def parse_progress(line):
    """从ffmpeg输出行解析进度信息"""
    frame_match = re.search(r'frame=\s*(\d+)', line)
    fps_match = re.search(r'fps=\s*(\d+\.?\d*)', line)
    time_match = re.search(r'time=\s*(\d+):(\d+):(\d+\.\d+)', line)
    speed_match = re.search(r'speed=\s*(\d+\.?\d*)x', line)
    
    info = {}
    if frame_match:
        info['frame'] = int(frame_match.group(1))
    if fps_match:
        info['fps'] = float(fps_match.group(1))
    if time_match:
        h, m, s = time_match.groups()
        info['time'] = float(h) * 3600 + float(m) * 60 + float(s)
    if speed_match:
        info['speed'] = float(speed_match.group(1))
    
    return info

# 高效裁剪函数 - 直接使用ffmpeg，避免多次读写和内存消耗
def efficient_crop_video(input_video_path, output_video_path, x, y, w, h):
    """使用ffmpeg直接裁剪视频，效率更高，显示进度条"""
    try:
        print(f"正在处理视频: {input_video_path}")
        print(f"裁剪参数: x={x}, y={y}, w={w}, h={h}")
        
        # 获取视频信息 - 使用utf-8编码运行命令，修复参数
        probe_cmd = [
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'stream=width,height',
            '-of', 'csv=p=0:s=,',
            input_video_path
        ]
        result = subprocess.run(probe_cmd, capture_output=True, text=True, encoding='utf-8')
        if result.returncode != 0:
            raise Exception(f"获取视频信息失败: {result.stderr}")
        
        width, height = map(int, result.stdout.strip().split(','))
        
        # 获取视频时长
        duration_cmd = [
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'csv=p=0',
            input_video_path
        ]
        duration_result = subprocess.run(duration_cmd, capture_output=True, text=True, encoding='utf-8')
        duration = float(duration_result.stdout.strip())
        
        print(f"原始视频尺寸: {width}x{height}, 时长: {duration:.2f}秒")
        
        # 检测最适合的硬件加速编码器
        video_encoder = check_hw_acceleration()
        
        # 基于视频高度选择处理方式
        if height > 1080:
            # 如果视频高度大于1080，裁剪后缩放到1080p
            filter_complex = f"crop={w}:{h}:{x}:{y},scale=1920:1080"
        else:
            # 如果视频高度小于等于1080，裁剪后添加黑边
            black_top = (1080 - h) // 2
            black_bottom = 1080 - h - black_top
            black_left = (1920 - w) // 2
            black_right = 1920 - w - black_left
            
            filter_complex = f"crop={w}:{h}:{x}:{y},pad=1920:1080:{black_left}:{black_top}:black"
        
        # 创建安全的命令字符串并执行
        cmd = [
            'ffmpeg', '-y',
            '-i', input_video_path,
            '-vf', filter_complex,
            '-c:v', video_encoder,  # 使用检测到的硬件加速
            '-preset', 'fast',  # 更快的预设
            '-crf', '23',  # 质量和速度的平衡
            '-c:a', 'aac',
            '-b:a', '192k',
            '-threads', '0',  # 自动使用最佳线程数
            output_video_path
        ]
        
        # 如果是libx264才添加tune选项，有些硬件编码器不支持此选项
        if video_encoder == 'libx264':
            cmd.insert(-1, '-tune')
            cmd.insert(-1, 'fastdecode')  # 优化解码速度
        
        print(f"执行命令: {' '.join(cmd)}")
        
        # 使用subprocess运行ffmpeg命令
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )
        
        # 创建进度条
        pbar = tqdm(total=int(duration), unit='sec', desc="处理进度", ncols=100)
        last_time = 0
        
        # 读取stderr输出并更新进度条
        while True:
            line = process.stderr.readline()
            if not line and process.poll() is not None:
                break
                
            if line:
                progress_info = parse_progress(line)
                if 'time' in progress_info and progress_info['time'] > last_time:
                    # 更新进度条
                    pbar.update(progress_info['time'] - last_time)
                    last_time = progress_info['time']
                    
                    # 显示额外信息
                    extra_info = {}
                    if 'fps' in progress_info:
                        extra_info['FPS'] = f"{progress_info['fps']:.1f}"
                    if 'speed' in progress_info:
                        extra_info['速度'] = f"{progress_info['speed']}x"
                        
                    pbar.set_postfix(**extra_info)
        
        pbar.close()
        
        returncode = process.poll()
        if returncode != 0:
            stderr = process.stderr.read()
            raise Exception(f"ffmpeg处理失败 (代码 {returncode}): {stderr}")
            
        print("\n视频处理完成！")
        return True
    except Exception as e:
        print(f"处理视频时出错: {str(e)}")
        # 失败时尝试使用基本命令
        try:
            print("尝试使用基本命令...")
            basic_cmd = [
                'ffmpeg', '-y',
                '-i', input_video_path,
                '-vf', f"crop={w}:{h}:{x}:{y},scale=1920:1080",
                '-c:v', 'libx264',
                '-preset', 'medium',
                '-crf', '23',
                '-c:a', 'aac',
                output_video_path
            ]
            print(f"执行命令: {' '.join(basic_cmd)}")
            subprocess.run(basic_cmd, check=True, encoding='utf-8')
            print("基本命令处理完成")
            return True
        except Exception as e2:
            print(f"基本命令也失败: {str(e2)}")
            return False

# 高效获取视频帧用于ROI选择
def get_frame_for_roi(video_path, frame_position=0.5):
    """高效地获取视频中的一帧用于ROI选择"""
    try:
        # 确保temp_frame_path是字符串
        temp_frame = str(temp_frame_path)
        
        # 获取视频时长 - 使用简化命令
        probe_cmd = [
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'csv=p=0',
            video_path
        ]
        result = subprocess.run(probe_cmd, capture_output=True, text=True, encoding='utf-8')
        if result.returncode != 0:
            raise Exception(f"获取视频信息失败: {result.stderr}")
        
        duration = float(result.stdout.strip())
        position = duration * frame_position
        
        # 提取帧 - 使用列表形式和utf-8编码，修正以避免image sequence警告
        extract_cmd = [
            'ffmpeg', '-y',
            '-ss', str(position),
            '-i', video_path,
            '-vframes', '1',
            '-q:v', '1',
            temp_frame
        ]
        subprocess.run(extract_cmd, check=True, encoding='utf-8')
        
        # 读取帧
        frame = cv2.imread(temp_frame)
        if frame is None:
            raise Exception("无法读取提取的帧")
            
        # 临时文件留着供调试，程序结束时会删除temp目录
        return frame
    except Exception as e:
        print(f"获取视频帧失败: {str(e)}")
        # 回退到OpenCV方法
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            raise Exception("无法打开视频文件")
            
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        cap.set(cv2.CAP_PROP_POS_FRAMES, int(frame_count * frame_position))
        ret, frame = cap.read()
        cap.release()
        
        if not ret:
            raise Exception("无法读取视频帧")
        return frame

if __name__ == '__main__':
    try:
        video_path = str(input_file)
        
        # 检查文件是否存在
        if not os.path.exists(video_path):
            print(f"错误: 文件不存在 {video_path}")
            exit(1)
        
        print(f"正在打开视频: {video_path}")
        
        # 优化：获取视频的单帧用于ROI选择，而不是加载整个视频
        frame = get_frame_for_roi(video_path)
        
        # 将视频帧调整为720p显示以适应屏幕
        display_height = 720
        scale_factor = display_height / frame.shape[0]
        display_width = int(frame.shape[1] * scale_factor)
        display_frame = cv2.resize(frame, (display_width, display_height))
        
        # 显示ROI选择提示
        instructions = "选择区域并按空格或回车确认"
        font = cv2.FONT_HERSHEY_SIMPLEX
        cv2.putText(display_frame, instructions, (10, 30), font, 1, (255, 255, 255), 2, cv2.LINE_AA)
        
        # 让用户选择ROI
        r = cv2.selectROI(display_frame)
        cv2.destroyAllWindows()
        
        # 转换回原始分辨率
        r_original = (
            int(r[0] / scale_factor), int(r[1] / scale_factor), 
            int(r[2] / scale_factor), int(r[3] / scale_factor)
        )
        
        x, y, w, h = r_original
        print('裁剪参数', r_original)
        
        # 创建输出文件路径，正确处理空格
        output_filename = input_file.name.replace(" ", "_")
        output_video_path = str(output_dir / output_filename)
        
        # 使用高效裁剪函数
        efficient_crop_video(video_path, output_video_path, x, y, w, h)
        
        end_time = time.time()
        print(f"处理完成！总耗时: {end_time - start_time:.2f}秒")
        print(f"成功保存了视频，地址为{output_video_path}")
        
        # 清理临时文件
        import shutil
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            
    except Exception as e:
        print(f"程序发生错误: {str(e)}")
        import traceback
        traceback.print_exc()