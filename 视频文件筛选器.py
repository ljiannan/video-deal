#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
视频文件筛选器
功能：
1. 检查指定文件夹中的所有视频文件
2. 筛选出损坏的、格式不正常的（如ts文件）、分辨率低于1080p的视频
3. 将这些视频移动到指定的文件夹
"""

import os
import sys
import shutil
import cv2
import logging
from pathlib import Path
from typing import List, Tuple, Optional
from datetime import datetime
import json

class VideoFileFilter:
    def __init__(self, source_folder: str, target_folder: str):
        """
        初始化视频文件筛选器
        
        Args:
            source_folder: 源文件夹路径
            target_folder: 目标文件夹路径（存放筛选出的文件）
        """
        self.source_folder = Path(source_folder)
        self.target_folder = Path(target_folder)
        
        # 支持的视频格式（正常格式）
        self.normal_video_formats = {
            '.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', 
            '.webm', '.m4v', '.3gp', '.f4v'
        }
        
        # 不正常的格式（需要移动）
        self.abnormal_formats = {
            '.ts', '.m2ts', '.vob', '.dat', '.rm', '.rmvb'
        }
        
        # 最小分辨率要求 (1080p)
        self.min_width = 1920
        self.min_height = 1080
        
        # 统计信息
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'corrupted_files': 0,
            'abnormal_format_files': 0,
            'low_resolution_files': 0,
            'moved_files': 0,
            'errors': 0
        }
        
        # 设置日志
        self.setup_logging()
        
        # 创建目标文件夹
        self.create_target_folders()
    
    def setup_logging(self):
        """设置日志系统"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"视频筛选日志_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def create_target_folders(self):
        """创建目标文件夹结构"""
        # 主目标文件夹
        self.target_folder.mkdir(parents=True, exist_ok=True)
        
        # 子文件夹用于分类存放
        self.corrupted_folder = self.target_folder / "损坏的视频"
        self.abnormal_format_folder = self.target_folder / "格式异常的视频"
        self.low_resolution_folder = self.target_folder / "低分辨率视频"
        
        for folder in [self.corrupted_folder, self.abnormal_format_folder, self.low_resolution_folder]:
            folder.mkdir(parents=True, exist_ok=True)
    
    def get_video_files(self) -> List[Path]:
        """获取所有视频文件"""
        video_extensions = {
            '.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm', 
            '.m4v', '.3gp', '.f4v', '.ts', '.m2ts', '.vob', '.dat',
            '.rm', '.rmvb', '.asf', '.mpg', '.mpeg', '.m1v', '.m2v'
        }
        
        video_files = []
        for ext in video_extensions:
            video_files.extend(self.source_folder.rglob(f"*{ext}"))
            video_files.extend(self.source_folder.rglob(f"*{ext.upper()}"))
        
        self.stats['total_files'] = len(video_files)
        self.logger.info(f"找到 {len(video_files)} 个视频文件")
        return video_files
    
    def check_video_file(self, video_path: Path) -> Tuple[bool, str]:
        """
        检查视频文件
        
        Returns:
            Tuple[bool, str]: (是否需要移动, 移动原因)
        """
        try:
            # 检查文件格式
            if video_path.suffix.lower() in self.abnormal_formats:
                return True, "格式异常"
            
            # 尝试打开视频文件
            cap = cv2.VideoCapture(str(video_path))
            
            if not cap.isOpened():
                cap.release()
                return True, "视频损坏"
            
            # 获取视频属性
            width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            fps = cap.get(cv2.CAP_PROP_FPS)
            
            cap.release()
            
            # 检查是否有有效的尺寸和帧数
            if width <= 0 or height <= 0 or frame_count <= 0:
                return True, "视频损坏"
            
            # 检查分辨率
            if width < self.min_width or height < self.min_height:
                return True, "分辨率过低"
            
            self.logger.info(f"正常视频: {video_path.name} - {width}x{height}, {frame_count}帧, {fps:.2f}fps")
            return False, "正常"
            
        except Exception as e:
            self.logger.error(f"检查视频文件时出错 {video_path}: {str(e)}")
            return True, "检查失败"
    
    def move_video_file(self, source_path: Path, reason: str) -> bool:
        """
        移动视频文件到对应的目标文件夹
        
        Args:
            source_path: 源文件路径
            reason: 移动原因
            
        Returns:
            bool: 是否成功移动
        """
        try:
            # 根据原因选择目标文件夹
            if "损坏" in reason or "失败" in reason:
                target_dir = self.corrupted_folder
                self.stats['corrupted_files'] += 1
            elif "格式异常" in reason:
                target_dir = self.abnormal_format_folder
                self.stats['abnormal_format_files'] += 1
            elif "分辨率过低" in reason:
                target_dir = self.low_resolution_folder
                self.stats['low_resolution_files'] += 1
            else:
                target_dir = self.target_folder
            
            # 确保目标文件名唯一
            target_path = target_dir / source_path.name
            counter = 1
            while target_path.exists():
                stem = source_path.stem
                suffix = source_path.suffix
                target_path = target_dir / f"{stem}_{counter}{suffix}"
                counter += 1
            
            # 移动文件
            shutil.move(str(source_path), str(target_path))
            self.logger.info(f"已移动: {source_path.name} -> {target_path} (原因: {reason})")
            self.stats['moved_files'] += 1
            return True
            
        except Exception as e:
            self.logger.error(f"移动文件失败 {source_path}: {str(e)}")
            self.stats['errors'] += 1
            return False
    
    def process_videos(self):
        """处理所有视频文件"""
        video_files = self.get_video_files()
        
        if not video_files:
            self.logger.info("未找到任何视频文件")
            return
        
        self.logger.info(f"开始处理 {len(video_files)} 个视频文件...")
        
        for i, video_path in enumerate(video_files, 1):
            self.logger.info(f"处理进度: {i}/{len(video_files)} - {video_path.name}")
            
            try:
                # 检查视频文件
                should_move, reason = self.check_video_file(video_path)
                
                if should_move:
                    # 移动文件
                    success = self.move_video_file(video_path, reason)
                    if not success:
                        self.stats['errors'] += 1
                
                self.stats['processed_files'] += 1
                
            except Exception as e:
                self.logger.error(f"处理文件时出错 {video_path}: {str(e)}")
                self.stats['errors'] += 1
        
        # 输出统计信息
        self.print_statistics()
    
    def print_statistics(self):
        """打印统计信息"""
        self.logger.info("=" * 50)
        self.logger.info("处理完成！统计信息:")
        self.logger.info(f"总文件数: {self.stats['total_files']}")
        self.logger.info(f"已处理: {self.stats['processed_files']}")
        self.logger.info(f"损坏的视频: {self.stats['corrupted_files']}")
        self.logger.info(f"格式异常的视频: {self.stats['abnormal_format_files']}")
        self.logger.info(f"低分辨率视频: {self.stats['low_resolution_files']}")
        self.logger.info(f"总共移动: {self.stats['moved_files']}")
        self.logger.info(f"处理错误: {self.stats['errors']}")
        self.logger.info("=" * 50)
        
        # 保存统计信息到JSON文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stats_file = f"视频筛选统计_{timestamp}.json"
        
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"统计信息已保存到: {stats_file}")


def main():
    """主函数"""
    print("=== 视频文件筛选器 ===")
    print("功能：检查并移动损坏、格式异常、低分辨率的视频文件")
    print()
    
    # 获取用户输入
    while True:
        source_folder = input("请输入源文件夹路径: ").strip().strip('"')
        if os.path.exists(source_folder):
            break
        print("文件夹不存在，请重新输入！")
    
    while True:
        target_folder = input("请输入目标文件夹路径（存放筛选出的文件）: ").strip().strip('"')
        if target_folder:
            break
        print("目标文件夹路径不能为空！")
    
    # 确认操作
    print(f"\n源文件夹: {source_folder}")
    print(f"目标文件夹: {target_folder}")
    print(f"筛选条件:")
    print(f"  - 损坏的视频文件")
    print(f"  - 格式异常的文件（如.ts文件）")
    print(f"  - 分辨率低于1080p的视频")
    print()
    
    confirm = input("确认开始处理？(y/n): ").strip().lower()
    if confirm != 'y':
        print("操作已取消")
        return
    
    # 开始处理
    try:
        filter_tool = VideoFileFilter(source_folder, target_folder)
        filter_tool.process_videos()
        
        print("\n处理完成！请查看日志文件获取详细信息。")
        input("按回车键退出...")
        
    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")
        input("按回车键退出...")


if __name__ == "__main__":
    main()
