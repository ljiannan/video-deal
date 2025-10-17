# -*- coding: utf-8 -*-
"""
Time:     2025/2/6 9:31
Author:   ZhaoQi Cao(czq)
Version:  V 0.3
File:     rename_file_all.py
Describe: Write during the python at zgxmt, Github link: https://github.com/caozhaoqi
"""
import os
import logging
import datetime
import random

# ==================== 配置区域 ====================
# 在这里修改你的配置
CONFIG = {
    # 要重命名的目录路径
    'directory_path': r"Z:\项目\11动作标注成品数据\XY组\9.10\合格可标注的数据",
    
    # 日志目录路径
    'log_directory': r"./logs",
    
    # 编号设置
    'shuffle_order': True,       # 是否随机打乱文件顺序（True: 随机顺序, False: 按文件名排序）
    'start_number': 91237,       # 开始编号（从此编号开始）
    'number_digits': 7,          # 编号位数
    'number_format': '08d',      # 格式化字符串（08d表示8位数字，不足补0）
    
    # 文件类型过滤（可选）
    'file_extensions': None,     # None表示处理所有文件，或设置如 ['.jpg', '.png', '.mp4']
    
    # 是否递归处理子目录
    'recursive': True,
    
    # 是否只重命名文件（不重命名文件夹）
    'rename_files_only': True,
    
    # 是否显示详细日志
    'verbose': True
}
# ==================== 配置区域结束 ====================

def setup_logging(log_directory):
    """配置日志记录到文件."""
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)  # 创建日志目录

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_directory, f"rename_log_{timestamp}.log")
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    print(f"日志记录到: {log_file}")

def should_process_file(filename):
    """判断是否应该处理该文件"""
    if CONFIG['file_extensions'] is None:
        return True
    
    file_ext = os.path.splitext(filename.lower())[1]
    return file_ext in CONFIG['file_extensions']

def collect_all_files(directory):
    """递归收集所有需要重命名的文件"""
    all_files = []
    
    def collect_recursive(dir_path):
        items = os.listdir(dir_path)
        items.sort()  # 先排序，确保一致性
        
        folders = [item for item in items if os.path.isdir(os.path.join(dir_path, item))]
        files = [item for item in items if os.path.isfile(os.path.join(dir_path, item))]
        
        # 根据配置决定处理哪些项目
        if CONFIG['rename_files_only']:
            items_to_add = [item for item in files if should_process_file(item)]
        else:
            items_to_add = files + folders
        
        # 添加完整路径
        for item in items_to_add:
            full_path = os.path.join(dir_path, item)
            all_files.append(full_path)
        
        # 递归处理子目录
        if CONFIG['recursive']:
            for folder in folders:
                collect_recursive(os.path.join(dir_path, folder))
    
    collect_recursive(directory)
    return all_files

def rename_files_with_mapping(file_list, number_list, stats):
    """根据编号映射重命名文件（使用临时名称避免冲突）"""
    # 第一阶段：重命名为临时名称
    temp_mapping = {}
    for i, file_path in enumerate(file_list):
        directory = os.path.dirname(file_path)
        old_name = os.path.basename(file_path)
        
        # 生成临时文件名（使用UUID避免冲突）
        import uuid
        temp_name = f"temp_{uuid.uuid4().hex}"
        if os.path.isfile(file_path):
            temp_name = temp_name + os.path.splitext(old_name)[1]
        
        temp_path = os.path.join(directory, temp_name)
        
        try:
            os.rename(file_path, temp_path)
            temp_mapping[temp_path] = (old_name, number_list[i], directory)
        except OSError as e:
            error_message = f"临时重命名 {old_name} 出错: {e}"
            logging.error(error_message)
            if CONFIG['verbose']:
                print(error_message)
            stats['error_count'] += 1
    
    # 第二阶段：重命名为最终名称
    for temp_path, (old_name, number, directory) in temp_mapping.items():
        new_name = format(number, CONFIG['number_format'])
        
        # 如果是文件，保留原扩展名
        if os.path.isfile(temp_path):
            new_name = new_name + os.path.splitext(temp_path)[1]
        
        new_path = os.path.join(directory, new_name)
        
        try:
            os.rename(temp_path, new_path)
            log_message = f"重命名: {old_name} -> {new_name}"
            logging.info(log_message)
            if CONFIG['verbose']:
                print(log_message)
            stats['renamed_count'] += 1
        except OSError as e:
            error_message = f"最终重命名 {old_name} 出错: {e}"
            logging.error(error_message)
            if CONFIG['verbose']:
                print(error_message)
            stats['error_count'] += 1

def main():
    """主函数"""
    # 验证配置
    if not os.path.exists(CONFIG['directory_path']):
        print(f"错误: 指定目录不存在: {CONFIG['directory_path']}")
        return
    
    # 验证编号设置
    if CONFIG['number_digits'] < 1:
        print("错误: 编号位数必须大于0")
        return
    
    if CONFIG['start_number'] < 0:
        print("错误: 开始编号不能为负数")
        return
    
    # 更新格式化字符串
    CONFIG['number_format'] = f"0{CONFIG['number_digits']}d"
    
    # 显示配置信息
    print("=" * 50)
    print("文件重命名工具配置:")
    print(f"目标目录: {CONFIG['directory_path']}")
    print(f"开始编号: {CONFIG['start_number']}")
    print(f"编号位数: {CONFIG['number_digits']}")
    print(f"编号格式: {CONFIG['number_format']}")
    print(f"随机打乱顺序: {CONFIG['shuffle_order']}")
    print(f"递归处理: {CONFIG['recursive']}")
    print(f"仅重命名文件: {CONFIG['rename_files_only']}")
    if CONFIG['file_extensions']:
        print(f"文件类型过滤: {CONFIG['file_extensions']}")
    else:
        print("文件类型过滤: 无（处理所有文件）")
    print("=" * 50)
    
    # 收集所有文件
    print("\n正在扫描文件...")
    all_files = collect_all_files(CONFIG['directory_path'])
    total_files = len(all_files)
    
    if total_files == 0:
        print("没有找到需要重命名的文件")
        return
    
    print(f"找到 {total_files} 个文件需要重命名")
    
    # 生成编号列表
    number_list = list(range(CONFIG['start_number'], CONFIG['start_number'] + total_files))
    
    # 如果需要随机打乱顺序
    if CONFIG['shuffle_order']:
        print("正在随机打乱文件顺序...")
        random.shuffle(all_files)  # 随机打乱文件列表
    
    # 确认操作
    confirm = input("\n确认开始重命名操作? (y/N): ").strip().lower()
    if confirm != 'y':
        print("操作已取消")
        return

    # 设置日志
    setup_logging(CONFIG['log_directory'])
    logging.info("开始批量重命名")
    logging.info(f"配置信息: {CONFIG}")
    logging.info(f"总文件数: {total_files}")
    
    # 开始重命名
    print("\n开始重命名...")
    stats = {'renamed_count': 0, 'error_count': 0}
    rename_files_with_mapping(all_files, number_list, stats)
    
    # 显示统计结果
    total_processed = stats['renamed_count'] + stats['error_count']
    print("\n" + "=" * 50)
    print("重命名操作统计结果:")
    print(f"成功重命名文件数: {stats['renamed_count']}")
    print(f"重命名失败文件数: {stats['error_count']}")
    print(f"总处理文件数: {total_processed}")
    print("=" * 50)
    
    logging.info("批量重命名完成")
    logging.info(f"统计结果: 成功 {stats['renamed_count']} 个, 失败 {stats['error_count']} 个, 总计 {total_processed} 个")
    print("重命名操作完成!")

if __name__ == "__main__":
    main()
