# -*- coding: utf-8 -*-
"""
Time:     2025/4/3 14:51
Author:   ZhaoQi Cao(czq)
Version:  V 1.0
File:     split_scene_tran.py
date:     2025/4/3.py
Describe: Write during the python at Tianjin
GitHub link: https://github.com/caozhaoqi
Blog link: https://caozhaoqi.github.io
WeChat Official Account: 码间拾遗（Code Snippets）
Power by macOS on Mac mini m4(2024)
"""
# -*- coding: utf-8 -*-
"""
Time:     2025/4/3 10:30
Author:   ZhaoQi Cao(czq)
Version:  V 2.0 (Added video splitting based on scenes)
File:     scene_split_mupl.py # Renamed file
date:     2025/4/3
Describe: Detects scenes in videos and splits videos with multiple scenes into individual clips using FFmpeg.
GitHub link: https://github.com/caozhaoqi
Blog link: https://caozhaoqi.github.io
WeChat Official Account: 码间拾遗（Code Snippets）
Power by macOS on Mac mini m4(2024)
"""
import os
import sys
import logging
import shutil
import glob
import threading
import subprocess # For running FFmpeg
import time
import platform

from scenedetect import SceneManager, ContentDetector, StatsManager, VideoOpenFailure
from scenedetect.backends import VideoStreamCv2
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import cv2

# --- Configuration ---
# (Import or define MinIO vars if still needed for other purposes, otherwise remove)
# ...

# --- Logging Setup ---
LOG_DIR = "./logs/"
os.makedirs(LOG_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, "scene_split_{time:YYYYMMDD}.log")

logging.basicConfig( # Use basicConfig for simplicity here, or keep Loguru if preferred
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "scene_split.log"), mode='a', encoding='utf-8'), # Append mode
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__) # Use standard logger

# --- Helper Functions ---

def check_ffmpeg():
    """Checks if ffmpeg command is available in PATH."""
    try:
        # Use '-version' which is standard and exits quickly
        subprocess.run(["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, creationflags=subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0)
        logger.info("FFmpeg found in PATH.")
        return True
    except FileNotFoundError:
        logger.critical("FFmpeg not found. Please install FFmpeg and ensure it's in your system's PATH.")
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg found but '-version' command failed: {e}")
        return False # Treat as unavailable if version check fails
    except Exception as e:
        logger.error(f"Unexpected error checking for FFmpeg: {e}")
        return False

def check_acceleration_support():
    """Checks for potential GPU acceleration support (primarily for OpenCV)."""
    # (Keep this function as is from previous version)
    # ... (Function code from previous version) ...
    pass # Placeholder if you removed the code


def detect_scenes(video_path, threshold=30.0):
    """Detects scene changes in a video using PySceneDetect with VideoStreamCv2."""
    video_manager = None
    try:
        logger.debug(f"Initializing VideoStreamCv2 for: {os.path.basename(video_path)}")
        video_manager = VideoStreamCv2(video_path)
        logger.info(f"Initialized VideoStreamCv2 for {os.path.basename(video_path)}")

        stats_manager = StatsManager()
        scene_manager = SceneManager(stats_manager)
        scene_manager.add_detector(ContentDetector(threshold=threshold))

        try:
            total_frames = video_manager.duration.get_frames()
            # base_timecode = video_manager.base_timecode # No longer needed for get_scene_list
            logging.debug(f"Video Info: Total Frames={total_frames}")
        except Exception as info_err:
             logger.error(f"Error getting video info (duration) for {os.path.basename(video_path)}: {info_err}", exc_info=True)
             return None, video_path, None # Return None for scene list

        if total_frames <= 0:
             logger.warning(f"Video {os.path.basename(video_path)} reported 0 frames. Skipping processing.")
             return 0, video_path, [] # Return 0 scenes, empty list

        # Execute scene detection (no progress bar here as callback wasn't reliable)
        logger.debug(f"Starting scene detection for {os.path.basename(video_path)}")
        scene_manager.detect_scenes(frame_source=video_manager)
        logger.debug(f"Finished scene detection for {os.path.basename(video_path)}")

        # Get scene list (without base_timecode)
        scene_list = scene_manager.get_scene_list()

        logger.info(f"Detected {len(scene_list)} scenes in {os.path.basename(video_path)}")

        # Return scene count, path, and the actual scene list for splitting
        return len(scene_list), video_path, scene_list

    except cv2.error as cv_err:
         logger.error(f"OpenCV error processing {os.path.basename(video_path)}: {cv_err}", exc_info=True)
         return None, video_path, None
    except VideoOpenFailure as vf_err:
         logger.error(f"Failed to open video {os.path.basename(video_path)}: {vf_err}", exc_info=True)
         return None, video_path, None
    except Exception as e:
        logger.error(f"Generic error during scene detection for {os.path.basename(video_path)}: {e}", exc_info=True)
        return None, video_path, None
    finally:
         # Ensure the video file is closed/released by deleting the reference
         if 'video_manager' in locals() and video_manager is not None:
             try:
                 # Explicitly call internal capture release if possible
                 if hasattr(video_manager, '_cap') and video_manager._cap is not None and hasattr(video_manager._cap, 'release'):
                     video_manager._cap.release()
                 del video_manager
             except Exception as del_e:
                 logger.warning(f"Exception while cleaning up video_manager for {os.path.basename(video_path)}: {del_e}")

def split_video_scene(input_path, output_path, start_timecode_str, end_timecode_str):
    """Splits a video segment using FFmpeg without re-encoding."""
    thread_name = threading.current_thread().name
    logger.info(f"[{thread_name}] Splitting scene: {os.path.basename(output_path)} ({start_timecode_str} -> {end_timecode_str})")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Construct FFmpeg command
    # Using -ss after -i and -copyts is generally more frame-accurate for splitting with -c copy
    command = [
        'ffmpeg',
        '-i', str(input_path),  # Input file
        '-ss', start_timecode_str,  # Start time (放 -i 后，精确查找)
        '-to', end_timecode_str,  # End time
        # '-c', 'copy',                # <--- 移除此选项
        # 可以指定编码器和参数以控制质量和大小，例如:
        '-c:v', 'libx264',  # 例如使用 H.264 编码器
        '-crf', '23',  # 控制视频质量 (值越小质量越好，文件越大)
        '-preset', 'fast',  # 编码速度预设
        '-c:a', 'aac',  # 例如使用 AAC 音频编码器
        '-b:a', '128k',  # 音频比特率
        # '-avoid_negative_ts', 'make_zero', # 重新编码时通常不需要
        '-y',  # Overwrite
        str(output_path)  # Output file
    ]

    # Quote arguments with spaces for subprocess safety, especially paths
    # (subprocess handles this reasonably well on POSIX, but crucial on Windows)
    # Alternatively, use shell=True carefully (less safe if paths aren't controlled)
    # command_str = subprocess.list2cmdline(command) # For debugging the command
    # logger.debug(f"Executing FFmpeg command: {command_str}")

    try:
        # Run FFmpeg command
        # creationflags prevents console window popup on Windows
        process = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True, # Raise CalledProcessError if ffmpeg returns non-zero exit code
            creationflags=subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0,
            encoding='utf-8', # Capture output as text
            errors='replace' # Handle potential decoding errors in ffmpeg output
        )
        logger.info(f"[{thread_name}] Successfully split: {os.path.basename(output_path)}")
        # logger.debug(f"FFmpeg stdout:\n{process.stdout}") # Usually empty for -c copy
        # logger.debug(f"FFmpeg stderr:\n{process.stderr}") # Contains progress/info
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"[{thread_name}] FFmpeg failed for {os.path.basename(output_path)}.")
        logger.error(f"  Command: {' '.join(command)}") # Log the command for easier debugging
        logger.error(f"  Return Code: {e.returncode}")
        logger.error(f"  Stderr:\n{e.stderr}")
        # Optionally log stdout too: logger.error(f"  Stdout:\n{e.stdout}")
        return False
    except FileNotFoundError:
        logger.critical(f"[{thread_name}] FFmpeg command not found during split. Ensure FFmpeg is installed and in PATH.")
        # No point continuing if ffmpeg isn't found for any split
        raise # Re-raise to potentially stop the whole process
    except Exception as e:
        logger.error(f"[{thread_name}] Unexpected error splitting {os.path.basename(output_path)}: {e}", exc_info=True)
        return False


def process_video(video_path, threshold, base_dest_dir, originals_dir, downscale_width):
    """Detects scenes, splits video if multiple scenes are found, and moves original."""
    thread_name = threading.current_thread().name
    base_name = os.path.splitext(os.path.basename(video_path))[0]
    file_ext = os.path.splitext(video_path)[1]
    logger.info(f"[{thread_name}] Starting processing for: {base_name}{file_ext}")
    start_time = time.time()
    split_success_count = 0
    split_fail_count = 0

    try:
        num_scenes, _, scene_list = detect_scenes(video_path, threshold) # Get scene_list now

        if num_scenes is None:
            logger.warning(f"[{thread_name}] Video {base_name}{file_ext} failed scene detection. Skipping.")
            # Optionally move failed files to a specific error directory
            return # Stop processing this file

        if num_scenes <= 1:
            logger.info(f"[{thread_name}] Video {base_name}{file_ext} has {num_scenes} scene(s). No splitting needed.")
            # Decide if you want to move single-scene videos somewhere else or leave them
            # Example: move_video(video_path, os.path.join(base_dest_dir, "single_scene"))
        else:
            logger.info(f"[{thread_name}] Video {base_name}{file_ext} has {num_scenes} scenes. Starting split...")
            # Create a subdirectory for this video's scenes
            video_scene_dir = os.path.join(base_dest_dir, base_name)
            os.makedirs(video_scene_dir, exist_ok=True)

            for i, (start_tc, end_tc) in enumerate(scene_list):
                scene_num = i + 1
                output_filename = f"{base_name}_scene_{scene_num:03d}{file_ext}"
                output_filepath = os.path.join(video_scene_dir, output_filename)

                # Use get_timecode() for FFmpeg compatible strings
                start_str = start_tc.get_timecode()
                end_str = end_tc.get_timecode()

                success = split_video_scene(video_path, output_filepath, start_str, end_str)
                if success:
                    split_success_count += 1
                else:
                    split_fail_count += 1

            # After attempting all splits, move the original file
            if split_fail_count == 0 and split_success_count > 0: # Only move original if all splits succeeded
                logger.info(f"[{thread_name}] All {split_success_count} scenes split successfully for {base_name}{file_ext}. Moving original.")
                move_video(video_path, originals_dir)
            elif split_success_count > 0: # Some splits succeeded, some failed
                logger.warning(f"[{thread_name}] Completed splitting for {base_name}{file_ext} with {split_fail_count} failures out of {num_scenes}. Original NOT moved.")
            else: # All splits failed
                logger.error(f"[{thread_name}] All {num_scenes} split attempts failed for {base_name}{file_ext}. Original NOT moved.")


    except Exception as e:
        logger.error(f"[{thread_name}] Top-level error processing {base_name}{file_ext}: {e}", exc_info=True)
    finally:
        end_time = time.time()
        result_summary = f"{num_scenes} scenes detected" if num_scenes is not None else "failed detection"
        if num_scenes is not None and num_scenes > 1:
             result_summary += f" ({split_success_count} split OK, {split_fail_count} split failed)"
        logger.info(f"[{thread_name}] Finished processing {base_name}{file_ext} ({result_summary}) in {end_time - start_time:.2f} seconds.")

def find_video_files(root_dir, extensions=('mp4', 'avi', 'mov', 'mkv')):
    """Recursively finds all video files in a directory with specified extensions."""
    video_files = []
    logger.info(f"Searching for video files with extensions {extensions} in {root_dir}...") # Use logger
    count = 0
    for ext in extensions:
        # Use os.path.normpath 确保路径格式一致
        pattern = os.path.join(os.path.normpath(root_dir), '**', f'*.{ext}')
        logger.debug(f"Searching pattern: {pattern}") # Use logger
        try:
            # glob 可能对非常大的目录效率不高，但对于几百个文件通常没问题
            files = glob.glob(pattern, recursive=True)
            video_files.extend(files)
            count += len(files)
            logger.debug(f"Found {len(files)} files with extension .{ext}") # Use logger
        except Exception as e:
             logger.error(f"Error searching for files with pattern {pattern}: {e}") # Use logger
    logger.info(f"Found a total of {len(video_files)} video files.") # Use logger
    return video_files # <-- ADD THIS LINE

def move_video(video_path, dest_dir):
    """Moves the video file to the destination directory."""
    thread_name = threading.current_thread().name
    if not os.path.exists(video_path):
        logging.error(f"[{thread_name}] Attempted to move non-existent file: {video_path}")
        return
    try:
        os.makedirs(dest_dir, exist_ok=True)
        dest_path = os.path.join(dest_dir, os.path.basename(video_path))

        if os.path.abspath(video_path) == os.path.abspath(dest_path):
            logging.warning(
                f"[{thread_name}] Source and destination paths are the same for {video_path}. Skipping move.")
            return
        if os.path.exists(dest_path):
            logging.warning(
                f"[{thread_name}] Destination file {dest_path} already exists. Skipping move for {video_path}.")
            return

        logging.info(f"[{thread_name}] Attempting to move {os.path.basename(video_path)} to {dest_dir}...")
        shutil.move(video_path, dest_path)
        logging.info(f"[{thread_name}] Successfully moved video {os.path.basename(video_path)} to {dest_dir}")
    except Exception as e:
        logging.error(f"[{thread_name}] Error moving video {video_path} to {dest_dir}: {e}", exc_info=True)


def main(root_dir, dest_dir, num_threads=4, threshold=30.0, downscale_width=640):
    """Main function to process multiple videos in parallel."""
    root_dir = os.path.normpath(root_dir)
    dest_dir = os.path.normpath(dest_dir)

    # Define directory for processed original files
    originals_processed_dir = os.path.join(dest_dir, "originals_processed")
    os.makedirs(originals_processed_dir, exist_ok=True)

    # Filter out files already in the destination or originals directory
    video_files_all = find_video_files(root_dir)
    abs_dest_dir = os.path.abspath(dest_dir) # Includes originals_processed_dir
    video_files = [f for f in video_files_all if not os.path.abspath(f).startswith(abs_dest_dir)]

    if len(video_files) < len(video_files_all):
         logger.warning(f"Filtered out {len(video_files_all) - len(video_files)} files potentially inside destination subdirectories.")

    if not video_files:
        logger.error(f"No valid video files found in {root_dir} (excluding destination).")
        return

    logging.info(f"Starting scene splitting on {len(video_files)} videos from {root_dir} using {num_threads} threads.")
    logging.info(f"Output segments will be in subfolders under: {dest_dir}")
    logging.info(f"Originals (if successfully split) will be moved to: {originals_processed_dir}")

    start_overall_time = time.time()
    processed_count = 0
    successful_runs = 0
    failed_runs = 0

    with ThreadPoolExecutor(max_workers=num_threads, thread_name_prefix='SceneSplitWorker') as executor:
        futures = {executor.submit(process_video, video_file, threshold, dest_dir, originals_processed_dir, downscale_width): video_file for video_file in video_files}

        logging.info("All tasks submitted. Waiting for completion...")
        from concurrent.futures import as_completed
        for future in tqdm(as_completed(futures), total=len(futures), desc="Overall Progress"):
            video_file = futures[future]
            processed_count += 1
            try:
                future.result() # Check for exceptions from the thread
                successful_runs += 1
            except Exception as exc:
                # Exception already logged in thread or called functions
                failed_runs += 1
                logging.debug(f"Worker thread for {os.path.basename(video_file)} failed top-level (already logged).")

    end_overall_time = time.time()
    logging.info(f"Scene splitting complete.")
    logging.info(f"Summary: {len(video_files)} videos submitted. {successful_runs} processed without top-level errors, {failed_runs} encountered errors.")
    logging.info(f"Total execution time: {end_overall_time - start_overall_time:.2f} seconds.")

if __name__ == "__main__":
    # --- CHECK FFMPEG FIRST ---
    if not check_ffmpeg():
        sys.exit(1) # Exit if FFmpeg is not found

    root_dir = r"H:\0415（106盘处理）\下方特殊"
    dest_dir = r"H:\0415（106盘处理）\下方特殊out"

    # Standard directory checks
    if not os.path.exists(root_dir) or not os.path.isdir(root_dir):
        logging.critical(f"Root directory '{root_dir}' not found or is not a directory.")
        sys.exit(1)
    try:
        os.makedirs(dest_dir, exist_ok=True)
        logging.info(f"Ensured base destination directory exists: {dest_dir}")
    except OSError as e:
        logging.critical(f"Failed to create base destination directory '{dest_dir}': {e}")
        sys.exit(1)
    except Exception as e:
         logging.critical(f"Unexpected error creating destination directory '{dest_dir}': {e}")
         sys.exit(1)

    check_acceleration_support()

    # Run main process
    # Adjust num_threads, threshold, downscale_width as needed
    main(root_dir, dest_dir, num_threads=4, threshold=27.0, downscale_width=640)