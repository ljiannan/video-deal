
import os
import numpy as np
import cv2

# ----------------------- 配置 -----------------------
yolo_net_cfg = r"C:\Users\DELL\Desktop\数据筛选\yolov3.cfg"
yolo_net_weights = r"C:\Users\DELL\Desktop\数据筛选\yolov3.weights"
yolo_net_names = r"C:\Users\DELL\Desktop\数据筛选\coco.names"
video_path = r"X:\视频样例\一般类\其他类\街头表演\354852_segment_1.mp4"
# ----------------------------------------------------

# 检查文件路径是否正确
print("正在检查路径...")
for path in [yolo_net_cfg, yolo_net_weights, yolo_net_names, video_path]:
    if not os.path.isfile(path):
        print(f"❌ 文件不存在: {path}")
        exit(1)
print("✅ 所有文件路径正确")

# 加载 YOLO 模型
try:
    net = cv2.dnn.readNet(yolo_net_weights, yolo_net_cfg)
except cv2.error as e:
    print(f"加载模型失败: {e}")
    exit(1)

# 获取输出层
layer_names = net.getLayerNames()
output_layers = [layer_names[i - 1] for i in net.getUnconnectedOutLayers()]

# 加载 COCO 类别
with open(yolo_net_names, 'r') as f:
    classes = [line.strip() for line in f.readlines()]

def generate_video_description(video_path):
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"无法打开视频文件：{video_path}")
        return "视频读取失败"

    frame_count = 0
    object_counts = {}

    print("🚀 开始视频分析...")
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        height, width, _ = frame.shape
        blob = cv2.dnn.blobFromImage(frame, 0.00392, (416, 416), (0, 0, 0), True, crop=False)
        net.setInput(blob)
        outputs = net.forward(output_layers)

        detected_objects = []
        for out in outputs:
            for detection in out:
                scores = detection[5:]
                class_id = np.argmax(scores)
                confidence = scores[class_id]
                if confidence > 0.5:
                    detected_objects.append(classes[class_id])

        for obj in detected_objects:
            object_counts[obj] = object_counts.get(obj, 0) + 1

        frame_count += 1
        if frame_count % 30 == 0:
            print(f"已处理帧数: {frame_count}")

    cap.release()

    # 生成统计描述
    description = f"该视频共有 {frame_count} 帧，总共检测到以下物体："
    for obj, count in object_counts.items():
        description += f"\n- {obj}: {count} 次"

    # 添加自然语言总结
    description += "\n\n简要描述：\n"
    if not object_counts:
        description += "视频中未检测到明显的物体，可能为静态或低亮度场景。"
    else:
        top_objs = sorted(object_counts.items(), key=lambda x: -x[1])[:3]
        summary = "视频中主要包含 "
        summary += "、".join([f"{obj}（出现约{cnt}次）" for obj, cnt in top_objs])
        summary += " 等内容。"
        description += summary

    return description

# 生成并打印视频描述
desc = generate_video_description(video_path)
print("\n📝 视频内容分析结果：\n")
print(desc)
