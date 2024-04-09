import os
import cv2
from PIL import Image
from ultralytics import YOLO

# Load and fuse the eXtreamly large pre-trained YOLOv8 model
model = YOLO('yolov8x.pt')
model.fuse()

# Define load and save paths
os.makedirs('results', exist_ok=True)
filename = 'airfield_planes'
input_img = f'data/{filename}.png'
output_img = f'results/{filename}_result.png'

# Load the image
image = Image.open(input_img)

# Perform object detection
inference = model(image)
result = inference[0].plot()

# Create a file with bounding boxes
cv2.imwrite(output_img, result)

# Show the img with boxes
cv2.imshow('Airplanes', result)
cv2.waitKey(0)
cv2.destroyAllWindows()