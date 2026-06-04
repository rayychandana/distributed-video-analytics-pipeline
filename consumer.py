import cv2
import numpy as np
import csv
import os
from datetime import datetime
from kafka import KafkaConsumer
from ultralytics import YOLO

# --- CONFIGURATION ---
KAFKA_TOPIC = "video-stream"
KAFKA_SERVER = "localhost:9092"
LOG_FILE = "analytics_events.csv"  # Renamed to indicate it logs EVENTS only

# --- SETUP 1: Wake up the Brain ---
print("Loading YOLO model...")
model = YOLO('yolov8n.pt')

# --- SETUP 2: Database Connection (CSV) ---
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp", "Event", "Person_Count", "Worker_ID"])

# --- SETUP 3: Connect to Kafka ---
print(f"Connecting to Kafka at {KAFKA_SERVER}...")
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id='my-video-group',
    auto_offset_reset='latest'
)

print("Waiting for video stream...")

# --- NEW: Initialize Memory ---
# We start with -1 so that the very first frame (even if 0 people) counts as a "change"
last_count = -1 

# --- THE WORK LOOP ---
for message in consumer:
    # 1. UNPACK & DECODE
    nparr = np.frombuffer(message.value, np.uint8)
    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    # 2. PROCESS (AI)
    results = model(frame, verbose=False) # verbose=False keeps terminal clean
    
    # Count People (Class ID 0)
    current_count = 0
    for box in results[0].boxes:
        if int(box.cls) == 0:
            current_count += 1
            
    # --- NEW: The "Smart" Filter ---
    # Only proceed if the count has CHANGED since the last time WE checked
    if current_count != last_count:
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        worker_id = f"Worker_{message.partition}"
        
        # Determine the event type
        if current_count > last_count:
            event = "Entry detected"
        else:
            event = "Exit detected"

        # Write to CSV
        with open(LOG_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, event, current_count, worker_id])
        
        # Print to terminal
        print(f"⚠️ EVENT [{worker_id}]: {event} | Count: {current_count}")
        
        # Update our memory
        last_count = current_count

    # 4. SHOW (Optional)
    annotated_frame = results[0].plot()
    cv2.imshow("Smart Consumer", annotated_frame)

    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cv2.destroyAllWindows()