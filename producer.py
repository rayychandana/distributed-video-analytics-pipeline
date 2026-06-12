import cv2
import time
from kafka import KafkaProducer

# --- CONFIGURATION ---
KAFKA_TOPIC = "video-stream"
KAFKA_SERVER = "localhost:9092"

# --- SETUP PRODUCER ---
# This acts like the "Delivery Truck Driver"
print(f"Connecting to Kafka at {KAFKA_SERVER}...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    # MAX REQUEST SIZE: By default Kafka rejects big files. 
    # We set this to allow 10MB frames (plenty for video).
    max_request_size=10485760 
)

# --- OPEN CAMERA ---
cap = cv2.VideoCapture(0)
if not cap.isOpened():
    print("Error: Webcam not found.")
    exit()

print(f"Streaming video to topic: '{KAFKA_TOPIC}'... Press 'q' to stop.")

try:
    while True:
        success, frame = cap.read()
        if not success:
            break

        # 1. COMPRESS: We can't send raw pixels (too huge).
        # We encode it as a JPEG (like zipping a file).
        # ret is True/False, buffer is the actual compressed data.
        ret, buffer = cv2.imencode('.jpg', frame)
        
        if not ret:
            continue

        # 2. SEND: Convert the buffer to bytes and ship it!
        # producer.send(Topic Name, Data)
        producer.send(KAFKA_TOPIC, buffer.tobytes())

        # OPTIONAL: Show what we are sending just to be sure
        cv2.imshow("Producer (Sender)", frame)
        
        # Quit if 'q' is pressed
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

except Exception as e:
    print(f"Error: {e}")

finally:
    cap.release()
    producer.close()
    cv2.destroyAllWindows()
    print("Producer stopped.")
