# Distributed High-Performance Video Analytics Pipeline

An event-driven, horizontally scalable video processing pipeline built with Python, Apache Kafka (KRaft), Docker, and YOLOv8. 

This project demonstrates how to decouple video ingestion from heavy AI processing, allowing for fault-tolerant, real-time object detection and analytics that can scale dynamically across multiple consumer nodes.

## Architecture & Workflow

Unlike standard monolithic AI scripts, this system uses a **Producer-Consumer Microservices Architecture**:
1. **Ingestion (Producer):** Captures video frames, compresses them into byte arrays, and publishes them to a Kafka topic.
2. **Message Broker (Kafka in Docker):** Acts as a high-throughput, fault-tolerant buffer using modern KRaft mode (No Zookeeper).
3. **Processing (Consumers):** Multiple AI workers subscribe to the Kafka topic, decode the frames, and run **YOLOv8** object detection.
4. **Event-Driven Analytics:** Consumers implement state-change memory, logging data to a CSV *only* when the detected object count changes (e.g., someone enters or exits), saving massive amounts of disk space and computational overhead.

## Tech Stack
* **Language:** Python 3.x
* **Computer Vision:** OpenCV (`opencv-python`)
* **AI/Deep Learning:** YOLOv8 (`ultralytics`)
* **Message Queue:** Apache Kafka (`kafka-python-ng`)
* **Infrastructure:** Docker & Docker Compose
* **Analytics:** Event-Driven CSV Logging

## Project Structure

```text
distributed-video-analytics
 ┣ docker-compose.yml     # Infrastructure blueprint for Kafka (KRaft mode)
 ┣ producer.py            # Video ingestion and compression node
 ┣ consumer.py            # AI processing, YOLOv8 inference, and analytics node
 ┣ yolov8n.pt             # Pre-trained Nano weights for YOLOv8
 ┗ README.md              # Project documentation