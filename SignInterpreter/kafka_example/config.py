import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_URL", 'localhost:29092')
TOPIC = os.getenv("TOPIC_NAME", "hand_keypoints")
