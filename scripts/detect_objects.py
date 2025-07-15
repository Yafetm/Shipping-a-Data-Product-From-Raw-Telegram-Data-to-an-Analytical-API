import os
import psycopg2
from ultralytics import YOLO
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(
    filename='logs/detect_objects.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
DB_PARAMS = {
    'dbname': 'telegram_data',
    'user': 'postgres',
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': 'localhost',
    'port': '5432'  # Change to 5433 if needed
}

DATA_LAKE_PATH = 'data/raw/telegram_messages/2025-07-12'

def create_table():
    """Create analytics.image_objects table."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS analytics;
            CREATE TABLE IF NOT EXISTS analytics.image_objects (
                image_path VARCHAR(255),
                message_id BIGINT,
                channel VARCHAR(255),
                object_class VARCHAR(50),
                confidence FLOAT
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Created analytics.image_objects table")
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise

def detect_objects():
    """Detect objects in images using YOLOv8 and store results."""
    try:
        model = YOLO('yolov8n.pt')  # Pre-trained YOLOv8 model
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        channels = ['CheMed123', 'lobelia4cosmetics', 'tikvahpharma', 'EthioMedicine']
        for channel in channels:
            image_dir = os.path.join(DATA_LAKE_PATH, channel)
            if not os.path.exists(image_dir):
                logger.warning(f"Image directory not found: {image_dir}")
                continue
            for image_file in os.listdir(image_dir):
                if image_file.endswith('.jpg'):
                    image_path = os.path.join(image_dir, image_file)
                    message_id = int(image_file.replace('image_', '').replace('.jpg', ''))
                    results = model(image_path)
                    for result in results:
                        for box in result.boxes:
                            class_id = int(box.cls)
                            object_class = model.names[class_id]
                            confidence = float(box.conf)
                            cur.execute("""
                                INSERT INTO analytics.image_objects (image_path, message_id, channel, object_class, confidence)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (image_path, message_id, channel, object_class, confidence))
                    logger.info(f"Processed image: {image_path}")
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Completed object detection")
    except Exception as e:
        logger.error(f"Error detecting objects: {str(e)}")
        raise

if __name__ == '__main__':
    os.makedirs('logs', exist_ok=True)
    create_table()
    detect_objects()