import os
import json
import psycopg2
from psycopg2.extras import Json
import logging

# Set up logging
logging.basicConfig(
    filename='logs/load_json_to_postgres.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_PARAMS = {
    'dbname': 'telegram_data',
    'user': 'postgres',
    'password': 'yafa@2002',  
    'host': 'localhost',
    'port': '5432'  # Use 5433 if you changed the port
}

# Data Lake path
DATA_LAKE_PATH = 'data/raw/telegram_messages/2025-07-12'

def create_table():
    """Create raw.telegram_messages table if it doesn't exist."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                message_id BIGINT,
                date TIMESTAMP,
                text TEXT,
                has_image BOOLEAN,
                channel VARCHAR(255),
                raw_data JSONB
            );
        """)
        conn.commit()
        logger.info("Created raw.telegram_messages table")
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise

def load_json_files():
    """Load JSON files into raw.telegram_messages table."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        for channel in ['CheMed123', 'lobelia4cosmetics', 'tikvahpharma', 'EthioMedicine']:
            json_path = os.path.join(DATA_LAKE_PATH, channel, 'messages.json')
            if not os.path.exists(json_path):
                logger.warning(f"JSON file not found: {json_path}")
                continue
            with open(json_path, 'r', encoding='utf-8') as f:
                messages = json.load(f)
            for msg in messages:
                cur.execute("""
                    INSERT INTO raw.telegram_messages (message_id, date, text, has_image, channel, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    msg['message_id'],
                    msg['date'],
                    msg['text'],
                    msg['has_image'],
                    msg['channel'],
                    Json(msg)
                ))
            logger.info(f"Loaded {len(messages)} messages from {json_path}")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error loading JSON: {str(e)}")
        raise

if __name__ == '__main__':
    os.makedirs('logs', exist_ok=True)
    create_table()
    load_json_files()