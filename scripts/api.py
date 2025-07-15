from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
import logging

# Set up logging
logging.basicConfig(
    filename='logs/api.log',
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

app = FastAPI(title="Telegram Data API", description="API for KAIM Week 7 Telegram Data Product")

def get_db_connection():
    """Establish PostgreSQL connection."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/messages/", summary="Retrieve messages with optional filters")
async def get_messages(channel: str = None, start_date: str = None, end_date: str = None):
    """Get messages, optionally filtered by channel and date range."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT f.message_id, f.date, c.channel_name, f.text, f.has_image
            FROM analytics.fct_messages f
            JOIN analytics.dim_channels c ON f.channel_id = c.channel_id
            WHERE 1=1
        """
        params = []
        if channel:
            query += " AND c.channel_name = %s"
            params.append(channel)
        if start_date:
            query += " AND f.date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND f.date <= %s"
            params.append(end_date)
        cur.execute(query, params)
        messages = cur.fetchall()
        cur.close()
        conn.close()
        logger.info(f"Retrieved {len(messages)} messages")
        return messages
    except Exception as e:
        logger.error(f"Error retrieving messages: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/channels/", summary="Retrieve all channels")
async def get_channels():
    """Get list of all channels."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT channel_id, channel_name FROM analytics.dim_channels")
        channels = cur.fetchall()
        cur.close()
        conn.close()
        logger.info(f"Retrieved {len(channels)} channels")
        return channels
    except Exception as e:
        logger.error(f"Error retrieving channels: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)