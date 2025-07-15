import schedule
import time
import subprocess
import logging
import os

# Set up logging
logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_scraping():
    """Run scrape_telegram.py."""
    try:
        subprocess.run(['python', 'scripts/scrape_telegram.py'], check=True)
        logger.info("Completed scraping task")
    except Exception as e:
        logger.error(f"Error in scraping task: {str(e)}")

def run_loading():
    """Run load_json_to_postgres.py."""
    try:
        subprocess.run(['python', 'scripts/load_json_to_postgres.py'], check=True)
        logger.info("Completed loading task")
    except Exception as e:
        logger.error(f"Error in loading task: {str(e)}")

def run_dbt():
    """Run dbt models."""
    try:
        subprocess.run(['dbt', 'run', '--project-dir', 'dbt/telegram_dbt'], check=True)
        logger.info("Completed dbt task")
    except Exception as e:
        logger.error(f"Error in dbt task: {str(e)}")

def run_object_detection():
    """Run detect_objects.py."""
    try:
        subprocess.run(['python', 'scripts/detect_objects.py'], check=True)
        logger.info("Completed object detection task")
    except Exception as e:
        logger.error(f"Error in object detection task: {str(e)}")

def run_pipeline():
    """Run full pipeline."""
    logger.info("Starting pipeline")
    run_scraping()
    run_loading()
    run_dbt()
    run_object_detection()
    logger.info("Pipeline completed")

if __name__ == '__main__':
    os.makedirs('logs', exist_ok=True)
    schedule.every().day.at("02:00").do(run_pipeline)
    while True:
        schedule.run_pending()
        time.sleep(60)