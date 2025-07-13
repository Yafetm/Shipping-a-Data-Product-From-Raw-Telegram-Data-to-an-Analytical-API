import os
import json
import logging
from datetime import datetime
from telethon.sync import TelegramClient
from telethon.tl.types import InputMessagesFilterPhotos
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    filename='logs/scrape_telegram.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
phone = os.getenv('TELEGRAM_PHONE')

# Define channels for message scraping
channels = ['@CheMed123', '@lobelia4cosmetics', '@tikvahpharma', '@EthioMedicine']  # Replace @EthioMedicine with actual channel from et.tgstat.com/medicine

# Define channels for image scraping
image_channels = ['@CheMed123', '@lobelia4cosmetics']

# Data Lake path
DATA_LAKE_PATH = 'data/raw/telegram_messages'

async def scrape_channel(client, channel, scrape_images=False):
    """Scrape messages and optionally images from a Telegram channel."""
    try:
        # Get current date for partitioning
        date_str = datetime.now().strftime('%Y-%m-%d')
        channel_name = channel.replace('@', '')
        output_dir = os.path.join(DATA_LAKE_PATH, date_str, channel_name)
        os.makedirs(output_dir, exist_ok=True)

        # Scrape text messages
        messages = []
        async for message in client.iter_messages(channel, limit=100):  # Adjust limit as needed
            msg_data = {
                'message_id': message.id,
                'date': message.date.isoformat(),
                'text': message.text or '',  # Handle empty messages
                'has_image': message.photo is not None,
                'channel': channel_name
            }
            messages.append(msg_data)
            logger.info(f"Scraped message {message.id} from {channel}")

        # Save text messages
        text_output_path = os.path.join(output_dir, 'messages.json')
        with open(text_output_path, 'w', encoding='utf-8') as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(messages)} messages to {text_output_path}")

        # Scrape images if specified
        if scrape_images:
            async for message in client.iter_messages(channel, filter=InputMessagesFilterPhotos, limit=50):
                image_path = os.path.join(output_dir, f"image_{message.id}.jpg")
                await message.download_media(file=image_path)
                logger.info(f"Downloaded image {message.id} to {image_path}")

    except Exception as e:
        logger.error(f"Error scraping {channel}: {str(e)}")

async def main():
    """Main function to scrape all channels."""
    async with TelegramClient('session', api_id, api_hash) as client:
        for channel in channels:
            # Determine if images should be scraped for this channel
            scrape_images = channel in image_channels
            logger.info(f"Starting scrape for {channel} (Images: {scrape_images})")
            await scrape_channel(client, channel, scrape_images)

if __name__ == '__main__':
    import asyncio
    # Create logs directory if not exists
    os.makedirs('logs', exist_ok=True)
    asyncio.run(main())