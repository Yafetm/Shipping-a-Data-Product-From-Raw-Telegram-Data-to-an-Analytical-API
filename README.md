# KAIM Week 7: Data Product from Telegram Data

This repository contains code for scraping Telegram channels and transforming data into a star schema for analysis, part of the 10 Academy KAIM Week 7 project.

## Project Structure
- `scrape_telegram.py`: Scrapes messages and images from Telegram channels (`@CheMed123`, `@lobelia4cosmetics`, `@tikvahpharma`, `@EthioMedicine`) using Telethon, storing data in `data/raw/telegram_messages/2025-07-12`.
- `load_json_to_postgres.py`: Loads JSON data into `raw.telegram_messages` table in PostgreSQL.
- `telegram_dbt/`: dbt project for transforming data into a star schema (`dim_channels`, `dim_dates`, `fct_messages`).
- `logs/`: Contains logs (`scrape_telegram.log`, `load_json_to_postgres.log`).
- `data/raw/telegram_messages/`: Data Lake storing JSON files and images.
- `interim_report.tex`: LaTeX source for the interim report.
- `interim_report.pdf`: Compiled interim report.

## Setup Instructions
1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt