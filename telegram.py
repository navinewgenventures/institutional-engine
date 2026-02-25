import requests
import logging
import time
import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

BASE_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"


def send_message(text, retries=3):

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram credentials missing.")
        return

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text
    }

    for attempt in range(retries):
        try:
            response = requests.post(
                BASE_URL,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            return

        except Exception:
            logging.warning(
                f"Telegram send failed (Attempt {attempt+1}/{retries})"
            )
            time.sleep(2)

    logging.error("Telegram message permanently failed.")