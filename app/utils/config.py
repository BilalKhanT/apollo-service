import logging
import os
from typing import List

try:
    from dotenv import load_dotenv
    load_dotenv()
    logging.info("Environment variables loaded from .env file")
except ImportError:
    logging.warning("python-dotenv not installed, using system environment variables")

# Crawler settings
CRAWLER_USER_AGENT: str = os.getenv(
    "CRAWLER_USER_AGENT", 
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
)
CRAWLER_TIMEOUT: int = int(os.getenv("CRAWLER_TIMEOUT", "30"))
CRAWLER_NUM_WORKERS: int = int(os.getenv("CRAWLER_NUM_WORKERS", "20"))
CRAWLER_DELAY_BETWEEN_REQUESTS: float = float(os.getenv("CRAWLER_DELAY_BETWEEN_REQUESTS", "0.5"))
CRAWLER_INACTIVITY_TIMEOUT: int = int(os.getenv("CRAWLER_INACTIVITY_TIMEOUT", "300"))
CRAWLER_SAVE_INTERVAL: int = int(os.getenv("CRAWLER_SAVE_INTERVAL", "20"))
CRAWLER_RESPECT_ROBOTS_TXT: bool = os.getenv("CRAWLER_RESPECT_ROBOTS_TXT", "True").lower() == "true"

# Facebook Scrapper
ACCESS_TOKEN: str = os.getenv("ACCESS_TOKEN", "")
PAGE_ID: str = os.getenv("PAGE_ID", "")
# URL patterns to ignore
DEFAULT_URL_PATTERNS_TO_IGNORE: List[str] = [
    r'logout', r'login', r'signin', r'signout',
    r'\.(zip|rar|exe|dmg|jpeg|png|gif|mov|jpg|mp3|m4v|avi|mp4|aspx)$',
    r'\.jpg',  
    r'/404$',  
]

# Link processor settings
# File extensions to categorize as files
FILE_EXTENSIONS: List[str] = [
    'pdf', 'xls', 'xlsx', 'doc', 'docx', 'ppt', 'pptx', 'xlsb',
    'csv', 'txt', 'rtf', 'zip', 'rar', 'tar', 'gz', 'jpg', 'jpeg', 'png'
]

# Keywords to categorize as social media links
SOCIAL_MEDIA_KEYWORDS: List[str] = [
    'instagram', 'facebook', 'linkedin', 'twitter', 'tiktok',
    'youtube', 'apps.google', 'appstore', 'play.google', 'apps.apple'
]

# Keywords to categorize as bank-related links
#fbl
BANK_KEYWORDS: List[str] = ['fbl', 'faysal']

#ubl
# BANK_KEYWORDS: List[str] = ['ubl', 'united']

# URL clusterer settings
CLUSTER_MIN_SIZE: int = int(os.getenv("CLUSTER_MIN_SIZE", "2"))
CLUSTER_PATH_DEPTH: int = int(os.getenv("CLUSTER_PATH_DEPTH", "2"))
CLUSTER_SIMILARITY_THRESHOLD: float = float(os.getenv("CLUSTER_SIMILARITY_THRESHOLD", "0.5"))

# Scraper and downloader settings
SCRAPER_OUTPUT_DIR: str = os.getenv("SCRAPER_OUTPUT_DIR", "scraped_content")
METADATA_DIR: str = os.getenv("METADATA_DIR", "document_metadata")
EXPIRY_DAYS: int = int(os.getenv("EXPIRY_DAYS", "90"))
FILE_DOWNLOAD_DIR: str = os.getenv("FILE_DOWNLOAD_DIR", "downloaded_files")
MAX_DOWNLOAD_WORKERS: int = int(os.getenv("MAX_DOWNLOAD_WORKERS", "20"))

# Storage settings
DATA_DIR: str = os.getenv("DATA_DIR", "apollo_data")
