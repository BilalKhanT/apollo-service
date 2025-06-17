import logging
import os
from typing import List

try:
    from dotenv import load_dotenv
    load_dotenv()
    logging.info("Environment variables loaded from .env file")
except ImportError:
    logging.warning("python-dotenv not installed, using system environment variables")

# document-bulk url
DOCUMENT_BULK_URL: str = os.getenv("DOCUMENT_BULK_SUBMIT_URL", "https://document-bulk.com/api/v1/")

# Crawler settings
CRAWLER_USER_AGENT: str = os.getenv("CRAWLER_USER_AGENT", "")
CRAWLER_TIMEOUT: int = int(os.getenv("CRAWLER_TIMEOUT", "30"))
CRAWLER_NUM_WORKERS: int = int(os.getenv("CRAWLER_NUM_WORKERS", "20"))
CRAWLER_DELAY_BETWEEN_REQUESTS: float = float(os.getenv("CRAWLER_DELAY_BETWEEN_REQUESTS", "0.5"))
CRAWLER_INACTIVITY_TIMEOUT: int = int(os.getenv("CRAWLER_INACTIVITY_TIMEOUT", "300"))
CRAWLER_SAVE_INTERVAL: int = int(os.getenv("CRAWLER_SAVE_INTERVAL", "20"))
CRAWLER_RESPECT_ROBOTS_TXT: bool = os.getenv("CRAWLER_RESPECT_ROBOTS_TXT", "True").lower() == "true"

# Facebook Scrapper
ACCESS_TOKEN: str = os.getenv("ACCESS_TOKEN", "")
PAGE_ID: str = os.getenv("PAGE_ID", "")

# HTML Cleanup Configuration - Load from environment
def load_cleanup_config() -> tuple[List[str], List[str]]:    
    tags_str = os.getenv("TAGS_TO_REMOVE", "")
    classes_str = os.getenv("CLASSES_TO_REMOVE", "")
    
    # Parse comma-separated values and clean whitespace
    tags_to_remove = [tag.strip() for tag in tags_str.split(",") if tag.strip()]
    classes_to_remove = [cls.strip() for cls in classes_str.split(",") if cls.strip()]
    
    # Fallback to minimal defaults if no configuration found
    if not tags_to_remove:
        logging.warning("No TAGS_TO_REMOVE found in environment, using minimal defaults")
        tags_to_remove = ['script', 'style', 'nav', 'footer', 'header']
    
    if not classes_to_remove:
        logging.info("No CLASSES_TO_REMOVE found in environment")
        classes_to_remove = []
    
    logging.info(f"Loaded cleanup config: {len(tags_to_remove)} tags, {len(classes_to_remove)} classes to remove")
    return tags_to_remove, classes_to_remove

# Load the cleanup configuration
TAGS_TO_REMOVE, CLASSES_TO_REMOVE = load_cleanup_config()

def detect_bank_name() -> str:
    bank_name = os.getenv("BANK_NAME", "")
    return bank_name

DETECTED_BANK_NAME = detect_bank_name()

# Dynamic bank keywords based on detected bank
def get_bank_keywords() -> List[str]:
    bank_keywords_map = {
        'UBL': ['ubl', 'united'],
        'FBL': ['fbl', 'faysal'], 
        'BAFL': ['bafl', 'bank alfalah', 'alfalah']
    }
    return bank_keywords_map.get(DETECTED_BANK_NAME, ['bank'])

BANK_KEYWORDS: List[str] = get_bank_keywords()

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