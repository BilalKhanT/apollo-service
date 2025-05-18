import os
from typing import List, Optional

# General settings
# These values should be set in environment variables in production
# Default values are provided for development

# Crawler settings
CRAWLER_USER_AGENT: str = os.getenv(
    "CRAWLER_USER_AGENT", 
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
)
CRAWLER_TIMEOUT: int = int(os.getenv("CRAWLER_TIMEOUT", "30"))
CRAWLER_NUM_WORKERS: int = int(os.getenv("CRAWLER_NUM_WORKERS", "8"))
CRAWLER_DELAY_BETWEEN_REQUESTS: float = float(os.getenv("CRAWLER_DELAY_BETWEEN_REQUESTS", "0.5"))
CRAWLER_INACTIVITY_TIMEOUT: int = int(os.getenv("CRAWLER_INACTIVITY_TIMEOUT", "100"))
CRAWLER_SAVE_INTERVAL: int = int(os.getenv("CRAWLER_SAVE_INTERVAL", "20"))
CRAWLER_RESPECT_ROBOTS_TXT: bool = os.getenv("CRAWLER_RESPECT_ROBOTS_TXT", "True").lower() == "true"

# URL patterns to ignore
DEFAULT_URL_PATTERNS_TO_IGNORE: List[str] = [
    r'logout', r'login', r'signin', r'signout',
    r'\.(zip|rar|exe|dmg|jpeg|png|gif|mov|jpg|mp3|m4v|avi|mp4|aspx)$',
    r'\.jpg',  # Match .jpg files
    r'/404$',  # Ignore URLs ending with /404
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
# In production, these should be customized based on the target bank
BANK_KEYWORDS: List[str] = ['ubl', 'united']

# URL clusterer settings
CLUSTER_MIN_SIZE: int = int(os.getenv("CLUSTER_MIN_SIZE", "2"))
CLUSTER_PATH_DEPTH: int = int(os.getenv("CLUSTER_PATH_DEPTH", "2"))
CLUSTER_SIMILARITY_THRESHOLD: float = float(os.getenv("CLUSTER_SIMILARITY_THRESHOLD", "0.5"))

# Scraper and downloader settings
SCRAPER_OUTPUT_DIR: str = os.getenv("SCRAPER_OUTPUT_DIR", "scraped_content")
METADATA_DIR: str = os.getenv("METADATA_DIR", "document_metadata")
EXPIRY_DAYS: int = int(os.getenv("EXPIRY_DAYS", "90"))
FILE_DOWNLOAD_DIR: str = os.getenv("FILE_DOWNLOAD_DIR", "downloaded_files")
MAX_DOWNLOAD_WORKERS: int = int(os.getenv("MAX_DOWNLOAD_WORKERS", "3"))

# Storage settings
DATA_DIR: str = os.getenv("DATA_DIR", "apollo_data")

# Redis
# REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
# REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
# REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
# REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD", None)