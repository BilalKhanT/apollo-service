import os
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.server_api import ServerApi
from beanie import init_beanie
from typing import Optional

# Load environment variables early
try:
    from dotenv import load_dotenv
    load_dotenv()
    logging.info("Environment variables loaded from .env file")
except ImportError:
    logging.warning("python-dotenv not installed, using system environment variables")

logger = logging.getLogger(__name__)

class Database:
    client: Optional[AsyncIOMotorClient] = None
    database = None

# MongoDB Atlas Configuration
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb+srv://bilalkhan:Nl8k9ZlwybiTi9nx@apollostagedb.iwunbq7.mongodb.net/?retryWrites=true&w=majority&appName=ApolloStageDB")
DATABASE_NAME = os.getenv("DATABASE_NAME", "apollo_crawler")
MONGODB_MIN_POOL_SIZE = int(os.getenv("MONGODB_MIN_POOL_SIZE", "5"))
MONGODB_MAX_POOL_SIZE = int(os.getenv("MONGODB_MAX_POOL_SIZE", "50"))
MONGODB_MAX_IDLE_TIME = int(os.getenv("MONGODB_MAX_IDLE_TIME", "30000"))
MONGODB_CONNECT_TIMEOUT = int(os.getenv("MONGODB_CONNECT_TIMEOUT", "20000"))
MONGODB_SERVER_SELECTION_TIMEOUT = int(os.getenv("MONGODB_SERVER_SELECTION_TIMEOUT", "10000"))

# Debug: Log what URL we're using
logger = logging.getLogger(__name__)
logger.info(f"MongoDB URL loaded: {MONGODB_URL[:50]}... (showing first 50 chars)")
logger.info(f"Database name: {DATABASE_NAME}")

db = Database()

async def connect_to_mongo():
    try:
        # Log the connection attempt (hide credentials)
        safe_url = MONGODB_URL
        if '@' in safe_url:
            parts = safe_url.split('@')
            if len(parts) > 1:
                credentials_part = parts[0]
                if '://' in credentials_part:
                    protocol = credentials_part.split('://')[0]
                    safe_url = f"{protocol}://***:***@{parts[1]}"
        
        logger.info(f"Connecting to MongoDB Atlas at {safe_url}")
        
        # Atlas-optimized connection parameters
        connection_params = {
            "server_api": ServerApi('1'),
            "retryWrites": True,
            "w": "majority",
            "tls": True,
            "tlsAllowInvalidCertificates": False,
            "minPoolSize": MONGODB_MIN_POOL_SIZE,
            "maxPoolSize": MONGODB_MAX_POOL_SIZE,
            "maxIdleTimeMS": MONGODB_MAX_IDLE_TIME,
            "connectTimeoutMS": MONGODB_CONNECT_TIMEOUT,
            "serverSelectionTimeoutMS": MONGODB_SERVER_SELECTION_TIMEOUT,
        }

        logger.info("Configured for MongoDB Atlas connection")

        db.client = AsyncIOMotorClient(MONGODB_URL, **connection_params)
        db.database = db.client[DATABASE_NAME]

        # Test the connection
        await db.client.admin.command('ping')
        logger.info("Successfully connected to MongoDB Atlas")

        # Import all models
        from app.models.database.crawl_result_model import CrawlResult
        from app.models.database.crawl_schedule_model import CrawlSchedule
        from app.models.database.restaurant_deal.restaurant_result_model import DealResult
        from app.models.database.restaurant_deal.deal_schedule_model import DealScrapeSchedule

        await init_beanie(
            database=db.database,
            document_models=[
                CrawlResult,
                CrawlSchedule,
                DealResult,
                # DealScrapeSchedule,
            ]
        )

        logger.info("Beanie ODM initialized successfully with all models")
        
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB Atlas: {str(e)}")
        logger.error("Atlas connection troubleshooting:")
        logger.error("1. Check your connection string includes username/password")
        logger.error("2. Verify your IP address is whitelisted in Atlas")
        logger.error("3. Ensure the database user has proper permissions")
        logger.error("4. Check if the cluster is running and accessible")
        logger.error(f"Current MONGODB_URL value: {MONGODB_URL[:50]}...")
        raise

async def close_mongo_connection():
    if db.client:
        db.client.close()
        logger.info("MongoDB Atlas connection closed")

def get_database():
    return db.database