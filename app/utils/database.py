import os
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.server_api import ServerApi
from beanie import init_beanie
from typing import Optional

logger = logging.getLogger(__name__)

class Database:
    client: Optional[AsyncIOMotorClient] = None
    database = None

MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("DATABASE_NAME", "apollo_crawler")
MONGODB_MIN_POOL_SIZE = int(os.getenv("MONGODB_MIN_POOL_SIZE", "5"))
MONGODB_MAX_POOL_SIZE = int(os.getenv("MONGODB_MAX_POOL_SIZE", "50"))
MONGODB_MAX_IDLE_TIME = int(os.getenv("MONGODB_MAX_IDLE_TIME", "30000"))
MONGODB_CONNECT_TIMEOUT = int(os.getenv("MONGODB_CONNECT_TIMEOUT", "20000"))
MONGODB_SERVER_SELECTION_TIMEOUT = int(os.getenv("MONGODB_SERVER_SELECTION_TIMEOUT", "10000"))

db = Database()

def is_atlas_connection(url: str) -> bool:
    return 'mongodb+srv' in url or 'mongodb.net' in url

async def connect_to_mongo():
    try:
        safe_url = MONGODB_URL
        if '@' in safe_url:
            parts = safe_url.split('@')
            if len(parts) > 1:
                credentials_part = parts[0]
                if '://' in credentials_part:
                    protocol = credentials_part.split('://')[0]
                    safe_url = f"{protocol}://***:***@{parts[1]}"
        
        logger.info(f"Connecting to MongoDB at {safe_url}")
        is_atlas = is_atlas_connection(MONGODB_URL)
        connection_params = {
            "minPoolSize": MONGODB_MIN_POOL_SIZE,
            "maxPoolSize": MONGODB_MAX_POOL_SIZE,
            "maxIdleTimeMS": MONGODB_MAX_IDLE_TIME,
            "connectTimeoutMS": MONGODB_CONNECT_TIMEOUT,
            "serverSelectionTimeoutMS": MONGODB_SERVER_SELECTION_TIMEOUT,
        }

        if is_atlas:
            connection_params.update({
                "server_api": ServerApi('1'),
                "retryWrites": True,
                "w": "majority",
                "tls": True,
                "tlsAllowInvalidCertificates": False,
            })
            logger.info("Configured for MongoDB Atlas connection")
        else:
            connection_params["server_api"] = None
            logger.info("Configured for local MongoDB connection")

        db.client = AsyncIOMotorClient(MONGODB_URL, **connection_params)
        db.database = db.client[DATABASE_NAME]

        await db.client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")

        if is_atlas:
            logger.info("Connected to MongoDB Atlas cloud database")
        else:
            logger.info("Connected to local MongoDB instance")

        from app.models.database.database_models import CrawlResult, ClusterDocument, YearDocument
        await init_beanie(
            database=db.database,
            document_models=[CrawlResult, ClusterDocument, YearDocument]
        )
        logger.info("Beanie initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        if is_atlas_connection(MONGODB_URL):
            logger.error("Atlas connection troubleshooting:")
            logger.error("1. Check your connection string includes username/password")
            logger.error("2. Verify your IP address is whitelisted in Atlas")
            logger.error("3. Ensure the database user has proper permissions")
            logger.error("4. Check if the cluster is running and accessible")
        raise

async def close_mongo_connection():
    if db.client:
        db.client.close()
        logger.info("MongoDB connection closed")

def get_database():
    return db.database