# app/utils/redis_client.py

import redis
import json
import logging
from typing import Dict, Any, Optional
from app.core.config import REDIS_HOST, REDIS_PORT, REDIS_PUBSUB_DB, REDIS_PASSWORD

logger = logging.getLogger(__name__)

class RedisClient:
    """Redis client for communication between components."""
    
    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        db: int = REDIS_PUBSUB_DB,
        password: Optional[str] = REDIS_PASSWORD
    ):
        self.redis_url = f"redis://{host}:{port}/{db}"
        self.redis = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )
        logger.info(f"Redis client initialized: {self.redis_url}")
        
    def publish(self, channel: str, message: Dict[str, Any]) -> int:
        try:
            json_message = json.dumps(message)
            return self.redis.publish(channel, json_message)
        except Exception as e:
            logger.error(f"Error publishing to Redis channel {channel}: {str(e)}")
            return 0
            
    def subscribe(self, channel: str):
        try:
            pubsub = self.redis.pubsub()
            pubsub.subscribe(channel)
            return pubsub
        except Exception as e:
            logger.error(f"Error subscribing to Redis channel {channel}: {str(e)}")
            return None
    
    def psubscribe(self, pattern: str):
        try:
            pubsub = self.redis.pubsub()
            pubsub.psubscribe(pattern)
            return pubsub
        except Exception as e:
            logger.error(f"Error pattern-subscribing to Redis channels {pattern}: {str(e)}")
            return None
    
    def get_message(self, pubsub, timeout: float = 0.01):
        try:
            return pubsub.get_message(timeout=timeout)
        except Exception as e:
            logger.error(f"Error getting message from PubSub: {str(e)}")
            return None

redis_client = RedisClient()