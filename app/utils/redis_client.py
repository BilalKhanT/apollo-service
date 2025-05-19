import redis
import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class RedisClient:
    """Redis client for real-time communication between components."""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None
    ):
        """Initialize the Redis client.
        
        Args:
            host: Redis server hostname
            port: Redis server port
            db: Redis database number
            password: Redis password (if required)
        """
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
        """Publish a message to a Redis channel.
        
        Args:
            channel: Channel name
            message: Message to publish (will be JSON-encoded)
            
        Returns:
            Number of clients that received the message
        """
        try:
            json_message = json.dumps(message)
            return self.redis.publish(channel, json_message)
        except Exception as e:
            logger.error(f"Error publishing to Redis channel {channel}: {str(e)}")
            return 0
            
    def subscribe(self, channel: str):
        """Subscribe to a Redis channel.
        
        Args:
            channel: Channel name to subscribe to
            
        Returns:
            PubSub object that can be used to receive messages
        """
        try:
            pubsub = self.redis.pubsub()
            pubsub.subscribe(channel)
            return pubsub
        except Exception as e:
            logger.error(f"Error subscribing to Redis channel {channel}: {str(e)}")
            return None
    
    def psubscribe(self, pattern: str):
        """Subscribe to Redis channels matching a pattern.
        
        Args:
            pattern: Pattern to match channel names (e.g., "task:*")
            
        Returns:
            PubSub object that can be used to receive messages
        """
        try:
            pubsub = self.redis.pubsub()
            pubsub.psubscribe(pattern)
            return pubsub
        except Exception as e:
            logger.error(f"Error pattern-subscribing to Redis channels {pattern}: {str(e)}")
            return None
    
    def get_message(self, pubsub, timeout: float = 0.01):
        """Get a message from a PubSub object.
        
        Args:
            pubsub: PubSub object
            timeout: Timeout in seconds
            
        Returns:
            Message or None if no message is available
        """
        try:
            return pubsub.get_message(timeout=timeout)
        except Exception as e:
            logger.error(f"Error getting message from PubSub: {str(e)}")
            return None