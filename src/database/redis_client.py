import redis
import hashlib
import json
from typing import Optional, Any
from loguru import logger
from src.config.settings import settings


class RedisClient:
    """Singleton Redis client for duplicate checking"""
    
    _instance: Optional['RedisClient'] = None
    _client: Optional[redis.Redis] = None
    
    def __new__(cls):
        """Ensure only one instance exists (Singleton pattern)"""
        if cls._instance is None:
            cls._instance = super(RedisClient, cls).__new__(cls)
            cls._instance._initialize_connection()
        return cls._instance
    
    def _initialize_connection(self):
        """Initialize Redis connection with connection pooling"""
        try:
            pool = redis.ConnectionPool(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD if settings.REDIS_PASSWORD else None,
                decode_responses=True,
                max_connections=10
            )
            
            self._client = redis.Redis(connection_pool=pool)
            
            # Test connection
            self._client.ping()
            logger.info(f"Redis connection established to {settings.REDIS_HOST}:{settings.REDIS_PORT}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def _generate_key(self, data: Any) -> str:
        """
        Generate a unique hash key from data
        
        Args:
            data: Data to hash (dict, list, or string)
            
        Returns:
            MD5 hash string
        """
        if isinstance(data, (dict, list)):
            data_str = json.dumps(data, sort_keys=True)
        else:
            data_str = str(data)
        
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def check_duplicate(self, key: str) -> bool:
        """
        Check if a key exists in Redis
        
        Args:
            key: Key to check
            
        Returns:
            True if key exists (duplicate), False otherwise
        """
        try:
            exists = self._client.exists(key)
            return bool(exists)
        except Exception as e:
            logger.error(f"Error checking duplicate for key {key}: {e}")
            raise
    
    def add_record(self, key: str, value: Any = "1", ttl: int = None):
        """
        Add a record to Redis
        
        Args:
            key: Key to store
            value: Value to store (default "1")
            ttl: Time to live in seconds (optional)
        """
        try:
            if ttl:
                self._client.setex(key, ttl, value)
            else:
                self._client.set(key, value)
            
            logger.debug(f"Added record with key: {key}")
        except Exception as e:
            logger.error(f"Error adding record: {e}")
            raise
    
    def get_record(self, key: str) -> Optional[str]:
        """
        Get a record from Redis
        
        Args:
            key: Key to retrieve
            
        Returns:
            Value if exists, None otherwise
        """
        try:
            return self._client.get(key)
        except Exception as e:
            logger.error(f"Error getting record for key {key}: {e}")
            raise
    
    def delete_record(self, key: str):
        """
        Delete a record from Redis
        
        Args:
            key: Key to delete
        """
        try:
            self._client.delete(key)
            logger.debug(f"Deleted record with key: {key}")
        except Exception as e:
            logger.error(f"Error deleting record: {e}")
            raise
    
    def check_and_add_if_new(self, data: Any, ttl: int = None) -> bool:
        """
        Check if data is duplicate and add if new
        
        Args:
            data: Data to check and add
            ttl: Time to live in seconds (optional)
            
        Returns:
            True if data is new (added), False if duplicate
        """
        key = self._generate_key(data)
        
        if self.check_duplicate(key):
            logger.debug(f"Duplicate found for key: {key}")
            return False
        
        self.add_record(key, "1", ttl or settings.DUPLICATE_TTL)
        logger.debug(f"New record added with key: {key}")
        return True
    
    def close(self):
        """Close Redis connection"""
        if self._client:
            self._client.close()
            logger.info("Redis connection closed")
