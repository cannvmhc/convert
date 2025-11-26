import os
from typing import Optional
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()


class Settings:
    """Application configuration settings"""
    
    # MySQL Configuration
    MYSQL_HOST: str = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_PORT: int = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_USER: str = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD: str = os.getenv("MYSQL_PASSWORD", "")
    MYSQL_DATABASE: str = os.getenv("MYSQL_DATABASE", "")
    
    # Redis Configuration
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD", None)
    
    # Application Settings
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "10"))
    DUPLICATE_TTL: int = int(os.getenv("DUPLICATE_TTL", "86400"))  # 24 hours
    
    # Performance Settings for large files
    PARSE_CHUNK_SIZE: int = int(os.getenv("PARSE_CHUNK_SIZE", "10000"))  # Rows to parse at once
    INSERT_CHUNK_SIZE: int = int(os.getenv("INSERT_CHUNK_SIZE", "10000"))  # Rows to insert per batch
    
    @classmethod
    def validate(cls) -> bool:
        """Validate required settings"""
        required_fields = [
            ("MYSQL_HOST", cls.MYSQL_HOST),
            ("MYSQL_DATABASE", cls.MYSQL_DATABASE),
            ("REDIS_HOST", cls.REDIS_HOST),
        ]
        
        for field_name, field_value in required_fields:
            if not field_value:
                logger.error(f"Missing required setting: {field_name}")
                return False
        
        logger.info("Configuration validated successfully")
        return True


# Configure logger
logger.add(
    "logs/app_{time}.log",
    rotation="1 day",
    retention="7 days",
    level=Settings.LOG_LEVEL
)

settings = Settings()
