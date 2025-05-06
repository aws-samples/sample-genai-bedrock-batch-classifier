import os
import logging
from typing import Dict, Any

# Configure logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)

class EnvironmentConfig:
    """
    Environment configuration for data classifier Lambda function.
    Manages and validates environment variables required for the classifier.
    """
    
    def __init__(self) -> None:
        """Initialize configuration from environment variables."""
        self._config: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load configuration from environment variables."""
        try:
            required_vars = [
                "BEDROCK_ROLE",
                "BEDROCK_MODEL_ID",
                "BEDROCK_JOB_PREFIX",
                "OUTPUT_FOLDER_NAME",
                "JOB_STATUS_TABLE"
            ]

            for var in required_vars:
                value = os.environ.get(var)
                if not value:
                    raise ValueError(f"Missing required environment variable: {var}")
                self._config[var.lower()] = value.strip()
            
            logger.info("Environment configuration loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value for a key.

        Args:
            key (str): Configuration key
            default (Any): Default value if key not found

        """
        try:
            return self._config.get(key.lower(), default)
        except Exception as e:
            logger.warning(f"Error retrieving config value for {key}: {str(e)}")
            return default