# config.py - Backend Configuration Settings

import os
from typing import Dict, Any
from datetime import timedelta


class Config:
    """Base configuration class."""

    # Basic Flask Configuration
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = False
    TESTING = False

    # File Upload Configuration
    MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500MB
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER', 'uploads')
    EXPORT_FOLDER = os.environ.get('EXPORT_FOLDER', 'exports')
    TEMP_FOLDER = os.environ.get('TEMP_FOLDER', 'temp')

    # CORS Configuration
    CORS_ORIGINS = ['http://localhost:4200', 'http://localhost:3000']

    # Database Configuration (for production)
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', 'sqlite:///broom.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Redis Configuration (for caching and task queue)
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

    # Celery Configuration (for async tasks)
    CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/1')
    CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379/2')

    # Pipeline Execution Configuration
    PIPELINE_TIMEOUT_SECONDS = int(os.environ.get('PIPELINE_TIMEOUT_SECONDS', '3600'))  # 1 hour
    MAX_CONCURRENT_EXECUTIONS = int(os.environ.get('MAX_CONCURRENT_EXECUTIONS', '5'))

    # Logging Configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Rate Limiting
    RATELIMIT_STORAGE_URL = os.environ.get('RATELIMIT_STORAGE_URL', 'redis://localhost:6379/3')

    # File Retention
    TEMP_FILE_RETENTION_HOURS = int(os.environ.get('TEMP_FILE_RETENTION_HOURS', '24'))
    EXPORT_FILE_RETENTION_DAYS = int(os.environ.get('EXPORT_FILE_RETENTION_DAYS', '7'))

    # Security
    JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY', SECRET_KEY)
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(hours=24)

    # MQTT Configuration
    MQTT_BROKER_HOST = os.environ.get('MQTT_BROKER_HOST', 'localhost')
    MQTT_BROKER_PORT = int(os.environ.get('MQTT_BROKER_PORT', '1883'))
    MQTT_USERNAME = os.environ.get('MQTT_USERNAME')
    MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD')

    # Health Check Configuration
    HEALTH_CHECK_ENABLED = os.environ.get('HEALTH_CHECK_ENABLED', 'true').lower() == 'true'

    # Monitoring
    PROMETHEUS_METRICS_ENABLED = os.environ.get('PROMETHEUS_METRICS_ENABLED', 'false').lower() == 'true'

    @staticmethod
    def init_app(app):
        """Initialize application with configuration."""
        # Create directories
        os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
        os.makedirs(Config.EXPORT_FOLDER, exist_ok=True)
        os.makedirs(Config.TEMP_FOLDER, exist_ok=True)

        # Set up logging
        import logging
        logging.basicConfig(
            level=getattr(logging, Config.LOG_LEVEL),
            format=Config.LOG_FORMAT
        )


class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True

    # Development-specific settings
    CORS_ORIGINS = ['http://localhost:4200', 'http://localhost:3000', 'http://127.0.0.1:4200']

    # Relaxed file size for development
    MAX_CONTENT_LENGTH = 1024 * 1024 * 1024  # 1GB

    # Development database
    SQLALCHEMY_DATABASE_URI = os.environ.get('DEV_DATABASE_URL', 'sqlite:///broom_dev.db')


class TestingConfig(Config):
    """Testing configuration."""
    TESTING = True
    DEBUG = True

    # Use in-memory database for testing
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'

    # Disable rate limiting for tests
    RATELIMIT_ENABLED = False

    # Use temporary directories for testing
    UPLOAD_FOLDER = 'test_uploads'
    EXPORT_FOLDER = 'test_exports'
    TEMP_FOLDER = 'test_temp'

    # Shorter timeouts for testing
    PIPELINE_TIMEOUT_SECONDS = 300  # 5 minutes


class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False

    # Production security
    SECRET_KEY = os.environ.get('SECRET_KEY')
    if not SECRET_KEY:
        raise ValueError("SECRET_KEY environment variable must be set in production")

    # Production database
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')
    if not SQLALCHEMY_DATABASE_URI:
        raise ValueError("DATABASE_URL environment variable must be set in production")

    # Enable monitoring in production
    PROMETHEUS_METRICS_ENABLED = True
    HEALTH_CHECK_ENABLED = True

    # Production CORS (restrict origins)
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', '').split(',')

    # Production file handling
    MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500MB strict limit

    @staticmethod
    def init_app(app):
        Config.init_app(app)

        # Production-specific initialization
        import logging
        from logging.handlers import RotatingFileHandler

        # Set up file logging
        file_handler = RotatingFileHandler(
            'logs/broom.log',
            maxBytes=10240000,  # 10MB
            backupCount=10
        )
        file_handler.setFormatter(logging.Formatter(Config.LOG_FORMAT))
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)


class DockerConfig(ProductionConfig):
    """Docker deployment configuration."""

    # Docker-specific settings
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'DATABASE_URL',
        'postgresql://broom:broom@postgres:5432/broom'
    )

    REDIS_URL = os.environ.get('REDIS_URL', 'redis://redis:6379/0')
    CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://redis:6379/1')
    CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://redis:6379/2')

    # Docker volume mounts
    UPLOAD_FOLDER = '/app/data/uploads'
    EXPORT_FOLDER = '/app/data/exports'
    TEMP_FOLDER = '/app/data/temp'


# Configuration mapping
config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'docker': DockerConfig,
    'default': DevelopmentConfig
}


def get_config(config_name: str = None) -> Config:
    """Get configuration class based on environment."""
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'default')

    return config.get(config_name, config['default'])


# Application-specific settings
BROOM_SETTINGS = {
    'app_name': 'BROOM',
    'app_version': '1.0.0',
    'app_description': 'Toolbox for IoT-Enhanced Process Mining',

    # Node execution settings
    'node_execution_timeout': 300,  # 5 minutes per node
    'max_node_memory_mb': 1024,  # 1GB per node

    # File processing settings
    'max_csv_rows': 1000000,  # 1M rows
    'max_json_size_mb': 100,  # 100MB
    'supported_encodings': ['utf-8', 'iso-8859-1', 'cp1252'],

    # CORE model settings
    'max_events_per_model': 100000,
    'max_objects_per_model': 50000,
    'max_relationships_per_model': 200000,

    # Export settings
    'export_formats': ['OCEL 2.0 JSON', 'OCEL 2.0 XML', 'CSV', 'Excel'],
    'max_export_size_mb': 500,

    # Validation settings
    'enable_strict_validation': True,
    'enable_data_quality_checks': True,

    # Performance settings
    'chunk_size_for_large_files': 10000,
    'parallel_processing_enabled': True,
    'max_worker_threads': 4
}


def get_app_settings() -> Dict[str, Any]:
    """Get application-specific settings."""
    return BROOM_SETTINGS


def validate_config(config_obj: Config) -> bool:
    """Validate configuration settings."""
    try:
        # Check required directories
        required_dirs = [config_obj.UPLOAD_FOLDER, config_obj.EXPORT_FOLDER, config_obj.TEMP_FOLDER]
        for directory in required_dirs:
            os.makedirs(directory, exist_ok=True)

        # Check file size limits
        if config_obj.MAX_CONTENT_LENGTH <= 0:
            raise ValueError("MAX_CONTENT_LENGTH must be positive")

        # Check timeout settings
        if config_obj.PIPELINE_TIMEOUT_SECONDS <= 0:
            raise ValueError("PIPELINE_TIMEOUT_SECONDS must be positive")

        return True

    except Exception as e:
        print(f"Configuration validation failed: {e}")
        return False