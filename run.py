# run.py - Application Startup Script

import logging
import os
import sys

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import app
from config import get_config, validate_config


def create_app(config_name=None):
    """Create and configure the Flask application."""
    # Get configuration
    config_obj = get_config(config_name)

    # Validate configuration
    if not validate_config(config_obj):
        sys.exit(1)

    # Configure the app
    app.config.from_object(config_obj)

    # Initialize app with configuration
    config_obj.init_app(app)

    return app


def setup_logging():
    """Set up application logging."""
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/broom.log') if os.path.exists('logs') else logging.NullHandler()
        ]
    )

    # Set up specific loggers
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def print_startup_info(app_instance):
    """Print startup information."""
    config_name = os.environ.get('FLASK_ENV', 'development')

    print("=" * 60)
    print("🧹 BROOM: Toolbox for IoT-Enhanced Process Mining")
    print("=" * 60)
    print(f"Environment: {config_name}")
    print(f"Debug Mode: {app_instance.config.get('DEBUG', False)}")
    print(f"Upload Folder: {app_instance.config.get('UPLOAD_FOLDER')}")
    print(f"Export Folder: {app_instance.config.get('EXPORT_FOLDER')}")
    print(f"Max File Size: {app_instance.config.get('MAX_CONTENT_LENGTH', 0) // (1024 * 1024)}MB")
    print(f"Pipeline Timeout: {app_instance.config.get('PIPELINE_TIMEOUT_SECONDS', 0)}s")
    print("=" * 60)
    print("📡 API Endpoints:")
    print("  - Health Check: GET /api/health")
    print("  - Upload Dataset: POST /api/dataset/upload")
    print("  - Validate Pipeline: POST /api/pipeline/validate")
    print("  - Execute Pipeline: POST /api/pipeline/execute")
    print("  - Export OCEL: POST /api/export/ocel/<execution_id>")
    print("=" * 60)
    print("🌐 Frontend URL: http://localhost:4200")
    print("🔧 Backend URL: http://localhost:5000")
    print("=" * 60)


def main():
    """Main application entry point."""
    # Set up logging first
    setup_logging()

    # Get command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='BROOM Backend Server')
    parser.add_argument('--config', default=None, help='Configuration environment')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--production', action='store_true', help='Run in production mode')

    args = parser.parse_args()

    # Determine configuration
    if args.production:
        config_name = 'production'
        os.environ['FLASK_ENV'] = 'production'
    elif args.config:
        config_name = args.config
        os.environ['FLASK_ENV'] = args.config
    else:
        config_name = os.environ.get('FLASK_ENV', 'development')

    # Create application
    app_instance = create_app(config_name)

    # Override debug setting if specified
    if args.debug:
        app_instance.config['DEBUG'] = True

    # Print startup information
    print_startup_info(app_instance)

    try:
        # Run the application
        if config_name == 'production':
            # In production, use a proper WSGI server
            print("⚠️  Running in production mode. Consider using gunicorn:")
            print("   gunicorn -w 4 -b 0.0.0.0:5000 'run:create_app(\"production\")'")
            print()

        app_instance.run(
            host=args.host,
            port=args.port,
            debug=app_instance.config.get('DEBUG', False),
            threaded=True
        )

    except KeyboardInterrupt:
        print("\n🛑 Shutting down BROOM backend...")
    except Exception as e:
        logging.error(f"Failed to start application: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()