# BROOM Backend - Toolbox for IoT-Enhanced Process Mining

This is the backend implementation for the BROOM (Toolbox for IoT-Enhanced Process Mining) project, which enables the bidirectional conversion of event logs between XES-based formats and the OCEL-based CORE metamodel.

## 🏗️ Architecture

The backend is built using Flask and follows a modular architecture:

```
broom-backend/
├── app.py                          # Main Flask application
├── run.py                          # Application startup script
├── config.py                       # Configuration management
├── requirements.txt                # Python dependencies
├── Dockerfile                      # Container configuration
├── docker-compose.yml              # Multi-service deployment
├── core/                           # Core CORE metamodel implementation
│   ├── ocel_wrapper.py            # Main CORE metamodel class
│   ├── event_definition.py        # Event data models
│   ├── object_definition.py       # Object data models
│   └── relationship_definitions.py # Relationship models
├── services/                       # Business logic services
│   ├── pipeline_service.py        # Pipeline execution logic
│   ├── node_executor.py           # Individual node execution
│   ├── file_service.py            # File upload and processing
│   └── ocel_service.py            # OCEL export functionality
├── models/                         # Data models
│   ├── pipeline_models.py         # Pipeline data structures
│   └── execution_models.py        # Execution results
├── uploads/                        # File upload directory
├── exports/                        # Export output directory
└── logs/                          # Application logs
```

## 🚀 Quick Start

### Local Development

1. **Clone and Setup**
   ```bash
   git clone <repository-url>
   cd broom-backend
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Run the Application**
   ```bash
   python run.py
   ```

   The backend will be available at `http://localhost:5000`

3. **Test the API**
   ```bash
   curl http://localhost:5000/api/health
   ```

### Docker Deployment

1. **Basic Deployment**
   ```bash
   docker-compose up -d
   ```

2. **Development with Hot Reload**
   ```bash
   docker-compose -f docker-compose.yml -f docker-compose.dev.yml up
   ```

3. **Production Deployment**
   ```bash
   docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
   ```

## 📡 API Endpoints

### Health and Status
- `GET /api/health` - Health check
- `GET /api/files/list` - List uploaded files
- `GET /api/executions/list` - List pipeline executions

### File Management
- `POST /api/dataset/upload` - Upload dataset file
- `GET /api/dataset/<file_id>` - Get dataset information

### Pipeline Operations
- `POST /api/pipeline/validate` - Validate pipeline definition
- `POST /api/pipeline/execute` - Execute pipeline
- `GET /api/pipeline/execution/<execution_id>` - Get execution status
- `POST /api/node/test` - Test single node configuration

### Export Operations
- `POST /api/export/ocel/<execution_id>` - Export to OCEL format

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLASK_ENV` | Application environment | `development` |
| `SECRET_KEY` | Flask secret key | `dev-secret-key` |
| `UPLOAD_FOLDER` | File upload directory | `uploads` |
| `EXPORT_FOLDER` | Export output directory | `exports` |
| `MAX_CONTENT_LENGTH` | Max file upload size (bytes) | `500MB` |
| `PIPELINE_TIMEOUT_SECONDS` | Pipeline execution timeout | `3600` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `DATABASE_URL` | Database connection string | `sqlite:///broom.db` |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379/0` |

### Configuration Environments

- **development**: For local development with debug enabled
- **testing**: For running tests with in-memory database
- **production**: For production deployment with security features
- **docker**: For containerized deployment

## 🔄 Pipeline Execution

The backend supports node-based pipeline execution with the following node types:

### Data Input & Loading
- **Read File**: Load data from CSV, XML, YAML, JSON, XES files
- **MQTT Connector**: Connect to MQTT sensor streams

### Data Processing
- **Column Selector**: Extract specific columns from data
- **Attribute Selector**: Select attributes from series data
- **Data Filter**: Apply filtering conditions
- **Data Mapper**: Transform and map data values

### CORE Model Creation
- **IoT Event**: Create IoT events from sensor data
- **Process Event**: Create process events with activity labels
- **Object Creator**: Create objects with classifications

### Utilities
- **Unique ID Generator**: Generate unique identifiers
- **Object Class Selector**: Select object classifications

### Relationships
- **Event-Object Relation**: Create event-object relationships
- **Event-Event Relation**: Create event derivation relationships

### Output & Export
- **Table Output**: Display data in tabular format
- **Export to OCEL**: Export CORE model to OCEL format
- **OCPM Discovery**: Discover object-centric process models

## 📊 CORE Metamodel

The CORE (Common Object-centric Representation for Event logs) metamodel supports:

- **Multiple Event Types**: IoT events, process events, observations
- **Object Classifications**: Data sources, business objects, general objects
- **Rich Relationships**: Event-object, event-event, object-object relationships
- **OCEL Compatibility**: Direct export to OCEL 2.0 JSON/XML formats

### Example Usage

```python
from core.ocel_wrapper import COREMetamodel
from core.event_definition import IotEvent, ProcessEvent
from core.object_definition import Object, ObjectClassEnum
from core.relationship_definitions import EventObjectRelationship

# Create CORE model components
iot_events = [IotEvent(...)]
process_events = [ProcessEvent(...)]
objects = [Object(...)]
relationships = [EventObjectRelationship(...)]

# Create CORE metamodel
core_model = COREMetamodel(
    iot_events=iot_events,
    process_events=process_events,
    objects=objects,
    event_object_relationships=relationships
)

# Export to OCEL
ocel = core_model.get_ocel()
extended_table = core_model.get_extended_table()
```

## 🔍 Monitoring and Logging

### Application Logs
Logs are written to both console and files (in production):
- Location: `logs/broom.log`
- Format: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`
- Rotation: 10MB files, 10 backups

### Health Checks
- **Application**: `GET /api/health`
- **Docker**: Built-in health check every 30 seconds
- **Components**: Database, Redis, file system checks

### Monitoring (Optional)
- **Prometheus**: Metrics collection on port 9090
- **Grafana**: Visualization dashboard on port 3000
- **Flower**: Celery task monitoring on port 5555

## 🧪 Testing

```bash
# Install test dependencies
pip install pytest pytest-flask pytest-cov

# Run tests
pytest

# Run with coverage
pytest --cov=.

# Run specific test file
pytest tests/test_pipeline_service.py
```

## 🐳 Docker Services

The docker-compose setup includes:

- **broom-backend**: Main Flask application
- **broom-worker**: Celery worker for background tasks
- **postgres**: PostgreSQL database
- **redis**: Redis for caching and task queue
- **nginx**: Reverse proxy (optional)
- **flower**: Celery monitoring (development)
- **prometheus/grafana**: Monitoring stack (optional)

## 🔒 Security

### Production Security Features
- Environment-based configuration
- CORS protection with configurable origins
- File upload validation and size limits
- SQL injection prevention with SQLAlchemy
- Rate limiting on API endpoints
- Security headers with Flask-Talisman

### File Security
- Secure filename handling
- File type validation
- Size limit enforcement
- Temporary file cleanup

## 🔧 Development

### Code Style
```bash
# Format code
black .

# Lint code
flake8 .

# Type checking
mypy .
```

### Adding New Node Types
1. Define node in `services/node_definitions.py`
2. Implement execution logic in `services/node_executor.py`
3. Add validation rules in `services/pipeline_service.py`
4. Update frontend node definitions

### Adding New Export Formats
1. Implement export logic in `services/ocel_service.py`
2. Add format to configuration
3. Update API endpoint handling

## 📝 API Documentation

Detailed API documentation is available at `/api/docs` when running the application with Flask-RESTX enabled.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes with tests
4. Run code quality checks
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:
- Check the logs in `logs/broom.log`
- Review the health check endpoint
- Consult the API documentation
- Submit issues to the project repository

---

**BROOM Backend** - Enabling interoperable IoT-enhanced process mining through the CORE metamodel.