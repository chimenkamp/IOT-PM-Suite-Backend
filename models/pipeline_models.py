# models/pipeline_models.py

from pydantic import BaseModel, Field, validator
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from enum import Enum


class PipelinePort(BaseModel):
    """Represents a port on a pipeline node."""
    id: str
    name: str
    dataType: str


class PipelineNode(BaseModel):
    """Represents a node in the pipeline."""
    id: str
    type: str
    position: Dict[str, float] = Field(default_factory=lambda: {"x": 0, "y": 0})
    config: Dict[str, Any] = Field(default_factory=dict)
    inputs: List[PipelinePort] = Field(default_factory=list)
    outputs: List[PipelinePort] = Field(default_factory=list)

    @validator('type')
    def validate_node_type(cls, v):
        valid_types = {
            # Data Input & Loading
            'read-file', 'mqtt-connector',

            # CAIRO XML Parsing Nodes
            'xml-trace-extractor', 'case-object-extractor', 'stream-point-extractor',
            'iot-event-from-stream', 'trace-event-linker',

            # Generic XML Processing Nodes
            'xml-element-selector', 'xml-attribute-extractor', 'nested-list-processor',

            # Stream Processing Nodes
            'lifecycle-calculator', 'stream-aggregator', 'stream-event-creator',
            'stream-metadata-extractor',

            # Enhanced Object Creation
            'dynamic-object-creator', 'attribute-mapper',

            # Enhanced Relationships
            'context-based-linker',

            # Data Processing
            'column-selector', 'attribute-selector', 'data-filter', 'data-mapper',

            # CORE Model Creation
            'iot-event', 'process-event', 'object-creator',

            # Utilities
            'unique-id-generator', 'object-class-selector',

            # Relationships
            'event-object-relation', 'event-event-relation',

            # CORE Model Construction
            'core-metamodel',

            # Output & Export
            'table-output', 'export-ocel', 'ocpm-discovery'
        }
        if v not in valid_types:
            raise ValueError(f'Invalid node type: {v}. Must be one of {valid_types}')
        return v


class PipelineConnection(BaseModel):
    """Represents a connection between two nodes."""
    id: Optional[str] = None
    fromNodeId: str
    fromPortId: str
    toNodeId: str
    toPortId: str
    dataType: Optional[str] = None


class PipelineMetadata(BaseModel):
    """Metadata for the pipeline."""
    name: str
    description: Optional[str] = None
    version: str = "1.0.0"
    createdAt: Optional[str] = None
    modifiedAt: Optional[str] = None
    author: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    parsing_format: Optional[str] = None  # Added for CAIRO format indication


class PipelineDefinition(BaseModel):
    """Complete pipeline definition."""
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    version: str = "1.0.0"
    createdAt: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    nodes: List[PipelineNode]
    connections: List[PipelineConnection]
    executionOrder: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

    @validator('nodes')
    def validate_nodes_not_empty(cls, v):
        if not v:
            raise ValueError('Pipeline must contain at least one node')
        return v

    @validator('connections')
    def validate_connections(cls, v, values):
        if 'nodes' not in values:
            return v

        node_ids = {node.id for node in values['nodes']}
        for conn in v:
            if conn.fromNodeId not in node_ids:
                raise ValueError(f'Connection references unknown fromNodeId: {conn.fromNodeId}')
            if conn.toNodeId not in node_ids:
                raise ValueError(f'Connection references unknown toNodeId: {conn.toNodeId}')
        return v


class ExecutionRequest(BaseModel):
    """Request to execute a pipeline."""
    pipeline: PipelineDefinition
    execution_id: str
    user_id: Optional[str] = "anonymous"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    options: Dict[str, Any] = Field(default_factory=dict)


class NodeTestRequest(BaseModel):
    """Request to test a single node."""
    node: PipelineNode
    mock_inputs: Optional[Dict[str, Any]] = Field(default_factory=dict)
    test_config: Dict[str, Any] = Field(default_factory=dict)


class FileUploadRequest(BaseModel):
    """Request for file upload metadata."""
    fileName: str
    fileType: str
    originalName: Optional[str] = None
    encoding: str = "UTF-8"
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ============ CAIRO-SPECIFIC DATA MODELS ============

class CAIROTraceData(BaseModel):
    """Data model for CAIRO trace information."""
    concept_name: str
    trace_data: Dict[str, Any]
    stream_points: List[Dict[str, Any]] = Field(default_factory=list)
    lifecycle_start: Optional[datetime] = None
    lifecycle_end: Optional[datetime] = None


class StreamPointData(BaseModel):
    """Data model for stream measurement points."""
    trace_concept_name: str
    timestamp: datetime
    stream_id: Optional[str] = None
    stream_source: Optional[str] = None
    stream_value: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    raw_data: Dict[str, Any] = Field(default_factory=dict)


class XMLElementData(BaseModel):
    """Data model for XML elements."""
    tag_name: str
    text_content: Optional[str] = None
    attributes: Dict[str, Any] = Field(default_factory=dict)
    children: List['XMLElementData'] = Field(default_factory=list)
    xpath: Optional[str] = None


class StreamMetadata(BaseModel):
    """Data model for stream metadata."""
    stream_id: str
    stream_source: str
    stream_type: Optional[str] = None
    measurement_unit: Optional[str] = None
    sensor_location: Optional[str] = None
    additional_metadata: Dict[str, Any] = Field(default_factory=dict)


class LifecycleData(BaseModel):
    """Data model for lifecycle information."""
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    total_events: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ============ ENHANCED VALIDATION MODELS ============

class DataSourceConfig(BaseModel):
    """Configuration for data source nodes."""
    source_type: str  # 'file', 'mqtt', 'database', etc.
    connection_params: Dict[str, Any]
    data_format: Optional[str] = None
    schema_info: Optional[Dict[str, Any]] = None
    parsing_format: Optional[str] = None  # 'cairo', 'xes', 'csv', etc.


class XMLParsingConfig(BaseModel):
    """Configuration for XML parsing operations."""
    xpath_expressions: Dict[str, str] = Field(default_factory=dict)
    attribute_mappings: Dict[str, str] = Field(default_factory=dict)
    namespace_prefixes: Dict[str, str] = Field(default_factory=dict)
    output_format: str = "structured"
    preserve_hierarchy: bool = True


class StreamProcessingConfig(BaseModel):
    """Configuration for stream processing operations."""
    aggregation_window: Optional[int] = None  # seconds
    aggregation_functions: List[str] = Field(default_factory=list)
    filtering_conditions: Dict[str, Any] = Field(default_factory=dict)
    metadata_extraction_rules: List[Dict[str, str]] = Field(default_factory=list)


class ProcessingConfig(BaseModel):
    """Configuration for data processing nodes."""
    operation: str
    parameters: Dict[str, Any]
    validation_rules: Optional[List[Dict[str, Any]]] = Field(default_factory=list)
    xml_config: Optional[XMLParsingConfig] = None
    stream_config: Optional[StreamProcessingConfig] = None


class COREModelConfig(BaseModel):
    """Configuration for CORE model creation."""
    event_types: List[str] = Field(default_factory=list)
    object_types: List[str] = Field(default_factory=list)
    relationship_types: List[str] = Field(default_factory=list)
    metadata_mapping: Dict[str, str] = Field(default_factory=dict)
    cairo_specific: Optional[Dict[str, Any]] = Field(default_factory=dict)


class ExportConfig(BaseModel):
    """Configuration for export operations."""
    format: str = "OCEL 2.0 JSON"
    filename: Optional[str] = None
    include_metadata: bool = True
    compression: Optional[str] = None
    export_options: Dict[str, Any] = Field(default_factory=dict)


class ValidationRule(BaseModel):
    """Validation rule for pipeline components."""
    rule_type: str
    field: str
    condition: str
    message: str
    severity: str = "error"  # error, warning, info
    node_types: List[str] = Field(default_factory=list)  # Applicable node types


# ============ CAIRO-SPECIFIC TEMPLATES ============

class CAIROPipelineTemplate(BaseModel):
    """Template specifically for CAIRO XML processing pipelines."""
    name: str = "CAIRO XML Processing Pipeline"
    description: str = "Template for processing CAIRO XML sensor stream logs"
    category: str = "CAIRO"
    template_version: str = "1.0.0"

    # Standard CAIRO processing flow
    node_sequence: List[str] = Field(default_factory=lambda: [
        'read-file',
        'xml-trace-extractor',
        'case-object-extractor',
        'stream-point-extractor',
        'iot-event-from-stream',
        'trace-event-linker',
        'core-metamodel',
        'export-ocel'
    ])

    default_config: Dict[str, Dict[str, Any]] = Field(default_factory=lambda: {
        'xml-trace-extractor': {
            'traceXPath': 'log/trace',
            'traceIdentifier': 'concept:name'
        },
        'case-object-extractor': {
            'caseIdAttribute': 'concept:name',
            'objectType': 'case_object',
            'extractLifecycle': True
        },
        'stream-point-extractor': {
            'streamPointsPath': 'list/list/list',
            'timestampField': 'date',
            'eventDataPath': 'string'
        },
        'iot-event-from-stream': {
            'streamIdField': 'stream:id',
            'streamSourceField': 'stream:source',
            'streamValueField': 'stream:value',
            'eventClass': 'iot_event'
        },
        'trace-event-linker': {
            'linkingAttribute': 'concept:name',
            'relationshipType': 'belongs_to'
        }
    })


class PipelineTemplate(BaseModel):
    """Template for creating pipelines."""
    name: str
    description: str
    category: str
    template_version: str
    node_templates: List[Dict[str, Any]]
    connection_templates: List[Dict[str, Any]]
    default_config: Dict[str, Any] = Field(default_factory=dict)
    required_inputs: List[str] = Field(default_factory=list)
    cairo_template: Optional[CAIROPipelineTemplate] = None


class PipelineValidationConfig(BaseModel):
    """Configuration for pipeline validation."""
    strict_type_checking: bool = True
    allow_cycles: bool = False
    require_input_nodes: bool = True
    require_output_nodes: bool = False
    custom_rules: List[ValidationRule] = Field(default_factory=list)
    cairo_validation: bool = False  # Enable CAIRO-specific validation


class ExecutionEnvironment(BaseModel):
    """Execution environment configuration."""
    environment_type: str = "local"  # local, cluster, cloud
    resource_limits: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: Optional[int] = None
    retry_config: Dict[str, Any] = Field(default_factory=dict)
    logging_config: Dict[str, Any] = Field(default_factory=dict)
    xml_parsing_config: Optional[XMLParsingConfig] = None


# ============ DATA TYPE DEFINITIONS ============

class DataTypeEnum(str, Enum):
    """Enumeration of supported data types."""
    # Core data types
    DATAFRAME = "DataFrame"
    SERIES = "Series"
    ATTRIBUTE = "Attribute"
    EVENT = "Event"
    OBJECT = "Object"
    RELATIONSHIP = "Relationship"
    CORE_MODEL = "COREModel"

    # CAIRO-specific data types
    TRACES = "Traces"
    STREAM_POINTS = "StreamPoints"
    STREAM_EVENTS = "StreamEvents"
    STREAM_METADATA = "StreamMetadata"
    LIFECYCLE_DATA = "LifecycleData"
    XML_ELEMENTS = "Elements"
    FLATTENED_DATA = "FlattenedData"
    CONTEXT_RELATIONSHIPS = "ContextRelationships"
    MAPPED_ATTRIBUTES = "MappedAttributes"


class NodeCategoryEnum(str, Enum):
    """Enumeration of node categories."""
    DATA_INPUT = "Data Input & Loading"
    CAIRO_XML_PARSING = "CAIRO XML Parsing"
    XML_PROCESSING = "Generic XML Processing"
    STREAM_PROCESSING = "Stream Processing"
    DATA_PROCESSING = "Data Processing"
    CORE_MODEL_CREATION = "CORE Model Creation"
    UTILITIES = "Utilities"
    RELATIONSHIPS = "Relationships"
    CORE_MODEL_CONSTRUCTION = "CORE Model Construction"
    OUTPUT_EXPORT = "Output & Export"


class CAIRONodeTypeEnum(str, Enum):
    """Enumeration of CAIRO-specific node types."""
    XML_TRACE_EXTRACTOR = "xml-trace-extractor"
    CASE_OBJECT_EXTRACTOR = "case-object-extractor"
    STREAM_POINT_EXTRACTOR = "stream-point-extractor"
    IOT_EVENT_FROM_STREAM = "iot-event-from-stream"
    TRACE_EVENT_LINKER = "trace-event-linker"
    XML_ELEMENT_SELECTOR = "xml-element-selector"
    XML_ATTRIBUTE_EXTRACTOR = "xml-attribute-extractor"
    NESTED_LIST_PROCESSOR = "nested-list-processor"
    LIFECYCLE_CALCULATOR = "lifecycle-calculator"
    STREAM_AGGREGATOR = "stream-aggregator"
    STREAM_EVENT_CREATOR = "stream-event-creator"
    STREAM_METADATA_EXTRACTOR = "stream-metadata-extractor"
    DYNAMIC_OBJECT_CREATOR = "dynamic-object-creator"
    ATTRIBUTE_MAPPER = "attribute-mapper"
    CONTEXT_BASED_LINKER = "context-based-linker"


# ============ CONFIGURATION VALIDATION ============

class NodeConfigurationSchema(BaseModel):
    """Schema for validating node configurations."""
    node_type: str
    required_fields: List[str] = Field(default_factory=list)
    optional_fields: List[str] = Field(default_factory=list)
    field_types: Dict[str, str] = Field(default_factory=dict)
    field_options: Dict[str, List[str]] = Field(default_factory=dict)
    validation_rules: List[Dict[str, Any]] = Field(default_factory=dict)


# CAIRO-specific configuration schemas
CAIRO_NODE_SCHEMAS = {
    'xml-trace-extractor': NodeConfigurationSchema(
        node_type='xml-trace-extractor',
        required_fields=['traceXPath', 'traceIdentifier'],
        optional_fields=['namespacePrefix'],
        field_types={
            'traceXPath': 'string',
            'traceIdentifier': 'string',
            'namespacePrefix': 'string'
        }
    ),
    'case-object-extractor': NodeConfigurationSchema(
        node_type='case-object-extractor',
        required_fields=['caseIdAttribute', 'objectType'],
        optional_fields=['extractLifecycle'],
        field_types={
            'caseIdAttribute': 'string',
            'objectType': 'string',
            'extractLifecycle': 'boolean'
        }
    ),
    'stream-point-extractor': NodeConfigurationSchema(
        node_type='stream-point-extractor',
        required_fields=['streamPointsPath', 'timestampField', 'eventDataPath'],
        optional_fields=['filterEmpty'],
        field_types={
            'streamPointsPath': 'string',
            'timestampField': 'string',
            'eventDataPath': 'string',
            'filterEmpty': 'boolean'
        }
    ),
    'iot-event-from-stream': NodeConfigurationSchema(
        node_type='iot-event-from-stream',
        required_fields=['streamIdField', 'eventClass'],
        optional_fields=['streamSourceField', 'streamValueField', 'eventIdPattern'],
        field_types={
            'streamIdField': 'string',
            'streamSourceField': 'string',
            'streamValueField': 'string',
            'eventClass': 'string',
            'eventIdPattern': 'string'
        },
        field_options={
            'eventClass': ['iot_event', 'sensor_event', 'measurement_event', 'observation_event']
        }
    ),
    'trace-event-linker': NodeConfigurationSchema(
        node_type='trace-event-linker',
        required_fields=['linkingAttribute', 'relationshipType'],
        optional_fields=['matchingStrategy'],
        field_types={
            'linkingAttribute': 'string',
            'relationshipType': 'string',
            'matchingStrategy': 'string'
        },
        field_options={
            'relationshipType': ['belongs_to', 'monitors', 'observes', 'measures', 'tracks'],
            'matchingStrategy': ['exact_match', 'contains', 'starts_with', 'regex']
        }
    ),
    'xml-element-selector': NodeConfigurationSchema(
        node_type='xml-element-selector',
        required_fields=['xpath', 'outputFormat'],
        optional_fields=['namespacePrefix'],
        field_types={
            'xpath': 'string',
            'outputFormat': 'string',
            'namespacePrefix': 'string'
        },
        field_options={
            'outputFormat': ['Element List', 'Text Values', 'Attribute Values']
        }
    ),
    'stream-aggregator': NodeConfigurationSchema(
        node_type='stream-aggregator',
        required_fields=['aggregationField', 'aggregationFunction'],
        optional_fields=['groupByField', 'timeWindow'],
        field_types={
            'aggregationField': 'string',
            'aggregationFunction': 'string',
            'groupByField': 'string',
            'timeWindow': 'number'
        },
        field_options={
            'aggregationFunction': ['mean', 'sum', 'count', 'min', 'max', 'std']
        }
    ),
    'attribute-mapper': NodeConfigurationSchema(
        node_type='attribute-mapper',
        required_fields=['sourceField', 'targetAttribute'],
        optional_fields=['transformation', 'prefix'],
        field_types={
            'sourceField': 'string',
            'targetAttribute': 'string',
            'transformation': 'string',
            'prefix': 'string'
        },
        field_options={
            'transformation': ['none', 'to_string', 'to_number', 'to_date', 'extract_uuid']
        }
    ),
    'context-based-linker': NodeConfigurationSchema(
        node_type='context-based-linker',
        required_fields=['contextAttribute', 'relationshipType'],
        optional_fields=['matchingStrategy'],
        field_types={
            'contextAttribute': 'string',
            'relationshipType': 'string',
            'matchingStrategy': 'string'
        },
        field_options={
            'relationshipType': ['belongs_to', 'monitors', 'observes', 'measures', 'tracks'],
            'matchingStrategy': ['exact_match', 'contains', 'starts_with', 'regex']
        }
    )
}


class PipelineSchedule(BaseModel):
    """Schedule configuration for pipeline execution."""
    schedule_type: str  # cron, interval, manual
    schedule_expression: Optional[str] = None
    timezone: str = "UTC"
    enabled: bool = True
    next_execution: Optional[datetime] = None
    last_execution: Optional[datetime] = None


class PipelineVersion(BaseModel):
    """Version information for pipelines."""
    version: str
    created_at: datetime
    created_by: str
    changelog: Optional[str] = None
    is_current: bool = False
    pipeline_definition: PipelineDefinition


class UserPreferences(BaseModel):
    """User preferences for pipeline editor."""
    theme: str = "dark"
    auto_save: bool = True
    show_tooltips: bool = True
    grid_size: int = 20
    snap_to_grid: bool = True
    default_node_colors: Dict[str, str] = Field(default_factory=dict)
    cairo_preferences: Dict[str, Any] = Field(default_factory=dict)


class ProjectConfig(BaseModel):
    """Project-level configuration."""
    project_id: str
    name: str
    description: Optional[str] = None
    owner: str
    collaborators: List[str] = Field(default_factory=list)
    settings: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime
    supported_formats: List[str] = Field(default_factory=lambda: ['CSV', 'XML', 'JSON', 'CAIRO'])


# ============ ADDITIONAL UTILITY CLASSES ============

class NodeCategory(BaseModel):
    """Category for organizing nodes."""
    name: str
    display_name: str
    description: Optional[str] = None
    icon: Optional[str] = None
    color: Optional[str] = None
    order: int = 0
    node_types: List[str] = Field(default_factory=list)


class DataType(BaseModel):
    """Data type definition."""
    name: str
    display_name: str
    description: Optional[str] = None
    color: str
    validation_schema: Optional[Dict[str, Any]] = None
    compatible_types: List[str] = Field(default_factory=list)


class PortDefinition(BaseModel):
    """Port definition for node types."""
    name: str
    display_name: str
    data_type: str
    required: bool = True
    multiple: bool = False
    description: Optional[str] = None


class NodeTypeDefinition(BaseModel):
    """Complete definition of a node type."""
    type: str
    display_name: str
    description: str
    category: str
    icon: Optional[str] = None
    color: str
    inputs: List[PortDefinition] = Field(default_factory=list)
    outputs: List[PortDefinition] = Field(default_factory=list)
    config_schema: Dict[str, Any] = Field(default_factory=dict)
    documentation_url: Optional[str] = None
    is_cairo_specific: bool = False


# ============ VALIDATION FUNCTIONS ============

def validate_cairo_pipeline(pipeline: PipelineDefinition) -> List[str]:
    """Validate CAIRO-specific pipeline requirements."""
    errors = []

    # Check for required CAIRO node sequence
    cairo_nodes = [node for node in pipeline.nodes if node.type in CAIRONodeTypeEnum.__members__.values()]

    if cairo_nodes:
        # If CAIRO nodes are present, validate the typical flow
        has_xml_input = any(node.type == 'read-file' and
                            node.config.get('fileType', '').upper() == 'XML'
                            for node in pipeline.nodes)

        if not has_xml_input:
            errors.append("CAIRO pipeline should start with XML file input")

        has_trace_extractor = any(node.type == 'xml-trace-extractor' for node in pipeline.nodes)
        if not has_trace_extractor:
            errors.append("CAIRO pipeline should include XML trace extractor")

        has_case_objects = any(node.type == 'case-object-extractor' for node in pipeline.nodes)
        has_stream_events = any(
            node.type in ['iot-event-from-stream', 'stream-event-creator'] for node in pipeline.nodes)

        if not (has_case_objects or has_stream_events):
            errors.append("CAIRO pipeline should create either case objects or stream events")

    return errors


def get_node_configuration_schema(node_type: str) -> Optional[NodeConfigurationSchema]:
    """Get configuration schema for a specific node type."""
    return CAIRO_NODE_SCHEMAS.get(node_type)