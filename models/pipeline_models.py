# models/pipeline_models.py - Pydantic Data Models for Pipeline

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


class DataSourceConfig(BaseModel):
    """Configuration for data source nodes."""
    source_type: str  # 'file', 'mqtt', 'database', etc.
    connection_params: Dict[str, Any]
    data_format: Optional[str] = None
    schema_info: Optional[Dict[str, Any]] = None


class ProcessingConfig(BaseModel):
    """Configuration for data processing nodes."""
    operation: str
    parameters: Dict[str, Any]
    validation_rules: Optional[List[Dict[str, Any]]] = Field(default_factory=list)


class COREModelConfig(BaseModel):
    """Configuration for CORE model creation."""
    event_types: List[str] = Field(default_factory=list)
    object_types: List[str] = Field(default_factory=list)
    relationship_types: List[str] = Field(default_factory=list)
    metadata_mapping: Dict[str, str] = Field(default_factory=dict)


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


class PipelineValidationConfig(BaseModel):
    """Configuration for pipeline validation."""
    strict_type_checking: bool = True
    allow_cycles: bool = False
    require_input_nodes: bool = True
    require_output_nodes: bool = False
    custom_rules: List[ValidationRule] = Field(default_factory=list)


class ExecutionEnvironment(BaseModel):
    """Execution environment configuration."""
    environment_type: str = "local"  # local, cluster, cloud
    resource_limits: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: Optional[int] = None
    retry_config: Dict[str, Any] = Field(default_factory=dict)
    logging_config: Dict[str, Any] = Field(default_factory=dict)


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


# Additional utility classes

class NodeCategory(BaseModel):
    """Category for organizing nodes."""
    name: str
    display_name: str
    description: Optional[str] = None
    icon: Optional[str] = None
    color: Optional[str] = None
    order: int = 0


class DataType(BaseModel):
    """Data type definition."""
    name: str
    display_name: str
    description: Optional[str] = None
    color: str
    validation_schema: Optional[Dict[str, Any]] = None


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