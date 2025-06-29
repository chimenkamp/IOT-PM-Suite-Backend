# models/execution_models.py - Pipeline Execution Result Models

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from enum import Enum


class ExecutionStatus(str, Enum):
    """Status of pipeline execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class NodeExecutionStatus(str, Enum):
    """Status of individual node execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class LogLevel(str, Enum):
    """Log level for execution logs."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ExecutionLog(BaseModel):
    """Individual log entry."""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    level: LogLevel = LogLevel.INFO
    message: str
    node_id: Optional[str] = None
    component: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class NodeExecutionResult(BaseModel):
    """Result of executing a single node."""
    node_id: str
    node_type: str
    status: NodeExecutionStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    data: Optional[Any] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    logs: List[ExecutionLog] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    metrics: Dict[str, Union[int, float, str]] = Field(default_factory=dict)


class ValidationResult(BaseModel):
    """Result of pipeline validation."""
    is_valid: bool
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    validated_at: datetime = Field(default_factory=datetime.utcnow)
    validation_details: Dict[str, Any] = Field(default_factory=dict)


class ExecutionMetrics(BaseModel):
    """Metrics for pipeline execution."""
    total_nodes: int = 0
    completed_nodes: int = 0
    failed_nodes: int = 0
    skipped_nodes: int = 0
    total_duration_seconds: Optional[float] = None
    data_processed_mb: Optional[float] = None
    memory_peak_mb: Optional[float] = None
    cpu_time_seconds: Optional[float] = None


class ExecutionResult(BaseModel):
    """Complete result of pipeline execution."""
    success: bool
    execution_id: str
    status: ExecutionStatus = ExecutionStatus.COMPLETED
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    results: Dict[str, Any] = Field(default_factory=dict)
    logs: List[str] = Field(default_factory=list)  # Simplified logs for API
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

    # Detailed execution information
    node_results: Dict[str, NodeExecutionResult] = Field(default_factory=dict)
    execution_logs: List[ExecutionLog] = Field(default_factory=list)
    metrics: Optional[ExecutionMetrics] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ExecutionSummary(BaseModel):
    """Summary of execution for listing purposes."""
    execution_id: str
    pipeline_name: str
    status: ExecutionStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    success: bool
    user_id: Optional[str] = None
    error_count: int = 0
    warning_count: int = 0


class COREModelResult(BaseModel):
    """Result containing CORE model data."""
    objects_count: int = 0
    iot_events_count: int = 0
    process_events_count: int = 0
    observations_count: int = 0
    event_object_relationships_count: int = 0
    event_event_relationships_count: int = 0
    object_object_relationships_count: int = 0
    extended_table_rows: int = 0
    model_metadata: Dict[str, Any] = Field(default_factory=dict)


class ExportResult(BaseModel):
    """Result of export operation."""
    success: bool
    export_id: str
    format: str
    filename: str
    file_size_bytes: Optional[int] = None
    download_url: Optional[str] = None
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class FileProcessingResult(BaseModel):
    """Result of file processing operations."""
    file_id: str
    original_filename: str
    processed_filename: str
    file_type: str
    rows_count: int = 0
    columns_count: int = 0
    file_size_bytes: int = 0
    encoding: str = "UTF-8"
    schema_info: Dict[str, Any] = Field(default_factory=dict)
    processing_errors: List[str] = Field(default_factory=list)
    processing_warnings: List[str] = Field(default_factory=list)


class DataQualityReport(BaseModel):
    """Data quality assessment report."""
    total_records: int = 0
    missing_values: Dict[str, int] = Field(default_factory=dict)
    duplicate_records: int = 0
    data_types: Dict[str, str] = Field(default_factory=dict)
    value_ranges: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    quality_score: Optional[float] = None
    recommendations: List[str] = Field(default_factory=list)


class NodeTestResult(BaseModel):
    """Result of testing a single node."""
    success: bool
    node_type: str
    test_duration_seconds: float
    message: str
    test_data: Optional[Any] = None
    configuration_valid: bool = True
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    suggestions: List[str] = Field(default_factory=list)


class PipelineExecutionPlan(BaseModel):
    """Execution plan for a pipeline."""
    execution_order: List[str]
    parallel_groups: List[List[str]] = Field(default_factory=list)
    estimated_duration_seconds: Optional[float] = None
    resource_requirements: Dict[str, Any] = Field(default_factory=dict)
    dependencies: Dict[str, List[str]] = Field(default_factory=dict)
    validation_checks: List[str] = Field(default_factory=list)


class ExecutionContext(BaseModel):
    """Context for pipeline execution."""
    execution_id: str
    user_id: str
    environment: str = "local"
    working_directory: str
    temp_directory: str
    resource_limits: Dict[str, Any] = Field(default_factory=dict)
    environment_variables: Dict[str, str] = Field(default_factory=dict)
    feature_flags: Dict[str, bool] = Field(default_factory=dict)


class ProgressUpdate(BaseModel):
    """Progress update during execution."""
    execution_id: str
    current_node: Optional[str] = None
    completed_nodes: int = 0
    total_nodes: int = 0
    progress_percentage: float = 0.0
    estimated_remaining_seconds: Optional[float] = None
    current_operation: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ExecutionError(BaseModel):
    """Detailed error information."""
    error_id: str = Field(default_factory=lambda: str(datetime.utcnow().timestamp()))
    error_type: str
    message: str
    node_id: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    stack_trace: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)
    recovery_suggestions: List[str] = Field(default_factory=list)


class HealthCheckResult(BaseModel):
    """Result of system health check."""
    status: str  # healthy, degraded, unhealthy
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    version: str
    components: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    response_time_ms: Optional[float] = None


class SystemResources(BaseModel):
    """System resource information."""
    cpu_usage_percent: float = 0.0
    memory_usage_percent: float = 0.0
    disk_usage_percent: float = 0.0
    active_executions: int = 0
    queued_executions: int = 0
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class BackupResult(BaseModel):
    """Result of backup operation."""
    backup_id: str
    backup_type: str  # full, incremental
    success: bool
    started_at: datetime
    completed_at: Optional[datetime] = None
    file_count: int = 0
    total_size_bytes: int = 0
    backup_location: str
    errors: List[str] = Field(default_factory=list)


class RestoreResult(BaseModel):
    """Result of restore operation."""
    restore_id: str
    backup_id: str
    success: bool
    started_at: datetime
    completed_at: Optional[datetime] = None
    restored_items: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


# API Response Models

class APIResponse(BaseModel):
    """Generic API response wrapper."""
    success: bool
    message: Optional[str] = None
    data: Optional[Any] = None
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    request_id: Optional[str] = None


class PaginatedResponse(BaseModel):
    """Paginated response for list endpoints."""
    items: List[Any]
    total_count: int
    page: int = 1
    page_size: int = 50
    total_pages: int
    has_next: bool = False
    has_previous: bool = False


class FileUploadResponse(BaseModel):
    """Response for file upload."""
    success: bool
    file_id: str
    filename: str
    original_name: str
    file_type: str
    size: int
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ValidationResponse(BaseModel):
    """Response for validation requests."""
    is_valid: bool
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    validated_at: datetime = Field(default_factory=datetime.utcnow)
    validation_id: Optional[str] = None