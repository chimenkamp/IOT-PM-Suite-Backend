# services/pipeline_service.py - Core Pipeline Execution Logic

import logging
import traceback
from datetime import datetime
from typing import Dict, List, Any

from core.ocel_wrapper import COREMetamodel
from models.execution_models import ExecutionResult, ValidationResult
from models.pipeline_models import PipelineDefinition, PipelineNode, ExecutionRequest
from services.file_service import FileService
from services.node_executor import NodeExecutor

logger = logging.getLogger(__name__)


class PipelineService:
    """Service for executing and managing pipelines."""

    def __init__(self):
        self.node_executor = NodeExecutor()
        self.file_service = FileService('uploads')

    def validate_pipeline(self, pipeline: PipelineDefinition) -> ValidationResult:
        """
        Validate a pipeline definition for correctness.

        :param pipeline: Pipeline to validate
        :return: ValidationResult with validation status and messages
        """
        errors = []
        warnings = []

        try:
            # Check if pipeline has nodes
            if not pipeline.nodes:
                errors.append("Pipeline must contain at least one node")
                return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

            # Check for input nodes (data sources)
            input_nodes = [n for n in pipeline.nodes if n.type in ['read-file', 'mqtt-connector']]
            if not input_nodes:
                errors.append("Pipeline must have at least one input node (Read File or MQTT Connector)")

            # Check for output nodes
            output_nodes = [n for n in pipeline.nodes if n.type in ['table-output', 'export-ocel', 'ocpm-discovery']]
            if not output_nodes:
                warnings.append("Consider adding an output node to visualize results")

            # Check for disconnected nodes
            connected_node_ids = set()
            for conn in pipeline.connections:
                connected_node_ids.add(conn.fromNodeId)
                connected_node_ids.add(conn.toNodeId)

            disconnected_nodes = [n for n in pipeline.nodes if n.id not in connected_node_ids]
            if disconnected_nodes:
                node_titles = [self._get_node_title(n.type) for n in disconnected_nodes]
                warnings.append(f"{len(disconnected_nodes)} node(s) are not connected: {', '.join(node_titles)}")

            # Check for type compatibility in connections
            for i, conn in enumerate(pipeline.connections):
                from_node = next((n for n in pipeline.nodes if n.id == conn.fromNodeId), None)
                to_node = next((n for n in pipeline.nodes if n.id == conn.toNodeId), None)

                if not from_node or not to_node:
                    errors.append(f"Connection {i + 1}: Invalid node reference")
                    continue

                from_port = next((p for p in from_node.outputs if p.id == conn.fromPortId), None)
                to_port = next((p for p in to_node.inputs if p.id == conn.toPortId), None)

                if not from_port or not to_port:
                    errors.append(f"Connection {i + 1}: Invalid port reference")
                    continue

                if from_port.dataType != to_port.dataType:
                    errors.append(
                        f"Connection {i + 1}: Type mismatch between {from_port.name} ({from_port.dataType}) and {to_port.name} ({to_port.dataType})")

            # Check for required node configurations
            for node in pipeline.nodes:
                validation = self._validate_node_config(node)
                if validation['errors']:
                    errors.extend([f"Node '{self._get_node_title(node.type)}': {err}" for err in validation['errors']])
                if validation['warnings']:
                    warnings.extend(
                        [f"Node '{self._get_node_title(node.type)}': {warn}" for warn in validation['warnings']])

            # Check for cycles
            if self._has_cycles(pipeline):
                errors.append("Pipeline contains cycles, which are not allowed")

            # Check execution order
            try:
                self._calculate_execution_order(pipeline)
            except Exception as e:
                errors.append(f"Cannot determine execution order: {str(e)}")

        except Exception as e:
            logger.error(f"Pipeline validation error: {str(e)}")
            errors.append(f"Validation error: {str(e)}")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )

    def execute_pipeline(self, execution_request: ExecutionRequest) -> ExecutionResult:
        """
        Execute a pipeline step by step.

        :param execution_request: Execution request with pipeline and metadata
        :return: ExecutionResult with success status and results
        """
        execution_id = execution_request.execution_id
        pipeline = execution_request.pipeline

        logger.info(f"Executing pipeline: {execution_id}")

        results = {}
        logs = []
        errors = []

        try:
            # Validate pipeline first
            validation = self.validate_pipeline(pipeline)
            if not validation.is_valid:
                return ExecutionResult(
                    success=False,
                    execution_id=execution_id,
                    results={},
                    logs=logs,
                    errors=validation.errors
                )

            logs.append(f"Pipeline validation passed at {datetime.utcnow().isoformat()}")

            # Calculate execution order
            execution_order = self._calculate_execution_order(pipeline)
            logs.append(f"Execution order calculated: {execution_order}")

            # Create node lookup
            node_map = {node.id: node for node in pipeline.nodes}

            # Execute nodes in order
            node_results = {}
            core_components = {
                'objects': [],
                'iot_events': [],
                'process_events': [],
                'observations': [],
                'event_object_relationships': [],
                'event_event_relationships': [],
                'object_object_relationships': []
            }

            for node_id in execution_order:
                node = node_map[node_id]

                try:
                    logs.append(f"Executing node: {node.id} ({node.type})")

                    # Prepare inputs for the node
                    node_inputs = self._prepare_node_inputs(node, pipeline.connections, node_results)

                    # Execute the node
                    node_result = self.node_executor.execute_node(
                        node=node,
                        inputs=node_inputs,
                        execution_context={
                            'execution_id': execution_id,
                            'file_service': self.file_service
                        }
                    )

                    if not node_result['success']:
                        error_msg = f"Node {node.id} failed: {node_result.get('error', 'Unknown error')}"
                        errors.append(error_msg)
                        logs.append(error_msg)
                        continue

                    # Store node results
                    node_results[node_id] = node_result['data']

                    # Collect CORE model components
                    self._collect_core_components(node, node_result['data'], core_components)

                    logs.append(f"Node {node.id} completed successfully")

                except Exception as e:
                    error_msg = f"Error executing node {node.id}: {str(e)}"
                    errors.append(error_msg)
                    logs.append(error_msg)
                    logger.error(f"{error_msg}\n{traceback.format_exc()}")

            # Create CORE metamodel if we have components
            if any(core_components.values()):
                try:
                    logs.append("Creating CORE metamodel")
                    core_model = COREMetamodel(
                        objects=core_components['objects'],
                        iot_events=core_components['iot_events'],
                        process_events=core_components['process_events'],
                        observations=core_components['observations'],
                        event_object_relationships=core_components['event_object_relationships'],
                        event_event_relationships=core_components['event_event_relationships'],
                        object_object_relationships=core_components['object_object_relationships']
                    )

                    results['core_model'] = core_model
                    results['extended_table'] = core_model.get_extended_table().to_dict('records')
                    logs.append("CORE metamodel created successfully")

                except Exception as e:
                    error_msg = f"Error creating CORE metamodel: {str(e)}"
                    errors.append(error_msg)
                    logs.append(error_msg)
                    logger.error(f"{error_msg}\n{traceback.format_exc()}")

            # Add node results to final results
            results['node_results'] = node_results
            results['core_components'] = {
                key: len(value) for key, value in core_components.items()
            }

            success = len(errors) == 0
            logs.append(f"Pipeline execution {'completed successfully' if success else 'completed with errors'}")

            return ExecutionResult(
                success=success,
                execution_id=execution_id,
                results=results,
                logs=logs,
                errors=errors
            )

        except Exception as e:
            error_msg = f"Pipeline execution failed: {str(e)}"
            errors.append(error_msg)
            logs.append(error_msg)
            logger.error(f"{error_msg}\n{traceback.format_exc()}")

            return ExecutionResult(
                success=False,
                execution_id=execution_id,
                results=results,
                logs=logs,
                errors=errors
            )

    def test_node(self, node_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Test a single node configuration.

        :param node_data: Node data to test
        :return: Test result dictionary
        """
        try:
            # Create a test node
            test_node = PipelineNode(
                id=node_data.get('id', 'test-node'),
                type=node_data.get('type'),
                position=node_data.get('position', {'x': 0, 'y': 0}),
                config=node_data.get('config', {}),
                inputs=node_data.get('inputs', []),
                outputs=node_data.get('outputs', [])
            )

            # Validate configuration
            validation = self._validate_node_config(test_node)
            if validation['errors']:
                return {
                    'success': False,
                    'message': f"Configuration errors: {', '.join(validation['errors'])}",
                    'errors': validation['errors']
                }

            # Test node execution (dry run)
            test_result = self.node_executor.test_node(test_node)

            return {
                'success': test_result['success'],
                'message': test_result.get('message', 'Node test completed'),
                'data': test_result.get('data'),
                'errors': test_result.get('errors', [])
            }

        except Exception as e:
            logger.error(f"Node test error: {str(e)}")
            return {
                'success': False,
                'message': str(e),
                'errors': [str(e)]
            }

    def _calculate_execution_order(self, pipeline: PipelineDefinition) -> List[str]:
        """Calculate execution order using topological sort."""
        node_ids = [node.id for node in pipeline.nodes]
        dependencies = {node_id: [] for node_id in node_ids}

        # Build dependency graph
        for conn in pipeline.connections:
            dependencies[conn.toNodeId].append(conn.fromNodeId)

        # Topological sort using Kahn's algorithm
        in_degree = {node_id: len(deps) for node_id, deps in dependencies.items()}
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            current = queue.pop(0)
            result.append(current)

            # Update in-degrees of dependent nodes
            for conn in pipeline.connections:
                if conn.fromNodeId == current:
                    in_degree[conn.toNodeId] -= 1
                    if in_degree[conn.toNodeId] == 0:
                        queue.append(conn.toNodeId)

        if len(result) != len(node_ids):
            raise ValueError("Pipeline contains cycles")

        return result

    def _has_cycles(self, pipeline: PipelineDefinition) -> bool:
        """Check if pipeline has cycles using DFS."""
        node_ids = [node.id for node in pipeline.nodes]
        graph = {node_id: [] for node_id in node_ids}

        # Build adjacency list
        for conn in pipeline.connections:
            graph[conn.fromNodeId].append(conn.toNodeId)

        visited = set()
        recursion_stack = set()

        def has_cycle_dfs(node_id: str) -> bool:
            visited.add(node_id)
            recursion_stack.add(node_id)

            for neighbor in graph[node_id]:
                if neighbor not in visited:
                    if has_cycle_dfs(neighbor):
                        return True
                elif neighbor in recursion_stack:
                    return True

            recursion_stack.remove(node_id)
            return False

        for node_id in node_ids:
            if node_id not in visited:
                if has_cycle_dfs(node_id):
                    return True

        return False

    def _prepare_node_inputs(self, node: PipelineNode, connections: List, node_results: Dict[str, Any]) -> Dict[
        str, Any]:
        """Prepare inputs for a node based on connections and previous results."""
        inputs = {}

        for conn in connections:
            if conn.toNodeId == node.id:
                # Find the output data from the source node
                source_node_id = conn.fromNodeId
                source_port_id = conn.fromPortId
                target_port_id = conn.toPortId

                if source_node_id in node_results:
                    source_data = node_results[source_node_id]

                    # Map port to input data
                    # Extract port name from port ID
                    port_name = self._extract_port_name(target_port_id)
                    inputs[port_name] = source_data

        return inputs

    def _collect_core_components(self, node: PipelineNode, node_data: Any, core_components: Dict[str, List]):
        """Collect CORE model components from node results."""
        if node.type == 'iot-event' and isinstance(node_data, list):
            core_components['iot_events'].extend(node_data)
        elif node.type == 'process-event' and isinstance(node_data, list):
            core_components['process_events'].extend(node_data)
        elif node.type == 'object-creator' and isinstance(node_data, list):
            core_components['objects'].extend(node_data)
        elif node.type == 'event-object-relation' and isinstance(node_data, list):
            core_components['event_object_relationships'].extend(node_data)
        elif node.type == 'event-event-relation' and isinstance(node_data, list):
            core_components['event_event_relationships'].extend(node_data)

    def _validate_node_config(self, node: PipelineNode) -> Dict[str, List[str]]:
        """Validate node configuration."""
        errors = []
        warnings = []

        # Type-specific validation
        if node.type == 'read-file':
            if not node.config.get('fileName'):
                errors.append("File must be selected")
            if not node.config.get('fileType'):
                errors.append("File type must be specified")

        elif node.type == 'mqtt-connector':
            if not node.config.get('brokerUrl'):
                errors.append("MQTT broker URL is required")
            if not node.config.get('topic'):
                errors.append("MQTT topic is required")

        elif node.type == 'column-selector':
            if not node.config.get('columnName'):
                errors.append("Column name must be specified")

        elif node.type == 'attribute-selector':
            if not node.config.get('attributeKey'):
                errors.append("Attribute key must be specified")

        elif node.type == 'data-filter':
            if not node.config.get('condition'):
                errors.append("Filter condition must be specified")

        elif node.type == 'data-mapper':
            if not node.config.get('expression'):
                errors.append("Mapping expression must be specified")

        return {'errors': errors, 'warnings': warnings}

    def _get_node_title(self, node_type: str) -> str:
        """Get human-readable title for node type."""
        titles = {
            'read-file': 'Read File',
            'mqtt-connector': 'MQTT Connector',
            'column-selector': 'Column Selector',
            'attribute-selector': 'Attribute Selector',
            'data-filter': 'Data Filter',
            'data-mapper': 'Data Mapper',
            'iot-event': 'IoT Event',
            'process-event': 'Process Event',
            'object-creator': 'Object Creator',
            'unique-id-generator': 'Unique ID Generator',
            'object-class-selector': 'Object Class Selector',
            'event-object-relation': 'Event-Object Relationship',
            'event-event-relation': 'Event-Event Relationship',
            'core-metamodel': 'CORE Metamodel',
            'table-output': 'Table Output',
            'export-ocel': 'Export to OCEL',
            'ocpm-discovery': 'OCPM Discovery'
        }
        return titles.get(node_type, node_type.replace('-', ' ').title())

    def _extract_port_name(self, port_id: str) -> str:
        """Extract port name from port ID."""
        # Port ID format: "node-X-port-name"
        parts = port_id.split('-')
        if len(parts) >= 3:
            return '-'.join(parts[2:])
        return port_id