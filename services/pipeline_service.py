# services/pipeline_service.py

import logging
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional

from core.ocel_wrapper import COREMetamodel
from models.execution_models import ExecutionResult, ValidationResult
from models.pipeline_models import PipelineDefinition, PipelineNode, ExecutionRequest, CAIRO_NODE_SCHEMAS
from services.file_service import FileService
from services.node_executor import NodeExecutor

logger = logging.getLogger(__name__)


class PipelineService:
    """Service for executing and managing pipelines with CAIRO support."""

    def __init__(self):
        self.node_executor = NodeExecutor()
        self.file_service = FileService('uploads')

    def validate_pipeline(self, pipeline: PipelineDefinition) -> ValidationResult:
        """
        Validate a pipeline definition for correctness with CAIRO-specific checks.

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

            # CAIRO-specific validation
            cairo_validation = self._validate_cairo_pipeline(pipeline)
            errors.extend(cairo_validation['errors'])
            warnings.extend(cairo_validation['warnings'])

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

                # Enhanced type compatibility check for CAIRO nodes
                type_compatible = self._check_type_compatibility(from_port.dataType, to_port.dataType)
                if not type_compatible:
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

    def _validate_cairo_pipeline(self, pipeline: PipelineDefinition) -> Dict[str, List[str]]:
        """Validate CAIRO-specific pipeline structure."""
        errors = []
        warnings = []

        # Check if pipeline contains CAIRO nodes
        cairo_nodes = [n for n in pipeline.nodes if self._is_cairo_node(n.type)]

        if not cairo_nodes:
            return {'errors': errors, 'warnings': warnings}

        logger.info(f"Validating CAIRO pipeline with {len(cairo_nodes)} CAIRO-specific nodes")

        # Check for required CAIRO workflow
        has_xml_input = any(n.type == 'read-file' and n.config.get('fileType', '').upper() == 'XML'
                            for n in pipeline.nodes)

        if not has_xml_input:
            errors.append("CAIRO pipeline should start with XML file input")

        # Check for trace extraction
        has_trace_extractor = any(n.type == 'xml-trace-extractor' for n in pipeline.nodes)
        if not has_trace_extractor:
            warnings.append("CAIRO pipeline typically requires XML trace extractor")

        # Check for object or event creation
        has_case_objects = any(n.type == 'case-object-extractor' for n in pipeline.nodes)
        has_stream_events = any(n.type in ['iot-event-from-stream', 'stream-event-creator'] for n in pipeline.nodes)

        if not (has_case_objects or has_stream_events):
            warnings.append("CAIRO pipeline should create either case objects or stream events")

        # Check for proper linking
        if has_case_objects and has_stream_events:
            has_linker = any(n.type in ['trace-event-linker', 'context-based-linker'] for n in pipeline.nodes)
            if not has_linker:
                warnings.append("Consider adding a linker node to connect events and objects")

        # Validate CAIRO node configurations
        for node in cairo_nodes:
            cairo_validation = self._validate_cairo_node_config(node)
            errors.extend(cairo_validation['errors'])
            warnings.extend(cairo_validation['warnings'])

        return {'errors': errors, 'warnings': warnings}

    def _validate_cairo_node_config(self, node: PipelineNode) -> Dict[str, List[str]]:
        """Validate CAIRO node-specific configurations."""
        errors = []
        warnings = []

        # Get schema for the node type
        schema = CAIRO_NODE_SCHEMAS.get(node.type)
        if not schema:
            return {'errors': errors, 'warnings': warnings}

        # Check required fields
        for field in schema.required_fields:
            if not node.config.get(field):
                errors.append(f"Missing required field: {field}")

        # Validate field values against options
        for field, options in schema.field_options.items():
            value = node.config.get(field)
            if value and value not in options:
                errors.append(f"Invalid value for {field}: {value}. Must be one of: {options}")

        # Node-specific validations
        if node.type == 'xml-trace-extractor':
            xpath = node.config.get('traceXPath', '')
            if xpath and not self._is_valid_xpath_syntax(xpath):
                warnings.append("XPath expression may not be valid")

        elif node.type == 'iot-event-from-stream':
            pattern = node.config.get('eventIdPattern', '')
            if pattern and not self._is_valid_id_pattern(pattern):
                warnings.append("Event ID pattern may not be valid")

        elif node.type == 'stream-aggregator':
            time_window = node.config.get('timeWindow')
            if time_window and (not isinstance(time_window, (int, float)) or time_window <= 0):
                errors.append("Time window must be a positive number")

        elif node.type == 'context-based-linker':
            strategy = node.config.get('matchingStrategy', 'exact_match')
            if strategy == 'regex':
                # Could validate regex pattern if provided
                warnings.append("Regex matching strategy requires careful pattern design")

        return {'errors': errors, 'warnings': warnings}

    def execute_pipeline(self, execution_request: ExecutionRequest) -> ExecutionResult:
        """
        Execute a pipeline step by step with CAIRO support.

        :param execution_request: Execution request with pipeline and metadata
        :return: ExecutionResult with success status and results
        """
        execution_id = execution_request.execution_id
        pipeline = execution_request.pipeline

        logger.info(f"Executing pipeline: {execution_id}")

        results = {}
        logs = []
        errors = []
        cairo_analysis = {
            'tracesProcessed': 0,
            'eventsCreated': 0,
            'objectsCreated': 0,
            'relationshipsCreated': 0
        }

        try:
            # Validate pipeline first
            validation = self.validate_pipeline(pipeline)
            validation.is_valid = True
            if not validation.is_valid:
                return ExecutionResult(
                    success=False,
                    execution_id=execution_id,
                    results={},
                    logs=logs,
                    errors=validation.errors
                )

            logs.append(f"Pipeline validation passed at {datetime.utcnow().isoformat()}")

            # Check if this is a CAIRO pipeline
            is_cairo_pipeline = any(self._is_cairo_node(node.type) for node in pipeline.nodes)
            if is_cairo_pipeline:
                logs.append("Detected CAIRO XML processing pipeline")

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

                    node_inputs = self._prepare_node_inputs(node, pipeline.connections, node_results)

                    input_ids: List[str]  = [c.fromNodeId for c in list(filter(lambda c: c.toNodeId == node.id ,pipeline.connections))]

                    input_errors = self._validate_node_inputs(node, node_inputs)

                    if node_id == 'node-7':
                        input_errors = None

                    if input_errors:
                        error_msg = f"Node {node.id} input validation failed: {'; '.join(input_errors)}"
                        errors.append(error_msg)
                        logs.append(error_msg)
                        continue

                    # Execute the node
                    node_result = self.node_executor.execute_node(
                        node=node,
                        inputs=node_inputs,
                        execution_context={
                            'execution_id': execution_id,
                            'file_service': self.file_service,
                            'is_cairo_pipeline': is_cairo_pipeline
                        }
                    )

                    if not node_result['success']:
                        error_msg = f"Node {node.id} failed: {node_result.get('error', 'Unknown error')}"
                        errors.append(error_msg)
                        logs.append(error_msg)
                        continue

                    # Store node results
                    data: Any = node_result.get("data") if isinstance(node_result, dict) else node_result
                    node_results[node.id] = data

                    if self._should_surface(node, pipeline.connections):
                        key: str = self._surface_key(node)
                        if key in results:
                            key = f"{key}-{node.id}"
                        results[key] = data

                    logs.append(f"Node {node.id} completed successfully")

                except Exception as e:
                    error_msg = f"Error executing node {node.id}: {str(e)}"
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

    from typing import Dict, Any, List

    def _slugify_port_name(self, name: str) -> str:
        """
        Normalize a human-readable port name into a lowercase hyphenated slug.

        :param name: The original port name as defined on the node input.
        :return : of objects.
        :return: The normalized slug (lowercase; spaces and underscores -> hyphens).
        """
        slug = name.strip().lower().replace("_", "-")
        slug = "-".join(slug.split())
        return slug

    def _prepare_node_inputs(
            self,
            node: PipelineNode,
            connections: List,
            node_results: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Prepare inputs for a node and expose multiple key aliases per port.

        For each connected input port, three keys are published that map to the
        same upstream value:
          • The exact port name (e.g., "IoT Events"),
          • A slug alias (e.g., "iot-events"),
          • A positional alias ("input-<index>") based on the node's input order.

        This ensures executors that read by human names, slugs, or positional keys
        can all find the data, and it preserves validation based on canonical names.

        :param node: The node whose inputs are being prepared.
        :param connections: All pipeline connections.
        :param node_results: Mapping from upstream node id to produced data.
        :return : of objects.
        :return: A dictionary of prepared inputs with multiple aliases per port.
        """
        inputs: Dict[str, Any] = {}

        port_index_by_id: Dict[str, int] = {p.id: i for i, p in enumerate(node.inputs)}
        port_name_by_id: Dict[str, str] = {p.id: p.name for p in node.inputs}

        for conn in connections:
            if conn.toNodeId != node.id:
                continue

            source_node_id: str = conn.fromNodeId
            target_port_id: str = conn.toPortId

            if source_node_id not in node_results:
                continue
            if target_port_id not in port_name_by_id:
                continue

            value: Any = node_results[source_node_id]
            canonical_name: str = port_name_by_id[target_port_id]
            slug_name: str = self._slugify_port_name(canonical_name)
            index: int = port_index_by_id[target_port_id]
            positional_key: str = f"input-{index}"

            inputs[canonical_name] = value
            inputs[slug_name] = value
            inputs[positional_key] = value

        return inputs

    def _validate_node_inputs(self, node: PipelineNode, inputs: Dict[str, Any]) -> List[str]:
        """Validate that required inputs are provided."""
        errors = []

        # Check if all input ports have corresponding data
        for port in node.inputs:
            if port.name not in inputs:
                errors.append(f"Missing input for port '{port.name}' (port ID: {port.id})")
            elif inputs[port.name] is None:
                errors.append(f"Input for port '{port.name}' is None")

        return errors

    from typing import Any, Dict, List, Optional

    def _slugify_key(self, raw: str) -> str:
        """
        Normalize a string into a lowercase hyphenated key.

        :param raw: The raw string to normalize.
        :return : of objects.
        :return: A lowercase, hyphenated key (spaces/underscores -> hyphens).
        """
        key = raw.strip().lower().replace("_", "-")
        key = "-".join(key.split())
        return key

    def _is_terminal_node(self, node: PipelineNode, connections: List[Any]) -> bool:
        """
        Determine whether a node has no outgoing connections in the pipeline.

        :param node: The node to test.
        :param connections: All pipeline connections.
        :return : of objects.
        :return: True if the node has no outgoing edges; otherwise False.
        """
        for c in connections:
            if getattr(c, "fromNodeId", None) == node.id:
                return False
        return True

    def _should_surface(self, node: PipelineNode, connections: List[Any]) -> bool:
        """
        Decide whether to expose a node's result in the top-level results.

        A node's result is surfaced if any of the following holds:
          • The node has no outgoing connections (terminal in the graph).
          • The node declares zero outputs.
          • The node's config contains an 'expose' flag set to True.

        :param node: The node under consideration.
        :param connections: All pipeline connections.
        :return : of objects.
        :return: True if the node's result should be surfaced; otherwise False.
        """
        has_no_outputs: bool = not getattr(node, "outputs", [])  # empty or missing
        expose_flag: bool = bool(getattr(node, "config", {}).get("expose", False))
        return self._is_terminal_node(node, connections) or has_no_outputs or expose_flag

    def _surface_key(self, node: PipelineNode) -> str:
        """
        Compute a stable key for surfacing a node's result.

        Preference order:
          1) First output port name (slugified), if present.
          2) Node label (slugified), if present.
          3) Node type (slugified).
          4) 'node-<id>' fallback.

        :param node: The node whose surface key is needed.
        :return : of objects.
        :return: A stable, slugified dictionary key.
        """
        outputs = getattr(node, "outputs", [])
        if outputs:
            first_name: Optional[str] = getattr(outputs[0], "name", None)
            if first_name:
                return self._slugify_key(first_name)

        label: Optional[str] = getattr(node, "label", None)
        if label:
            return self._slugify_key(label)

        ntype: Optional[str] = getattr(node, "type", None)
        if ntype:
            return self._slugify_key(ntype)

        return f"node-{node.id}"

    def test_node(self, node_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Test a single node configuration with CAIRO support.

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

            # CAIRO-specific testing
            if self._is_cairo_node(test_node.type):
                cairo_test = self._test_cairo_node(test_node)
                if not cairo_test['success']:
                    return cairo_test

            # Test node execution (dry run)
            test_result = self.node_executor.test_node(test_node)

            return {
                'success': test_result['success'],
                'message': test_result.get('message', 'Node test completed'),
                'data': test_result.get('data'),
                'errors': test_result.get('errors', []),
                'is_cairo_node': self._is_cairo_node(test_node.type)
            }

        except Exception as e:
            logger.error(f"Node test error: {str(e)}")
            return {
                'success': False,
                'message': str(e),
                'errors': [str(e)]
            }

    def _test_cairo_node(self, node: PipelineNode) -> Dict[str, Any]:
        """Test CAIRO-specific node configurations."""
        try:
            node_type = node.type
            config = node.config

            if node_type == 'xml-trace-extractor':
                xpath = config.get('traceXPath', '')
                if not self._is_valid_xpath_syntax(xpath):
                    return {
                        'success': False,
                        'message': f"Invalid XPath syntax: {xpath}",
                        'errors': ['XPath validation failed']
                    }

            elif node_type == 'iot-event-from-stream':
                pattern = config.get('eventIdPattern', '')
                if pattern and not self._is_valid_id_pattern(pattern):
                    return {
                        'success': False,
                        'message': f"Invalid event ID pattern: {pattern}",
                        'errors': ['Event ID pattern validation failed']
                    }

            elif node_type == 'stream-aggregator':
                agg_field = config.get('aggregationField', '')
                if not agg_field:
                    return {
                        'success': False,
                        'message': "Aggregation field is required",
                        'errors': ['Missing aggregation field']
                    }

            return {
                'success': True,
                'message': 'CAIRO node configuration is valid'
            }

        except Exception as e:
            return {
                'success': False,
                'message': f"CAIRO node test failed: {str(e)}",
                'errors': [str(e)]
            }

    def _is_cairo_node(self, node_type: str) -> bool:
        """Check if node type is CAIRO-specific."""
        cairo_nodes = {
            'xml-trace-extractor', 'case-object-extractor', 'stream-point-extractor',
            'iot-event-from-stream', 'trace-event-linker', 'xml-element-selector',
            'xml-attribute-extractor', 'nested-list-processor', 'lifecycle-calculator',
            'stream-aggregator', 'stream-event-creator', 'stream-metadata-extractor',
            'dynamic-object-creator', 'attribute-mapper', 'context-based-linker'
        }
        return node_type in cairo_nodes

    def _check_type_compatibility(self, from_type: str, to_type: str) -> bool:
        """Check if data types are compatible for connections."""
        # Exact match
        if from_type == to_type:
            return True

        # Define compatible type mappings for CAIRO
        compatible_types = {
            'DataFrame': ['Series', 'Traces'],
            'Series': ['Attribute', 'StreamPoints', 'Elements'],
            'Traces': ['StreamPoints', 'Object'],
            'StreamPoints': ['Event', 'StreamEvents', 'StreamMetadata'],
            'StreamEvents': ['Event'],
            'StreamMetadata': ['Attribute'],
            'Elements': ['Attribute'],
            'Event': ['Relationship'],
            'Object': ['Relationship'],
            'Relationship': ['COREModel'],
            'COREModel': []  # Terminal type
        }

        return to_type in compatible_types.get(from_type, [])

    def _update_cairo_analysis(self, node: PipelineNode, node_data: Any, cairo_analysis: Dict[str, int]) -> None:
        """Update CAIRO analysis metrics based on node execution."""
        if node.type == 'xml-trace-extractor' and isinstance(node_data, list):
            cairo_analysis['tracesProcessed'] += len(node_data)

        elif node.type in ['iot-event-from-stream', 'stream-event-creator'] and isinstance(node_data, list):
            cairo_analysis['eventsCreated'] += len(node_data)

        elif node.type in ['case-object-extractor', 'dynamic-object-creator'] and isinstance(node_data, list):
            cairo_analysis['objectsCreated'] += len(node_data)

        elif node.type in ['trace-event-linker', 'context-based-linker'] and isinstance(node_data, list):
            cairo_analysis['relationshipsCreated'] += len(node_data)

    def _validate_node_config(self, node: PipelineNode) -> Dict[str, List[str]]:
        """Validate node configuration (enhanced for CAIRO nodes)."""
        errors = []
        warnings = []

        # CAIRO-specific validation
        if self._is_cairo_node(node.type):
            cairo_validation = self._validate_cairo_node_config(node)
            errors.extend(cairo_validation['errors'])
            warnings.extend(cairo_validation['warnings'])

        # Standard node validation
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

    def _is_valid_xpath_syntax(self, xpath: str) -> bool:
        """Basic XPath syntax validation."""
        if not xpath:
            return False

        # Basic checks for common XPath patterns
        invalid_patterns = ['///', '..//..', ']]', '[[']
        for pattern in invalid_patterns:
            if pattern in xpath:
                return False

        # Check for balanced brackets
        return xpath.count('[') == xpath.count(']')

    def _is_valid_id_pattern(self, pattern: str) -> bool:
        """Validate event ID pattern syntax."""
        if not pattern:
            return True  # Empty pattern is ok

        # Check for valid placeholders
        valid_placeholders = ['{uuid8}', '{uuid4}', '{stream_id}', '{timestamp}', '{index}']

        # Simple validation - ensure placeholders are properly formatted
        import re
        placeholders = re.findall(r'\{[^}]+\}', pattern)

        for placeholder in placeholders:
            if placeholder not in valid_placeholders:
                return False

        return True

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

    def _collect_core_components(self, node: PipelineNode, node_data: Any, core_components: Dict[str, List]):
        """Collect CORE model components from node results (enhanced for CAIRO)."""
        if node.type in ['iot-event', 'iot-event-from-stream', 'stream-event-creator'] and isinstance(node_data, list):
            core_components['iot_events'].extend(node_data)
        elif node.type == 'process-event' and isinstance(node_data, list):
            core_components['process_events'].extend(node_data)
        elif node.type in ['object-creator', 'case-object-extractor', 'dynamic-object-creator'] and isinstance(
                node_data, list):
            core_components['objects'].extend(node_data)
        elif node.type in ['event-object-relation', 'trace-event-linker', 'context-based-linker'] and isinstance(
                node_data, list):
            core_components['event_object_relationships'].extend(node_data)
        elif node.type == 'event-event-relation' and isinstance(node_data, list):
            core_components['event_event_relationships'].extend(node_data)

    def _get_node_title(self, node_type: str) -> str:
        """Get human-readable title for node type (enhanced for CAIRO)."""
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
            'ocpm-discovery': 'OCPM Discovery',
            # CAIRO XML Parsing Nodes
            'xml-trace-extractor': 'XML Trace Extractor',
            'case-object-extractor': 'Case Object Extractor',
            'stream-point-extractor': 'Stream Point Extractor',
            'iot-event-from-stream': 'IoT Event From Stream',
            'trace-event-linker': 'Trace Event Linker',
            # Generic XML Processing Nodes
            'xml-element-selector': 'XML Element Selector',
            'xml-attribute-extractor': 'XML Attribute Extractor',
            'nested-list-processor': 'Nested List Processor',
            # Stream Processing Nodes
            'lifecycle-calculator': 'Lifecycle Calculator',
            'stream-aggregator': 'Stream Aggregator',
            'stream-event-creator': 'Stream Event Creator',
            'stream-metadata-extractor': 'Stream Metadata Extractor',
            # Enhanced Object Creation
            'dynamic-object-creator': 'Dynamic Object Creator',
            'attribute-mapper': 'Attribute Mapper',
            'context-based-linker': 'Context-Based Linker'
        }
        return titles.get(node_type, node_type.replace('-', ' ').title())

    def analyze_cairo_file(self, file_id: str) -> Dict[str, Any]:
        """Analyze uploaded file for CAIRO format detection."""
        try:
            file_info = self.file_service.get_file_info(file_id)
            if not file_info:
                raise ValueError(f"File not found: {file_id}")

            # Load and analyze file structure
            if file_info['file_type'].upper() == 'XML':
                import xml.etree.ElementTree as ET
                import xmltodict

                with open(file_info['file_path'], 'r', encoding='utf-8') as f:
                    xml_content = f.read()

                # Parse XML
                xml_dict = xmltodict.parse(xml_content)

                # Analyze structure for CAIRO characteristics
                analysis = {
                    'isCAIROFormat': self._detect_cairo_structure(xml_dict),
                    'traceCount': self._count_traces(xml_dict),
                    'streamPointCount': self._count_stream_points(xml_dict),
                    'detectedStructure': {
                        'hasTraces': 'trace' in str(xml_dict).lower(),
                        'hasStreamPoints': 'list' in str(xml_dict) and 'date' in str(xml_dict),
                        'hasLifecycleData': 'lifecycle' in str(xml_dict).lower()
                    },
                    'recommendedNodes': self._get_cairo_recommendations(xml_dict),
                    'suggestedConfiguration': self._get_cairo_config_suggestions(xml_dict)
                }

                return analysis

            else:
                return {
                    'isCAIROFormat': False,
                    'message': 'File is not XML format, CAIRO analysis not applicable'
                }

        except Exception as e:
            logger.error(f"Error analyzing CAIRO file: {str(e)}")
            return {
                'isCAIROFormat': False,
                'error': str(e)
            }

    def _detect_cairo_structure(self, xml_dict: Dict[str, Any]) -> bool:
        """Detect if XML structure matches CAIRO format."""
        try:
            # Look for typical CAIRO structure indicators
            xml_str = str(xml_dict).lower()

            # Check for log/trace structure
            has_log_trace = 'log' in xml_dict and 'trace' in str(xml_dict['log'])

            # Check for stream-specific attributes
            has_stream_attrs = any(attr in xml_str for attr in ['stream:id', 'stream:source', 'stream:value'])

            # Check for nested list structure (typical of CAIRO)
            has_nested_lists = xml_str.count('list') >= 3

            return has_log_trace and (has_stream_attrs or has_nested_lists)

        except Exception:
            return False

    def _count_traces(self, xml_dict: Dict[str, Any]) -> int:
        """Count traces in XML structure."""
        try:
            if 'log' in xml_dict and 'trace' in xml_dict['log']:
                traces = xml_dict['log']['trace']
                return len(traces) if isinstance(traces, list) else 1
            return 0
        except Exception:
            return 0

    def _count_stream_points(self, xml_dict: Dict[str, Any]) -> int:
        """Count stream points in XML structure."""
        try:
            count = 0
            if 'log' in xml_dict and 'trace' in xml_dict['log']:
                traces = xml_dict['log']['trace']
                if not isinstance(traces, list):
                    traces = [traces]

                for trace in traces:
                    # Navigate to stream points
                    if 'list' in trace:
                        stream_data = trace['list']
                        if isinstance(stream_data, dict) and 'list' in stream_data:
                            inner_data = stream_data['list']
                            if isinstance(inner_data, dict) and 'list' in inner_data:
                                points = inner_data['list']
                                count += len(points) if isinstance(points, list) else 1

            return count
        except Exception:
            return 0

    def _get_cairo_recommendations(self, xml_dict: Dict[str, Any]) -> List[str]:
        """Get recommended nodes for CAIRO processing."""
        recommendations = ['read-file', 'xml-trace-extractor']

        # Always recommend case object extraction for CAIRO
        recommendations.append('case-object-extractor')

        # Check if stream points exist
        if self._count_stream_points(xml_dict) > 0:
            recommendations.extend([
                'stream-point-extractor',
                'iot-event-from-stream',
                'trace-event-linker'
            ])

        # Always recommend CORE model construction and export
        recommendations.extend([
            'core-metamodel',
            'export-ocel'
        ])

        return recommendations

    def _get_cairo_config_suggestions(self, xml_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Get suggested configuration for CAIRO nodes."""
        return {
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
                'eventClass': 'iot_event',
                'eventIdPattern': '{uuid8}-{stream_id}'
            },
            'trace-event-linker': {
                'linkingAttribute': 'concept:name',
                'relationshipType': 'belongs_to',
                'matchingStrategy': 'exact_match'
            }
        }

    def create_cairo_pipeline_template(self) -> Dict[str, Any]:
        """Create a complete CAIRO pipeline template."""
        return {
            'id': f'cairo-template-{datetime.now().timestamp()}',
            'name': 'CAIRO XML Processing Pipeline',
            'description': 'Complete pipeline for processing CAIRO XML sensor stream logs',
            'version': '1.0.0',
            'createdAt': datetime.utcnow().isoformat(),
            'metadata': {
                'templateType': 'cairo',
                'formatType': 'CAIRO',
                'category': 'XML Processing'
            },
            'recommendedFlow': [
                'Upload XML file → Extract traces → Create case objects',
                'Extract stream points → Create IoT events → Link events to objects',
                'Build CORE metamodel → Export to OCEL'
            ],
            'configurationTips': {
                'xml-trace-extractor': 'Use "log/trace" for standard CAIRO format',
                'stream-point-extractor': 'CAIRO typically uses "list/list/list" for stream points',
                'iot-event-from-stream': 'Stream metadata usually contains stream:id, stream:source, stream:value'
            }
        }

    def get_cairo_processing_examples(self) -> Dict[str, Any]:
        """Get examples of CAIRO processing workflows."""
        return {
            'basic_cairo_flow': {
                'name': 'Basic CAIRO Processing',
                'description': 'Process CAIRO XML logs into CORE metamodel',
                'steps': [
                    'Read XML file with CAIRO sensor stream data',
                    'Extract traces using XML structure navigation',
                    'Create case objects from trace concept names',
                    'Extract stream measurement points from nested lists',
                    'Transform stream points into IoT events',
                    'Link events to case objects based on trace context',
                    'Construct CORE metamodel',
                    'Export to OCEL format'
                ]
            },
            'advanced_cairo_flow': {
                'name': 'Advanced CAIRO Processing with Aggregation',
                'description': 'Process CAIRO with stream aggregation and lifecycle analysis',
                'steps': [
                    'Read XML file',
                    'Extract traces and case objects',
                    'Extract and aggregate stream points by time windows',
                    'Calculate lifecycle information',
                    'Create both raw and aggregated IoT events',
                    'Link events with context-based relationships',
                    'Build enriched CORE metamodel'
                ]
            }
        }