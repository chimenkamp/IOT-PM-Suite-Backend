# services/node_executor.py - Individual Node Execution Logic

import logging
import pandas as pd
import json
import uuid
from typing import Dict, Any, List
from datetime import datetime
import traceback

from models.pipeline_models import PipelineNode
from core.event_definition import IotEvent, ProcessEvent, Observation
from core.object_definition import Object, ObjectClassEnum
from core.relationship_definitions import EventObjectRelationship, EventEventRelationship

logger = logging.getLogger(__name__)


class NodeExecutor:
    """Executes individual nodes based on their type."""

    def __init__(self):
        self.node_handlers = {
            # Data Input & Loading
            'read-file': self._execute_read_file,
            'mqtt-connector': self._execute_mqtt_connector,

            # Data Processing
            'column-selector': self._execute_column_selector,
            'attribute-selector': self._execute_attribute_selector,
            'data-filter': self._execute_data_filter,
            'data-mapper': self._execute_data_mapper,

            # CORE Model Creation
            'iot-event': self._execute_iot_event,
            'process-event': self._execute_process_event,
            'object-creator': self._execute_object_creator,

            # Utilities
            'unique-id-generator': self._execute_unique_id_generator,
            'object-class-selector': self._execute_object_class_selector,

            # Relationships
            'event-object-relation': self._execute_event_object_relation,
            'event-event-relation': self._execute_event_event_relation,

            # Output
            'table-output': self._execute_table_output,
            'export-ocel': self._execute_export_ocel,
            'ocpm-discovery': self._execute_ocpm_discovery
        }

    def execute_node(self, node: PipelineNode, inputs: Dict[str, Any], execution_context: Dict[str, Any]) -> Dict[
        str, Any]:
        """
        Execute a node with given inputs.

        :param node: Node to execute
        :param inputs: Input data for the node
        :param execution_context: Execution context
        :return: Execution result
        """
        try:
            logger.info(f"Executing node {node.id} of type {node.type}")

            if node.type not in self.node_handlers:
                return {
                    'success': False,
                    'error': f"Unknown node type: {node.type}",
                    'data': None
                }

            handler = self.node_handlers[node.type]
            result = handler(node, inputs, execution_context)

            logger.info(f"Node {node.id} executed successfully")
            return {
                'success': True,
                'data': result,
                'metadata': {
                    'executedAt': datetime.utcnow().isoformat(),
                    'nodeId': node.id,
                    'nodeType': node.type
                }
            }

        except Exception as e:
            error_msg = f"Error executing node {node.id}: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return {
                'success': False,
                'error': error_msg,
                'data': None
            }

    def test_node(self, node: PipelineNode) -> Dict[str, Any]:
        """
        Test a node configuration without full execution.

        :param node: Node to test
        :return: Test result
        """
        try:
            # Create mock inputs for testing
            mock_inputs = self._create_mock_inputs(node)
            mock_context = {'execution_id': 'test', 'file_service': None}

            # Try to execute with mock data
            if node.type in ['read-file', 'mqtt-connector']:
                # For file/MQTT nodes, just validate configuration
                return {
                    'success': True,
                    'message': f"Node configuration is valid",
                    'data': {'config_valid': True}
                }
            else:
                result = self.execute_node(node, mock_inputs, mock_context)
                return {
                    'success': result['success'],
                    'message': 'Node test completed' if result['success'] else result.get('error'),
                    'data': result.get('data')
                }

        except Exception as e:
            return {
                'success': False,
                'message': f"Node test failed: {str(e)}",
                'errors': [str(e)]
            }

    # ============ DATA INPUT & LOADING NODES ============

    def _execute_read_file(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> pd.DataFrame:
        """Execute Read File node."""
        file_service = context.get('file_service')
        if not file_service:
            raise ValueError("File service not available in context")

        filename = node.config.get('fileName')
        file_type = node.config.get('fileType', 'CSV')
        encoding = node.config.get('encoding', 'UTF-8')

        if not filename:
            raise ValueError("No file specified")

        # Load file through file service
        file_data = file_service.load_file(filename, file_type, encoding)

        logger.info(f"Loaded file {filename} with shape {file_data.shape}")
        return file_data

    def _execute_mqtt_connector(self, node: PipelineNode, inputs: Dict[str, Any],
                                context: Dict[str, Any]) -> pd.DataFrame:
        """Execute MQTT Connector node."""
        broker_url = node.config.get('brokerUrl')
        topic = node.config.get('topic')
        username = node.config.get('username')
        password = node.config.get('password')

        if not broker_url or not topic:
            raise ValueError("MQTT broker URL and topic are required")

        # For demo purposes, return mock MQTT data
        # In production, implement actual MQTT connection
        mock_data = pd.DataFrame({
            'timestamp': [datetime.utcnow().isoformat()],
            'sensor_id': ['sensor_001'],
            'value': [23.5],
            'unit': ['celsius']
        })

        logger.info(f"Connected to MQTT broker {broker_url}, topic {topic}")
        return mock_data

    # ============ DATA PROCESSING NODES ============

    def _execute_column_selector(self, node: PipelineNode, inputs: Dict[str, Any],
                                 context: Dict[str, Any]) -> pd.Series:
        """Execute Column Selector node."""
        raw_data = inputs.get('raw-data')
        if raw_data is None or not isinstance(raw_data, pd.DataFrame):
            raise ValueError("Raw data input is required and must be a DataFrame")

        column_name = node.config.get('columnName')
        if not column_name:
            raise ValueError("Column name must be specified")

        if column_name not in raw_data.columns:
            raise ValueError(f"Column '{column_name}' not found in data. Available columns: {list(raw_data.columns)}")

        return raw_data[column_name]

    def _execute_attribute_selector(self, node: PipelineNode, inputs: Dict[str, Any],
                                    context: Dict[str, Any]) -> pd.Series:
        """Execute Attribute Selector node."""
        series_data = inputs.get('series')
        if series_data is None or not isinstance(series_data, pd.Series):
            raise ValueError("Series input is required")

        attribute_key = node.config.get('attributeKey')
        default_value = node.config.get('defaultValue', None)

        if not attribute_key:
            raise ValueError("Attribute key must be specified")

        # For demo, just return the series with attribute key as name
        result = series_data.copy()
        result.name = attribute_key

        return result

    def _execute_data_filter(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> pd.Series:
        """Execute Data Filter node."""
        series_data = inputs.get('series')
        if series_data is None or not isinstance(series_data, pd.Series):
            raise ValueError("Series input is required")

        condition = node.config.get('condition')
        operator = node.config.get('operator')

        if not condition or not operator:
            raise ValueError("Filter condition and operator must be specified")

        # Apply filter based on operator
        try:
            if operator == '>':
                threshold = float(condition)
                return series_data[series_data > threshold]
            elif operator == '<':
                threshold = float(condition)
                return series_data[series_data < threshold]
            elif operator == '>=':
                threshold = float(condition)
                return series_data[series_data >= threshold]
            elif operator == '<=':
                threshold = float(condition)
                return series_data[series_data <= threshold]
            elif operator == '==':
                return series_data[series_data == condition]
            elif operator == '!=':
                return series_data[series_data != condition]
            elif operator == 'contains':
                return series_data[series_data.astype(str).str.contains(condition, na=False)]
            elif operator == 'startswith':
                return series_data[series_data.astype(str).str.startswith(condition, na=False)]
            elif operator == 'endswith':
                return series_data[series_data.astype(str).str.endswith(condition, na=False)]
            else:
                raise ValueError(f"Unsupported operator: {operator}")

        except Exception as e:
            raise ValueError(f"Error applying filter: {str(e)}")

    def _execute_data_mapper(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> pd.Series:
        """Execute Data Mapper node."""
        series_data = inputs.get('series')
        if series_data is None or not isinstance(series_data, pd.Series):
            raise ValueError("Series input is required")

        mapping_type = node.config.get('mappingType')
        expression = node.config.get('expression')

        if not expression:
            raise ValueError("Mapping expression must be specified")

        try:
            if mapping_type == 'Value Mapping':
                # Expect JSON object for value mapping
                mapping_dict = json.loads(expression)
                return series_data.map(mapping_dict).fillna(series_data)

            elif mapping_type == 'Expression':
                # Apply lambda expression (be careful with eval!)
                if expression.startswith('lambda'):
                    func = eval(expression)
                    return series_data.apply(func)
                else:
                    raise ValueError("Expression must be a lambda function")

            elif mapping_type == 'Format Conversion':
                # Common format conversions
                if expression == 'upper':
                    return series_data.astype(str).str.upper()
                elif expression == 'lower':
                    return series_data.astype(str).str.lower()
                elif expression == 'strip':
                    return series_data.astype(str).str.strip()
                else:
                    raise ValueError(f"Unsupported format conversion: {expression}")

            else:
                raise ValueError(f"Unsupported mapping type: {mapping_type}")

        except Exception as e:
            raise ValueError(f"Error applying mapping: {str(e)}")

    # ============ CORE MODEL CREATION NODES ============

    def _execute_iot_event(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> List[IotEvent]:
        """Execute IoT Event node."""
        event_id = inputs.get('id')
        event_type = inputs.get('type')
        timestamp = inputs.get('timestamp')
        metadata = inputs.get('metadata', {})

        # Convert Series to list if needed
        if isinstance(event_id, pd.Series):
            event_ids = event_id.tolist()
        else:
            event_ids = [event_id] if event_id else [str(uuid.uuid4())]

        if isinstance(event_type, pd.Series):
            event_types = event_type.tolist()
        else:
            default_type = node.config.get('eventType', 'sensor_reading')
            event_types = [event_type] if event_type else [default_type]

        if isinstance(timestamp, pd.Series):
            timestamps = timestamp.tolist()
        else:
            timestamps = [timestamp] if timestamp else [datetime.utcnow()]

        # Ensure all lists are same length
        max_len = max(len(event_ids), len(event_types), len(timestamps))
        event_ids = (event_ids * max_len)[:max_len]
        event_types = (event_types * max_len)[:max_len]
        timestamps = (timestamps * max_len)[:max_len]

        # Create IoT events
        iot_events = []
        for i in range(max_len):
            event = IotEvent(
                event_id=str(event_ids[i]),
                event_type=str(event_types[i]),
                timestamp=timestamps[i] if isinstance(timestamps[i], datetime) else datetime.fromisoformat(
                    str(timestamps[i])),
                attributes=metadata if isinstance(metadata, dict) else {}
            )
            iot_events.append(event)

        return iot_events

    def _execute_process_event(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> List[
        ProcessEvent]:
        """Execute Process Event node."""
        event_id = inputs.get('id')
        event_type = inputs.get('type')
        timestamp = inputs.get('timestamp')
        metadata = inputs.get('metadata', {})
        activity_label = inputs.get('activity-label')

        # Convert Series to list if needed
        if isinstance(event_id, pd.Series):
            event_ids = event_id.tolist()
        else:
            event_ids = [event_id] if event_id else [str(uuid.uuid4())]

        if isinstance(activity_label, pd.Series):
            activity_labels = activity_label.tolist()
        else:
            activity_labels = [activity_label] if activity_label else ['activity']

        if isinstance(timestamp, pd.Series):
            timestamps = timestamp.tolist()
        else:
            timestamps = [timestamp] if timestamp else [datetime.utcnow()]

        # Ensure all lists are same length
        max_len = max(len(event_ids), len(activity_labels), len(timestamps))
        event_ids = (event_ids * max_len)[:max_len]
        activity_labels = (activity_labels * max_len)[:max_len]
        timestamps = (timestamps * max_len)[:max_len]

        # Create process events
        process_events = []
        for i in range(max_len):
            event = ProcessEvent(
                event_id=str(event_ids[i]),
                event_type=node.config.get('eventType', 'process_activity'),
                activity=str(activity_labels[i]),
                timestamp=timestamps[i] if isinstance(timestamps[i], datetime) else datetime.fromisoformat(
                    str(timestamps[i])),
                attributes=metadata if isinstance(metadata, dict) else {}
            )
            process_events.append(event)

        return process_events

    def _execute_object_creator(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> List[
        Object]:
        """Execute Object Creator node."""
        object_id = inputs.get('id')
        object_type = inputs.get('type')
        object_class = inputs.get('class')
        metadata = inputs.get('metadata', {})

        # Convert Series to list if needed
        if isinstance(object_id, pd.Series):
            object_ids = object_id.tolist()
        else:
            object_ids = [object_id] if object_id else [str(uuid.uuid4())]

        if isinstance(object_type, pd.Series):
            object_types = object_type.tolist()
        else:
            object_types = [object_type] if object_type else ['object']

        if isinstance(object_class, pd.Series):
            object_classes = object_class.tolist()
        else:
            default_class = node.config.get('defaultObjectClass', 'BUSINESS_OBJECT')
            object_classes = [object_class] if object_class else [default_class]

        # Ensure all lists are same length
        max_len = max(len(object_ids), len(object_types), len(object_classes))
        object_ids = (object_ids * max_len)[:max_len]
        object_types = (object_types * max_len)[:max_len]
        object_classes = (object_classes * max_len)[:max_len]

        # Create objects
        objects = []
        for i in range(max_len):
            obj = Object(
                object_id=str(object_ids[i]),
                object_type=str(object_types[i]),
                object_class=ObjectClassEnum(object_classes[i]),
                attributes=metadata if isinstance(metadata, dict) else {}
            )
            objects.append(obj)

        return objects

    # ============ UTILITY NODES ============

    def _execute_unique_id_generator(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> List[
        str]:
        """Execute Unique ID Generator node."""
        id_type = node.config.get('idType', 'UUID4')
        prefix = node.config.get('prefix', '')
        count = node.config.get('count', 1)

        ids = []
        for i in range(count):
            if id_type == 'UUID4':
                new_id = str(uuid.uuid4())
            elif id_type == 'UUID1':
                new_id = str(uuid.uuid1())
            elif id_type == 'Incremental':
                new_id = str(i + 1)
            elif id_type == 'Timestamp-based':
                new_id = str(int(datetime.utcnow().timestamp() * 1000000) + i)
            else:
                new_id = str(uuid.uuid4())

            if prefix:
                new_id = f"{prefix}_{new_id}"

            ids.append(new_id)

        return ids[0] if count == 1 else ids

    def _execute_object_class_selector(self, node: PipelineNode, inputs: Dict[str, Any],
                                       context: Dict[str, Any]) -> str:
        """Execute Object Class Selector node."""
        object_class = node.config.get('objectClass')
        if not object_class:
            raise ValueError("Object class must be selected")

        return object_class

    # ============ RELATIONSHIP NODES ============

    def _execute_event_object_relation(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> \
    List[EventObjectRelationship]:
        """Execute Event-Object Relationship node."""
        events = inputs.get('event', [])
        objects = inputs.get('object', [])
        relationship_type = node.config.get('relationshipType', 'related')

        if not events or not objects:
            raise ValueError("Both events and objects are required")

        # Ensure we have lists
        if not isinstance(events, list):
            events = [events]
        if not isinstance(objects, list):
            objects = [objects]

        relationships = []
        for event in events:
            for obj in objects:
                rel = EventObjectRelationship(
                    event_id=event.event_id if hasattr(event, 'event_id') else str(event),
                    object_id=obj.object_id if hasattr(obj, 'object_id') else str(obj),
                    qualifier=relationship_type
                )
                relationships.append(rel)

        return relationships

    def _execute_event_event_relation(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> \
    List[EventEventRelationship]:
        """Execute Event-Event Relationship node."""
        source_events = inputs.get('source-event', [])
        target_events = inputs.get('target-event', [])
        qualifier = node.config.get('qualifier', 'derived_from')

        if not source_events or not target_events:
            raise ValueError("Both source and target events are required")

        # Ensure we have lists
        if not isinstance(source_events, list):
            source_events = [source_events]
        if not isinstance(target_events, list):
            target_events = [target_events]

        relationships = []
        for source in source_events:
            for target in target_events:
                rel = EventEventRelationship(
                    event_id=target.event_id if hasattr(target, 'event_id') else str(target),
                    derived_from_event_id=source.event_id if hasattr(source, 'event_id') else str(source),
                    qualifier=qualifier
                )
                relationships.append(rel)

        return relationships

    # ============ OUTPUT NODES ============

    def _execute_table_output(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> Dict[
        str, Any]:
        """Execute Table Output node."""
        data = inputs.get('data')
        max_rows = node.config.get('maxRows', 100)

        if data is None:
            return {'message': 'No data to display'}

        if isinstance(data, pd.DataFrame):
            result_data = data.head(max_rows).to_dict('records')
        elif isinstance(data, pd.Series):
            result_data = data.head(max_rows).tolist()
        elif isinstance(data, list):
            result_data = data[:max_rows]
        else:
            result_data = str(data)

        return {
            'type': 'table',
            'data': result_data,
            'totalRows': len(data) if hasattr(data, '__len__') else 1,
            'displayedRows': min(max_rows, len(data) if hasattr(data, '__len__') else 1)
        }

    def _execute_export_ocel(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> Dict[
        str, Any]:
        """Execute Export to OCEL node."""
        core_metamodel = inputs.get('core-metamodel')
        format_type = node.config.get('format', 'OCEL 2.0 JSON')
        filename = node.config.get('filename', 'export.ocel')

        if not core_metamodel:
            raise ValueError("CORE metamodel is required")

        # Export configuration
        export_config = {
            'format': format_type,
            'filename': filename,
            'timestamp': datetime.utcnow().isoformat()
        }

        return {
            'type': 'export',
            'config': export_config,
            'status': 'ready_for_export'
        }

    def _execute_ocpm_discovery(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> Dict[
        str, Any]:
        """Execute OCPM Discovery node."""
        core_metamodel = inputs.get('core-metamodel')
        algorithm = node.config.get('algorithm', 'Directly-Follows Graph')
        filter_noise = node.config.get('filterNoise', False)

        if not core_metamodel:
            raise ValueError("CORE metamodel is required")

        # Discovery configuration
        discovery_config = {
            'algorithm': algorithm,
            'filterNoise': filter_noise,
            'timestamp': datetime.utcnow().isoformat()
        }

        return {
            'type': 'process_discovery',
            'config': discovery_config,
            'status': 'ready_for_discovery'
        }

    def _create_mock_inputs(self, node: PipelineNode) -> Dict[str, Any]:
        """Create mock inputs for testing."""
        mock_inputs = {}

        for input_port in node.inputs:
            data_type = input_port.dataType

            if data_type == 'DataFrame':
                mock_inputs[input_port.name] = pd.DataFrame({
                    'col1': [1, 2, 3],
                    'col2': ['a', 'b', 'c']
                })
            elif data_type == 'Series':
                mock_inputs[input_port.name] = pd.Series([1, 2, 3])
            elif data_type == 'Attribute':
                mock_inputs[input_port.name] = 'test_attribute'
            elif data_type == 'Event':
                mock_inputs[input_port.name] = [IotEvent(
                    event_id='test_event',
                    event_type='test',
                    timestamp=datetime.utcnow(),
                    attributes={}
                )]
            elif data_type == 'Object':
                mock_inputs[input_port.name] = [Object(
                    object_id='test_object',
                    object_type='test',
                    object_class=ObjectClassEnum.BUSINESS_OBJECT,
                    attributes={}
                )]
            else:
                mock_inputs[input_port.name] = 'mock_data'

        return mock_inputs