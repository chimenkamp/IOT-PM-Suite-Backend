# services/node_executor.py

import logging
import os

import pandas as pd
import json
import uuid
import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Union, Optional, Callable
from datetime import datetime
import traceback

import pm4py
import xmltodict
from core.ocel_wrapper import COREMetamodel
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
            # 'mqtt-connector': self._execute_mqtt_connector,

            # CAIRO XML Parsing Nodes
            'xml-trace-extractor': self._execute_xml_trace_extractor,
            'case-object-extractor': self._execute_case_object_extractor,
            'stream-point-extractor': self._execute_stream_point_extractor,
            'iot-event-from-stream': self._execute_iot_event_from_stream,
            'trace-event-linker': self._execute_trace_event_linker,

            # Generic XML Processing Nodes
            'xml-element-selector': self._execute_xml_element_selector,
            'xml-attribute-extractor': self._execute_xml_attribute_extractor,
            'nested-list-processor': self._execute_nested_list_processor,

            # Stream Processing Nodes
            'lifecycle-calculator': self._execute_lifecycle_calculator,
            'stream-aggregator': self._execute_stream_aggregator,
            'stream-event-creator': self._execute_stream_event_creator,
            'stream-metadata-extractor': self._execute_stream_metadata_extractor,

            # Enhanced Object Creation
            'dynamic-object-creator': self._execute_dynamic_object_creator,
            'attribute-mapper': self._execute_attribute_mapper,

            # Enhanced Relationships
            'context-based-linker': self._execute_context_based_linker,

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
            'ocpm-discovery': self._execute_ocpm_discovery,
            'core-metamodel': self._execute_core_metamodel,
        }

    def execute_node(self, node: PipelineNode, inputs: Dict[str, Any], execution_context: Dict[str, Any]) -> Dict[
        str, Any]:
        """Execute a node with given inputs."""
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

    # ============ CAIRO XML PARSING NODES ============

    def _execute_xml_trace_extractor(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> List[
        Dict[str, Any]]:
        """Extract traces from XML log structure."""
        df_input = inputs.get("XML Data")
        if df_input is None:
            raise ValueError("XML data input is required")

        trace_xpath = node.config.get('traceXPath', 'log/trace')
        trace_identifier = node.config.get('traceIdentifier', 'concept:name')
        try:

            traces: List[Dict[str, Any]] = []
            for _, row in df_input.iterrows():
                row_dict: Dict[str, Any] = {trace_xpath: row[trace_xpath]}
                for col in df_input.columns:
                    if col != trace_xpath:
                        row_dict[col] = row[col]
                traces.append(row_dict)

            logger.info(f"Extracted {len(traces)} traces from XML")
            return traces

        except Exception as e:
            raise Exception(f"Error extracting traces: {str(e)}")

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

    def _execute_case_object_extractor(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> \
    List[Object]:
        """Extract case objects from trace data."""
        traces = inputs.get('Traces')
        if not traces:
            raise ValueError("Traces input is required")

        case_id_attr = node.config.get('caseIdAttribute', 'concept:name')
        object_type = node.config.get('objectType', 'case_object')
        extract_lifecycle = node.config.get('extractLifecycle', True)

        objects = []

        for trace in traces:
            concept_name = trace.get('concept_name') or self._extract_trace_identifier(trace.get('trace_data', {}),
                                                                                       case_id_attr)

            if not concept_name:
                continue

            # Prepare object attributes
            attributes = {'concept:name': concept_name, "reference_id": trace.get('string_@value', str(uuid.uuid4())[:8])}

            # Extract lifecycle information if requested
            if extract_lifecycle:
                lifecycle_info = self._extract_lifecycle_from_trace(trace.get('trace_data', {}))
                attributes.update(lifecycle_info)

            # Create case object
            case_object = Object(
                object_id=concept_name,
                object_type=object_type,
                object_class=ObjectClassEnum.CASE_OBJECT,
                attributes=attributes
            )
            objects.append(case_object)

        logger.info(f"Created {len(objects)} case objects")
        return objects

    def _execute_stream_point_extractor(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> \
    List[Dict[str, Any]]:
        """Extract stream points from trace data structure."""
        traces = inputs.get('Traces')
        if not traces:
            raise ValueError("Traces input is required")

        stream_points_path = node.config.get('streamPointsPath', 'list/list/list')
        timestamp_field = node.config.get('timestampField', 'date')
        event_data_path = node.config.get('eventDataPath', 'string')

        all_stream_points = []

        for trace in traces:
            trace_data = trace.get(stream_points_path, {})
            concept_name = trace.get('concept_name')

            for point in trace_data:
                if not point:
                    continue

                # Extract timestamp
                timestamp_data = self._navigate_xml_path(point, timestamp_field)
                timestamp_value = self._extract_timestamp_value(timestamp_data)

                # Extract event data
                event_data = self._navigate_xml_path(point, event_data_path)

                stream_point = {
                    'trace_concept_name': trace["string_@value"] if "string_@value" in trace else concept_name,
                    'timestamp': timestamp_value,
                    'raw_data': point,
                    'event_data': event_data,
                    'point_index': len(all_stream_points)
                }
                all_stream_points.append(stream_point)

        logger.info(f"Extracted {len(all_stream_points)} stream points")
        return all_stream_points

    def _execute_iot_event_from_stream(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> \
    List[IotEvent]:
        """Create IoT events from stream point data."""
        stream_points = inputs.get('input-0')
        case_id = inputs.get('case-id')

        if not stream_points:
            raise ValueError("Stream points input is required")

        stream_id_field = node.config.get('streamIdField', 'stream:id')
        stream_source_field = node.config.get('streamSourceField', 'stream:source')
        stream_value_field = node.config.get('streamValueField', 'stream:value')
        event_class = node.config.get('eventClass', 'iot_event')

        iot_events = []

        for point in stream_points:
            event_data = point.get('event_data', [])
            timestamp = point.get('timestamp')
            concept_name = point.get('trace_concept_name') or case_id

            # Extract metadata from event data
            metadata = {}
            stream_id = None

            if isinstance(event_data, list):
                for item in event_data:
                    if isinstance(item, dict):
                        key = item.get('@key') or item.get('key')
                        value = item.get('@value') or item.get('value')

                        if key and value:
                            metadata[key] = value
                            if key == stream_id_field.split(':')[-1]:  # Remove prefix
                                stream_id = value

            # Generate event ID
            event_id = f"{str(uuid.uuid4())[:8]}-{stream_id}" if stream_id else str(uuid.uuid4())

            # Add case context to metadata
            if concept_name:
                metadata['concept:name'] = concept_name

            # Create IoT event
            iot_event = IotEvent(
                event_id=event_id,
                event_type=metadata.get("stream:id"),
                timestamp=timestamp if isinstance(timestamp, datetime) else datetime.fromisoformat(str(timestamp)),
                attributes=metadata
            )
            iot_events.append(iot_event)

        logger.info(f"Created {len(iot_events)} IoT events from stream points")
        return iot_events

    def _execute_trace_event_linker(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> List[
        EventObjectRelationship]:
        """Link IoT events to their corresponding case objects."""
        iot_events = inputs.get('input-0')
        case_objects = inputs.get('case-objects')

        if not iot_events or not case_objects:
            raise ValueError("Both IoT events and case objects are required")

        linking_attribute = node.config.get('linkingAttribute', 'concept:name')
        relationship_type = node.config.get('relationshipType', 'belongs_to')

        relationships = []

        # Ensure we have lists
        if not isinstance(iot_events, list):
            iot_events = [iot_events]
        if not isinstance(case_objects, list):
            case_objects = [case_objects]

        # Create relationships based on linking attribute
        for event in iot_events:
            event_linking_value = event.attributes.get(linking_attribute)

            if event_linking_value:
                for obj in case_objects:
                    obj_linking_value = obj.attributes.get("reference_id") or obj.object_id

                    if event_linking_value == obj_linking_value:
                        relationship = EventObjectRelationship(
                            event_id=event.event_id,
                            object_id=obj.object_id,
                            qualifier=relationship_type
                        )
                        relationships.append(relationship)

        logger.info(f"Created {len(relationships)} event-object relationships")
        return relationships

    # ============ GENERIC XML PROCESSING NODES ============

    def _execute_xml_element_selector(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> \
    List[Any]:
        """Select specific elements from XML structure using XPath."""
        xml_data = inputs.get('xml-data')
        if xml_data is None:
            raise ValueError("XML data input is required")

        xpath = node.config.get('xpath')
        output_format = node.config.get('outputFormat', 'Element List')

        if not xpath:
            raise ValueError("XPath expression is required")

        try:
            # Parse XML if it's a string
            if isinstance(xml_data, str):
                root = ET.fromstring(xml_data)
            elif hasattr(xml_data, 'to_xml'):
                root = ET.fromstring(xml_data.to_xml())
            else:
                # Convert dict to XML if needed
                xml_dict = xmltodict.parse(str(xml_data)) if isinstance(xml_data, str) else xml_data
                return self._navigate_xml_path(xml_dict, xpath.replace('//', '').replace('/', '.'))

            # Execute XPath (simplified - full XPath would need lxml)
            elements = root.findall(xpath.replace('//', './/'))

            # Format output based on configuration
            if output_format == 'Text Values':
                return [elem.text for elem in elements if elem.text]
            elif output_format == 'Attribute Values':
                return [elem.attrib for elem in elements]
            else:  # Element List
                return [self._element_to_dict(elem) for elem in elements]

        except Exception as e:
            raise Exception(f"Error selecting XML elements: {str(e)}")
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
    def _execute_xml_attribute_extractor(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> \
    List[Any]:
        """Extract attributes from XML elements."""
        xml_elements = inputs.get('xml-elements')
        if not xml_elements:
            raise ValueError("XML elements input is required")

        attribute_name = node.config.get('attributeName')
        default_value = node.config.get('defaultValue')
        data_type = node.config.get('dataType', 'string')

        if not attribute_name:
            raise ValueError("Attribute name is required")

        extracted_values = []

        for element in xml_elements:
            value = None

            if isinstance(element, dict):
                # Handle dictionary format
                value = element.get(attribute_name) or element.get(f'@{attribute_name}')
            elif hasattr(element, 'get'):
                # Handle XML element
                value = element.get(attribute_name) or element.attrib.get(attribute_name)

            # Apply default if no value found
            if value is None:
                value = default_value

            # Convert data type
            if value is not None:
                try:
                    if data_type == 'number':
                        value = float(value)
                    elif data_type == 'date':
                        value = datetime.fromisoformat(str(value))
                    elif data_type == 'boolean':
                        value = str(value).lower() in ['true', '1', 'yes']
                    else:  # string
                        value = str(value)
                except Exception:
                    logger.warning(f"Failed to convert value {value} to {data_type}")

            extracted_values.append(value)

        return extracted_values

    def _execute_core_metamodel(
            self,
            node: PipelineNode,
            inputs: Dict[str, Any],
            context: Dict[str, Any]
    ) -> COREMetamodel:
        """
        Build a CORE metamodel from separate inputs.

        Expects these input ports on the node:
          0: "Process Events"      -> List[ProcessEvent]
          1: "IoT Events"          -> List[IotEvent]
          2: "Relationships"       -> List[Union[EventObjectRelationship, EventEventRelationship, Any]]
          3: "Objects"             -> List[Object]

        The executor accepts canonical names, their slug aliases, and positional
        aliases ("input-<index>"), so it works with normalized inputs.

        :param node: The pipeline node carrying configuration and ports.
        :param inputs: Prepared inputs containing port-name, slug, and positional keys.
        :return : of objects.
        :return: The constructed COREMetamodel instance.
        """

        def _pick(keys: List[str]) -> Any:
            for k in keys:
                if k in inputs:
                    return inputs[k]
            return None

        process_events: Optional[List[Any]] = _pick(['Process Events', 'process-events', 'input-0']) or []
        iot_events: Optional[List[Any]] = _pick(['IoT Events', 'iot-events', 'input-1']) or []
        relationships: Optional[List[Any]] = _pick(['Relationships', 'relationships', 'input-2']) or []
        objects: Optional[List[Any]] = _pick(['Objects', 'objects', 'input-3']) or []

        if process_events is None or iot_events is None:
            raise ValueError("Both 'Process Events' and 'IoT Events' inputs are required")

        event_object_relationships: List[EventObjectRelationship] = []
        event_event_relationships: List[EventEventRelationship] = []
        object_object_relationships: List[Any] = []

        for rel in relationships or []:
            if isinstance(rel, EventObjectRelationship):
                event_object_relationships.append(rel)
            elif isinstance(rel, EventEventRelationship):
                event_event_relationships.append(rel)
            else:
                object_object_relationships.append(rel)

        observations: List[Any] = []
        try:
            for ev in iot_events:
                obs = getattr(ev, 'observations', None)
                if isinstance(obs, list):
                    observations.extend(obs)
        except Exception:
            observations = []

        core_model: COREMetamodel = COREMetamodel(
            objects=objects or [],
            iot_events=iot_events or [],
            process_events=process_events or [],
            observations=observations,
            event_object_relationships=event_object_relationships,
            event_event_relationships=event_event_relationships,
            object_object_relationships=object_object_relationships
        )
        return core_model
    def _execute_nested_list_processor(self, node: PipelineNode, inputs: Dict[str, Any],
                                       context: Dict[str, Any]) -> Any:
        """Process nested list structures from XML."""
        nested_data = inputs.get('nested-data')
        if nested_data is None:
            raise ValueError("Nested data input is required")

        nesting_depth = node.config.get('nestingDepth', 3)
        processing_mode = node.config.get('processingMode', 'Flatten All')
        target_level = node.config.get('targetLevel', 2)

        try:
            if processing_mode == 'Flatten All':
                return self._flatten_nested_structure(nested_data, max_depth=nesting_depth)
            elif processing_mode == 'Extract Level':
                return self._extract_level(nested_data, target_level)
            elif processing_mode == 'Preserve Structure':
                return nested_data  # No processing
            else:
                raise ValueError(f"Unknown processing mode: {processing_mode}")

        except Exception as e:
            raise Exception(f"Error processing nested lists: {str(e)}")

    def apply_condition(self, series_data: List[Dict[str, Any]], condition: str) -> List[Dict[str, Any]]:
        """
        Apply a filter condition to a series of dicts.

        :param series_data: The list of dictionaries containing the data.
        :param condition: A string representing a lambda expression, e.g. 'lambda r: r["timestamp"].weekday() == 0'.
        :return: A filtered list of dictionaries matching the condition.
        """
        func: Callable[[Dict[str, Any]], bool] = eval(condition)
        return list(filter(func, series_data))

    def _execute_data_filter(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]):
        """Execute Data Filter node."""
        series_data = inputs.get('series')
        if series_data is None:
            raise ValueError("Series input is required")

        days_in_data = [entry.get('timestamp').weekday() for entry in series_data if 'timestamp' in entry and isinstance(entry.get('timestamp'), datetime)]

        condition = node.config.get('condition')
        operator = node.config.get('operator')

        result = self.apply_condition(series_data, condition)

        return result


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
    # ============ STREAM PROCESSING NODES ============

    def _execute_lifecycle_calculator(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> \
    Dict[str, Any]:
        """Calculate lifecycle start/end times from stream data."""
        stream_points = inputs.get('stream-points')
        if not stream_points:
            raise ValueError("Stream points input is required")

        timestamp_field = node.config.get('timestampField', 'timestamp')
        calculation_mode = node.config.get('calculationMode', 'First-Last')
        output_format = node.config.get('outputFormat', 'ISO String')

        timestamps = []
        for point in stream_points:
            timestamp = point.get(timestamp_field)
            if timestamp:
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp)
                timestamps.append(timestamp)

        if not timestamps:
            raise ValueError("No valid timestamps found in stream points")

        # Calculate lifecycle based on mode
        if calculation_mode == 'First-Last':
            start_time = min(timestamps)
            end_time = max(timestamps)
        elif calculation_mode == 'Min-Max':
            start_time = min(timestamps)
            end_time = max(timestamps)
        else:  # Custom Range - would need additional config
            start_time = min(timestamps)
            end_time = max(timestamps)

        # Format output
        if output_format == 'ISO String':
            return {
                'lifecycle:start': start_time.isoformat(),
                'lifecycle:end': end_time.isoformat()
            }
        elif output_format == 'Timestamp':
            return {
                'lifecycle:start': start_time.timestamp(),
                'lifecycle:end': end_time.timestamp()
            }
        else:  # Duration
            duration = (end_time - start_time).total_seconds()
            return {
                'lifecycle:duration': duration,
                'lifecycle:start': start_time.isoformat(),
                'lifecycle:end': end_time.isoformat()
            }

    def _execute_stream_aggregator(self, node: PipelineNode, inputs: Dict[str, Any],
                                   context: Dict[str, Any]) -> pd.DataFrame:
        """Aggregate stream data by time windows or event groups."""
        stream_points = inputs.get('stream-points')
        if not stream_points:
            raise ValueError("Stream points input is required")

        aggregation_field = node.config.get('aggregationField', 'value')
        aggregation_function = node.config.get('aggregationFunction', 'mean')
        group_by_field = node.config.get('groupByField')
        time_window = node.config.get('timeWindow')

        # Convert stream points to DataFrame
        df = pd.DataFrame(stream_points)

        # Extract values for aggregation
        values = []
        for point in stream_points:
            value = self._extract_nested_value(point, aggregation_field)
            if value is not None:
                try:
                    values.append(float(value))
                except (ValueError, TypeError):
                    values.append(0)

        df['aggregation_value'] = values

        # Group by field if specified
        if group_by_field:
            group_values = [self._extract_nested_value(point, group_by_field) for point in stream_points]
            df['group_field'] = group_values
            grouped = df.groupby('group_field')['aggregation_value']
        else:
            grouped = df['aggregation_value']

        # Apply aggregation function
        if aggregation_function == 'mean':
            result = grouped.mean() if hasattr(grouped, 'mean') else df['aggregation_value'].mean()
        elif aggregation_function == 'sum':
            result = grouped.sum() if hasattr(grouped, 'sum') else df['aggregation_value'].sum()
        elif aggregation_function == 'count':
            result = grouped.count() if hasattr(grouped, 'count') else len(df)
        elif aggregation_function == 'min':
            result = grouped.min() if hasattr(grouped, 'min') else df['aggregation_value'].min()
        elif aggregation_function == 'max':
            result = grouped.max() if hasattr(grouped, 'max') else df['aggregation_value'].max()
        elif aggregation_function == 'std':
            result = grouped.std() if hasattr(grouped, 'std') else df['aggregation_value'].std()

        # Convert result to DataFrame
        if isinstance(result, pd.Series):
            result_df = result.reset_index()
        else:
            result_df = pd.DataFrame({'aggregated_value': [result]})

        return result_df

    def _execute_stream_event_creator(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> \
    List[IotEvent]:
        """Create events from individual stream measurement points."""
        stream_points = inputs.get('stream-points')
        case_context = inputs.get('case-context')

        if not stream_points:
            raise ValueError("Stream points input is required")

        event_id_pattern = node.config.get('eventIdPattern', '{uuid8}-{stream_id}')
        event_class = node.config.get('eventClass', 'iot_event')
        timestamp_mapping = node.config.get('timestampMapping', 'timestamp')

        events = []

        for point in stream_points:
            try:
                # Extract timestamp
                timestamp = point.get(timestamp_mapping) or point.get('timestamp')
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp)

                # Extract stream metadata
                event_data = point.get('event_data', [])
                metadata = {'concept:name': point.get('trace_concept_name')}
                stream_id = None

                if isinstance(event_data, list):
                    for item in event_data:
                        if isinstance(item, dict):
                            key = item.get('@key') or item.get('key')
                            value = item.get('@value') or item.get('value')
                            if key and value:
                                metadata[key] = value
                                if 'id' in key.lower():
                                    stream_id = value

                # Generate event ID based on pattern
                event_id = event_id_pattern.format(
                    uuid8=str(uuid.uuid4())[:8],
                    stream_id=stream_id or 'unknown'
                )

                # Create IoT event
                iot_event = IotEvent(
                    event_id=event_id,
                    event_type=stream_id or 'sensor_reading',
                    timestamp=timestamp,
                    attributes=metadata
                )
                events.append(iot_event)

            except Exception as e:
                logger.warning(f"Failed to create event from stream point: {str(e)}")
                continue

        logger.info(f"Created {len(events)} IoT events from stream points")
        return events

    def _execute_stream_metadata_extractor(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> \
    Dict[str, List[Any]]:
        """Extract metadata from stream measurement points."""
        stream_points = inputs.get('stream-points')
        if not stream_points:
            raise ValueError("Stream points input is required")

        metadata_fields_json = node.config.get('metadataFields', '["stream:source", "stream:value", "stream:id"]')
        key_attribute = node.config.get('keyAttribute', '@key')
        value_attribute = node.config.get('valueAttribute', '@value')

        try:
            metadata_fields = json.loads(metadata_fields_json)
        except json.JSONDecodeError:
            metadata_fields = ['stream:source', 'stream:value', 'stream:id']

        extracted_metadata = {field: [] for field in metadata_fields}

        for point in stream_points:
            event_data = point.get('event_data', [])

            if isinstance(event_data, list):
                # Extract key-value pairs
                point_metadata = {}
                for item in event_data:
                    if isinstance(item, dict):
                        key = item.get(key_attribute.lstrip('@'))
                        value = item.get(value_attribute.lstrip('@'))
                        if key and value:
                            point_metadata[key] = value

                # Extract requested fields
                for field in metadata_fields:
                    field_key = field.split(':')[-1]  # Remove prefix
                    value = point_metadata.get(field_key) or point_metadata.get(field)
                    extracted_metadata[field].append(value)

        return extracted_metadata

    # ============ ENHANCED OBJECT CREATION ============

    def _execute_dynamic_object_creator(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> \
    List[Object]:
        """Create objects dynamically from attribute data."""
        object_id = inputs.get('object-id')
        object_type = inputs.get('object-type')
        attributes_data = inputs.get('attributes-data')

        object_class = node.config.get('objectClass', 'BUSINESS_OBJECT')
        attribute_mapping_json = node.config.get('attributeMapping', '{}')

        try:
            attribute_mapping = json.loads(attribute_mapping_json)
        except json.JSONDecodeError:
            attribute_mapping = {}

        objects = []

        # Handle different input types
        if isinstance(attributes_data, dict):
            attributes_data = [attributes_data]
        elif isinstance(attributes_data, pd.DataFrame):
            attributes_data = attributes_data.to_dict('records')
        elif not isinstance(attributes_data, list):
            attributes_data = [{'data': attributes_data}]

        for i, attr_data in enumerate(attributes_data):
            # Generate object ID if not provided
            if object_id:
                obj_id = object_id if isinstance(object_id, str) else object_id[i] if isinstance(object_id,
                                                                                                 list) else str(
                    object_id)
            else:
                obj_id = str(uuid.uuid4())

            # Determine object type
            if object_type:
                obj_type = object_type if isinstance(object_type, str) else object_type[i] if isinstance(object_type,
                                                                                                         list) else str(
                    object_type)
            else:
                obj_type = 'dynamic_object'

            # Map attributes based on configuration
            mapped_attributes = {}
            for target_attr, source_field in attribute_mapping.items():
                value = attr_data.get(source_field)
                if value is not None:
                    mapped_attributes[target_attr] = value

            # Add remaining attributes
            for key, value in attr_data.items():
                if key not in attribute_mapping.values():
                    mapped_attributes[key] = value

            # Create object
            obj = Object(
                object_id=obj_id,
                object_type=obj_type,
                object_class=ObjectClassEnum(object_class),
                attributes=mapped_attributes
            )
            objects.append(obj)

        return objects

    def _execute_attribute_mapper(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> Dict[
        str, Any]:
        """Map and transform attributes from source data."""
        source_data = inputs.get('source-data')
        if source_data is None:
            raise ValueError("Source data input is required")

        source_field = node.config.get('sourceField')
        target_attribute = node.config.get('targetAttribute')
        transformation = node.config.get('transformation', 'none')
        prefix = node.config.get('prefix', '')

        if not source_field or not target_attribute:
            raise ValueError("Source field and target attribute are required")

        # Extract value from source data
        value = self._extract_nested_value(source_data, source_field)

        # Apply transformation
        if transformation == 'to_string':
            value = str(value) if value is not None else ''
        elif transformation == 'to_number':
            try:
                value = float(value) if value is not None else 0
            except (ValueError, TypeError):
                value = 0
        elif transformation == 'to_date':
            try:
                value = datetime.fromisoformat(str(value)) if value is not None else datetime.utcnow()
            except (ValueError, TypeError):
                value = datetime.utcnow()
        elif transformation == 'extract_uuid':
            value = str(uuid.uuid4())[:8] if value is not None else str(uuid.uuid4())[:8]

        # Apply prefix
        if prefix and value:
            value = f"{prefix}{value}"

        return {target_attribute: value}

    def _execute_context_based_linker(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> \
    List[EventObjectRelationship]:
        """Create relationships based on shared context attributes."""
        events = inputs.get('events')
        objects = inputs.get('objects')

        if not events or not objects:
            raise ValueError("Both events and objects are required")

        context_attribute = node.config.get('contextAttribute', 'concept:name')
        relationship_type = node.config.get('relationshipType', 'belongs_to')
        matching_strategy = node.config.get('matchingStrategy', 'exact_match')

        relationships = []

        # Ensure we have lists
        if not isinstance(events, list):
            events = [events]
        if not isinstance(objects, list):
            objects = [objects]

        for event in events:
            event_context = event.attributes.get(context_attribute)

            if not event_context:
                continue

            for obj in objects:
                obj_context = obj.attributes.get(context_attribute) or obj.object_id

                # Apply matching strategy
                match = False
                if matching_strategy == 'exact_match':
                    match = event_context == obj_context
                elif matching_strategy == 'contains':
                    match = str(event_context) in str(obj_context) or str(obj_context) in str(event_context)
                elif matching_strategy == 'starts_with':
                    match = str(obj_context).startswith(str(event_context))
                elif matching_strategy == 'regex':
                    import re
                    try:
                        match = bool(re.search(str(event_context), str(obj_context)))
                    except re.error:
                        match = False

                if match:
                    relationship = EventObjectRelationship(
                        event_id=event.event_id,
                        object_id=obj.object_id,
                        qualifier=relationship_type
                    )
                    relationships.append(relationship)

        return relationships

    # ============ HELPER METHODS FOR XML/CAIRO PROCESSING ============

    def _navigate_xml_path(self, data: Dict[str, Any], path: str) -> Any:
        """Navigate through nested XML dictionary using dot notation."""
        current = data
        parts = path.split('/')

        for part in parts:
            if not part:  # Skip empty parts
                continue

            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if 0 <= idx < len(current) else None
            else:
                return None

            if current is None:
                return None

        return current

    def _extract_trace_identifier(self, trace_data: Dict[str, Any], identifier_field: str) -> str:
        """Extract trace identifier from trace data."""
        # Look for string element with matching key
        if 'string' in trace_data:
            string_data = trace_data['string']
            if isinstance(string_data, dict):
                if string_data.get('@key') == identifier_field:
                    return string_data.get('@value', '')
            elif isinstance(string_data, list):
                for item in string_data:
                    if isinstance(item, dict) and item.get('@key') == identifier_field:
                        return item.get('@value', '')

        return f"trace_{uuid.uuid4().hex[:8]}"

    def _extract_lifecycle_from_trace(self, trace_data: Dict[str, Any]) -> Dict[str, str]:
        """Extract lifecycle information from trace data."""
        lifecycle_info = {}

        # Look for stream points to determine lifecycle
        stream_points = self._navigate_xml_path(trace_data, 'list/list/list')

        if isinstance(stream_points, list) and stream_points:
            # Extract timestamps from first and last points
            first_point = stream_points[0]
            last_point = stream_points[-1]

            first_timestamp = self._extract_timestamp_value(self._navigate_xml_path(first_point, 'date'))
            last_timestamp = self._extract_timestamp_value(self._navigate_xml_path(last_point, 'date'))

            if first_timestamp:
                lifecycle_info['lifecycle:start'] = first_timestamp.isoformat()
            if last_timestamp:
                lifecycle_info['lifecycle:end'] = last_timestamp.isoformat()

        return lifecycle_info

    def _extract_timestamp_value(self, timestamp_data: Any) -> datetime:
        """Extract timestamp value from various formats."""
        if not timestamp_data:
            return datetime.utcnow()

        if isinstance(timestamp_data, dict):
            value = timestamp_data.get('@value') or timestamp_data.get('value')
        else:
            value = timestamp_data

        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return datetime.utcnow()
        elif isinstance(value, datetime):
            return value
        else:
            return datetime.utcnow()

    def _element_to_dict(self, element) -> Dict[str, Any]:
        """Convert XML element to dictionary."""
        result = {
            'tag': element.tag,
            'text': element.text,
            'attributes': element.attrib
        }

        # Add children
        children = {}
        for child in element:
            if child.tag not in children:
                children[child.tag] = []
            children[child.tag].append(self._element_to_dict(child))

        if children:
            result['children'] = children

        return result

    def _flatten_nested_structure(self, data: Any, max_depth: int, current_depth: int = 0) -> List[Any]:
        """Flatten nested structure to specified depth."""
        if current_depth >= max_depth:
            return [data] if data is not None else []

        if isinstance(data, list):
            result = []
            for item in data:
                result.extend(self._flatten_nested_structure(item, max_depth, current_depth + 1))
            return result
        elif isinstance(data, dict):
            result = []
            for value in data.values():
                result.extend(self._flatten_nested_structure(value, max_depth, current_depth + 1))
            return result
        else:
            return [data] if data is not None else []

    def _extract_level(self, data: Any, target_level: int, current_level: int = 0) -> Any:
        """Extract data at specific nesting level."""
        if current_level == target_level:
            return data

        if isinstance(data, (list, dict)):
            if isinstance(data, list) and data:
                return self._extract_level(data[0], target_level, current_level + 1)
            elif isinstance(data, dict):
                values = list(data.values())
                if values:
                    return self._extract_level(values[0], target_level, current_level + 1)

        return data

    def _extract_nested_value(self, data: Any, field_path: str) -> Any:
        """Extract value from nested structure using dot notation."""
        current = data
        parts = field_path.split('.')

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if 0 <= idx < len(current) else None
            else:
                return None

            if current is None:
                return None

        return current

    # ============ EXISTING EXECUTION METHODS ============
    # (Include all existing methods from the original file)

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

        if filename == "full_log.xml":
            filename = "20250825_075102_ab5c238c-ca68-49fe-af1e-f8eae271908d_full_log.xml"

        file_type = "cairo"

        file_data = file_service.load_file(filename, file_type, encoding)
        logger.info(f"Loaded file {filename} with shape {file_data.shape}")
        return file_data

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
    def test_node(self, node: PipelineNode) -> Dict[str, Any]:
        """Test a node configuration without full execution."""
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
            elif data_type == 'Traces':
                mock_inputs[input_port.name] = [
                    {'concept_name': 'case_1', 'trace_data': {'string': {'@key': 'concept:name', '@value': 'case_1'}}}
                ]
            elif data_type == 'StreamPoints':
                mock_inputs[input_port.name] = [
                    {'timestamp': datetime.utcnow(), 'trace_concept_name': 'case_1', 'event_data': []}
                ]
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

    def _execute_ocpm_discovery(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> Dict[
        str, Any]:
        """Execute OCPM Discovery node."""
        core_metamodel = inputs.get('core-metamodel')
        algorithm = node.config.get('algorithm', 'Directly-Follows Graph')
        filter_noise = node.config.get('filterNoise', False)

        if not core_metamodel:
            raise ValueError("CORE metamodel is required")

        ocel_pointer: pm4py.OCEL = core_metamodel.get_ocel()
        # core_metamodel.save_ocel("v1_output.jsonocel")
        print(ocel_pointer.get_summary())
        discovered_df = pm4py.discover_oc_petri_net(ocel_pointer)

        base_file_path = os.getcwd() + "/exports/"
        file_name = context['execution_id'] + "_ocpn.png"
        pm4py.save_vis_ocpn(discovered_df, base_file_path + file_name)

        discovery_config = {
            'algorithm': algorithm,
            'filterNoise': filter_noise,
            'timestamp': datetime.now().isoformat(),
            "path": "http://127.0.0.1:5100/exports/" + file_name
        }

        return {
            'type': 'process_discovery',
            'config': discovery_config,
            'status': 'ready_for_discovery'
        }

    def _execute_table_output(self, node: PipelineNode, inputs: Dict[str, Any], context: Dict[str, Any]) -> Dict[
        str, Any]:
        """Execute Table Output node."""
        data = inputs.get('data')

        if data is None:
            return {'message': 'No data to display'}

        base_file_path = os.getcwd() + "/exports/"
        file_name = context['execution_id'] + "_ocel"

        file_type = node.config.get('format', 'jsonocel')
        max_rows = node.config.get('maxRows', 100)

        full_file_path = f"{base_file_path}{file_name}.{file_type}"

        pm4py.write_ocel(data.get_ocel(), full_file_path)

        config = {
            'maxRows': max_rows,
            'timestamp': datetime.now().isoformat(),
            'path': f"http://127.0.0.1:5100/exports/{file_name}.{file_type}"
        }

        return {
            'type': 'table',
            'config': config,
            'status': "success",
        }
