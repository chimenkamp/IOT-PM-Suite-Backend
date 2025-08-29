# services/file_service.py

import os
import uuid
import json
import logging
import pandas as pd
import xml.etree.ElementTree as ET

import pm4py
import yaml
import xmltodict
from typing import Dict, Any, Optional, List, Union, MutableMapping
from datetime import datetime
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage
import mimetypes

logger = logging.getLogger(__name__)


def flatten(dictionary, parent_key='', separator='_'):
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)

class FileService:
    """Service for handling file uploads and processing with CAIRO support."""

    def __init__(self, upload_folder: str):
        self.upload_folder = upload_folder
        self.allowed_extensions = {
            'csv', 'json', 'xml', 'yaml', 'yml', 'xes', 'txt', 'cairo'
        }
        self.file_registry = {}  # In production, use database

        # Ensure upload folder exists
        os.makedirs(upload_folder, exist_ok=True)

    def save_uploaded_file(self, file: FileStorage, file_type: str, original_filename: str,
                           analyze_cairo: bool = False) -> Dict[str, Any]:
        """
        Save an uploaded file to the server with optional CAIRO analysis.

        :param file: FileStorage object from Flask
        :param file_type: Type of the file (CSV, JSON, etc.)
        :param original_filename: Original filename
        :param analyze_cairo: Whether to perform CAIRO format analysis
        :return: File information dictionary with CAIRO analysis if requested
        """
        try:
            # Generate unique file ID and secure filename
            file_id = str(uuid.uuid4())
            filename = secure_filename(original_filename)

            # Add timestamp to avoid conflicts
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            base_name, ext = os.path.splitext(filename)
            saved_filename = f"{timestamp}_{file_id}_{base_name}{ext}"

            # Save file
            file_path = os.path.join(self.upload_folder, saved_filename)
            file.save(file_path)

            # Get file size
            file_size = os.path.getsize(file_path)

            # Store file information
            file_info = {
                'file_id': file_id,
                'filename': saved_filename,
                'original_name': original_filename,
                'file_type': file_type,
                'file_path': file_path,
                'size': file_size,
                'uploaded_at': datetime.utcnow().isoformat(),
                'mime_type': mimetypes.guess_type(original_filename)[0]
            }

            # Perform CAIRO analysis if requested and file is XML
            if analyze_cairo and file_type.upper() == 'XML':
                try:
                    cairo_analysis = self.analyze_cairo_format(file_path)
                    file_info['cairo_analysis'] = cairo_analysis
                    logger.info(f"CAIRO analysis completed for {file_id}")
                except Exception as e:
                    logger.warning(f"CAIRO analysis failed for {file_id}: {str(e)}")

            self.file_registry[file_id] = file_info

            logger.info(f"File uploaded: {file_id} - {original_filename} ({file_size} bytes)")

            return file_info

        except Exception as e:
            logger.error(f"Error saving file: {str(e)}")
            raise Exception(f"Failed to save file: {str(e)}")

    def analyze_cairo_format(self, file_path: str) -> Dict[str, Any]:
        """
        Analyze XML file for CAIRO format characteristics.

        :param file_path: Path to the XML file
        :return: CAIRO analysis results
        """
        try:
            # Read and parse XML
            with open(file_path, 'r', encoding='utf-8') as f:
                xml_content = f.read()

            # Parse XML to dictionary
            xml_dict = xmltodict.parse(xml_content)

            # Analyze structure
            analysis = {
                'is_cairo_format': self._detect_cairo_structure(xml_dict),
                'structure_analysis': self._analyze_xml_structure(xml_dict),
                'trace_information': self._analyze_traces(xml_dict),
                'stream_analysis': self._analyze_stream_data(xml_dict),
                'recommended_configuration': self._generate_cairo_config(xml_dict),
                'processing_hints': self._generate_processing_hints(xml_dict)
            }

            logger.info(f"CAIRO analysis completed: {analysis['is_cairo_format']}")
            return analysis

        except Exception as e:
            logger.error(f"Error analyzing CAIRO format: {str(e)}")
            return {
                'is_cairo_format': False,
                'error': str(e),
                'analysis_failed': True
            }

    def _detect_cairo_structure(self, xml_dict: Dict[str, Any]) -> bool:
        """Detect if XML structure matches CAIRO format."""
        try:
            xml_str = str(xml_dict).lower()

            # Check for log/trace structure
            has_log_trace = (
                    'log' in xml_dict and
                    'trace' in str(xml_dict.get('log', {}))
            )

            # Check for stream-specific attributes
            stream_indicators = ['stream:id', 'stream:source', 'stream:value', 'concept:name']
            has_stream_attrs = any(attr in xml_str for attr in stream_indicators)

            # Check for nested list structure (typical of CAIRO sensor data)
            has_nested_lists = xml_str.count('list') >= 3

            # Check for date/timestamp fields
            has_temporal_data = any(field in xml_str for field in ['date', 'timestamp', 'time:'])

            return has_log_trace and has_stream_attrs and has_nested_lists and has_temporal_data

        except Exception as e:
            logger.warning(f"CAIRO structure detection failed: {str(e)}")
            return False

    def _analyze_xml_structure(self, xml_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze overall XML structure."""
        try:
            structure = {
                'root_element': list(xml_dict.keys())[0] if xml_dict else None,
                'has_log_element': 'log' in xml_dict,
                'has_traces': False,
                'trace_count': 0,
                'has_events': False,
                'has_attributes': False,
                'nesting_depth': self._calculate_nesting_depth(xml_dict)
            }

            if 'log' in xml_dict:
                log_data = xml_dict['log']

                if 'trace' in log_data:
                    structure['has_traces'] = True
                    traces = log_data['trace']
                    structure['trace_count'] = len(traces) if isinstance(traces, list) else 1

                # Check for events and attributes
                xml_str = str(xml_dict).lower()
                structure['has_events'] = 'event' in xml_str
                structure['has_attributes'] = '@' in str(xml_dict) or 'attributes' in xml_str

            return structure

        except Exception as e:
            logger.error(f"Error analyzing XML structure: {str(e)}")
            return {'error': str(e)}

    def _analyze_traces(self, xml_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze trace information in CAIRO format."""
        try:
            trace_info = {
                'total_traces': 0,
                'sample_trace_names': [],
                'trace_structure': {},
                'has_concept_names': False,
                'has_lifecycle_data': False
            }

            if 'log' not in xml_dict or 'trace' not in xml_dict['log']:
                return trace_info

            traces = xml_dict['log']['trace']
            if not isinstance(traces, list):
                traces = [traces]

            trace_info['total_traces'] = len(traces)

            # Analyze first few traces
            for i, trace in enumerate(traces[:5]):  # Sample first 5 traces
                # Extract concept name
                concept_name = self._extract_concept_name(trace)
                if concept_name:
                    trace_info['sample_trace_names'].append(concept_name)
                    trace_info['has_concept_names'] = True

                # Analyze trace structure (use first trace as template)
                if i == 0:
                    trace_info['trace_structure'] = self._analyze_trace_structure(trace)

            # Check for lifecycle data indicators
            xml_str = str(xml_dict).lower()
            trace_info['has_lifecycle_data'] = any(
                indicator in xml_str for indicator in ['lifecycle:', 'start', 'end', 'duration']
            )

            return trace_info

        except Exception as e:
            logger.error(f"Error analyzing traces: {str(e)}")
            return {'error': str(e)}

    def _analyze_stream_data(self, xml_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze stream data characteristics."""
        try:
            stream_info = {
                'total_stream_points': 0,
                'stream_sources': set(),
                'stream_types': set(),
                'temporal_range': {},
                'sample_values': [],
                'has_sensor_data': False
            }

            # Navigate through traces to find stream points
            if 'log' in xml_dict and 'trace' in xml_dict['log']:
                traces = xml_dict['log']['trace']
                if not isinstance(traces, list):
                    traces = [traces]

                timestamps = []

                for trace in traces:
                    # Find stream points in nested structure
                    stream_points = self._extract_stream_points_from_trace(trace)
                    stream_info['total_stream_points'] += len(stream_points)

                    # Analyze stream points
                    for point in stream_points[:10]:  # Sample first 10 points
                        # Extract timestamp
                        timestamp = self._extract_timestamp_from_point(point)
                        if timestamp:
                            timestamps.append(timestamp)

                        # Extract stream metadata
                        metadata = self._extract_stream_metadata(point)
                        if metadata:
                            if 'stream:source' in metadata:
                                stream_info['stream_sources'].add(metadata['stream:source'])
                            if 'stream:id' in metadata:
                                stream_info['stream_types'].add(metadata['stream:id'])
                            if 'stream:value' in metadata:
                                stream_info['sample_values'].append(metadata['stream:value'])

                # Calculate temporal range
                if timestamps:
                    stream_info['temporal_range'] = {
                        'start': min(timestamps).isoformat(),
                        'end': max(timestamps).isoformat(),
                        'duration_seconds': (max(timestamps) - min(timestamps)).total_seconds()
                    }

                # Convert sets to lists for JSON serialization
                stream_info['stream_sources'] = list(stream_info['stream_sources'])
                stream_info['stream_types'] = list(stream_info['stream_types'])

                # Check if this looks like sensor data
                stream_info['has_sensor_data'] = (
                        len(stream_info['stream_sources']) > 0 or
                        any('sensor' in str(source).lower() for source in stream_info['stream_sources']) or
                        len(stream_info['sample_values']) > 0
                )

            return stream_info

        except Exception as e:
            logger.error(f"Error analyzing stream data: {str(e)}")
            return {'error': str(e)}

    def _generate_cairo_config(self, xml_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Generate recommended configuration for CAIRO processing."""
        config = {
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
            }
        }

        # Analyze actual structure to refine configuration
        try:
            if 'log' in xml_dict and 'trace' in xml_dict['log']:
                # Check actual trace structure
                traces = xml_dict['log']['trace']
                if not isinstance(traces, list):
                    traces = [traces]

                if traces:
                    sample_trace = traces[0]

                    # Check for alternative concept name fields
                    if 'string' in sample_trace:
                        string_data = sample_trace['string']
                        if isinstance(string_data, dict) and string_data.get('@key') == 'concept:name':
                            config['case-object-extractor']['caseIdAttribute'] = 'concept:name'
                        elif isinstance(string_data, list):
                            for item in string_data:
                                if isinstance(item, dict) and item.get('@key') == 'concept:name':
                                    config['case-object-extractor']['caseIdAttribute'] = 'concept:name'
                                    break

                    # Analyze nested structure depth
                    nesting_depth = self._calculate_nesting_depth(sample_trace)
                    if nesting_depth > 3:
                        config['stream-point-extractor']['streamPointsPath'] = 'list/list/list'
                    elif nesting_depth == 2:
                        config['stream-point-extractor']['streamPointsPath'] = 'list/list'

        except Exception as e:
            logger.warning(f"Error refining CAIRO config: {str(e)}")

        return config

    def _generate_processing_hints(self, xml_dict: Dict[str, Any]) -> List[str]:
        """Generate processing hints for CAIRO data."""
        hints = []

        try:
            # Check data characteristics
            stream_analysis = self._analyze_stream_data(xml_dict)

            if stream_analysis.get('total_stream_points', 0) > 1000:
                hints.append("Large number of stream points detected - consider using stream aggregation")

            if len(stream_analysis.get('stream_sources', [])) > 1:
                hints.append("Multiple stream sources detected - consider grouping by source")

            if stream_analysis.get('temporal_range', {}).get('duration_seconds', 0) > 86400:  # > 1 day
                hints.append("Long temporal range detected - consider time window aggregation")

            # Check trace characteristics
            trace_info = self._analyze_traces(xml_dict)

            if trace_info.get('total_traces', 0) > 100:
                hints.append("Many traces detected - consider parallel processing configuration")

            if not trace_info.get('has_concept_names'):
                hints.append("No concept names detected - verify trace identifier configuration")

            # General recommendations
            hints.append("Use XML Trace Extractor to parse log structure")
            hints.append("Create case objects for trace context")
            hints.append("Extract stream points for sensor measurements")
            hints.append("Link events to objects using trace context")

        except Exception as e:
            logger.warning(f"Error generating processing hints: {str(e)}")
            hints.append("Standard CAIRO processing workflow recommended")

        return hints

    def load_file(self, filename: str, file_type: str, encoding: str = 'UTF-8') -> pd.DataFrame:
        """
        Load a file and return as DataFrame (enhanced for CAIRO XML).

        :param filename: Name of the file to load
        :param file_type: Type of the file
        :param encoding: File encoding
        :return: Loaded data as DataFrame
        """
        try:
            # Find file path (could be filename or file_id)
            file_path = self._resolve_file_path(filename)

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {filename}")

            logger.info(f"Loading file: {file_path} as {file_type}")

            # Load based on file type
            if file_type.upper() == 'CSV':
                data = self._load_csv(file_path, encoding)
            elif file_type.upper() == 'JSON':
                data = self._load_json(file_path, encoding)
            elif file_type.upper() == 'XML':
                data = self._load_xml(file_path, encoding)
            elif file_type.upper() in ['YAML', 'YML']:
                data = self._load_yaml(file_path, encoding)
            elif file_type.upper() == 'XES':
                data = self._load_xes(file_path, encoding)
            elif file_type.upper() == 'CAIRO':
                data = self._load_cairo(file_path, encoding)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            logger.info(f"File loaded successfully: {data.shape}")
            return data

        except Exception as e:
            logger.error(f"Error loading file {filename}: {str(e)}")
            raise Exception(f"Failed to load file: {str(e)}")

    def _load_json(self, file_path: str, encoding: str) -> pd.DataFrame:
        """Load JSON file."""
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                json_data = json.load(f)

            # Handle different JSON structures
            if isinstance(json_data, list):
                return pd.DataFrame(json_data)
            elif isinstance(json_data, dict):
                # Try to find the main data array
                for key, value in json_data.items():
                    if isinstance(value, list) and len(value) > 0:
                        return pd.DataFrame(value)
                # If no array found, create single-row DataFrame
                return pd.DataFrame([json_data])
            else:
                raise ValueError("JSON structure not supported")

        except Exception as e:
            raise Exception(f"Error loading JSON: {str(e)}")

    def _load_yaml(self, file_path: str, encoding: str) -> pd.DataFrame:
        """Load YAML file."""
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                yaml_data = yaml.safe_load(f)

            # Handle different YAML structures
            if isinstance(yaml_data, list):
                return pd.DataFrame(yaml_data)
            elif isinstance(yaml_data, dict):
                # Try to find the main data array
                for key, value in yaml_data.items():
                    if isinstance(value, list) and len(value) > 0:
                        return pd.DataFrame(value)
                # If no array found, create single-row DataFrame
                return pd.DataFrame([yaml_data])
            else:
                return pd.DataFrame([{'data': yaml_data}])

        except Exception as e:
            raise Exception(f"Error loading YAML: {str(e)}")

    def _load_xes(self, file_path: str, encoding: str) -> pd.DataFrame:
        """Load XES (eXtensible Event Stream) file."""
        return pd.read_xml(file_path, encoding=encoding)


    def _load_xml(self, file_path: str, encoding: str) -> pd.DataFrame:
        """Load XML file with CAIRO-aware processing."""
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                xml_content = f.read()

            # Try to detect if it's CAIRO format
            try:
                xml_dict = xmltodict.parse(xml_content)
                if self._detect_cairo_structure(xml_dict):
                    logger.info("Detected CAIRO format XML - using specialized parsing")
                    return self._load_cairo_xml(xml_dict)
            except Exception as e:
                logger.warning(f"CAIRO detection failed, falling back to generic XML parsing: {str(e)}")

            # Generic XML parsing
            tree = ET.parse(file_path)
            root = tree.getroot()

            # Extract data from XML structure
            records = []

            # Try to find repeating elements
            children = list(root)
            if children:
                # Use first level children as records
                for child in children:
                    record = {}
                    for elem in child:
                        record[elem.tag] = elem.text
                    if record:  # Only add non-empty records
                        records.append(record)

            if not records:
                # Try to extract attributes and text from root
                record = {}
                for elem in root:
                    record[elem.tag] = elem.text
                records = [record] if record else [{'root': root.text or ''}]

            return pd.DataFrame(records)

        except Exception as e:
            raise Exception(f"Error loading XML: {str(e)}")



    def _load_cairo_xml(self, xml_dict: Dict[str, Any]) -> pd.DataFrame:
        """Load CAIRO XML format into a structured DataFrame."""
        try:
            records = []

            if 'log' in xml_dict and 'trace' in xml_dict['log']:
                traces = xml_dict['log']['trace']
                if not isinstance(traces, list):
                    traces = [traces]

                for trace_idx, trace in enumerate(traces):
                    # Extract trace information
                    concept_name = self._extract_concept_name(trace)

                    # Extract stream points
                    stream_points = self._extract_stream_points_from_trace(trace)

                    for point_idx, point in enumerate(stream_points):
                        # Extract timestamp
                        timestamp = self._extract_timestamp_from_point(point)

                        # Extract stream metadata
                        metadata = self._extract_stream_metadata(point)

                        # Create record
                        record = {
                            'trace_id': concept_name or f'trace_{trace_idx}',
                            'point_index': point_idx,
                            'timestamp': timestamp.isoformat() if timestamp else None
                        }

                        # Add stream metadata
                        record.update(metadata)

                        records.append(record)

            # Convert to DataFrame
            df = pd.DataFrame(records)

            # Convert timestamp column to datetime
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

            logger.info(f"CAIRO XML loaded into DataFrame with {len(df)} rows")
            return df

        except Exception as e:
            raise Exception(f"Error loading CAIRO XML: {str(e)}")

    # ============ HELPER METHODS FOR CAIRO ANALYSIS ============

    def _extract_concept_name(self, trace: Dict[str, Any]) -> Optional[str]:
        """Extract concept name from trace."""
        try:
            if 'string' in trace:
                string_data = trace['string']
                if isinstance(string_data, dict):
                    if string_data.get('@key') == 'concept:name':
                        return string_data.get('@value')
                elif isinstance(string_data, list):
                    for item in string_data:
                        if isinstance(item, dict) and item.get('@key') == 'concept:name':
                            return item.get('@value')
            return None
        except Exception:
            return None

    def _extract_stream_points_from_trace(self, trace: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract stream points from trace structure."""
        try:
            # Navigate to nested list structure
            if 'list' in trace:
                list_data = trace['list']
                if isinstance(list_data, dict) and 'list' in list_data:
                    inner_list = list_data['list']
                    if isinstance(inner_list, dict) and 'list' in inner_list:
                        stream_points = inner_list['list']
                        if isinstance(stream_points, list):
                            return stream_points
                        else:
                            return [stream_points]
            return []
        except Exception:
            return []

    def _extract_timestamp_from_point(self, point: Dict[str, Any]) -> Optional[datetime]:
        """Extract timestamp from stream point."""
        try:
            if 'date' in point:
                date_data = point['date']
                if isinstance(date_data, dict):
                    timestamp_str = date_data.get('@value')
                    if timestamp_str:
                        return datetime.fromisoformat(timestamp_str)
                elif isinstance(date_data, str):
                    return datetime.fromisoformat(date_data)
            return None
        except Exception:
            return None

    def _extract_stream_metadata(self, point: Dict[str, Any]) -> Dict[str, Any]:
        """Extract stream metadata from point."""
        metadata = {}

        try:
            if 'string' in point:
                string_data = point['string']
                if isinstance(string_data, list):
                    for item in string_data:
                        if isinstance(item, dict):
                            key = item.get('@key')
                            value = item.get('@value')
                            if key and value:
                                metadata[key] = value
                elif isinstance(string_data, dict):
                    key = string_data.get('@key')
                    value = string_data.get('@value')
                    if key and value:
                        metadata[key] = value
        except Exception:
            pass

        return metadata

    def _analyze_trace_structure(self, trace: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze the structure of a single trace."""
        structure = {
            'has_string_attributes': 'string' in trace,
            'has_nested_lists': 'list' in trace,
            'has_events': 'event' in trace,
            'elements': list(trace.keys()) if isinstance(trace, dict) else []
        }

        # Analyze nesting depth
        structure['nesting_depth'] = self._calculate_nesting_depth(trace)

        return structure

    def _calculate_nesting_depth(self, data: Any, current_depth: int = 0) -> int:
        """Calculate maximum nesting depth of data structure."""
        if not isinstance(data, (dict, list)):
            return current_depth

        max_depth = current_depth

        if isinstance(data, dict):
            for value in data.values():
                depth = self._calculate_nesting_depth(value, current_depth + 1)
                max_depth = max(max_depth, depth)
        elif isinstance(data, list):
            for item in data:
                depth = self._calculate_nesting_depth(item, current_depth + 1)
                max_depth = max(max_depth, depth)

        return max_depth

    def get_cairo_file_analysis(self, file_id: str) -> Dict[str, Any]:
        """Get CAIRO analysis for a specific file."""
        file_info = self.get_file_info(file_id)
        if not file_info:
            raise ValueError(f"File not found: {file_id}")

        # Check if analysis already exists
        if 'cairo_analysis' in file_info:
            return file_info['cairo_analysis']

        # Perform analysis if file is XML
        if file_info['file_type'].upper() == 'XML':
            analysis = self.analyze_cairo_format(file_info['file_path'])

            # Store analysis in registry
            file_info['cairo_analysis'] = analysis
            self.file_registry[file_id] = file_info

            return analysis
        else:
            return {
                'is_cairo_format': False,
                'message': 'File is not XML format'
            }

    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an uploaded file.

        :param file_id: File ID
        :return: File information or None if not found
        """
        return self.file_registry.get(file_id)

    def list_files(self) -> List[Dict[str, Any]]:
        """
        List all uploaded files.

        :return: List of file information
        """
        return list(self.file_registry.values())

    def delete_file(self, file_id: str) -> bool:
        """
        Delete an uploaded file.

        :param file_id: File ID to delete
        :return: Success status
        """
        try:
            if file_id not in self.file_registry:
                return False

            file_info = self.file_registry[file_id]
            file_path = file_info['file_path']

            # Remove physical file
            if os.path.exists(file_path):
                os.remove(file_path)

            # Remove from registry
            del self.file_registry[file_id]

            logger.info(f"File deleted: {file_id}")
            return True

        except Exception as e:
            logger.error(f"Error deleting file {file_id}: {str(e)}")
            return False

    def analyze_file_structure(self, file_id: str) -> Dict[str, Any]:
        """
        Analyze the structure of an uploaded file (enhanced for CAIRO).

        :param file_id: File ID to analyze
        :return: File structure analysis
        """
        try:
            file_info = self.get_file_info(file_id)
            if not file_info:
                raise ValueError(f"File not found: {file_id}")

            file_path = file_info['file_path']
            file_type = file_info['file_type']

            # Load a sample of the data
            data = self.load_file(file_info['filename'], file_type)

            # Basic analysis
            analysis = {
                'file_id': file_id,
                'file_type': file_type,
                'rows': len(data),
                'columns': len(data.columns) if hasattr(data, 'columns') else 0,
                'column_info': {},
                'sample_data': data.head(5).to_dict('records') if len(data) > 0 else [],
                'data_types': data.dtypes.to_dict() if hasattr(data, 'dtypes') else {},
                'missing_values': data.isnull().sum().to_dict() if hasattr(data, 'isnull') else {},
                'memory_usage': data.memory_usage(deep=True).sum() if hasattr(data, 'memory_usage') else 0
            }

            # Enhanced analysis for XML files
            if file_type.upper() == 'XML' and 'cairo_analysis' in file_info:
                analysis['cairo_analysis'] = file_info['cairo_analysis']
                analysis['is_cairo_format'] = file_info['cairo_analysis'].get('is_cairo_format', False)

                if analysis['is_cairo_format']:
                    analysis['processing_recommendations'] = [
                        'Use CAIRO XML Parsing nodes',
                        'Start with XML Trace Extractor',
                        'Create case objects and stream events',
                        'Link events to objects using trace context'
                    ]

            # Detailed column analysis
            if hasattr(data, 'columns'):
                for col in data.columns:
                    analysis['column_info'][col] = {
                        'type': str(data[col].dtype),
                        'non_null_count': data[col].count(),
                        'unique_count': data[col].nunique(),
                        'sample_values': data[col].dropna().head(3).tolist()
                    }

            return analysis

        except Exception as e:
            logger.error(f"Error analyzing file {file_id}: {str(e)}")
            raise Exception(f"Failed to analyze file: {str(e)}")

    def _resolve_file_path(self, filename: str) -> str:
        """Resolve filename to full file path."""
        # Check if it's a file_id
        if filename in self.file_registry:
            return self.file_registry[filename]['file_path']

        # Check if it's a stored filename
        for file_info in self.file_registry.values():
            if file_info['filename'] == filename or file_info['original_name'] == filename:
                return file_info['file_path']

        # Assume it's a direct path in upload folder
        return os.path.join(self.upload_folder, filename)

    def _load_csv(self, file_path: str, encoding: str) -> pd.DataFrame:
        """Load CSV file."""
        try:
            # Try different separators and handle various CSV formats
            separators = [',', ';', '\t', '|']

            for sep in separators:
                try:
                    data = pd.read_csv(
                        file_path,
                        encoding=encoding,
                        sep=sep,
                        parse_dates=True,
                        infer_datetime_format=True,
                        low_memory=False
                    )

                    # Check if we got reasonable column separation
                    if len(data.columns) > 1 or sep == separators[-1]:
                        logger.info(f"CSV loaded with separator '{sep}', shape: {data.shape}")
                        return data

                except Exception:
                    continue

            # Fallback to basic read
            return pd.read_csv(file_path, encoding=encoding)

        except Exception as e:
            raise Exception(f"Error loading CSV: {str(e)}")

    def validate_file_format(self, file_path: str, expected_type: str) -> Dict[str, Any]:
        """
        Validate file format and structure with CAIRO support.

        :param file_path: Path to the file
        :param expected_type: Expected file type
        :return: Validation result
        """
        try:
            validation_result = {
                'valid': True,
                'errors': [],
                'warnings': [],
                'info': {},
                'cairo_specific': {}
            }

            # Check file exists
            if not os.path.exists(file_path):
                validation_result['valid'] = False
                validation_result['errors'].append('File does not exist')
                return validation_result

            # Check file size
            file_size = os.path.getsize(file_path)
            validation_result['info']['file_size'] = file_size

            if file_size == 0:
                validation_result['valid'] = False
                validation_result['errors'].append('File is empty')
                return validation_result

            if file_size > 500 * 1024 * 1024:  # 500MB limit
                validation_result['warnings'].append('File is very large (>500MB)')

            # CAIRO-specific validation for XML files
            if expected_type.upper() == 'XML':
                try:
                    cairo_analysis = self.analyze_cairo_format(file_path)
                    validation_result['cairo_specific'] = cairo_analysis

                    if cairo_analysis.get('is_cairo_format'):
                        validation_result['info']['is_cairo'] = True
                        validation_result['info']['trace_count'] = cairo_analysis.get('trace_information', {}).get(
                            'total_traces', 0)
                        validation_result['info']['stream_points'] = cairo_analysis.get('stream_analysis', {}).get(
                            'total_stream_points', 0)

                        # Add CAIRO-specific recommendations
                        validation_result['cairo_specific']['recommendations'] = [
                            'Use CAIRO XML Parsing nodes for optimal processing',
                            'Start with XML Trace Extractor',
                            'Consider stream aggregation for large datasets'
                        ]

                except Exception as e:
                    validation_result['warnings'].append(f'CAIRO analysis failed: {str(e)}')

            # Try to load and validate structure
            try:
                data = self.load_file(os.path.basename(file_path), expected_type)
                validation_result['info']['rows'] = len(data)
                validation_result['info']['columns'] = len(data.columns) if hasattr(data, 'columns') else 0

                if len(data) == 0:
                    validation_result['warnings'].append('File contains no data rows')

            except Exception as e:
                validation_result['valid'] = False
                validation_result['errors'].append(f'Failed to parse file: {str(e)}')

            return validation_result

        except Exception as e:
            return {
                'valid': False,
                'errors': [f'Validation error: {str(e)}'],
                'warnings': [],
                'info': {},
                'cairo_specific': {}
            }

    def _load_cairo(self, file_path, encoding):
        with open(file_path, 'r') as file:
            xml_string = file.read()

        xml_dict = xmltodict.parse(xml_string)

        flattened_dict = [flatten(x) for x in xml_dict['log']['trace']]
        return pd.DataFrame(flattened_dict)