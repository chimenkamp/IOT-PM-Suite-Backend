# services/file_service.py - File Upload and Processing Service

import os
import uuid
import json
import logging
import pandas as pd
import xml.etree.ElementTree as ET
import yaml
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage
import mimetypes

logger = logging.getLogger(__name__)


class FileService:
    """Service for handling file uploads and processing."""

    def __init__(self, upload_folder: str):
        self.upload_folder = upload_folder
        self.allowed_extensions = {
            'csv', 'json', 'xml', 'yaml', 'yml', 'xes', 'txt'
        }
        self.file_registry = {}  # In production, use database

        # Ensure upload folder exists
        os.makedirs(upload_folder, exist_ok=True)

    def save_uploaded_file(self, file: FileStorage, file_type: str, original_filename: str) -> Dict[str, Any]:
        """
        Save an uploaded file to the server.

        :param file: FileStorage object from Flask
        :param file_type: Type of the file (CSV, JSON, etc.)
        :param original_filename: Original filename
        :return: File information dictionary
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

            self.file_registry[file_id] = file_info

            logger.info(f"File uploaded: {file_id} - {original_filename} ({file_size} bytes)")

            return file_info

        except Exception as e:
            logger.error(f"Error saving file: {str(e)}")
            raise Exception(f"Failed to save file: {str(e)}")

    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an uploaded file.

        :param file_id: File ID
        :return: File information or None if not found
        """
        return self.file_registry.get(file_id)

    def load_file(self, filename: str, file_type: str, encoding: str = 'UTF-8') -> pd.DataFrame:
        """
        Load a file and return as DataFrame.

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
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            logger.info(f"File loaded successfully: {data.shape}")
            return data

        except Exception as e:
            logger.error(f"Error loading file {filename}: {str(e)}")
            raise Exception(f"Failed to load file: {str(e)}")

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
        Analyze the structure of an uploaded file.

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

            # Analyze structure
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

    def _load_xml(self, file_path: str, encoding: str) -> pd.DataFrame:
        """Load XML file."""
        try:
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
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            events = []

            # Parse XES structure
            for log in root.findall('.//log'):
                for trace in log.findall('trace'):
                    case_id = None
                    # Get case ID from trace attributes
                    for attr in trace.findall('string'):
                        if attr.get('key') == 'concept:name':
                            case_id = attr.get('value')
                            break

                    # Parse events in trace
                    for event in trace.findall('event'):
                        event_data = {'case_id': case_id}

                        # Extract event attributes
                        for attr in event:
                            key = attr.get('key')
                            value = attr.get('value')
                            event_data[key] = value

                        events.append(event_data)

            return pd.DataFrame(events)

        except Exception as e:
            raise Exception(f"Error loading XES: {str(e)}")

    def validate_file_format(self, file_path: str, expected_type: str) -> Dict[str, Any]:
        """
        Validate file format and structure.

        :param file_path: Path to the file
        :param expected_type: Expected file type
        :return: Validation result
        """
        try:
            validation_result = {
                'valid': True,
                'errors': [],
                'warnings': [],
                'info': {}
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
                'info': {}
            }