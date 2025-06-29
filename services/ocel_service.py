# services/ocel_service.py - OCEL Export Service

import os
import json
import logging
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom

from core.ocel_wrapper import COREMetamodel

logger = logging.getLogger(__name__)


class OCELService:
    """Service for exporting CORE models to OCEL format."""

    def __init__(self, export_folder: str):
        self.export_folder = export_folder
        os.makedirs(export_folder, exist_ok=True)

    def export_core_model(self, core_model: COREMetamodel, format_type: str, filename: str) -> Dict[str, Any]:
        """
        Export CORE model to OCEL format.

        :param core_model: CORE metamodel to export
        :param format_type: Export format (OCEL 2.0 JSON or OCEL 2.0 XML)
        :param filename: Output filename
        :return: Export result
        """
        try:
            if not core_model:
                raise ValueError("CORE model is required")

            logger.info(f"Exporting CORE model to {format_type}")

            # Get OCEL object from CORE model
            ocel = core_model.get_ocel()

            # Generate unique export ID
            export_id = str(uuid.uuid4())

            # Create filename with timestamp if not provided
            if not filename:
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                extension = ".json" if "JSON" in format_type else ".xml"
                filename = f"core_export_{timestamp}{extension}"

            file_path = os.path.join(self.export_folder, filename)

            # Export based on format
            if "JSON" in format_type.upper():
                export_result = self._export_to_json(ocel, file_path)
            elif "XML" in format_type.upper():
                export_result = self._export_to_xml(ocel, file_path)
            else:
                raise ValueError(f"Unsupported export format: {format_type}")

            # Get file size
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0

            result = {
                'success': True,
                'export_id': export_id,
                'format': format_type,
                'filename': filename,
                'file_path': file_path,
                'file_size': file_size,
                'exported_at': datetime.utcnow().isoformat(),
                'statistics': export_result.get('statistics', {})
            }

            logger.info(f"CORE model exported successfully: {filename} ({file_size} bytes)")
            return result

        except Exception as e:
            logger.error(f"Error exporting CORE model: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'export_id': None,
                'filename': filename
            }

    def _export_to_json(self, ocel, file_path: str) -> Dict[str, Any]:
        """Export OCEL to JSON format."""
        try:
            # Create OCEL 2.0 JSON structure
            ocel_data = {
                "ocel:global-log": {
                    "ocel:version": "2.0",
                    "ocel:ordering": "timestamp",
                    "ocel:attribute-names": self._get_attribute_names(ocel),
                    "ocel:object-types": self._get_object_types(ocel),
                    "ocel:event-types": self._get_event_types(ocel)
                },
                "ocel:events": self._convert_events_to_dict(ocel.events),
                "ocel:objects": self._convert_objects_to_dict(ocel.objects),
                "ocel:relations": self._convert_relations_to_dict(ocel.relations)
            }

            # Add object-to-object relations if available
            if hasattr(ocel, 'o2o') and not ocel.o2o.empty:
                ocel_data["ocel:o2o"] = self._convert_o2o_to_dict(ocel.o2o)

            # Write to JSON file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(ocel_data, f, indent=2, default=str, ensure_ascii=False)

            statistics = {
                'events_count': len(ocel.events),
                'objects_count': len(ocel.objects),
                'relations_count': len(ocel.relations),
                'object_types': len(set(ocel.objects[ocel.object_type_column].unique())),
                'event_types': len(set(ocel.events[ocel.event_activity].unique()))
            }

            return {'success': True, 'statistics': statistics}

        except Exception as e:
            raise Exception(f"JSON export failed: {str(e)}")

    def _export_to_xml(self, ocel, file_path: str) -> Dict[str, Any]:
        """Export OCEL to XML format."""
        try:
            # Create root element
            root = ET.Element("log")
            root.set("xes.version", "2.0")
            root.set("xes.features", "nested-attributes")
            root.set("openxes.version", "1.0RC7")

            # Add global attributes
            self._add_xml_global_attributes(root, ocel)

            # Group events by case (object)
            cases = self._group_events_by_case(ocel)

            # Add traces (cases)
            for case_id, case_events in cases.items():
                trace_elem = ET.SubElement(root, "trace")

                # Add case attributes
                case_name_attr = ET.SubElement(trace_elem, "string")
                case_name_attr.set("key", "concept:name")
                case_name_attr.set("value", str(case_id))

                # Add events to trace
                for _, event_row in case_events.iterrows():
                    event_elem = ET.SubElement(trace_elem, "event")

                    # Add event attributes
                    for col, value in event_row.items():
                        if pd.notna(value) and col not in ['case_id']:
                            attr_elem = self._create_xml_attribute(col, value)
                            if attr_elem is not None:
                                event_elem.append(attr_elem)

            # Create tree and write to file
            tree = ET.ElementTree(root)

            # Pretty print XML
            rough_string = ET.tostring(root, encoding='unicode')
            reparsed = minidom.parseString(rough_string)
            pretty_xml = reparsed.toprettyxml(indent="  ")

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(pretty_xml)

            statistics = {
                'events_count': len(ocel.events),
                'objects_count': len(ocel.objects),
                'traces_count': len(cases),
                'relations_count': len(ocel.relations)
            }

            return {'success': True, 'statistics': statistics}

        except Exception as e:
            raise Exception(f"XML export failed: {str(e)}")

    def _get_attribute_names(self, ocel) -> List[str]:
        """Get all attribute names from events and objects."""
        attribute_names = set()

        # From events
        for col in ocel.events.columns:
            if col.startswith("ocel:attr:"):
                attribute_names.add(col[10:])  # Remove "ocel:attr:" prefix

        # From objects
        for col in ocel.objects.columns:
            if col.startswith("ocel:attr:"):
                attribute_names.add(col[10:])

        return sorted(list(attribute_names))

    def _get_object_types(self, ocel) -> List[str]:
        """Get all object types."""
        if ocel.object_type_column in ocel.objects.columns:
            return sorted(ocel.objects[ocel.object_type_column].unique().tolist())
        return []

    def _get_event_types(self, ocel) -> List[str]:
        """Get all event types."""
        if ocel.event_activity in ocel.events.columns:
            return sorted(ocel.events[ocel.event_activity].unique().tolist())
        return []

    def _convert_events_to_dict(self, events_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert events DataFrame to list of dictionaries."""
        events_list = []

        for _, row in events_df.iterrows():
            event_dict = {}

            for col, value in row.items():
                if pd.notna(value):
                    # Convert column names to OCEL format
                    if col.startswith("ocel:attr:"):
                        # Attribute
                        attr_name = col[10:]
                        if "ocel:attributes" not in event_dict:
                            event_dict["ocel:attributes"] = {}
                        event_dict["ocel:attributes"][attr_name] = self._convert_value(value)
                    else:
                        # Standard OCEL field
                        event_dict[col] = self._convert_value(value)

            events_list.append(event_dict)

        return events_list

    def _convert_objects_to_dict(self, objects_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert objects DataFrame to list of dictionaries."""
        objects_list = []

        for _, row in objects_df.iterrows():
            object_dict = {}

            for col, value in row.items():
                if pd.notna(value):
                    if col.startswith("ocel:attr:"):
                        # Attribute
                        attr_name = col[10:]
                        if "ocel:attributes" not in object_dict:
                            object_dict["ocel:attributes"] = {}
                        object_dict["ocel:attributes"][attr_name] = self._convert_value(value)
                    else:
                        # Standard OCEL field
                        object_dict[col] = self._convert_value(value)

            objects_list.append(object_dict)

        return objects_list

    def _convert_relations_to_dict(self, relations_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert relations DataFrame to list of dictionaries."""
        relations_list = []

        for _, row in relations_df.iterrows():
            relation_dict = {}
            for col, value in row.items():
                if pd.notna(value):
                    relation_dict[col] = self._convert_value(value)
            relations_list.append(relation_dict)

        return relations_list

    def _convert_o2o_to_dict(self, o2o_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert object-to-object relations DataFrame to list of dictionaries."""
        o2o_list = []

        for _, row in o2o_df.iterrows():
            o2o_dict = {}
            for col, value in row.items():
                if pd.notna(value):
                    o2o_dict[col] = self._convert_value(value)
            o2o_list.append(o2o_dict)

        return o2o_list

    def _convert_value(self, value) -> Any:
        """Convert pandas/numpy values to JSON-serializable types."""
        if pd.isna(value):
            return None
        elif hasattr(value, 'isoformat'):  # datetime
            return value.isoformat()
        elif hasattr(value, 'item'):  # numpy types
            return value.item()
        else:
            return value

    def _add_xml_global_attributes(self, root: ET.Element, ocel) -> None:
        """Add global attributes to XML root."""
        # Add global event attributes
        global_event = ET.SubElement(root, "global")
        global_event.set("scope", "event")

        # Standard event attributes
        event_attrs = ["concept:name", "time:timestamp", "lifecycle:transition"]
        for attr in event_attrs:
            attr_elem = ET.SubElement(global_event, "string")
            attr_elem.set("key", attr)
            attr_elem.set("value", "string")

        # Add global trace attributes
        global_trace = ET.SubElement(root, "global")
        global_trace.set("scope", "trace")

        trace_attr = ET.SubElement(global_trace, "string")
        trace_attr.set("key", "concept:name")
        trace_attr.set("value", "string")

    def _group_events_by_case(self, ocel) -> Dict[str, pd.DataFrame]:
        """Group events by case (object) for XES format."""
        cases = {}

        # If we have relations, use them to group events by objects
        if hasattr(ocel, 'relations') and not ocel.relations.empty:
            for obj_id in ocel.objects[ocel.object_id_column].unique():
                # Find events related to this object
                related_events = ocel.relations[
                    ocel.relations[ocel.object_id_column] == obj_id
                    ][ocel.event_id_column].unique()

                if len(related_events) > 0:
                    case_events = ocel.events[
                        ocel.events[ocel.event_id_column].isin(related_events)
                    ].copy()
                    case_events['case_id'] = obj_id
                    cases[obj_id] = case_events
        else:
            # If no relations, create one case with all events
            case_events = ocel.events.copy()
            case_events['case_id'] = 'case_1'
            cases['case_1'] = case_events

        return cases

    def _create_xml_attribute(self, key: str, value: Any) -> Optional[ET.Element]:
        """Create XML attribute element based on value type."""
        if pd.isna(value):
            return None

        # Determine attribute type
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if isinstance(value, int):
                attr_elem = ET.Element("int")
            else:
                attr_elem = ET.Element("float")
        elif isinstance(value, bool):
            attr_elem = ET.Element("boolean")
            value = str(value).lower()
        elif hasattr(value, 'isoformat'):  # datetime
            attr_elem = ET.Element("date")
            value = value.isoformat()
        else:
            attr_elem = ET.Element("string")
            value = str(value)

        attr_elem.set("key", key)
        attr_elem.set("value", str(value))

        return attr_elem

    def get_export_info(self, export_id: str) -> Optional[Dict[str, Any]]:
        """Get information about an export."""
        # In production, this would query a database
        # For now, we'll check if file exists
        export_files = [f for f in os.listdir(self.export_folder) if export_id in f]

        if export_files:
            file_path = os.path.join(self.export_folder, export_files[0])
            file_size = os.path.getsize(file_path)

            return {
                'export_id': export_id,
                'filename': export_files[0],
                'file_path': file_path,
                'file_size': file_size,
                'created_at': datetime.fromtimestamp(os.path.getctime(file_path)).isoformat()
            }

        return None

    def list_exports(self) -> List[Dict[str, Any]]:
        """List all available exports."""
        exports = []

        for filename in os.listdir(self.export_folder):
            file_path = os.path.join(self.export_folder, filename)
            if os.path.isfile(file_path):
                exports.append({
                    'filename': filename,
                    'file_path': file_path,
                    'file_size': os.path.getsize(file_path),
                    'created_at': datetime.fromtimestamp(os.path.getctime(file_path)).isoformat(),
                    'modified_at': datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                })

        return sorted(exports, key=lambda x: x['created_at'], reverse=True)

    def delete_export(self, export_id: str) -> bool:
        """Delete an export file."""
        try:
            export_info = self.get_export_info(export_id)
            if export_info and os.path.exists(export_info['file_path']):
                os.remove(export_info['file_path'])
                logger.info(f"Export deleted: {export_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting export {export_id}: {str(e)}")
            return False