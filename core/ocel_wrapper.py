from typing import List, Dict, Any, Optional
import pandas as pd
import logging
import pm4py

from core.event_definition import IotEvent, ProcessEvent, Observation, Event
from core.object_definition import Object, ObjectClassEnum
from core.relationship_definitions import EventObjectRelationship, EventEventRelationship, ObjectObjectRelationship

logger = logging.getLogger(__name__)

ATTRIBUTE_KEY_PREFIX = "ocel:attr:"


def get_event_by_id(events: pd.DataFrame, event_id: str, ocel_string: str) -> Dict[str, Any]:
    """
    Retrieves an event by its ID from a list of events.

    :param events: Dataframe of events.
    :param event_id: ID of the event to retrieve.
    :param ocel_string: The column name of the event ID.
    :return: The event with the specified ID.
    """
    return events.loc[events[ocel_string] == event_id].to_dict()


def _get_event_sub_type_label(event: Event, event_class: str) -> str:
    """Determine the event sub-type label based on event class."""
    if event_class == "process_event":
        return getattr(event, 'activity', 'NO EVENT TYPE')
    elif event_class == "iot_event":
        return event.event_type
    elif event_class == "observation":
        return "observed"
    return "NO EVENT TYPE"


class COREMetamodel:
    def __init__(
            self,
            objects: Optional[List[Object]] = None,
            iot_events: Optional[List[IotEvent]] = None,
            process_events: Optional[List[ProcessEvent]] = None,
            observations: Optional[List[Observation]] = None,
            object_object_relationships: Optional[List[ObjectObjectRelationship]] = None,
            event_object_relationships: Optional[List[EventObjectRelationship]] = None,
            event_event_relationships: Optional[List[EventEventRelationship]] = None
    ) -> None:
        """Initialize the COREMetamodel with strongly typed data structures."""
        self.ocel = pm4py.OCEL()

        self.objects = objects or []
        self.iot_events = iot_events or []
        self.process_events = process_events or []
        self.observations = observations or []
        self.object_object_relationships = object_object_relationships or []
        self.event_object_relationships = event_object_relationships or []
        self.event_event_relationships = event_event_relationships or []

        self._process_data()

    def _process_data(self) -> None:
        """Process the data by adding objects, events, and relationships to the OCEL."""

        logger.info("Processing CORE metamodel data...")

        self._add_event_event_relationships(self.event_event_relationships)
        logger.info("Event-event relationships added.")

        self._add_objects(self.objects)
        logger.info("Objects added.")

        self._add_events(self.observations + self.iot_events + self.process_events)
        logger.info("Events added.")

        self._add_object_relationships(self.object_object_relationships)
        logger.info("Object relationships added.")

        self._process_relationships()
        logger.info("Relationships processed.")

    def _process_relationships(self) -> None:
        """Process and create the relationships DataFrame with optimized performance."""
        logger.info("Processing relationships...")

        if not self.event_object_relationships:
            logger.warning("No event-object relationships to process")
            return

        # Pre-compute object type mappings
        object_type_dict = {}
        if not self.ocel.objects.empty:
            object_type_dict = (
                self.ocel.objects[["ocel:oid", "ocel:type"]]
                .set_index("ocel:oid")["ocel:type"]
                .to_dict()
            )

        # Pre-compute event activity mappings
        event_activity_dict = {}
        if not self.ocel.events.empty:
            event_activity_dict = (
                self.ocel.events[["ocel:eid", "ocel:activity"]]
                .set_index("ocel:eid")["ocel:activity"]
                .to_dict()
            )

        # Extract relationship data once
        event_ids = [x.event_id for x in self.event_object_relationships]
        object_ids = [x.object_id for x in self.event_object_relationships]

        # Use dictionary lookups instead of DataFrame operations
        resolved_o_types = [
            object_type_dict.get(obj_id, "undefined")
            for obj_id in object_ids
        ]

        resolved_activities = [
            event_activity_dict.get(event_id, "undefined")
            for event_id in event_ids
        ]

        # Create relationships dictionary in one go
        relationships = {
            self.ocel.event_id_column: event_ids,
            self.ocel.object_id_column: object_ids,
            self.ocel.object_type_column: resolved_o_types,
            self.ocel.event_activity: resolved_activities,
            "ocel:qualifier": [rel.qualifier for rel in self.event_object_relationships]
        }

        # Create DataFrame in a single operation
        self.ocel.relations = pd.DataFrame(relationships)

    def _add_objects(self, objects: List[Object]) -> None:
        """Add objects to the OCEL."""
        if not objects:
            return

        new_rows = []
        for obj in objects:
            new_row = {
                self.ocel.object_id_column: obj.object_id,
                self.ocel.object_type_column: obj.object_type,
                "ocel:object_class": str(obj.object_class)
            }

            for key, value in obj.attributes.items():
                new_row[ATTRIBUTE_KEY_PREFIX + key] = value

            if not any(x[self.ocel.object_id_column] == obj.object_id for x in new_rows):
                new_rows.append(new_row)

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            self.ocel.objects = pd.concat([self.ocel.objects, new_df], ignore_index=True)

    def _add_events(self, events: List[Event]) -> None:
        """Add events to the OCEL."""
        if not events:
            return

        new_rows = []
        for event in events:
            event_sub_type_label: str = _get_event_sub_type_label(event, event.event_class)

            new_row = {
                self.ocel.event_id_column: event.event_id,
                self.ocel.event_activity: event_sub_type_label,
                self.ocel.event_timestamp: event.timestamp,
                "ocel:event_type": event_sub_type_label,
                "ocel:event_class": event.event_class
            }

            for key, value in event.attributes.items():
                new_row[ATTRIBUTE_KEY_PREFIX + key] = value

            new_rows.append(new_row)

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            self.ocel.events = pd.concat([self.ocel.events, new_df], ignore_index=True)

    def _add_object_relationships(self, relationships: List[ObjectObjectRelationship]) -> None:
        """Add object-object relationships to the OCEL."""
        if not relationships:
            return

        new_rows = [{
            self.ocel.object_id_column: rel.object_id,
            self.ocel.object_id_column + "_2": rel.related_object_id,
            self.ocel.qualifier: rel.qualifier
        } for rel in relationships]

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            self.ocel.o2o = pd.concat([self.ocel.o2o, new_df], ignore_index=True)

    def _add_event_object_relationships(self, relationships: List[EventObjectRelationship]) -> None:
        """Add event-object relationships to the OCEL."""
        if not relationships:
            return

        new_rows = []
        for rel in relationships:
            obj = next((x for x in self.objects if x.object_id == rel.object_id), None)
            if not obj:
                continue

            event = next(
                (x for x in self.iot_events + self.process_events + self.observations if x.event_id == rel.event_id),
                None
            )
            if not event:
                continue

            new_row = {
                self.ocel.event_id_column: rel.event_id,
                self.ocel.event_activity: getattr(event, 'activity', event.event_type),
                self.ocel.event_timestamp: event.timestamp,
                self.ocel.object_type_column: obj.object_type,
                self.ocel.object_id_column: obj.object_id,
                self.ocel.qualifier: rel.qualifier
            }
            new_rows.append(new_row)

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            self.ocel.relations = pd.concat([self.ocel.relations, new_df], ignore_index=True)

    def _add_event_event_relationships(self, relationships: List[EventEventRelationship]) -> None:
        """Add event-event relationships to the OCEL by creating an e2o relationship with a linking object."""

        for relationship in relationships:
            # Create a linking object
            linking_object = Object(
                object_id=f"e2o_{relationship.event_id}_{relationship.derived_from_event_id}",
                object_type="link",
                object_class=ObjectClassEnum.LINK,
                attributes={"relationship_type": relationship.qualifier}
            )

            self.objects.append(linking_object)

            # Create the relationships
            self.event_object_relationships.append(
                EventObjectRelationship(
                    object_id=linking_object.object_id,
                    event_id=relationship.event_id,
                    qualifier="derived_from"
                )
            )
            self.event_object_relationships.append(
                EventObjectRelationship(
                    object_id=linking_object.object_id,
                    event_id=relationship.derived_from_event_id,
                    qualifier="derived_to"
                )
            )

    def get_ocel(self) -> pm4py.OCEL:
        """Return the OCEL object."""
        return self.ocel

    def get_extended_table(self) -> pd.DataFrame:
        """Transform the current OCEL data structure into a Pandas DataFrame."""
        return self.ocel.get_extended_table()

    def save_ocel(self, path: str) -> None:
        """Save the OCEL object to a file."""
        try:
            # Create a simplified OCEL export
            ocel_data = {
                "events": self.ocel.events.to_dict('records'),
                "objects": self.ocel.objects.to_dict('records'),
                "relations": self.ocel.relations.to_dict('records'),
                "o2o": self.ocel.o2o.to_dict('records') if not self.ocel.o2o.empty else []
            }

            # Save as JSON for now
            import json
            with open(path, 'w') as f:
                json.dump(ocel_data, f, indent=2, default=str)

            logger.info(f"OCEL saved to {path}")

        except Exception as e:
            logger.error(f"Error saving OCEL: {str(e)}")
            raise

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the CORE model."""
        return {
            "objects_count": len(self.objects),
            "iot_events_count": len(self.iot_events),
            "process_events_count": len(self.process_events),
            "observations_count": len(self.observations),
            "event_object_relationships_count": len(self.event_object_relationships),
            "event_event_relationships_count": len(self.event_event_relationships),
            "object_object_relationships_count": len(self.object_object_relationships),
            "total_events": len(self.iot_events) + len(self.process_events) + len(self.observations),
            "extended_table_rows": len(self.get_extended_table())
        }