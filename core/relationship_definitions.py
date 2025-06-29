# core/relationship_definitions.py - Relationship Models

from pydantic import BaseModel


class ObjectObjectRelationship(BaseModel):
    object_id: str
    related_object_id: str
    qualifier: str = "associated_with"


class EventObjectRelationship(BaseModel):
    event_id: str
    object_id: str
    qualifier: str = "related"


class EventEventRelationship(BaseModel):
    event_id: str
    derived_from_event_id: str
    qualifier: str = "derived_from"