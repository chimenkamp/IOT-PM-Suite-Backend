from pydantic import BaseModel


class ObjectObjectRelationship(BaseModel):
    """
    Represents a relationship between two objects.
    """
    object_id: str
    related_object_id: str
    qualifier: str = "associated_with"


class EventObjectRelationship(BaseModel):
    """
    Represents a relationship between an event and an object.
    """
    event_id: str
    object_id: str
    qualifier: str = "related"


class EventEventRelationship(BaseModel):
    """
    Represents a relationship between two events.
    """
    event_id: str
    derived_from_event_id: str
    qualifier: str = "derived_from"