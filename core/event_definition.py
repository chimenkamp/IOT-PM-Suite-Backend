# core/event_definition.py - Event Data Models

import datetime
from typing import Dict, Any, Literal, Optional

from pydantic import BaseModel, ConfigDict


class Event(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    event_id: str
    event_class: Literal["iot_event", "process_event", "observation"]
    event_type: str
    timestamp: datetime.datetime
    attributes: Dict[str, Any]

    def __str__(self):
        return f"{self.event_class}:{self.event_id}"


class IotEvent(Event):
    event_class: Literal["iot_event"] = "iot_event"


class ProcessEvent(Event):
    event_class: Literal["process_event"] = "process_event"
    activity: str


class Observation(Event):
    event_class: Literal["observation"] = "observation"