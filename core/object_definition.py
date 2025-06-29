# core/object_definition.py - Object Data Models

import abc
from enum import Enum
from typing import Dict, Any

from pydantic import BaseModel, ConfigDict


class ObjectClassBase(abc.ABC):
    def __init__(self, str_repr: str, category: str):
        self.str_repr: str = str_repr
        self.category: str = category

    def __str__(self):
        return f"{self.category}:{self.str_repr}"

    def __repr__(self):
        return self.__str__()


class DataSource(ObjectClassBase):
    def __init__(self, str_repr: str):
        super().__init__(str_repr, "data_source")


class BusinessObject(ObjectClassBase):
    def __init__(self, str_repr: str):
        super().__init__(str_repr, "business_object")


class GeneralObject(ObjectClassBase):
    def __init__(self, str_repr: str):
        super().__init__(str_repr, "general_object")


class ObjectClassEnum(str, Enum):
    # DataSource types
    SENSOR = "sensor"
    ACTUATOR = "actuator"
    INFORMATION_SYSTEM = "information_system"
    LINK = "link"

    # BusinessObject types
    CASE_OBJECT = "case_object"
    MACHINE = "machine"
    BUSINESS_OBJECT = "business_object"
    PROCESS = "process"

    # GeneralObject types
    ACTIVITY = "activity"
    SUBPROCESS = "subprocess"
    RESOURCE = "resource"

    def get_category(self) -> str:
        if self in {self.SENSOR, self.ACTUATOR, self.INFORMATION_SYSTEM, self.LINK}:
            return "data_source"
        elif self in {self.CASE_OBJECT, self.MACHINE, self.BUSINESS_OBJECT, self.PROCESS}:
            return "business_object"
        else:
            return "general_object"

    def __str__(self):
        return f"{self.get_category()}:{self.value}"


class Object(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    object_id: str
    object_type: str
    object_class: ObjectClassEnum
    attributes: Dict[str, Any]