import abc
import json
from enum import Enum
from typing import Any, Dict, Literal, Type, TypeVar

from pydantic import BaseModel, ConfigDict, field_serializer, field_validator

T = TypeVar("T", bound="ObjectClassBase")


class ObjectClassBase(BaseModel, abc.ABC):
    """
    Base class for object classes, providing a string representation and category.

    :param parma: Unused.
    :return : of objects.
    :return: The abstract base for concrete object classes.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    str_repr: str
    category: Literal["data_source", "business_object", "general_object"]

    def __str__(self) -> str:
        """
        Return the compact string representation.

        :param parma: Unused.
        :return : of objects.
        :return: A string in the form "<category>:<str_repr>".
        """
        return f"{self.category}:{self.str_repr}"

    def __repr__(self) -> str:
        """
        Return the debug representation.

        :param parma: Unused.
        :return : of objects.
        :return: The same string as __str__().
        """
        return self.__str__()

    def to_dict(self) -> Dict[str, str]:
        """
        Convert the object class to a dictionary.

        :param parma: Unused.
        :return : of objects.
        :return: A mapping with keys 'category' and 'str_repr'.
        """
        return {"category": self.category, "str_repr": self.str_repr}

    def to_json(self) -> str:
        """
        Convert the object class to a JSON string.

        :param parma: Unused.
        :return : of objects.
        :return: A JSON string representing the object class.
        """
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """
        Create an instance from a dictionary.

        :param parma: The input mapping containing 'category' and 'str_repr'.
        :return : of objects.
        :return: A concrete ObjectClassBase instance inferred from 'category'.
        """
        category: str = str(data["category"])
        str_repr: str = str(data["str_repr"])
        if category == "data_source":
            return DataSource(str_repr=str_repr)  # type: ignore[return-value]
        if category == "business_object":
            return BusinessObject(str_repr=str_repr)  # type: ignore[return-value]
        if category == "general_object":
            return GeneralObject(str_repr=str_repr)  # type: ignore[return-value]
        raise ValueError(f"Unknown category '{category}'")

    @classmethod
    def from_json(cls: Type[T], data: str) -> T:
        """
        Parse an instance from a JSON string.

        :param parma: The JSON string to parse.
        :return : of objects.
        :return: A concrete ObjectClassBase instance.
        """
        payload: Dict[str, Any] = json.loads(data)
        return cls.from_dict(payload)

    @classmethod
    def from_string(cls: Type[T], value: str) -> T:
        """
        Parse a compact string like 'data_source:sensor' into an instance.

        :param parma: The compact string value to parse.
        :return : of objects.
        :return: A concrete ObjectClassBase instance parsed from the string.
        """
        if ":" not in value:
            raise ValueError("Expected format '<category>:<str_repr>'")
        category, str_repr = value.split(":", 1)
        return cls.from_dict({"category": category, "str_repr": str_repr})


class DataSource(ObjectClassBase):
    """
    Represents a data source object class, such as sensors or information systems.

    :param parma: The string representation for the data source.
    :return : of objects.
    :return: A DataSource instance with category 'data_source'.
    """

    def __init__(self, str_repr: str) -> None:
        """
        Initialize a DataSource.

        :param parma: The string representation (e.g., 'sensor').
        :return : of objects.
        :return: None.
        """
        super().__init__(str_repr=str_repr, category="data_source")


class BusinessObject(ObjectClassBase):
    """
    Represents a business object class, such as case objects or machines.

    :param parma: The string representation for the business object.
    :return : of objects.
    :return: A BusinessObject instance with category 'business_object'.
    """

    def __init__(self, str_repr: str) -> None:
        """
        Initialize a BusinessObject.

        :param parma: The string representation (e.g., 'machine').
        :return : of objects.
        :return: None.
        """
        super().__init__(str_repr=str_repr, category="business_object")


class GeneralObject(ObjectClassBase):
    """
    Represents a general object class.

    :param parma: The string representation for the general object.
    :return : of objects.
    :return: A GeneralObject instance with category 'general_object'.
    """

    def __init__(self, str_repr: str) -> None:
        """
        Initialize a GeneralObject.

        :param parma: The string representation (e.g., 'activity').
        :return : of objects.
        :return: None.
        """
        super().__init__(str_repr=str_repr, category="general_object")


class ObjectClassEnum(str, Enum):
    """
    Enumeration of object class types. The string form includes the category.

    :param parma: Unused.
    :return : of objects.
    :return: Enum members representing object types.
    """

    SENSOR = "sensor"
    ACTUATOR = "actuator"
    INFORMATION_SYSTEM = "information_system"
    LINK = "link"
    CASE_OBJECT = "case_object"
    MACHINE = "machine"
    BUSINESS_OBJECT = "business_object"
    PROCESS = "process"
    ACTIVITY = "activity"
    SUBPROCESS = "subprocess"
    RESOURCE = "resource"

    def get_category(self) -> str:
        """
        Return the category for this enum value.

        :param parma: Unused.
        :return : of objects.
        :return: One of 'data_source', 'business_object', or 'general_object'.
        """
        if self in {
            self.SENSOR,
            self.ACTUATOR,
            self.INFORMATION_SYSTEM,
            self.LINK,
        }:
            return "data_source"
        if self in {
            self.CASE_OBJECT,
            self.MACHINE,
            self.BUSINESS_OBJECT,
            self.PROCESS,
        }:
            return "business_object"
        return "general_object"

    def __str__(self) -> str:
        """
        Return the compact string with category prefix.

        :param parma: Unused.
        :return : of objects.
        :return: A string in the form '<category>:<value>'.
        """
        return f"{self.get_category()}:{self.value}"


class Object(BaseModel):
    """
    Serializable object entity linking an id, type, class, and attributes.

    :param parma: Unused.
    :return : of objects.
    :return: A Pydantic model representing an Object.
    """

    model_config = ConfigDict(extra="forbid")

    object_id: str
    object_type: str
    object_class: ObjectClassEnum
    attributes: Dict[str, Any]

    @field_serializer("object_class")
    def serialize_object_class(self, v: ObjectClassEnum) -> str:
        """
        Serialize the object_class with category prefix.

        :param parma: The enum value to serialize.
        :return : of objects.
        :return: A string in the form '<category>:<value>'.
        """
        return str(v)

    @field_validator("object_class", mode="before")
    @classmethod
    def parse_object_class(cls, v: Any) -> ObjectClassEnum:
        """
        Parse object_class from either plain value or '<category>:<value>'.

        :param parma: The incoming value for the field.
        :return : of objects.
        :return: A valid ObjectClassEnum instance.
        """
        if isinstance(v, ObjectClassEnum):
            return v
        if isinstance(v, str):
            value: str = v.split(":", 1)[-1]
            return ObjectClassEnum(value)
        raise TypeError("object_class must be ObjectClassEnum or string")

    def to_dict(self) -> Dict[str, Any]:
        """
        Dump the object to a dictionary suitable for JSON serialization.

        :param parma: Unused.
        :return : of objects.
        :return: A JSON-serializable dictionary.
        """
        return self.model_dump(mode="json")

    def to_json(self) -> str:
        """
        Dump the object directly to a JSON string.

        :param parma: Unused.
        :return : of objects.
        :return: A JSON string representing the object.
        """
        return self.model_dump_json()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Object":
        """
        Create an Object instance from a dictionary.

        :param parma: The dictionary to parse.
        :return : of objects.
        :return: A validated Object instance.
        """
        return cls.model_validate(data)

    @classmethod
    def from_json(cls, data: str) -> "Object":
        """
        Create an Object instance from a JSON string.

        :param parma: The JSON string to parse.
        :return : of objects.
        :return: A validated Object instance.
        """
        return cls.model_validate_json(data)
