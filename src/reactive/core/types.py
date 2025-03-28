import json
from datetime import datetime
from typing import TypeVar, Generic, Dict, List, Any, Callable, Optional, Union

from pydantic import BaseModel

K = TypeVar("K")
V = TypeVar("V")


class Json(BaseModel):
    """Base class for JSON-serializable types"""

    def to_json(self) -> str:
        return json.dumps(self.model_dump())

    @classmethod
    def from_json(cls, data: str) -> "Json":
        return cls.model_validate(json.loads(data))


class Change(BaseModel, Generic[K, V]):
    """Represents a change in a collection"""

    key: K
    old_value: Optional[V]
    new_value: Optional[V]
    timestamp: datetime = datetime.now()


class DependencyNode(BaseModel):
    """Represents a node in the dependency graph"""

    id: str
    dependencies: List[str] = []
    dependents: List[str] = []
    invalidated: bool = False
    last_computed: Optional[datetime] = None
    # set of instance_id to change callback
    change_callbacks: Dict[str, Callable[[Change], None]] = {}


class ResourceInstance(BaseModel):
    """Instance of a resource"""

    id: str
    resource_name: str
    params: Dict[str, Any]


class SSEMessage(BaseModel):
    """Server-sent event message"""

    event: str
    data: Any
    id: Optional[str] = None
    retry: Optional[int] = None

    def format(self) -> str:
        lines = []
        if self.id is not None:
            lines.append(f"id: {self.id}")
        if self.event:  # Only add event if not empty
            lines.append(f"event: {self.event}")

        # Handle multiline data
        if isinstance(self.data, (dict, list)):
            data_str = json.dumps(self.data)
        else:
            data_str = str(self.data)

        # Split data into multiple 'data:' lines if it contains newlines
        for data_line in data_str.split("\n"):
            lines.append(f"data: {data_line}")

        if self.retry is not None:
            lines.append(f"retry: {self.retry}")
        return "\n".join(lines) + "\n\n"


CollectionKey = TypeVar("CollectionKey")
CollectionValue = TypeVar("CollectionValue")
MapperFunc = Callable[[CollectionKey, CollectionValue], Union[CollectionValue, None]]
