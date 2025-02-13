import logging
import threading
from datetime import datetime
from typing import Dict, Any, Optional, Type, List

from pydantic import BaseModel
from sqlalchemy import create_engine, MetaData, Table, select, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from ..core.collection import Collection
from ..core.types import K, V, Change

logger = logging.getLogger(__name__)


class PostgresConfig(BaseModel):
    host: str
    port: int = 5432
    database: str
    user: str
    password: str
    schema: str = "public"
    poll_interval: float = 1.0  # seconds


class PostgresCollection(Collection[K, V]):
    def __init__(
        self,
        name: str,
        table: Table,
        engine: Engine,
        key_column: str,
        value_type: Type[V],
        poll_interval: float = 1.0,
    ):
        super().__init__(name)
        self.table = table
        self.engine = engine
        self.key_column = key_column
        self.value_type = value_type
        self.poll_interval = poll_interval
        self._last_poll: Optional[datetime] = None
        self._stop_event = threading.Event()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()

    def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._poll()
            except Exception as e:
                logger.error(f"Error polling PostgreSQL: {e}")
            self._stop_event.wait(self.poll_interval)

    def _poll(self) -> None:
        with Session(self.engine) as session:
            query = select(self.table)
            if self._last_poll:
                # Add condition for modified timestamp if available
                if hasattr(self.table.c, "updated_at"):
                    query = query.where(self.table.c.updated_at > self._last_poll)

            results = session.execute(query).fetchall()

            changes: List[Change[K, V]] = []
            for row in results:
                key = getattr(row, self.key_column)
                # Convert row to dict, excluding SQLAlchemy specific attributes
                value_dict = {
                    k: v
                    for k, v in row._mapping.items()
                    if not k.startswith("_") and k != self.key_column
                }

                # Convert to Pydantic model if needed
                if issubclass(self.value_type, BaseModel):
                    value = self.value_type.model_validate(value_dict)
                else:
                    value = self.value_type(**value_dict)  # type: ignore

                old_value = self.get(key)
                if old_value != value:
                    changes.append(
                        Change(key=key, old_value=old_value, new_value=value)
                    )
                    self._data[key] = value

            self._last_poll = datetime.now()

            # Notify observers of changes
            for change in changes:
                self._notify_observers(change)

    def stop(self) -> None:
        self._stop_event.set()
        self._poll_thread.join()


class PostgresAdapter:
    def __init__(self, config: PostgresConfig):
        self.config = config
        self.engine = create_engine(
            f"postgresql://{config.user}:{config.password}@"
            f"{config.host}:{config.port}/{config.database}"
        )
        self.metadata = MetaData(schema=config.schema)
        self.collections: Dict[str, PostgresCollection] = {}

        # Enable PostgreSQL NOTIFY/LISTEN
        event.listen(self.engine, "after_cursor_execute", self._after_cursor_execute)

    def _after_cursor_execute(
        self,
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        # Detect changes and notify relevant collections
        if statement.lower().startswith(("insert", "update", "delete")):
            table_name = statement.split()[2]  # Rough extraction of table name
            if table_name in self.collections:
                conn.execute(f"NOTIFY {table_name}_changes")

    def create_collection(
        self, name: str, table_name: str, key_column: str, value_type: Type[V]
    ) -> PostgresCollection[Any, V]:
        # Reflect table from database
        table = Table(table_name, self.metadata, autoload_with=self.engine)

        collection = PostgresCollection(
            name=name,
            table=table,
            engine=self.engine,
            key_column=key_column,
            value_type=value_type,
            poll_interval=self.config.poll_interval,
        )

        self.collections[table_name] = collection
        return collection

    async def stop(self) -> None:
        for collection in self.collections.values():
            collection.stop()
