import asyncio
import logging
import uuid
import weakref
from datetime import datetime, timedelta
from typing import Dict, Optional, Any

from .collection import Collection
from .types import ResourceInstance, SSEMessage

logger = logging.getLogger(__name__)


class ResourceManager:
    def __init__(self):
        self._instances: Dict[str, ResourceInstance] = {}
        self._collections: Dict[str, Collection] = {}
        self._subscribers: Dict[str, set[weakref.ref[asyncio.Queue]]] = {}
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_interval = timedelta(minutes=5)

    async def start(self) -> None:
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self) -> None:
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def _cleanup_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._cleanup_interval.total_seconds())
                await self._cleanup_inactive_instances()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")

    async def _cleanup_inactive_instances(self) -> None:
        now = datetime.now()
        to_remove = []

        for instance_id, instance in self._instances.items():
            if now - instance.last_accessed > self._cleanup_interval:
                to_remove.append(instance_id)

        for instance_id in to_remove:
            await self.destroy_instance(instance_id)

    async def create_instance(
        self, resource_name: str, params: Dict[str, Any], collection: Collection
    ) -> str:
        instance_id = str(uuid.uuid4())
        self._instances[instance_id] = ResourceInstance(
            id=instance_id, resource_name=resource_name, params=params
        )
        self._collections[instance_id] = collection
        self._subscribers[instance_id] = set()
        return instance_id

    async def destroy_instance(self, instance_id: str) -> None:
        if instance_id in self._instances:
            del self._instances[instance_id]
            if instance_id in self._collections:
                del self._collections[instance_id]
            if instance_id in self._subscribers:
                # Notify subscribers of closure
                message = SSEMessage(
                    event="close", data={"reason": "Resource instance destroyed"}
                )
                await self._notify_subscribers(instance_id, message)
                del self._subscribers[instance_id]

    async def subscribe(self, instance_id: str, queue: asyncio.Queue) -> None:
        if instance_id not in self._subscribers:
            raise ValueError(f"Invalid instance ID: {instance_id}")

        self._subscribers[instance_id].add(
            weakref.ref(queue, lambda ref: self._cleanup_subscriber(instance_id, ref))
        )

        # Send initial data
        collection = self._collections[instance_id]
        message = SSEMessage(event="init", data=list(collection.iter_items()))
        await queue.put(message)

    def _cleanup_subscriber(self, instance_id: str, ref: weakref.ref) -> None:
        if instance_id in self._subscribers:
            self._subscribers[instance_id].discard(ref)

    async def _notify_subscribers(self, instance_id: str, message: SSEMessage) -> None:
        if instance_id not in self._subscribers:
            return

        dead_refs = set()
        for ref in self._subscribers[instance_id]:
            queue = ref()
            if queue is not None:
                try:
                    await queue.put(message)
                except Exception as e:
                    logger.error(f"Error notifying subscriber: {e}")
                    dead_refs.add(ref)
            else:
                dead_refs.add(ref)

        # Cleanup dead references
        self._subscribers[instance_id] -= dead_refs

    def get_instance(self, instance_id: str) -> Optional[ResourceInstance]:
        instance = self._instances.get(instance_id)
        if instance:
            instance.last_accessed = datetime.now()
        return instance

    def get_collection(self, instance_id: str) -> Optional[Collection]:
        return self._collections.get(instance_id)
