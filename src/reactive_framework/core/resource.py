import asyncio
import logging
import uuid
import weakref
from typing import Dict, Optional, Any

from .compute_graph import ComputedCollection
from .types import ResourceInstance, SSEMessage, Change

logger = logging.getLogger(__name__)


class ResourceManager:
    def __init__(self):
        self._instances: Dict[str, ResourceInstance] = {}
        self._collections: Dict[str, ComputedCollection] = {}
        self._subscribers: Dict[str, set[weakref.ref[asyncio.Queue]]] = {}
        # Map to track instances by resource_name and params
        self._instances_by_params: Dict[str, Dict[str, str]] = {}

    def _get_param_hash(self, resource_name: str, params: Dict[str, Any]) -> str:
        """Create a unique identifier based on resource name and parameters"""
        # Sort params to ensure consistent order
        sorted_params = dict(sorted(params.items()))
        # Convert to string representation for hashing
        param_str = str(sorted_params)
        return f"{resource_name}:{param_str}"

    async def find_existing_instance(
        self, resource_name: str, params: Dict[str, Any]
    ) -> Optional[str]:
        """Find an existing instance with matching resource name and parameters"""
        param_hash = self._get_param_hash(resource_name, params)

        # Check if we have an instance with these params
        if resource_name in self._instances_by_params:
            instance_id = self._instances_by_params[resource_name].get(param_hash)
            if instance_id and instance_id in self._instances:
                return instance_id

        return None

    async def create_instance(
        self, resource_name: str, params: Dict[str, Any], collection: ComputedCollection
    ) -> str:
        # First check if an instance with these params already exists
        existing_id = await self.find_existing_instance(resource_name, params)
        if existing_id:
            logger.info(
                f"Returning existing instance {existing_id} for {resource_name}"
            )
            return existing_id

        # Create new instance
        instance_id = str(uuid.uuid4())
        self._instances[instance_id] = ResourceInstance(
            id=instance_id, resource_name=resource_name, params=params
        )
        self._collections[instance_id] = collection
        self._subscribers[instance_id] = set()

        # Store the instance by its parameters
        param_hash = self._get_param_hash(resource_name, params)
        if resource_name not in self._instances_by_params:
            self._instances_by_params[resource_name] = {}
        self._instances_by_params[resource_name][param_hash] = instance_id

        return instance_id

    async def destroy_instance(self, instance_id: str) -> None:
        if instance_id in self._instances:
            # Get the resource name and params to remove from param index
            instance = self._instances[instance_id]
            resource_name = instance.resource_name
            param_hash = self._get_param_hash(resource_name, instance.params)

            # Remove from instances
            del self._instances[instance_id]

            # Remove from param index
            if (
                resource_name in self._instances_by_params
                and param_hash in self._instances_by_params[resource_name]
            ):
                del self._instances_by_params[resource_name][param_hash]
                if not self._instances_by_params[resource_name]:
                    del self._instances_by_params[resource_name]

            # Clean up collections and subscribers
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

        collection = self._collections[instance_id]

        # Add subscriber
        self._subscribers[instance_id].add(
            weakref.ref(queue, lambda ref: self._remove_subscriber(instance_id, ref))
        )

        # Send initial data
        message = SSEMessage(event="init", data=list(collection.iter_items()))
        await queue.put(message)

        # Set up change callback
        def on_change(change: Change) -> None:
            msg = SSEMessage(
                event="update",
                data=[[change.key, [change.new_value] if change.new_value else []]],
            )
            asyncio.create_task(self._notify_subscribers(instance_id, msg))

        collection.add_change_callback(instance_id, on_change)

    def _remove_subscriber(self, instance_id: str, ref: weakref.ref) -> None:
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
                    print("PUT SSE MESSAGE:", message, "queue length: ", queue.qsize())
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
        return instance

    def get_collection(self, instance_id: str) -> Optional[ComputedCollection]:
        return self._collections.get(instance_id)
