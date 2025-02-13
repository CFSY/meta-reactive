import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Type

from flask import Flask, Response, request

from ..core.collection import Collection
from ..core.compute_graph import ComputeGraph
from ..core.resource import ResourceManager

logger = logging.getLogger(__name__)


class MetaService:
    def __init__(self, name: str, host: str = "localhost", port: int = 8080):
        self.name = name
        self.host = host
        self.port = port
        self.compute_graph = ComputeGraph()
        self.resource_manager = ResourceManager()
        self.collections: Dict[str, Collection] = {}
        self.resources: Dict[str, Any] = {}
        self.app = Flask(name)
        self.executor = ThreadPoolExecutor()
        self._setup_routes()

    def register_collection(self, collection_cls: Type) -> None:
        # Create collection instance
        collection = collection_cls(collection_cls._collection_name, self.compute_graph)

        # Register collection
        self.collections[collection_cls._collection_name] = collection

        # Set up dependencies based on analysis
        for method_name, deps in collection_cls._dependencies.items():
            for dep_name in deps:
                if dep_name in self.collections:
                    self.compute_graph.add_dependency(
                        collection, self.collections[dep_name]
                    )

    def register_resource(self, resource_wrapper: Type) -> None:
        # Create resource instance
        resource = resource_wrapper(self.compute_graph)

        # Register resource
        self.resources[resource._resource_name] = resource

    def _setup_routes(self) -> None:
        @self.app.route("/v1/streams/<resource_name>", methods=["POST"])
        async def create_stream(resource_name: str) -> tuple[Dict[str, str], int]:
            if resource_name not in self.resources:
                return {"error": "Resource not found"}, 404

            params = request.get_json()
            resource = self.resources[resource_name]

            try:
                collection = await resource(**params)
                instance_id = await self.resource_manager.create_instance(
                    resource_name, params, collection
                )
                return {"instance_id": instance_id}, 200
            except Exception as e:
                logger.error(f"Error creating stream: {e}")
                return {"error": str(e)}, 400

        @self.app.route("/v1/streams/<instance_id>", methods=["GET"])
        async def get_stream(instance_id: str) -> Response:
            instance = self.resource_manager.get_instance(instance_id)
            if not instance:
                return Response({"error": "Stream not found"}, status=404)

            async def generate() -> str:
                queue: asyncio.Queue = asyncio.Queue()
                await self.resource_manager.subscribe(instance_id, queue)

                while True:
                    try:
                        message = await queue.get()
                        yield message.format()
                    except Exception as e:
                        logger.error(f"Error in stream: {e}")
                        break

            return Response(
                generate(),
                mimetype="text/event-stream",
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
            )

    async def start(self) -> None:
        await self.resource_manager.start()
        self.executor.submit(self.app.run, host=self.host, port=self.port, debug=False)

    async def stop(self) -> None:
        await self.resource_manager.stop()
        self.executor.shutdown(wait=True)
