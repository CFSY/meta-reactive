import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

from flask import Flask, Response, request

from .resource import Resource
from ..core.collection import Collection
from ..core.compute_graph import ComputeGraph
from ..core.resource import ResourceManager

logger = logging.getLogger(__name__)


class Service:
    def __init__(self, name: str, host: str = "localhost", port: int = 8080):
        self.name = name
        self.host = host
        self.port = port
        self.compute_graph = ComputeGraph()
        self.resource_manager = ResourceManager()
        self.resources: Dict[str, Resource] = {}
        self.collections: Dict[str, Collection] = {}
        self.app = Flask(name)
        self.executor = ThreadPoolExecutor()
        self._setup_routes()

    def _setup_routes(self) -> None:
        @self.app.route("/v1/streams/<resource_name>", methods=["POST"])
        def create_stream(resource_name: str) -> tuple[Dict[str, str], int]:
            if resource_name not in self.resources:
                return {"error": "Resource not found"}, 404

            params = request.get_json()
            resource = self.resources[resource_name]

            try:
                collection = resource.instantiate(params)
                instance_id = asyncio.run(
                    self.resource_manager.create_instance(
                        resource_name, params, collection
                    )
                )
                return {"instance_id": instance_id}, 200
            except Exception as e:
                logger.error(f"Error creating stream: {e}")
                return {"error": str(e)}, 400

        @self.app.route("/v1/streams/<instance_id>", methods=["GET"])
        def get_stream(instance_id: str) -> Response:
            instance = self.resource_manager.get_instance(instance_id)
            if not instance:
                return Response({"error": "Stream not found"}, status=404)

            def generate() -> str:
                queue: asyncio.Queue = asyncio.Queue()
                asyncio.run(self.resource_manager.subscribe(instance_id, queue))

                while True:
                    try:
                        message = asyncio.run(queue.get())
                        yield message.format()
                    except Exception as e:
                        logger.error(f"Error in stream: {e}")
                        break

            return Response(
                generate(),
                mimetype="text/event-stream",
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
            )

        @self.app.route("/v1/streams/<instance_id>", methods=["DELETE"])
        def delete_stream(instance_id: str) -> tuple[Dict[str, str], int]:
            try:
                asyncio.run(self.resource_manager.destroy_instance(instance_id))
                return {"status": "success"}, 200
            except Exception as e:
                return {"error": str(e)}, 400

    def add_resource(self, name: str, resource: Resource) -> None:
        self.resources[name] = resource

    def add_collection(self, name: str, collection: Collection) -> None:
        self.collections[name] = collection
        self.compute_graph.add_node(collection)

    async def start(self) -> None:
        await self.resource_manager.start()
        self.executor.submit(self.app.run, host=self.host, port=self.port, debug=False)

    async def stop(self) -> None:
        await self.resource_manager.stop()
        self.executor.shutdown(wait=True)
