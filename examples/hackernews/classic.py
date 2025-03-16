import asyncio
from datetime import datetime
from typing import List

from pydantic import BaseModel

from src.reactive_framework.classic.mapper import (
    OneToOneMapper,
    ManyToOneMapper,
    create_mapped_collection,
)
from src.reactive_framework.classic.resource import Resource, ResourceParams
from src.reactive_framework.classic.service import Service
from src.reactive_framework.external.postgres import PostgresAdapter, PostgresConfig


# Data Models
class Post(BaseModel):
    id: int
    title: str
    url: str
    author_id: int
    created_at: datetime
    score: int = 0


class User(BaseModel):
    id: int
    username: str
    karma: int = 0


class Vote(BaseModel):
    id: int
    post_id: int
    user_id: int
    created_at: datetime


# Mappers
class PostScoreMapper(ManyToOneMapper[int, Vote, int]):
    def map_values(self, votes: List[Vote]) -> int:
        return len(votes)


class EnrichedPostMapper(OneToOneMapper[int, Post, dict]):
    def __init__(self, users_collection):
        self.users = users_collection

    def map_value(self, post: Post) -> dict:
        author = self.users.get(post.author_id)
        return {
            "id": post.id,
            "title": post.title,
            "url": post.url,
            "score": post.score,
            "author": author.username if author else "unknown",
            "created_at": post.created_at.isoformat(),
        }


# Resource Parameters
class TopPostsParams(ResourceParams):
    limit: int = 10
    min_score: int = 0


# Resource Implementation
class TopPostsResource(Resource[int, dict]):
    def __init__(
        self,
        name: str,
        posts_collection,
        votes_collection,
        users_collection,
        compute_graph,
    ):
        super().__init__(name, TopPostsParams, compute_graph)
        self.posts = posts_collection
        self.votes = votes_collection
        self.users = users_collection

    def setup_resource_collection(self, params: TopPostsParams):
        # Create intermediate collections for vote counting
        post_scores = create_mapped_collection(
            self.votes, PostScoreMapper(), self.compute_graph, f"{self.name}_scores"
        )

        # Update post scores
        def update_scores():
            for post_id, score in post_scores.iter_items():
                post = self.posts.get(post_id)
                if post:
                    post.score = score
                    self.posts.set(post_id, post)

        post_scores.add_observer(lambda changes: update_scores())

        # Create final collection with enriched posts
        enriched_posts = create_mapped_collection(
            self.posts,
            EnrichedPostMapper(self.users),
            self.compute_graph,
            f"{self.name}_enriched",
        )

        return enriched_posts

    def setup_dependencies(self, collection, params: TopPostsParams):
        self.compute_graph.add_dependency(collection, self.posts)
        self.compute_graph.add_dependency(collection, self.votes)
        self.compute_graph.add_dependency(collection, self.users)


async def main():
    # Initialize PostgreSQL adapter
    pg_adapter = PostgresAdapter(
        PostgresConfig(
            host="localhost",
            database="hackernews",
            user="postgres",
            password="password",
        )
    )

    # Create collections from database tables
    posts_collection = pg_adapter.create_collection("posts", "posts", "id", Post)

    users_collection = pg_adapter.create_collection("users", "users", "id", User)

    votes_collection = pg_adapter.create_collection("votes", "votes", "id", Vote)

    # Initialize service
    service = Service("hackernews", host="localhost", port=8080)

    # Add collections
    service.add_collection("posts", posts_collection)
    service.add_collection("users", users_collection)
    service.add_collection("votes", votes_collection)

    # Create and add resource
    top_posts = TopPostsResource(
        "top_posts",
        posts_collection,
        votes_collection,
        users_collection,
        service.compute_graph,
    )
    service.add_resource("top_posts", top_posts)

    # Start service
    await service.start()

    try:
        # Keep service running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
