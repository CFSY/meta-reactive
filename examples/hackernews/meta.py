import asyncio
from datetime import datetime
from typing import List

from pydantic import BaseModel

from src.reactive_framework.external.postgres import PostgresAdapter, PostgresConfig
from src.reactive_framework.meta.decorators import collection, resource, computed, reactive
from src.reactive_framework.meta.service import MetaService


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

# Collections
@collection(name="posts")
class PostsCollection:
    def __init__(self, name: str, compute_graph):
        self.name = name
        self._posts: dict[int, Post] = {}
        self._votes: dict[int, List[Vote]] = {}
        self._users: dict[int, User] = {}

    @computed
    def post_scores(self) -> dict[int, int]:
        scores = {}
        for post_id, votes in self._votes.items():
            scores[post_id] = len(votes)
        return scores

    @reactive
    def update_post(self, post: Post) -> None:
        # The framework automatically detects that this method
        # modifies self._posts and invalidates dependent computations
        self._posts[post.id] = post

    @computed
    def enriched_posts(self) -> dict[int, dict]:
        result = {}
        for post_id, post in self._posts.items():
            author = self._users.get(post.author_id)
            result[post_id] = {
                "id": post.id,
                "title": post.title,
                "url": post.url,
                "score": self.post_scores.get(post.id, 0),
                "author": author.username if author else "unknown",
                "created_at": post.created_at.isoformat()
            }
        return result

# Resource
@resource(name="top_posts")
class TopPostsResource:
    def __init__(self, limit: int = 10, min_score: int = 0):
        self.limit = limit
        self.min_score = min_score
        self._posts_collection = None

    def _setup_collection(self, compute_graph):
        self._posts_collection = compute_graph.get_collection("posts")
        return self._posts_collection

    @computed
    def filtered_posts(self) -> List[dict]:
        posts = list(self._posts_collection.enriched_posts.values())
        posts.sort(key=lambda p: p["score"], reverse=True)
        return [
            post for post in posts
            if post["score"] >= self.min_score
        ][:self.limit]

async def main():
    # Initialize PostgreSQL adapter
    pg_adapter = PostgresAdapter(PostgresConfig(
        host="localhost",
        database="hackernews",
        user="postgres",
        password="password"
    ))

    # Initialize service
    service = MetaService("hackernews", host="localhost", port=8080)

    # Register collections and resources
    service.register_collection(PostsCollection)
    service.register_resource(TopPostsResource)

    # Create database collections
    posts_db = pg_adapter.create_collection(
        "posts_db",
        "posts",
        "id",
        Post
    )

    users_db = pg_adapter.create_collection(
        "users_db",
        "users",
        "id",
        User
    )

    votes_db = pg_adapter.create_collection(
        "votes_db",
        "votes",
        "id",
        Vote
    )

    # The framework automatically detects dependencies and sets up
    # the computation graph based on the code analysis

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
