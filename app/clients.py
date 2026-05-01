import os

import redis
from pymongo import MongoClient

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "migration_artifacts")

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

mongo_client = MongoClient(MONGODB_URL)
mongo_db = mongo_client[MONGODB_DATABASE]