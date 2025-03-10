from app.models.storage import Storage
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging

logger = logging.getLogger(__name__)

class StorageService():
    def __init__(self):
        try:
            # Get MongoDB URI from environment with fallback
            mongodb_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/annotation')
            
            # Extract database name from URI if present
            if '/' in mongodb_uri:
                base_uri = mongodb_uri.rsplit('/', 1)[0]
                db_name = mongodb_uri.rsplit('/', 1)[1]
            else:
                base_uri = mongodb_uri
                db_name = 'annotation'

            self.client = AsyncIOMotorClient(mongodb_uri)
            self.db = self.client[db_name]
            self.collection = self.db['storage']
            logger.info(f"Successfully connected to MongoDB at {base_uri}")
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB connection: {str(e)}")
            raise

    async def save_async(self, annotation):
        try:
            document = {
                "user_id": annotation["current_user_id"],
                "request": annotation["request"],
                "query": annotation["query"],
                "title": annotation["title"],
                "summary": annotation["summary"],
                "question": annotation["question"],
                "answer": annotation["answer"],
                "node_types": annotation["node_types"],
                "created_at": annotation.get("created_at"),
                "updated_at": annotation.get("updated_at")
            }
            result = await self.collection.insert_one(document)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error in save_async: {str(e)}")
            raise

    def save(self, annotation):
        try:
            data = Storage(
                user_id=annotation["current_user_id"],
                request=annotation["request"],
                query=annotation["query"],
                title=annotation.get("title", ""),
                summary=annotation.get("summary", ""),
                question=annotation.get("question"),
                answer=annotation.get("answer"),
                node_count=annotation.get("node_count"),
                edge_count=annotation.get("edge_count"),
                node_types=annotation["node_types"],
                node_count_by_label=annotation.get("node_count_by_label"),
                edge_count_by_label=annotation.get("edge_count_by_label")
            )
            id = data.save()
            return id
        except Exception as e:
            logger.error(f"Error in save: {str(e)}")
            raise

    def get(self, user_id):
        try:
            data = Storage.find({"user_id": user_id}, one=True)
            return data
        except Exception as e:
            logger.error(f"Error in get: {str(e)}")
            raise
    
    async def get_async(self, user_id):
        try:
            data = await self.collection.find_one({"user_id": user_id})
            return Storage.from_document(data) if data else None
        except Exception as e:
            logger.error(f"Error in get_async: {str(e)}")
            raise
    
    def get_all(self, user_id, page_number):
        try:
            data = Storage.find({"user_id": user_id}).sort('_id', -1).skip((page_number - 1) * 10).limit(10)
            return data
        except Exception as e:
            logger.error(f"Error in get_all: {str(e)}")
            raise
    
    async def get_all_async(self, user_id, page_number):
        try:
            cursor = self.collection.find({"user_id": user_id})
            cursor.sort('_id', -1).skip((page_number - 1) * 10).limit(10)
            documents = await cursor.to_list(length=10)
            return [Storage.from_document(doc) for doc in documents]
        except Exception as e:
            logger.error(f"Error in get_all_async: {str(e)}")
            raise
    
    def get_by_id(self, id):
        try:
            data = Storage.find_by_id(id)
            return data
        except Exception as e:
            logger.error(f"Error in get_by_id: {str(e)}")
            raise

    async def get_by_id_async(self, id):
        try:
            data = await self.collection.find_one({"_id": id})
            return Storage.from_document(data) if data else None
        except Exception as e:
            logger.error(f"Error in get_by_id_async: {str(e)}")
            raise

    def get_user_query(self, annotation_id, user_id, query):
        try:
            data = Storage.find_one({"_id": annotation_id, "user_id": user_id, "query": query})
            return data
        except Exception as e:
            logger.error(f"Error in get_user_query: {str(e)}")
            raise
    
    async def get_user_query_async(self, annotation_id, user_id, query):
        try:
            data = await self.collection.find_one({"_id": annotation_id, "user_id": user_id, "query": query})
            return Storage.from_document(data) if data else None
        except Exception as e:
            logger.error(f"Error in get_user_query_async: {str(e)}")
            raise
    
    def update(self, id, data):
        try:
            result = Storage.update({"_id": id}, {"$set": data}, many=False)
            return result
        except Exception as e:
            logger.error(f"Error in update: {str(e)}")
            raise

    async def update_async(self, id, data):
        try:
            result = await self.collection.update_one({"_id": id}, {"$set": data})
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error in update_async: {str(e)}")
            raise

    def delete(self, id):
        try:
            data = Storage.delete({"_id": id})
            return data
        except Exception as e:
            logger.error(f"Error in delete: {str(e)}")
            raise

    async def delete_async(self, id):
        try:
            result = await self.collection.delete_one({"_id": id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error in delete_async: {str(e)}")
            raise
