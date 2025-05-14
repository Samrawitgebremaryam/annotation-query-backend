from flask import Flask
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_socketio import SocketIO
from app.services.schema_manager import SchemaManager
from app.services.cypher_generator import CypherQueryGenerator
from app.services.metta_generator import MettaQueryGenerator
from db import mongo_init
from app.lib.llm_handler import LLMHandler
from app.persistence.annotation_storage import AnnotationStorageService
import os
import logging
import yaml
from flask_redis import FlaskRedis
from app.error import ThreadStopException
import threading
from app.constants import TaskStatus, GRAPH_INFO_PATH
import json
import redis
from typing import List, Dict

app = Flask(__name__)
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading",
    logger=True,
    engineio_logger=True,
)

app.config["REDIS_URL"] = os.getenv("REDIS_URL")

# Initialize Redis client
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
)


def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        logging.info("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logging.error(f"Config file not found at: {config_path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {e}")
        raise


config = load_config()

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per minute"],
)

mongo_init()

# Initialize database instance
db_instance = CypherQueryGenerator(os.getenv("DATASET_PATH", "data/dataset"))

# Initialize schema manager with database connection
schema_manager = SchemaManager(db_instance=db_instance)

# Generate initial schema from database
try:
    schema_manager.generate_schema_from_db()
    logging.info("Schema generated successfully from database")
except Exception as e:
    app.logger.warning(
        f"Could not generate schema from database: {e}. Using default schema."
    )

# Initialize other components
llm_handler = LLMHandler()
metta_generator = MettaQueryGenerator(os.getenv("DATASET_PATH", "data/dataset"))

# Set schema manager for query generators
db_instance.set_schema_manager(schema_manager)
metta_generator.set_schema_manager(schema_manager)

# Configure application
app.config["llm_handler"] = llm_handler
app.config["db_instance"] = db_instance
app.config["schema_manager"] = schema_manager
app.config["metta_generator"] = metta_generator
app.config["annotation_threads"] = {}  # holding the stop event for each annotation task
app.config["annotation_lock"] = threading.Lock()

# Load graph info
try:
    graph_info = json.load(open(GRAPH_INFO_PATH))
    app.config["graph_info"] = graph_info
except Exception as e:
    app.logger.warning(f"Could not load graph info: {e}")
    app.config["graph_info"] = {}


def refresh_schema():
    """Refresh schema from database and update cache."""
    try:
        schema_manager.generate_schema_from_db()
        logging.info("Schema refreshed successfully from database")
        return True
    except Exception as e:
        logging.error(f"Failed to refresh schema: {e}")
        return False


# Import routes at the end to avoid circular imports
from app import routes
from app.annotation_controller import handle_client_request, process_full_data, requery

type_mapping = {
    "String": "string",
    "Integer": "integer",
    "Float": "float",
    "Boolean": "boolean",
    "DateTime": "datetime",
    # ... more types ...
}


def refresh_schema(self):
    """Refresh schema from database and update cache."""
    return self.generate_schema_from_db()
