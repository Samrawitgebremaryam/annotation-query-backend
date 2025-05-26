from flask import Flask
from flask_cors import CORS
from app.services.schema_manager import DynamicSchemaManager
from app.services.metta_generator import MeTTa_Query_Generator
from app.services.llm_handler import LLMHandler
from app.services.autocomplete_service import AutocompleteService
from neo4j import GraphDatabase
import os
from app import app

# Initialize Neo4j driver
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
)

# Initialize schema manager with Neo4j driver
schema_manager = DynamicSchemaManager(driver)
metta_generator = MeTTa_Query_Generator("config/schema", schema_manager)

# Initialize LLM handler
llm = LLMHandler()
app.config["llm_handler"] = llm

# Initialize autocomplete service
autocomplete_service = AutocompleteService()
app.config["autocomplete_service"] = autocomplete_service

# Set the schema manager and db instance for the app
app.schema_manager = schema_manager
app.db_instance = metta_generator

# Trigger initial indexing of properties
try:
    autocomplete_service.reindex_from_schema(schema_manager)
except Exception as e:
    print(f"Warning: Failed to perform initial indexing: {e}")

# Import routes after initializing the app
from app import routes

if __name__ == "__main__":
    app.run(debug=True)
