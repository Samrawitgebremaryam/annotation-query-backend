from flask import Flask
from flask_cors import CORS
from app.services.test_schema_manager import TestSchemaManager
from app.services.metta_generator import MeTTa_Query_Generator
from app.services.llm_handler import LLMHandler
from app.services.autocomplete_service import AutocompleteService
import os
from app import app

# Initialize test schema manager
test_schema_manager = TestSchemaManager()
test_metta_generator = MeTTa_Query_Generator("config/schema", test_schema_manager)

# Initialize LLM handler
llm = LLMHandler()
app.config["llm_handler"] = llm

# Initialize autocomplete service
autocomplete_service = AutocompleteService()
app.config["autocomplete_service"] = autocomplete_service

# Set the schema manager and db instance for the app
app.schema_manager = test_schema_manager
app.db_instance = test_metta_generator

# Trigger initial indexing of properties
try:
    autocomplete_service.reindex_from_schema(test_schema_manager)
except Exception as e:
    print(f"Warning: Failed to perform initial indexing: {e}")

# Import routes after initializing the app
from app import routes

if __name__ == "__main__":
    app.run(debug=True)
