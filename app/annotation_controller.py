import logging
from flask import Response, request
from app import app, db_instance, schema_manager
import json
import os
import threading
import datetime
from typing import Dict, List, Optional, Any
from app.workers.task_handler import (
    generate_result,
    start_thread,
    reset_task,
    reset_status,
)
from app.lib import convert_to_csv, generate_file_path, adjust_file_path
import time
from app.constants import TaskStatus
from app.persistence import AnnotationStorageService

llm = app.config["llm_handler"]
EXP = os.getenv("REDIS_EXPIRATION", 3600)  # expiration time of redis cache


def validate_request_data(request_data: Dict) -> List[str]:
    """Validate request data against schema."""
    errors = []

    # Validate nodes
    if "nodes" in request_data:
        for node in request_data["nodes"]:
            if "type" not in node:
                errors.append(f"Node missing required 'type' field")
                continue

            node_errors = schema_manager.validate_node(
                node["type"], node.get("properties", {})
            )
            errors.extend(node_errors)

    # Validate relationships
    if "relationships" in request_data:
        for rel in request_data["relationships"]:
            if not all(k in rel for k in ["type", "source", "target"]):
                errors.append(
                    f"Relationship missing required fields (type, source, target)"
                )
                continue

            # Get source and target node types
            source_node = next(
                (n for n in request_data["nodes"] if n["id"] == rel["source"]), None
            )
            target_node = next(
                (n for n in request_data["nodes"] if n["id"] == rel["target"]), None
            )

            if not source_node or not target_node:
                errors.append(
                    f"Invalid source or target node reference in relationship"
                )
                continue

            rel_errors = schema_manager.validate_relationship(
                rel["type"],
                source_node["type"],
                target_node["type"],
                rel.get("properties", {}),
            )
            errors.extend(rel_errors)

    return errors


def handle_client_request(
    query: str, request_data: Dict, current_user_id: str, node_types: List[str]
) -> Response:
    """Handle client request with schema validation."""
    # Validate request data against schema
    validation_errors = validate_request_data(request_data)
    if validation_errors:
        return Response(
            json.dumps({"error": "Invalid request data", "details": validation_errors}),
            status=400,
            mimetype="application/json",
        )

    annotation_id = request_data.get("annotation_id")

    # Check if annotation exists
    if annotation_id:
        existing_query = AnnotationStorageService.get_user_query(
            annotation_id, str(current_user_id), query[0]
        )
    else:
        existing_query = None

    # Event to track tasks
    result_done = threading.Event()
    total_count_done = threading.Event()
    label_count_done = threading.Event()

    if existing_query:
        # Update existing annotation
        title = existing_query.title
        summary = existing_query.summary
        annotation_id = existing_query.id
        meta_data = {
            "node_count": existing_query.node_count,
            "edge_count": existing_query.edge_count,
            "node_count_by_label": existing_query.node_count_by_label,
            "edge_count_by_label": existing_query.edge_count_by_label,
        }

        AnnotationStorageService.update(
            annotation_id,
            {"status": TaskStatus.PENDING.value, "updated_at": datetime.datetime.now()},
        )
        reset_status(annotation_id)

        args = {
            "all_status": {
                "result_done": result_done,
                "total_count_done": total_count_done,
                "label_count_done": label_count_done,
            },
            "query": query,
            "request": request_data,
            "summary": summary,
            "meta_data": meta_data,
        }

        start_thread(annotation_id, args)

    elif annotation_id is None:
        # Create new annotation
        title = llm.generate_title(query[0])
        annotation = {
            "current_user_id": str(current_user_id),
            "query": query[0],
            "request": request_data,
            "title": title,
            "node_types": node_types,
            "status": TaskStatus.PENDING.value,
        }

        annotation_id = AnnotationStorageService.save(annotation)

        args = {
            "all_status": {
                "result_done": result_done,
                "total_count_done": total_count_done,
                "label_count_done": label_count_done,
            },
            "query": query,
            "request": request_data,
            "summary": None,
            "meta_data": None,
        }
        start_thread(annotation_id, args)

    else:
        # Update annotation with new query
        title = llm.generate_title(query[0])
        del request_data["annotation_id"]

        annotation = {
            "query": query[0],
            "request": request_data,
            "title": title,
            "node_types": node_types,
            "status": TaskStatus.PENDING.value,
            "node_count": None,
            "edge_count": None,
            "node_count_by_label": None,
            "edge_count_by_label": None,
        }

        AnnotationStorageService.update(annotation_id, annotation)
        reset_task(annotation_id)

        args = {
            "all_status": {
                "result_done": result_done,
                "total_count_done": total_count_done,
                "label_count_done": label_count_done,
            },
            "query": query,
            "request": request_data,
            "summary": None,
            "meta_data": None,
        }
        start_thread(annotation_id, args)

    return Response(
        json.dumps({"annotation_id": str(annotation_id)}), mimetype="application/json"
    )


def process_full_data(current_user_id: str, annotation_id: str) -> Optional[str]:
    """Process and export full data for an annotation."""
    cursor = AnnotationStorageService.get_by_id(annotation_id)
    if cursor is None:
        return None

    query, title = cursor.query, cursor.title

    try:
        # Generate file path
        file_path = generate_file_path(
            file_name=title, user_id=current_user_id, extension="xls"
        )

        # Return existing file if it exists
        if os.path.exists(file_path):
            file_path = adjust_file_path(file_path)
            return f"{request.host_url}{file_path}"

        # Run query and parse results
        result = db_instance.run_query(query)
        parsed_result = db_instance.convert_to_dict(result, schema_manager.schema)

        # Convert to CSV and return link
        file_path = convert_to_csv(
            parsed_result, user_id=current_user_id, file_name=title
        )
        file_path = adjust_file_path(file_path)
        return f"{request.host_url}{file_path}"

    except Exception as e:
        logging.error(f"Error processing full data: {str(e)}")
        raise


def requery(annotation_id: str, query: str, request_data: Dict) -> None:
    """Rerun a query for an existing annotation."""
    # Validate request data
    validation_errors = validate_request_data(request_data)
    if validation_errors:
        raise ValueError(f"Invalid request data: {validation_errors}")

    # Set up events
    result_done = threading.Event()

    # Update annotation status
    AnnotationStorageService.update(annotation_id, {"status": TaskStatus.PENDING.value})

    # Reset thread and status
    app.config["annotation_threads"][str(annotation_id)] = threading.Event()
    reset_status(annotation_id)

    def send_annotation():
        time.sleep(0.1)
        try:
            generate_result(
                query,
                annotation_id,
                request_data,
                result_done,
                status=TaskStatus.COMPLETE.value,
            )
        except Exception as e:
            logging.error(f"Error generating result graph: {str(e)}")

    # Start result generator thread
    result_generator = threading.Thread(name="result_generator", target=send_annotation)
    result_generator.start()
