from flask import request, Response
from app import (
    app,
    schema_manager,
    db_instance,
    socketio,
    redis_client,
    ThreadStopException,
)
import logging
import json
import os
import threading
import time
from typing import Dict, List, Optional, Any, Union
from app.lib import Graph
from app.constants import TaskStatus
from app.persistence import AnnotationStorageService
from pathlib import Path

llm = app.config["llm_handler"]
EXP = os.getenv("REDIS_EXPIRATION", 3600)  # expiration time of redis cache


# Create a function to get and ensure graph storage directory exists
def get_graph_storage_dir() -> Path:
    """Get and ensure graph storage directory exists."""
    storage_dir = Path(__file__).parent.parent.parent / "storage" / "graph"
    storage_dir.mkdir(parents=True, exist_ok=True)
    return storage_dir


def update_task(annotation_id, graph=None):
    with app.config["annotation_lock"]:
        status = TaskStatus.PENDING.value
        # Get the cached data (Handle case where cache is None)
        cache = redis_client.get(str(annotation_id))
        
        cache = json.loads(cache) if cache else {"graph": None, "status": status}
        
        status = cache["status"]
        # Merge graph updates
        graph = graph if graph else cache["graph"]
        if status == TaskStatus.COMPLETE.value:
            redis_client.setex(
                str(annotation_id),
                EXP,
                json.dumps({"graph": graph, "status": TaskStatus.COMPLETE.value}),
            )
            AnnotationStorageService.update(annotation_id, {"status": status})
            redis_client.delete(f"{annotation_id}_tasks")
            return TaskStatus.COMPLETE.value
        
        # Increment task count atomically and get the new count
        task_num = redis_client.incr(f"{annotation_id}_tasks")
    
        if status in [TaskStatus.FAILED.value, TaskStatus.CANCELLED.value]:
            if task_num >= 4 and cache["status"] == TaskStatus.CANCELLED.value:
                AnnotationStorageService.delete(annotation_id)
                redis_client.delete(f"{annotation_id}_tasks")
                redis_client.delete(str(annotation_id))
                with app.config["annotation_lock"]:
                    annotation_threads = app.config["annotation_threads"]
                    del annotation_threads[str(annotation_id)]
        else:
            status = (
                TaskStatus.COMPLETE.value if task_num >= 4 else TaskStatus.PENDING.value
            )
        if (
            status in [TaskStatus.FAILED.value, TaskStatus.COMPLETE.value]
            and task_num >= 4
        ):
            redis_client.setex(
                str(annotation_id), EXP, json.dumps({"graph": graph, "status": status})
            )
            AnnotationStorageService.update(annotation_id, {"status": status})
            redis_client.delete(f"{annotation_id}_tasks")
        elif status == TaskStatus.PENDING.value:
            redis_client.set(
                str(annotation_id), json.dumps({"graph": graph, "status": status})
            )
        
        return status


def get_status(annotation_id):
    with app.config["annotation_lock"]:
        cache = redis_client.get(str(annotation_id))
        if cache is not None:
            cache = json.loads(cache)
            status = cache["status"]
            return status
        else:
            return TaskStatus.PENDING.value
    

def set_status(annotation_id, status):
    with app.config["annotation_lock"]:
        cache = redis_client.get(str(annotation_id))
        if cache is not None:
            cache = json.loads(cache)
            cache["status"] = status
            redis_client.set(str(annotation_id), json.dumps(cache))
        else:
            redis_client.set(
                str(annotation_id), json.dumps({"graph": None, "status": status})
            )


def get_annotation_redis(annotation_id):
    cache = redis_client.get(str(annotation_id))
    if cache is not None:
        cache = json.loads(cache)
        return cache
    else:
        return None


def reset_status(annotation_id):
    redis_client.delete(f"{annotation_id}_tasks")
    set_status(annotation_id, TaskStatus.PENDING.value)


def reset_task(annotation_id):
    redis_client.delete(f"{annotation_id}_tasks")
    redis_client.delete(str(annotation_id))


def generate_summary(annotation_id, request, all_status, summary=None):
    """Generate a summary of the graph data using schema-aware processing."""
    result_done, total_count_done, label_count_done = all_status.values()
    # wait for all threads to finish
    result_done.wait()
    total_count_done.wait()
    label_count_done.wait()

    if get_status(annotation_id) == TaskStatus.FAILED.value:
        summary = "Failed to generate summary"
        status = TaskStatus.FAILED.value
        status = update_task(annotation_id)
        socketio.emit(
            "update",
            {"status": status, "update": {"summary": summary}},
            to=str(annotation_id),
        )
        return 

    if summary is not None:
        status = update_task(annotation_id)
        socketio.emit(
            "update",
            {"status": status, "update": {"summary": summary}},
            to=str(annotation_id),
        )
        return
    
    try:
    meta_data = AnnotationStorageService.get_by_id(annotation_id)
        if not meta_data:
            raise ValueError(f"No metadata found for annotation {annotation_id}")

    cache = redis_client.get(str(annotation_id))
    if cache is not None:
        cache = json.loads(cache)
            graph = cache.get("graph")
            response = {
                "nodes": graph["nodes"] if graph else [],
                "edges": graph["edges"] if graph else [],
                "node_count": meta_data.node_count,
                "edge_count": meta_data.edge_count,
                "node_count_by_label": meta_data.node_count_by_label,
                "edge_count_by_label": meta_data.edge_count_by_label,
                "schema": schema_manager.schema,  # Add schema for context-aware summarization
            }
        else:
            response = {
                "nodes": [],
                "edges": [],
                "node_count": 0,
                "edge_count": 0,
                "node_count_by_label": [],
                "edge_count_by_label": [],
            }

        if len(response["nodes"]) == 0:
            summary = "No data found in the graph"
        else:
            # Generate domain-aware summary using schema context
            summary = llm.generate_summary(response, request) 
            if not summary:
                summary = "Unable to generate summary for this graph structure"

        AnnotationStorageService.update(annotation_id, {"summary": summary})
        status = update_task(annotation_id)
        socketio.emit(
            "update",
            {"status": status, "update": {"summary": summary}},
            to=str(annotation_id),
        )

    except ThreadStopException as e:
        set_status(annotation_id, TaskStatus.CANCELLED.value)
        update_task(annotation_id)
        socketio.emit(
            "update",
            {
                "status": TaskStatus.CANCELLED.value,
                "update": {"summary": "Summary generation cancelled"},
            },
        )
        logging.error("Summary generation stopped: %s", e)
    except Exception as e:
        logging.exception("Error generating summary: %s", e)
        status = update_task(annotation_id)
        socketio.emit(
            "update",
            {
                "status": status,
                "update": {"summary": "Unable to generate summary due to an error"},
            },
            to=str(annotation_id),
        )


def validate_request(request_data: Dict) -> List[str]:
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


def generate_result(
    query: Union[str, List[str]],
    annotation_id: str,
    request_data: Dict,
    result_done: threading.Event,
    status: str = TaskStatus.COMPLETE.value,
) -> None:
    """Generate query results and store them."""
    try:
        # Validate request data
        validation_errors = validate_request(request_data)
        if validation_errors:
            raise ValueError(f"Invalid request data: {validation_errors}")

        # Execute query and parse results
        if isinstance(query, list):
            query = query[0]  # Take first query if list provided

        results = db_instance.run_query(query)
        parsed_results = db_instance.parse_results(
            results, schema_manager.schema, request_data, "graph"
        )

        # Generate graph visualization
        graph = Graph(parsed_results)
        graph_data = graph.to_dict()

        # Store results
        graph_file = get_graph_storage_dir() / f"{annotation_id}.json"
        with open(graph_file, "w") as f:
            json.dump(graph_data, f)

        # Update annotation status
        AnnotationStorageService.update(
            annotation_id,
            {
                "status": status,
                "node_count": len(graph_data["nodes"]),
                "edge_count": len(graph_data["edges"]),
                "node_count_by_label": _count_by_type(graph_data["nodes"], "type"),
                "edge_count_by_label": _count_by_type(graph_data["edges"], "type"),
            },
        )

        # Emit update event
        socketio.emit(
            "update", {"status": status, "graph": graph_data}, room=annotation_id
        )

        result_done.set()

    except Exception as e:
        logging.error(f"Error generating result: {str(e)}")
        AnnotationStorageService.update(
            annotation_id, {"status": TaskStatus.ERROR.value}
        )
        socketio.emit("update", {"status": TaskStatus.ERROR.value}, room=annotation_id)
        raise


def _count_by_type(items: List[Dict], type_field: str) -> Dict[str, int]:
    """Count items by their type."""
    counts = {}
    for item in items:
        item_type = item.get(type_field)
        if item_type:
            counts[item_type] = counts.get(item_type, 0) + 1
    return counts


def generate_total_count(
    count_query, annotation_id, requests, total_count_status, meta_data=None
):
    if get_status(annotation_id) == TaskStatus.FAILED.value:
        socketio.emit(
            "update",
            {
                "status": TaskStatus.FAILED.value,
                "update": {"node_count": 0, "edge_count": 0},
            },
        )
        status = update_task(annotation_id)
        total_count_status.set()
        return
    
    if get_status(annotation_id) == TaskStatus.FAILED.value:
        socketio.emit(
            "update",
            {
                "status": TaskStatus.CANCELLED.value,
                "update": {"node_count": 0, "edge_count": 0},
            },
        )
        status = update_task(annotation_id)
        total_count_status.set()
        return
    
    annotation_threads = app.config["annotation_threads"]
    stop_event = annotation_threads[str(annotation_id)]
    
    if stop_event.is_set():
        raise ThreadStopException("Stoping result generation thread")
    
    if meta_data:
        status = update_task(annotation_id)
        socketio.emit(
            "update",
            {
                "status": status,
                "update": {
                    "node_count": meta_data["node_count"],
                    "edge_count": meta_data["edge_count"],
                },
            },
            to=str(annotation_id),
        )
        total_count_status.set()
        return
        
    try:

        total_count = db_instance.run_query(count_query, stop_event)

        if len(total_count) == 0:
            status = update_task(annotation_id)
            AnnotationStorageService.update(
                annotation_id, {"status": status, "node_count": 0, "edge_count": 0}
            )
            socketio.emit(
                "update",
                {"status": status, "update": {"node_count": 0, "edge_count": 0}},
            )
            total_count_status.set()
            return

        count_result = [total_count[0], {}]
        graph_components = {
            "nodes": requests["nodes"],
            "predicates": requests["predicates"],
            "properties": False,
        }
        response = db_instance.parse_and_serialize(
            count_result, schema_manager.schema, graph_components, "count"
        )

        status = update_task(annotation_id)

        AnnotationStorageService.update(
            annotation_id,
            {
                "node_count": response["node_count"],
                "edge_count": response["edge_count"],
                "status": status,
            },
        )

        socketio.emit(
            "update",
            {
                "status": status,
                "update": {
                    "node_count": response["node_count"],
                    "edge_count": response["edge_count"],
                },
            },
            to=str(annotation_id),
        )
        total_count_status.set()
    except ThreadStopException as e:
        set_status(annotation_id, TaskStatus.CANCELLED.value)
        update_task(annotation_id)
        socketio.emit(
            "update",
            {
                "status": TaskStatus.CANCELLED.value,
                "update": {"node_count": 0, "edge_count": 0},
            },
        )
        total_count_status.set()
        logging.error("Error generating total count %s", e)
    except Exception as e:
        set_status(annotation_id, TaskStatus.FAILED.value)
        update_task(annotation_id)
        AnnotationStorageService.update(
            annotation_id,
            {"status": TaskStatus.FAILED.value, "node_count": 0, "edge_count": 0},
        )
        socketio.emit(
            "update",
            {
                "status": TaskStatus.FAILED.value,
                "update": {"node_count": 0, "edge_count": 0},
            },
        )
        total_count_status.set()
        logging.error("Error generating total count %s", e)


def generate_empty_label_count(requests, schema_manager):
    """Generate empty label counts with schema validation."""
    update = {"node_count_by_label": [], "edge_count_by_label": []}
    node_count_by_label = {}
    edge_count_by_label = {}

    # Validate node types against schema
    for node in requests["nodes"]:
        node_type = node["type"]
        if schema_manager.get_node_type(node_type):
            node_count_by_label[node_type] = 0

    # Validate relationship types against schema
    for edge in requests.get("predicates", []):
        edge_type = edge["type"]
        if schema_manager.get_relationship_type(edge_type):
            edge_count_by_label[edge_type] = 0

    # Format counts
    for key, value in node_count_by_label.items():
        update["node_count_by_label"].append({"label": key, "count": value})

    for key, value in edge_count_by_label.items():
        update["edge_count_by_label"].append({"label": key, "count": value})

    return update


def generate_label_count(
    count_query, annotation_id, requests, count_label_status, meta_data=None
):
    """Generate label counts with schema validation and error handling."""
    if get_status(annotation_id) == TaskStatus.FAILED.value:
        update = generate_empty_label_count(requests, schema_manager)
        status = update_task(annotation_id)
        socketio.emit("update", {"status": status, "update": update})
        count_label_status.set()
        return
    
    if get_status(annotation_id) == TaskStatus.CANCELLED.value:
        update = generate_empty_label_count(requests, schema_manager)
        status = update_task(annotation_id)
        socketio.emit(
            "update",
            {
                "status": TaskStatus.CANCELLED.value,
                "update": update,
            },
        )
        count_label_status.set()
        return

    annotation_threads = app.config["annotation_threads"]
    stop_event = annotation_threads[str(annotation_id)]
    
    if stop_event.is_set():
        raise ThreadStopException("Stoping result generation thread")

    try:
        if meta_data:
            status = update_task(annotation_id)
            socketio.emit(
                "update",
                {
                    "status": status,
                    "update": {
                        "node_count_by_label": meta_data["node_count_by_label"],
                        "edge_count_by_label": meta_data["edge_count_by_label"],
                    },
                },
                to=str(annotation_id),
            )
            count_label_status.set()
            return

        # Execute count query with schema validation
        label_count = db_instance.run_query(count_query, stop_event)
        if not label_count:
            raise ValueError("No results returned from count query")

        count_result = [{}, label_count[0]]
        graph_components = {
            "nodes": requests["nodes"],
            "predicates": requests.get("predicates", []),
            "properties": False,
        }

        # Parse results with schema context
        response = db_instance.parse_and_serialize(
            count_result, schema_manager.schema, graph_components, "count"
        )

        # Validate counts against schema
        validated_counts = {
            "node_count_by_label": [
                count
                for count in response["node_count_by_label"]
                if schema_manager.get_node_type(count["label"])
            ],
            "edge_count_by_label": [
                count
                for count in response["edge_count_by_label"]
                if schema_manager.get_relationship_type(count["label"])
            ],
        }

        AnnotationStorageService.update(annotation_id, validated_counts)
        status = update_task(annotation_id)
        socketio.emit(
            "update",
            {"status": status, "update": validated_counts},
            to=str(annotation_id),
        )
        count_label_status.set()

    except ThreadStopException as e:
        set_status(annotation_id, TaskStatus.CANCELLED.value)
        update_task(annotation_id)
        update = generate_empty_label_count(requests, schema_manager)
        socketio.emit(
            "update",
            {"status": TaskStatus.CANCELLED.value, "update": update},
        )
        count_label_status.set()
        logging.error("Label count generation stopped: %s", e)
    except Exception as e:
        set_status(annotation_id, TaskStatus.FAILED.value)
        update_task(annotation_id)
        update = generate_empty_label_count(requests, schema_manager)
        AnnotationStorageService.update(
            annotation_id,
            {
                "status": TaskStatus.FAILED.value,
                "node_count_by_label": update["node_count_by_label"],
                "edge_count_by_label": update["edge_count_by_label"],
            },
        )
        socketio.emit("update", {"status": TaskStatus.FAILED.value, "update": update})
        count_label_status.set()
        logging.error("Error generating label count: %s", e)


def start_thread(annotation_id: str, args: Dict) -> None:
    """Start processing threads for an annotation."""
    try:
        # Initialize thread tracking
        annotation_threads = app.config["annotation_threads"]
    annotation_threads[str(annotation_id)] = threading.Event()

        def process_annotation():
            time.sleep(0.1)  # Small delay to ensure proper initialization
            try:
                generate_result(
                    args["query"],
                    annotation_id,
                    args["request"],
                    args["all_status"]["result_done"],
                    TaskStatus.COMPLETE.value,
                )
            except ThreadStopException:
                logging.info(f"Thread stopped for annotation {annotation_id}")
        except Exception as e:
                logging.error(f"Error processing annotation {annotation_id}: {str(e)}")

        # Start processing thread
        processor = threading.Thread(
            name="annotation_processor", target=process_annotation
        )
        processor.start()

        except Exception as e:
        logging.error(f"Error starting thread for annotation {annotation_id}: {str(e)}")
        raise


def reset_task(annotation_id: str) -> None:
    """Reset task state for an annotation."""
    try:
        # Stop existing thread if any
        if str(annotation_id) in app.config["annotation_threads"]:
            app.config["annotation_threads"][str(annotation_id)].set()

        # Clear existing results
        graph_file = get_graph_storage_dir() / f"{annotation_id}.json"
        if graph_file.exists():
            graph_file.unlink()

        # Reset status
        reset_status(annotation_id)

        except Exception as e:
        logging.error(f"Error resetting task for annotation {annotation_id}: {str(e)}")
        raise


def reset_status(annotation_id: str) -> None:
    """Reset status for an annotation."""
    try:
        AnnotationStorageService.update(
            annotation_id,
            {
                "status": TaskStatus.PENDING.value,
                "node_count": None,
                "edge_count": None,
                "node_count_by_label": None,
                "edge_count_by_label": None,
            },
        )
        except Exception as e:
        logging.error(
            f"Error resetting status for annotation {annotation_id}: {str(e)}"
        )
        raise
