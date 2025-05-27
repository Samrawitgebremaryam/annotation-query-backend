from flask import (
    copy_current_request_context,
    request,
    jsonify,
    Response,
    send_from_directory,
)
import logging
import json
import os
import threading
from app import app, schema_manager, db_instance, socketio, redis_client
from app.lib import validate_request
from flask_cors import CORS
from flask_socketio import disconnect, join_room, send
from dotenv import load_dotenv
from distutils.util import strtobool
import datetime
from app.lib import Graph, heuristic_sort
from app.annotation_controller import handle_client_request, process_full_data, requery
from app.constants import TaskStatus
from app.workers.task_handler import get_annotation_redis
from app.persistence import AnnotationStorageService
from app.services.autocomplete_service import AutocompleteService

# Load environmental variables
load_dotenv()

# set mongo logging
logging.getLogger("pymongo").setLevel(logging.CRITICAL)

# set redis logging
logging.getLogger("flask_redis").setLevel(logging.CRITICAL)

llm = app.config["llm_handler"]
EXP = os.getenv("REDIS_EXPIRATION", 3600)  # expiration time of redis cache

CORS(app)

# Initialize autocomplete service
autocomplete_service = AutocompleteService()


# Simplified socket handling
@socketio.on("connect")
def on_connect():
    logging.info("Client connected")
    send("Connected to server")


@socketio.on("disconnect")
def on_disconnect():
    logging.info("Client disconnected")
    disconnect()


@socketio.on("join")
def on_join(data):
    room = data["room"]
    join_room(room)
    logging.info(f"Client joined room: {room}")
    cache = get_annotation_redis(room)

    if cache != None:
        status = cache["status"]
        graph = cache["graph"]
        graph_status = True if graph is not None else False

        if status == TaskStatus.COMPLETE.value:
            socketio.emit(
                "update",
                {"status": status, "update": {"graph": graph_status}},
                to=str(room),
            )


@app.route("/kg-info", methods=["GET"])
def get_graph_info():
    graph_info = json.dumps(schema_manager.graph_info, indent=4)
    return Response(graph_info, mimetype="application/json")


@app.route("/nodes", methods=["GET"])
def get_nodes_endpoint():
    nodes = json.dumps(schema_manager.get_nodes(), indent=4)
    return Response(nodes, mimetype="application/json")


@app.route("/edges", methods=["GET"])
def get_edges_endpoint():
    edges = json.dumps(schema_manager.get_edges(), indent=4)
    return Response(edges, mimetype="application/json")


@app.route("/relations/<node_label>", methods=["GET"])
def get_relations_for_node_endpoint(node_label):
    relations = json.dumps(schema_manager.get_relations_for_node(node_label), indent=4)
    return Response(relations, mimetype="application/json")


@app.route("/schema-list", methods=["GET"])
def get_schema_list():
    schema_list = schema_manager.schema_list
    response = {
        "schemas": schema_list,
    }
    return Response(json.dumps(response, indent=4), mimetype="application/json")


@app.route("/schema", methods=["GET"])
def get_schema_by_source():
    try:
        schema = schema_manager.schmea_representation
        response = {"nodes": [], "edges": []}
        query_string = request.args.getlist("source")

        # If no source specified, return all
        if not query_string:
            response["nodes"] = schema["nodes"]
            response["edges"] = schema["edges"]
            return Response(json.dumps(response, indent=4), mimetype="application/json")

        # Process specified sources
        for schema_type in query_string:
            source = schema_type.upper()
            sub_schema = schema.get(source, None)

            if sub_schema is None:
                return jsonify({"error": "Invalid schema source"}), 400

            for key, _ in sub_schema["edges"].items():
                edge = sub_schema["edges"][key]
                edge_data = {"label": schema["edges"][key]["output_label"], **edge}
                response["edges"].append(edge_data)
                response["nodes"].append(schema["nodes"][edge["source"]])
                response["nodes"].append(schema["nodes"][edge["target"]])

            if len(response["edges"]) == 0:
                for node in sub_schema["nodes"]:
                    response["nodes"].append(schema["nodes"][node])

        return Response(json.dumps(response, indent=4), mimetype="application/json")
    except Exception as e:
        logging.error(f"Error fetching schema: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/query", methods=["POST"])  # type: ignore
def process_query():
    data = request.get_json()
    if not data or "requests" not in data:
        return jsonify({"error": "Missing requests data"}), 400

    limit = request.args.get("limit")
    properties = request.args.get("properties")
    # can be either hypothesis or ai_assistant
    source = request.args.get("source")

    if properties:
        properties = bool(strtobool(properties))
    else:
        properties = True

    if limit:
        try:
            limit = int(limit)
        except ValueError:
            return (
                jsonify({"error": "Invalid limit value. It should be an integer."}),
                400,
            )
    else:
        limit = None
    try:
        requests = data["requests"]
        question = requests.get("question", None)
        answer = None
        # Validate the request data before processing
        node_map = validate_request(requests, schema_manager.schema, source)
        if node_map is None:
            return (
                jsonify({"error": "Invalid node_map returned by validate_request"}),
                400,
            )

        # convert id to appropriate format
        requests = db_instance.parse_id(requests)

        # sort the predicate based on the the edge count
        requests = heuristic_sort(requests, node_map)

        node_only = True if source == "hypothesis" else False

        # Generate the query code
        query = db_instance.query_Generator(requests, node_map, limit, node_only)

        result_query = query[0]
        total_count_query = query[1]
        count_by_label_query = query[2]

        # Extract node types
        nodes = requests["nodes"]
        node_types = set()

        for node in nodes:
            node_types.add(node["type"])

        node_types = list(node_types)

        if source is None:
            return handle_client_request(query, requests, node_types)
        result = db_instance.run_query(result_query)

        graph_components = {
            "nodes": requests["nodes"],
            "predicates": requests["predicates"],
            "properties": properties,
        }

        result_graph = db_instance.parse_and_serialize(
            result, schema_manager.schema, graph_components, result_type="graph"
        )

        if source == "hypothesis":
            response = {"nodes": result_graph["nodes"]}
            formatted_response = json.dumps(response, indent=4)
            return Response(formatted_response, mimetype="application/json")

        total_count = db_instance.run_query(total_count_query)
        count_by_label = db_instance.run_query(count_by_label_query)

        count_result = [total_count[0], count_by_label[0]]

        meta_data = db_instance.parse_and_serialize(
            count_result, schema_manager.schema, graph_components, result_type="count"
        )

        title = llm.generate_title(result_query)

        summary = (
            llm.generate_summary(result_graph, requests)
            or "Graph too big, could not summarize"
        )

        answer = llm.generate_summary(result_graph, requests, question, False, summary)

        graph = Graph()
        if len(result_graph["edges"]) == 0:
            response = graph.group_node_only(result_graph)
        else:
            response = graph.group_graph(result_graph)
        response["node_count"] = meta_data["node_count"]
        response["edge_count"] = meta_data["edge_count"]
        response["node_count_by_label"] = meta_data["node_count_by_label"]
        response["edge_count_by_label"] = meta_data["edge_count_by_label"]

        annotation = {
            "request": requests,
            "query": result_query,
            "title": title,
            "summary": summary,
            "node_count": response["node_count"],
            "edge_count": response["edge_count"],
            "node_types": node_types,
            "node_count_by_label": response["node_count_by_label"],
            "edge_count_by_label": response["edge_count_by_label"],
            "answer": answer,
            "question": question,
            "status": TaskStatus.COMPLETE.value,
        }

        annotation_id = AnnotationStorageService.save(annotation)
        redis_client.setex(
            str(annotation_id),
            EXP,
            json.dumps(
                {
                    "task": 4,
                    "graph": {"nodes": response["nodes"], "edges": response["edges"]},
                }
            ),
        )
        response = {
            "annotation_id": str(annotation_id),
            "question": question,
            "answer": answer,
        }
        formatted_response = json.dumps(response, indent=4)
        return Response(formatted_response, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error processing query: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/email-query/<id>", methods=["POST"])
def process_email_query(id):
    data = request.get_json()
    if "email" not in data:
        return jsonify({"error": "Email missing"}), 400

    @copy_current_request_context
    def send_full_data():
        try:
            email = data["email"]

            link = process_full_data(annotation_id=id)

            subject = "Full Data"
            body = f"Hello {email}. click this link {link}\
            to download the full data you requested."

            send_email(subject, [email], body)
        except Exception as e:
            logging.error(f"Error processing query: {e}")

    sender = threading.Thread(name="main_sender", target=send_full_data)
    sender.start()
    return jsonify({"message": "Email sent successfully"}), 200


@app.route("/history", methods=["GET"])
def process_user_history():
    page_number = request.args.get("page_number")
    if page_number is not None:
        page_number = int(page_number)
    else:
        page_number = 1
    return_value = []
    cursor = AnnotationStorageService.get_all(page_number)

    if cursor is None:
        return jsonify("No value Found"), 200

    for document in cursor:
        return_value.append(
            {
                "annotation_id": str(document["_id"]),
                "request": document["request"],
                "title": document["title"],
                "node_count": document["node_count"],
                "edge_count": document["edge_count"],
                "node_types": document["node_types"],
                "status": document["status"],
                "created_at": document["created_at"].isoformat(),
                "updated_at": document["updated_at"].isoformat(),
            }
        )
    return Response(json.dumps(return_value, indent=4), mimetype="application/json")


@app.route("/annotation/<id>", methods=["GET"])
def get_by_id(id):
    response_data = {}
    cursor = AnnotationStorageService.get_by_id(id)

    if cursor is None:
        return jsonify("No value Found"), 404

    limit = request.args.get("limit")
    properties = request.args.get("properties")

    # can be either hypothesis or ai_assistant
    source = request.args.get("source")

    if properties:
        properties = bool(strtobool(properties))
    else:
        properties = False

    if limit:
        try:
            limit = int(limit)
        except ValueError:
            return (
                jsonify({"error": "Invalid limit value. It should be an integer."}),
                400,
            )

    json_request = cursor.request
    query = cursor.query
    title = cursor.title
    summary = cursor.summary
    annotation_id = cursor.id
    question = cursor.question
    answer = cursor.answer
    node_count = cursor.node_count
    edge_count = cursor.edge_count
    node_count_by_label = cursor.node_count_by_label
    edge_count_by_label = cursor.edge_count_by_label
    status = cursor.status
    file_path = cursor.path_url

    # Extract node types
    nodes = json_request["nodes"]
    node_types = set()
    for node in nodes:
        node_types.add(node["type"])
    node_types = list(node_types)

    try:
        if question:
            response_data["question"] = question

        if answer:
            response_data["answer"] = answer

        if source == "ai-assistant":
            response = {
                "annotation_id": str(annotation_id),
                "question": question,
                "answer": answer,
            }
            formatted_response = json.dumps(response, indent=4)
            return Response(formatted_response, mimetype="application/json")

        response_data["annotation_id"] = str(annotation_id)
        response_data["request"] = json_request
        response_data["title"] = title

        if summary is not None:
            response_data["summary"] = summary
        if node_count is not None:
            response_data["node_count"] = node_count
            response_data["edge_count"] = edge_count
        if node_count_by_label is not None:
            response_data["node_count_by_label"] = node_count_by_label
            response_data["edge_count_by_label"] = edge_count_by_label
        response_data["status"] = status

        cache = redis_client.get(str(annotation_id))

        if cache is not None:
            cache = json.loads(cache)
            graph = cache["graph"]
            if graph is not None:
                response_data["nodes"] = graph["nodes"]
                response_data["edges"] = graph["edges"]

            return Response(
                json.dumps(response_data, indent=4), mimetype="application/json"
            )

        if (
            status in [TaskStatus.PENDING.value, TaskStatus.COMPLETE.value]
            and source is None
        ):
            if status == TaskStatus.COMPLETE.value:
                if os.path.exists(file_path):
                    # open the file and read the graph
                    with open(file_path, "r") as file:
                        graph = json.load(file)

                    response_data["nodes"] = graph["nodes"]
                    response_data["edges"] = graph["edges"]
                else:
                    response_data["status"] = TaskStatus.PENDING.value
                    requery(annotation_id, query, json_request)
            formatted_response = json.dumps(response_data, indent=4)
            return Response(formatted_response, mimetype="application/json")

        # Run the query and parse the results
        result = db_instance.run_query(query)
        graph_components = {"properties": properties}
        response_data = db_instance.parse_and_serialize(
            result, schema_manager.schema, graph_components, result_type="graph"
        )
        graph = Graph()
        if len(response_data["edges"]) == 0:
            response_data = graph.group_node_only(response_data)
        else:
            grouped_graph = graph.group_graph(response_data)
        response_data["nodes"] = grouped_graph["nodes"]
        response_data["edges"] = grouped_graph["edges"]

        if source == "hypothesis":
            response = {
                "nodes": response_data["nodes"],
                "edges": response_data["edges"],
            }
            formatted_response = json.dumps(response, indent=4)
            return Response(formatted_response, mimetype="application/json")

        if "nodes" in response_data and len(response_data["nodes"]) == 0:
            response = jsonify({"error": "No data found for the query"})
            response = Response(response.response, status=404)
            response.status = "404 No matching results for the query"
            return response
        # if limit:
        # response_data = limit_graph(response_data, limit)

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error processing query: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/annotation/<id>", methods=["POST"])
def process_by_id(id):
    data = request.get_json()
    if not data or "requests" not in data:
        return jsonify({"error": "Missing requests data"}), 400

    if "question" not in data["requests"]:
        return jsonify({"error": "Missing question data"}), 400

    question = data["requests"]["question"]
    response_data = {}
    cursor = AnnotationStorageService.get_by_id(id)

    limit = request.args.get("limit")
    properties = request.args.get("properties")
    # can be either hypothesis or ai_assistant
    source = request.args.get("source")

    if properties:
        properties = bool(strtobool(properties))
    else:
        properties = False

    if limit:
        try:
            limit = int(limit)
        except ValueError:
            return (
                jsonify({"error": "Invalid limit value. It should be an integer."}),
                400,
            )

    if cursor is None:
        return jsonify("No value Found"), 200

    query = cursor.query
    summary = cursor.summary
    json_request = cursor.request
    node_count_by_label = cursor.node_count_by_label
    edge_count_by_label = cursor.edge_count_by_label

    try:
        if question:
            response_data["question"] = question

        cache = redis_client.get(str(id))

        if cache is not None:
            cache = json.loads(cache)
            graph = cache["graph"]
            if graph is not None:
                response_data["nodes"] = graph["nodes"]
                response_data["edges"] = graph["edges"]
        else:
            # Run the query and parse the results
            result = db_instance.run_query(query)
            graph_components = {"properties": properties}
            response_data = db_instance.parse_and_serialize(
                result, schema_manager.schema, graph_components, result_type="graph"
            )

        response_data["node_count_by_label"] = node_count_by_label
        response_data["edge_count_by_label"] = edge_count_by_label

        answer = (
            llm.generate_summary(response_data, json_request, question, False, summary)
            if question
            else None
        )

        AnnotationStorageService.update(
            id, {"answer": answer, "updated_at": datetime.datetime.now()}
        )

        response = {"annotation_id": str(id), "question": question, "answer": answer}

        formatted_response = json.dumps(response, indent=4)
        return Response(formatted_response, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error processing query: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/annotation/<id>/full", methods=["GET"])
def process_full_annotation(id):
    try:
        link = process_full_data(annotation_id=id)
        if link is None:
            return jsonify("No value Found"), 200

        response_data = {"link": link}

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error processing query: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/public/<file_name>")
def serve_file(file_name):
    public_folder = os.path.join(os.getcwd(), "public")
    return send_from_directory(public_folder, file_name)


@app.route("/annotation/<id>", methods=["DELETE"])
def delete_by_id(id):
    try:
        # first check if there is any running running annoation
        with app.config["annotation_lock"]:
            thread_event = app.config["annotation_threads"]
            stop_event = thread_event.get(id, None)

            # if there is stop the running annoation
            if stop_event is not None:
                stop_event.set()

                response_data = {"message": f"Annotation {id} has been cancelled."}

                formatted_response = json.dumps(response_data, indent=4)
                return Response(formatted_response, mimetype="application/json")

        # else delete the annotation from the db
        existing_record = AnnotationStorageService.get_by_id(id)

        if existing_record is None:
            return jsonify("No value Found"), 404

        deleted_record = AnnotationStorageService.delete(id)

        if deleted_record is None:
            return jsonify("Failed to delete the annotation"), 500

        response_data = {"message": "Annotation deleted successfully"}

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error deleting annotation: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/annotation/<id>/title", methods=["PUT"])
def update_title(id):
    data = request.get_json()

    if "title" not in data:
        return jsonify({"error": "Title is required"}), 400

    title = data["title"]

    try:
        existing_record = AnnotationStorageService.get_by_id(id)

        if existing_record is None:
            return jsonify("No value Found"), 404

        AnnotationStorageService.update(id, {"title": title})

        response_data = {
            "message": "title updated successfully",
            "title": title,
        }

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error updating title: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/annotation/delete", methods=["POST"])
def delete_many():
    data = request.data.decode("utf-8").strip()

    if not data or data.startswith("'") and data.endswith("'"):
        data = data[1:-1]

    try:
        data = json.loads(data)
    except json.JSONDecodeError:
        return {"error": "Invalid JSON"}, 400

    if "annotation_ids" not in data:
        return jsonify({"error": "Missing annotation ids"}), 400

    annotation_ids = data["annotation_ids"]

    # Remove user access check since we're not using user management
    if not isinstance(annotation_ids, list):
        return jsonify({"error": "Annotation ids must be a list"}), 400

    if len(annotation_ids) == 0:
        return jsonify({"error": "Annotation ids must not be empty"}), 400

    try:
        delete_count = AnnotationStorageService.delete_many_by_id(annotation_ids)

        response_data = {
            "message": f"Out of {len(annotation_ids)}, {delete_count} were successfully deleted."
        }

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error deleting annotations: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/autocomplete", methods=["GET"])
def get_autocomplete_suggestions():
    try:
        query = request.args.get("query", "")
        node_type = request.args.get("node_type")
        size = int(request.args.get("size", 10))

        if not query:
            return jsonify({"suggestions": [], "total": 0})

        suggestions = autocomplete_service.search_suggestions(
            query=query, node_type=node_type, size=size
        )

        return jsonify({"suggestions": suggestions, "total": len(suggestions)})

    except Exception as e:
        logging.error(f"Error in autocomplete endpoint: {e}")
        return jsonify({"error": str(e), "suggestions": [], "total": 0}), 500


@app.route("/api/autocomplete/reindex", methods=["POST"])
def reindex_autocomplete():
    try:
        from app.services.schema_manager import DynamicSchemaManager
        from app.services.cypher_generator import CypherQueryGenerator

        # Initialize schema manager and reindex
        cypher_generator = CypherQueryGenerator(dataset_path="cypher_data")
        schema_manager = DynamicSchemaManager(cypher_generator.driver)

        success = autocomplete_service.force_reindex(schema_manager)

        if success:
            return jsonify(
                {"message": "Reindexing completed successfully", "status": "success"}
            )
        else:
            return jsonify({"message": "Reindexing failed", "status": "error"}), 500

    except Exception as e:
        logging.error(f"Error during reindexing: {e}")
        return jsonify({"error": str(e), "status": "error"}), 500
