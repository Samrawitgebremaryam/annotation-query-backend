from flask import (
    copy_current_request_context,
    request,
    jsonify,
    Response,
    send_from_directory,
    stream_with_context,
)
import logging
import json
import yaml
import os
import asyncio
import threading
from functools import wraps
from app import app, databases, schema_manager, db_instance
from app.lib import validate_request
from flask_cors import CORS
from app.lib import limit_graph
from app.lib.auth import token_required
from app.lib.email import init_mail, send_email
from dotenv import load_dotenv
from distutils.util import strtobool
import datetime
from app.lib import convert_to_csv
from app.lib import generate_file_path
from app.lib import adjust_file_path

# Load environmental variables
load_dotenv()

# set mongo loggin
logging.getLogger("pymongo").setLevel(logging.CRITICAL)

# Flask-Mail configuration
app.config["MAIL_SERVER"] = os.getenv("MAIL_SERVER")
app.config["MAIL_PORT"] = os.getenv("MAIL_PORT")
app.config["MAIL_USE_TLS"] = bool(strtobool(os.getenv("MAIL_USE_TLS")))
app.config["MAIL_USE_SSL"] = bool(strtobool(os.getenv("MAIL_USE_SSL")))
app.config["MAIL_USERNAME"] = os.getenv("MAIL_USERNAME")
app.config["MAIL_PASSWORD"] = os.getenv("MAIL_PASSWORD")
app.config["MAIL_DEFAULT_SENDER"] = os.getenv("MAIL_DEFAULT_SENDER")

llm = app.config["llm_handler"]
storage_service = app.config["storage_service"]

# Initialize Flask-Mail
init_mail(app)

CORS(app)

# Setup basic logging
logging.basicConfig(level=logging.DEBUG)


def async_route(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(f(*args, **kwargs))
        finally:
            loop.close()

    return wrapped


@app.route("/kg-info", methods=["GET"])
@token_required
def get_graph_info(current_user_id):
    graph_info = json.dumps(schema_manager.graph_info, indent=4)
    return Response(graph_info, mimetype="application/json")


@app.route("/nodes", methods=["GET"])
@token_required
def get_nodes_endpoint(current_user_id):
    nodes = json.dumps(schema_manager.get_nodes(), indent=4)
    return Response(nodes, mimetype="application/json")


@app.route("/edges", methods=["GET"])
@token_required
def get_edges_endpoint(current_user_id):
    edges = json.dumps(schema_manager.get_edges(), indent=4)
    return Response(edges, mimetype="application/json")


@app.route("/relations/<node_label>", methods=["GET"])
@token_required
def get_relations_for_node_endpoint(current_user_id, node_label):
    relations = json.dumps(schema_manager.get_relations_for_node(node_label), indent=4)
    return Response(relations, mimetype="application/json")


@app.route("/query", methods=["POST"])
@token_required
@async_route
async def process_query(current_user_id):
    data = request.get_json()
    if not data or "requests" not in data:
        return jsonify({"error": "Missing requests data"}), 400

    limit = request.args.get("limit")
    properties = request.args.get("properties")
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
        annotation_id = None
        question = None
        answer = None

        if "annotation_id" in requests:
            annotation_id = requests["annotation_id"]

        if "question" in requests:
            question = requests["question"]

        # Validate the request data before processing
        node_map = validate_request(requests, schema_manager.schema)
        if node_map is None:
            return (
                jsonify({"error": "Invalid node_map returned by validate_request"}),
                400,
            )

        # convert id to appropriate format
        requests = db_instance.parse_id(requests)

        node_only, run_count = (
            (True, False) if source == "hypothesis" else (False, True)
        )

        # Generate the query code
        query_code = await db_instance.query_Generator(
            requests, node_map, limit, node_only
        )

        # Split queries - main query is first, count queries are the rest
        main_query = query_code[0] if isinstance(query_code, list) else query_code
        count_queries = query_code[1:] if isinstance(query_code, list) else None

        # Run main query first
        result = await db_instance.run_query_async(main_query, run_count=False)
        response_data = db_instance.parse_and_serialize(
            result, schema_manager.schema, properties
        )

        # Extract node types
        nodes = requests["nodes"]
        node_types = set()
        for node in nodes:
            node_types.add(node["type"])
        node_types = list(node_types)

        if source == "hypothesis":
            response = {
                "nodes": response_data["nodes"],
                "edges": response_data["edges"],
            }
            formatted_response = json.dumps(response, indent=4)
            return Response(formatted_response, mimetype="application/json")

        # Start count queries asynchronously
        if count_queries and run_count:

            async def run_count_queries():
                try:
                    count_results = await db_instance.run_query_async(
                        count_queries, run_count=True
                    )
                    count_data = db_instance.parse_and_serialize(
                        count_results, schema_manager.schema, properties
                    )

                    if annotation_id:
                        await storage_service.update_async(
                            annotation_id,
                            {
                                "node_count": count_data.get("node_count", 0),
                                "edge_count": count_data.get("edge_count", 0),
                                "node_count_by_label": count_data.get(
                                    "node_count_by_label", {}
                                ),
                                "edge_count_by_label": count_data.get(
                                    "edge_count_by_label", []
                                ),
                            },
                        )
                except Exception as e:
                    logging.error(f"Error in count queries: {str(e)}")

            # Create task for count queries
            asyncio.create_task(run_count_queries())

        if annotation_id:
            existing_query = await storage_service.get_user_query_async(
                annotation_id, str(current_user_id), main_query
            )
        else:
            existing_query = None

        if existing_query is None:
            title = llm.generate_title(main_query)

            if not response_data.get("nodes") and not response_data.get("edges"):
                summary = "No data found for the query"
            else:
                summary = (
                    llm.generate_summary(response_data)
                    or "Graph too big, could not summarize"
                )

            answer = (
                llm.generate_summary(response_data, question, False, summary)
                if question
                else None
            )

            # Create annotation with default timestamps
            if annotation_id is None:
                annotation = {
                    "current_user_id": str(current_user_id),
                    "request": requests,
                    "query": main_query,
                    "question": question,
                    "answer": answer,
                    "title": title,
                    "summary": summary,
                    "node_types": node_types,
                    "node_count": 0,  # Add default values
                    "edge_count": 0,
                    "created_at": datetime.datetime.now(),
                    "updated_at": datetime.datetime.now(),
                }
                annotation_id = await storage_service.save_async(annotation)

            # Get updated data with proper error handling
            updated_data = await storage_service.get_by_id_async(annotation_id)
            if updated_data:
                response_data["title"] = title
                response_data["summary"] = summary
                response_data["annotation_id"] = str(annotation_id)
                response_data["created_at"] = updated_data.get(
                    "created_at", datetime.datetime.now()
                ).isoformat()
                response_data["updated_at"] = updated_data.get(
                    "updated_at", datetime.datetime.now()
                ).isoformat()
            else:
                # Provide default values if document not found
                response_data["created_at"] = datetime.datetime.now().isoformat()
                response_data["updated_at"] = datetime.datetime.now().isoformat()

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

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype="application/json")

    except Exception as e:
        logging.error(f"Error processing query: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/query/count/<annotation_id>", methods=["GET"])
@token_required
def get_count_status(current_user_id, annotation_id):
    try:
        query = storage_service.get_by_id(annotation_id)
        if not query:
            return jsonify({"error": "Query not found"}), 404

        count_data = {
            "node_count": getattr(query, "node_count", None),
            "edge_count": getattr(query, "edge_count", None),
            "node_count_by_label": getattr(query, "node_count_by_label", None),
            "edge_count_by_label": getattr(query, "edge_count_by_label", None),
        }

        # Check if counts are available
        if all(v is None for v in count_data.values()):
            return jsonify({"status": "counting"}), 202

        return jsonify({"status": "complete", "counts": count_data})

    except Exception as e:
        logging.error(f"Error getting count status: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/email-query/<id>", methods=["POST"])
@token_required
def process_email_query(current_user_id, id):
    data = request.get_json()
    if "email" not in data:
        return jsonify({"error": "Email missing"}), 400

    @copy_current_request_context
    def send_full_data():
        try:
            email = data["email"]

            link = process_full_data(current_user_id=current_user_id, annotation_id=id)

            subject = "Full Data"
            body = f"Hello {email}. click this link {link} to download the full data you requested."

            send_email(subject, [email], body)
        except Exception as e:
            logging.error(f"Error processing query: {e}")

    sender = threading.Thread(name="main_sender", target=send_full_data)
    sender.start()
    return jsonify({"message": "Email sent successfully"}), 200


@app.route("/history", methods=["GET"])
@token_required
def process_user_history(current_user_id):
    page_number = request.args.get("page_number")
    if page_number is not None:
        page_number = int(page_number)
    else:
        page_number = 1
    return_value = []
    cursor = storage_service.get_all(str(current_user_id), page_number)

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
                "created_at": document["created_at"].isoformat(),
                "updated_at": document["updated_at"].isoformat(),
            }
        )
    return Response(json.dumps(return_value, indent=4), mimetype="application/json")


@app.route("/annotation/<id>", methods=["GET"])
@token_required
def get_by_id(current_user_id, id):
    response_data = {}
    cursor = storage_service.get_by_id(id)

    limit = request.args.get("limit")
    properties = request.args.get("properties")
    source = request.args.get("source")  # can be either hypothesis or ai_assistant

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

    limit = request.args.get("limit")
    properties = request.args.get("properties")

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
    else:
        limit = None

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

        # Run the query and parse the results
        result = db_instance.run_query(query, False)
        response_data = db_instance.parse_and_serialize(
            result, schema_manager.schema, properties
        )

        if source == "hypothesis":
            response = {
                "nodes": response_data["nodes"],
                "edges": response_data["edges"],
            }
            formatted_response = json.dumps(response, indent=4)
            return Response(formatted_response, mimetype="application/json")

        response_data["annotation_id"] = str(annotation_id)
        response_data["request"] = json_request
        response_data["title"] = title
        response_data["summary"] = summary
        response_data["node_count"] = node_count
        response_data["edge_count"] = edge_count
        response_data["node_count_by_label"] = node_count_by_label
        response_data["edge_count_by_label"] = edge_count_by_label

        # if limit:
        # response_data = limit_graph(response_data, limit)

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error processing query: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/annotation/<id>", methods=["POST"])
@token_required
def process_by_id(current_user_id, id):
    data = request.get_json()
    if not data or "requests" not in data:
        return jsonify({"error": "Missing requests data"}), 400

    if "question" not in data["requests"]:
        return jsonify({"error": "Missing question data"}), 400

    question = data["requests"]["question"]
    response_data = {}
    cursor = storage_service.get_by_id(id)

    limit = request.args.get("limit")
    properties = request.args.get("properties")
    source = request.args.get("source")  # can be either hypothesis or ai_assistant

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
    limit = request.args.get("limit")
    properties = request.args.get("properties")

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
    else:
        limit = None

    try:
        if question:
            response_data["question"] = question

        # Run the query and parse the results
        result = db_instance.run_query(query, source)
        response_data = db_instance.parse_and_serialize(
            result, schema_manager.schema, properties
        )

        answer = (
            llm.generate_summary(response_data, question, False, summary)
            if question
            else None
        )

        storage_service.update(
            id, {"answer": answer, "updated_at": datetime.datetime.now()}
        )

        response = {"annotation_id": str(id), "question": question, "answer": answer}

        formatted_response = json.dumps(response, indent=4)
        return Response(formatted_response, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error processing query: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/annotation/<id>/full", methods=["GET"])
@token_required
def process_full_annotation(current_user_id, id):
    try:
        link = process_full_data(current_user_id=current_user_id, annotation_id=id)
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


def process_full_data(current_user_id, annotation_id):
    cursor = storage_service.get_by_id(annotation_id)

    if cursor is None:
        return None

    query, title = cursor.query, cursor.title

    try:
        file_path = generate_file_path(
            file_name=title, user_id=current_user_id, extension="xls"
        )
        exists = os.path.exists(file_path)

        if exists:
            file_path = adjust_file_path(file_path)
            link = f"{request.host_url}{file_path}"

            return link

        # Run the query and parse the results
        result = db_instance.run_query(query)
        parsed_result = db_instance.convert_to_dict(result, schema_manager.schema)

        file_path = convert_to_csv(
            parsed_result, user_id=current_user_id, file_name=title
        )
        file_path = adjust_file_path(file_path)

        link = f"{request.host_url}{file_path}"
        return link

    except Exception as e:
        raise e


@app.route("/annotation/<id>", methods=["DELETE"])
@token_required
def delete_by_id(current_user_id, id):
    try:
        existing_record = storage_service.get_by_id(id)

        if existing_record is None:
            return jsonify("No value Found"), 404

        deleted_record = storage_service.delete(id)

        if deleted_record is None:
            return jsonify("Failed to delete the annotation"), 500

        response_data = {"message": "Annotation deleted successfully"}

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error deleting annotation: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/annotation/<id>/title", methods=["PUT"])
@token_required
def update_title(current_user_id, id):
    data = request.get_json()

    if "title" not in data:
        return jsonify({"error": "Title is required"}), 400

    title = data["title"]

    try:
        existing_record = storage_service.get_by_id(id)

        if existing_record is None:
            return jsonify("No value Found"), 404

        updated_data = storage_service.update(id, {"title": title})

        response_data = {
            "message": "title updated successfully",
            "title": title,
        }

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error updating title: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/graph", methods=["POST"])
@token_required
def process_graph(current_user_id):
    """
    Process graph data from database instance and return formatted results
    """
    try:
        request_json = request.json
        if not request_json or "requests" not in request_json:
            return jsonify({"error": "No requests data provided"}), 400

        # Get request data and pagination parameters
        request_data = request_json.get("requests", {})
        limit = request_json.get("limit", 1000)

        # Get data from database instance
        result = db_instance.run_query(request_data, limit)

        # Parse and format results
        formatted_result = db_instance.parse_and_serialize(
            result, schema_manager.schema, True
        )

        return jsonify(formatted_result)

    except Exception as e:
        logging.error(f"Error processing graph: {str(e)}")
        return jsonify({"error": str(e)}), 500
