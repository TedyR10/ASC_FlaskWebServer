from app import webserver
from flask import request, jsonify
from logging.handlers import RotatingFileHandler
import time
from app.logger import logger

import os
import json

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)
    else:
        # Method Not Allowed
        return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    # Check if job_id is valid
    if int(job_id) not in webserver.tasks_runner.done_tasks:
        logger.error(f"Invalid job_id {job_id}")
        return jsonify({'status': 'error', 'reason': 'Invalid job_id'})

    # If job is done, return the result
    if webserver.tasks_runner.done_tasks[int(job_id)] == True:
        logger.info(f"Job {job_id} is done")
        return jsonify({"status" : "done", "data": webserver.tasks_runner.get_task_result(int(job_id))})
    
    logger.info(f"Job {job_id} is still running")
    return jsonify({'status': 'running'})

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # Get request data
    data = request.json

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    job_id = webserver.job_counter
    webserver.job_counter += 1

    # Add task to the task runner
    webserver.tasks_runner.add_task(data, job_id, 'states_mean')

    return jsonify({"job_id": job_id})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json

    job_id = webserver.job_counter
    webserver.job_counter += 1

    # Add task to the task runner
    webserver.tasks_runner.add_task(data, job_id, 'state_mean')

    return jsonify({"job_id": job_id})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json

    job_id = webserver.job_counter
    webserver.job_counter += 1

    # Add task to the task runner
    webserver.tasks_runner.add_task(data, job_id, 'best5')

    return jsonify({"job_id": job_id})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json

    job_id = webserver.job_counter
    webserver.job_counter += 1

    # Add task to the task runner
    webserver.tasks_runner.add_task(data, job_id, 'worst5')

    return jsonify({"job_id": job_id})  

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json

    job_id = webserver.job_counter
    webserver.job_counter += 1

    # Add task to the task runner
    webserver.tasks_runner.add_task(data, job_id, 'global_mean')

    return jsonify({"job_id": job_id})  

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json

    job_id = webserver.job_counter
    webserver.job_counter += 1

    # Add task to the task runner
    webserver.tasks_runner.add_task(data, job_id, 'diff_from_mean')

    return jsonify({"job_id": job_id})  

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json

    job_id = webserver.job_counter
    webserver.job_counter += 1

    # Add task to the task runner
    webserver.tasks_runner.add_task(data, job_id, 'state_diff_from_mean')

    return jsonify({"job_id": job_id})  

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json

    job_id = webserver.job_counter
    webserver.job_counter += 1

    # Add task to the task runner
    webserver.tasks_runner.add_task(data, job_id, 'mean_by_category')

    return jsonify({"job_id": job_id})  

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json

    job_id = webserver.job_counter
    webserver.job_counter += 1

    # Add task to the task runner
    webserver.tasks_runner.add_task(data, job_id, 'state_mean_by_category')

    return jsonify({"job_id": job_id})  

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
