**Name: Theodor-Ioan Rolea**

**Group: 333CA**

# HW1 ASC - Flask Web Server

## Overview

* This project aims to implement a Flask web server that handles a series of
requests given a dataset. The dataset is a CSV file containing information
about nutrition, physical activity and obesity in the United States from 2011
to 2022.

***

# Code Structure

* The project is structured in the following way:
    * `app/__init__.py` - the initialization file for the Flask app
    * `app/data_ingestor.py` - the file that contains the class that handles the dataset
    * `app/logger.py` - the file that contains the logger configuration
    * `app/routes.py` - the file that contains the routes for the Flask app
    * `app/task_runner.py` - the file that contains the class that handles the tasks
    * `api_server.py` - the file that contains the main Flask app
    * `README.md` - the file that contains the documentation

***

# How the Server Works

* After a request is given to the server, it will handle it by
creating a task and adding it to the task queue. The task will be processed by
a worker thread and the result will be sent back to the client when a GET
request is made to the server. The results are stored in a results/ folder.

***

# Development Insights

* No particular synchronization was needed for this project, as the tasks are
stored inside a Queue which is thread safe. I initially wanted to use a lock to
write in the dictionary, but I realized that because the Queue is thread safe,
only one thread can access a given task at a time, so no synchronization was
needed. The same can be said about the results file for each job_id.

***

# Final Thoughts

* This project was a great opportunity to learn more about Flask and how to
handle requests in a web server. I learned how to use the Flask library and how
to create a simple web server that can handle requests and return results to
the client. I also learned how to use the Pandas library to handle CSV files
and how to use the logging library to log information about the server. I
enjoyed working on this project and I am looking forward to working on the next
one.
