from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool
import os

webserver = Flask(__name__)
webserver.tasks_runner = ThreadPool()

webserver.tasks_runner.start()

webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

webserver.job_counter = 1

if not os.path.exists('results'):
    os.makedirs('results')

from app import routes
