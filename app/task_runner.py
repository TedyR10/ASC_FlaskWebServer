import json
import os
from queue import Queue
from threading import Thread, Event
from flask import jsonify
from app import DataIngestor
from app.logger import logger

# Function that handles the task
def handle_task(data, task_type, data_ingestor, state = None):
    """Function that handles the task"""
    logger.info("Handling task %s, %s", task_type, data["question"])
    # Check the task_type of the task
    # Filter the data based on the question
    # Calculate the mean of the data
    # Sort the data
    # Return the result
    if task_type == "states_mean":
        filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
        mean = filtered_data.groupby("LocationDesc")["Data_Value"].mean().reset_index()
        sorted_mean = mean.sort_values(by="Data_Value", ascending=True)
        res = dict(zip(sorted_mean["LocationDesc"], sorted_mean["Data_Value"]))
        logger.info("States mean is %s", str(res))
        return res
    if task_type == "state_mean":
        filtered_data = data_ingestor.data[(data_ingestor.data["Question"] == data["question"]) & (data_ingestor.data["LocationDesc"] == state)]
        mean = filtered_data["Data_Value"].mean()
        logger.info("State mean for %s is %s", state, mean)
        return { state : mean }
    if task_type == "best5":
        if data["question"] in data_ingestor.questions_best_is_min:
            filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
            mean = filtered_data.groupby("LocationDesc")["Data_Value"].mean()
            sorted_mean = mean.sort_values(ascending=True).head(5).to_dict()
            logger.info("Best 5 states for %s are %s", data["question"], str(sorted_mean))
            return sorted_mean
        if data["question"] in data_ingestor.questions_best_is_max:
            filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
            mean = filtered_data.groupby("LocationDesc")["Data_Value"].mean()
            sorted_mean = mean.sort_values(ascending=False).head(5).to_dict()
            logger.info("Best 5 states for %s are %s", data["question"], str(sorted_mean))
            return sorted_mean
    if task_type == "worst5":
        if data["question"] in data_ingestor.questions_best_is_min:
            filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
            mean = filtered_data.groupby("LocationDesc")["Data_Value"].mean()
            sorted_mean = mean.sort_values(ascending=False).head(5).to_dict()
            logger.info("Worst 5 states for %s are %s", data["question"], str(sorted_mean))
            return sorted_mean
        if data["question"] in data_ingestor.questions_best_is_max:
            filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
            mean = filtered_data.groupby("LocationDesc")["Data_Value"].mean()
            sorted_mean = mean.sort_values(ascending=True).head(5).to_dict()
            logger.info("Worst 5 states for %s are %s", data["question"], str(sorted_mean))
            return sorted_mean
    if task_type == "global_mean":
        filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
        mean = filtered_data["Data_Value"].mean()
        logger.info("Global mean for %s is %s", data["question"], mean)
        return {"global_mean": mean}
    if task_type == "diff_from_mean":
        filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
        global_mean = filtered_data["Data_Value"].mean()
        state_means = filtered_data.groupby("LocationDesc")["Data_Value"].mean()
        differences = global_mean - state_means
        diff_dict = differences.to_dict()
        logger.info("Differences from global mean are %s", str(diff_dict))
        return diff_dict
    if task_type == "state_diff_from_mean":
        filtered_data = data_ingestor.data[(data_ingestor.data["Question"] == data["question"]) & (data_ingestor.data["LocationDesc"] == state)]
        global_mean = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]["Data_Value"].mean()
        state_mean = filtered_data["Data_Value"].mean()
        logger.info("Difference from global mean for %s is %s", state, global_mean - state_mean)
        return { state : global_mean - state_mean }
    if task_type == "mean_by_category":
        filtered_data = data_ingestor.data[(data_ingestor.data["Question"] == data["question"])]
        mean = filtered_data.groupby(["LocationDesc", "StratificationCategory1", "Stratification1"])["Data_Value"].mean().reset_index()
        res = {}
        for _, row in mean.iterrows():
            key = tuple(row[['LocationDesc', 'StratificationCategory1', 'Stratification1']])
            value = row['Data_Value']
            res[str(key)] = value
        logger.info("Mean by category is %s", str(res))
        return res
    if task_type == "state_mean_by_category":
        filtered_data = data_ingestor.data[(data_ingestor.data["Question"] == data["question"]) & (data_ingestor.data["LocationDesc"] == state)]
        mean_by_category = filtered_data.groupby(["StratificationCategory1", "Stratification1"])["Data_Value"].mean().reset_index()
        res = {}
        for _, row in mean_by_category.iterrows():
            key = tuple(row[['StratificationCategory1', 'Stratification1']])
            value = row['Data_Value']
            res[str(key)] = value
        logger.info("Mean by category for %s is %s", state, str(res))
        return { state: res }
    return "Invalid task task_type"

class Task:
    """Class to represent a task"""
    def __init__(self, job_id, task_type, data):
        """Initialize the task"""
        self.job_id = job_id
        self.task_type = task_type
        self.data = data

    def execute(self, data_ingestor):
        """Execute the task"""
        logger.info("Executing task %s", self.job_id)
        return handle_task(self.data, self.task_type, data_ingestor, self.data.get("state", None))

class ThreadPool:
    """Class to represent a thread pool"""
    def __init__(self):
        """Initialize the thread pool"""
        self.tasks = Queue()
        self.done_tasks = {}
        self.threads = []
        self.shutdown_event = Event()
        self.graceful_shutdown_event = Event()
        self.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

        if "TP_NUM_OF_THREADS" in os.environ:
            self.num_threads = int(os.environ["TP_NUM_OF_THREADS"])
        else:
            self.num_threads = os.cpu_count()

        for _ in range(self.num_threads):
            self.threads.append(TaskRunner(self.tasks, self.done_tasks, self.data_ingestor, self.graceful_shutdown_event))

    def start(self):
        """Start the threads"""
        for thread in self.threads:
            thread.start()

    def get_task_result(self, job_id):
        """Get the result of the task with the given job_id"""
        result = None
        with open(os.path.join("results", f"{job_id}.json"), "r") as f:
            result = json.load(f)
        return result

    def add_task(self, data, job_id, task_type):
        """Add a task to the queue"""
        if not self.graceful_shutdown_event.is_set():
            task = Task(job_id, task_type, data)
            self.tasks.put(task)
            self.done_tasks[job_id] = False

    def shutdown(self):
        """Shutdown the threads"""
        for thread in self.threads:
            thread.join()

    def graceful_shutdown(self):
        """Gracefully shutdown the threads"""
        self.shutdown()

class TaskRunner(Thread):
    """Class to represent a thread"""
    def __init__(self, queue, done_tasks, data_ingestor, graceful_shutdown_event):
        """Initialize the thread"""
        super().__init__()
        self.shutdown_event = Event()
        self.done = Event()
        self.queue = queue
        self.done_tasks = done_tasks
        self.data_ingestor = data_ingestor
        self.graceful_shutdown_event = graceful_shutdown_event

    def run(self):
        """Run the thread"""
        logger.info("Thread started")
        while not self.graceful_shutdown_event.is_set():
            while not self.queue.empty():
                task = self.queue.get()
                if task.task_type == "graceful_shutdown":
                    logger.info("Graceful shutdown started")
                    self.graceful_shutdown_event.set()
                    return
                if task.task_type == "jobs":
                    tasks_left = {}
                    for key in self.done_tasks:
                        if self.done_tasks[key] is False:
                            tasks_left[key] = "running"
                        else:
                            tasks_left[key] = "done"
                    result = jsonify({"status": "done", "data": tasks_left})
                elif task.task_type == "num_jobs":
                    result = len(filter(lambda x: x is False, self.done_tasks.values())) - len(filter(lambda x: x is True, self.done_tasks.values()))
                else:
                    result = task.execute(self.data_ingestor)
                    logger.info("Task %s completed with result %s", str(task.job_id), str(result))
                    with open(os.path.join("results", f"{task.job_id}.json"), "w") as f:
                        json.dump(result, f)
                        self.done_tasks[task.job_id] = True
