import json
import os
from queue import Queue
from threading import Thread, Event, Lock
from flask import jsonify
from app import DataIngestor
from app.logger import logger

def handleTask(data, type, data_ingestor, state = None):
    logger.info(f"Handling task {type}, {data["question"]}")
    if type == "states_mean":
        filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
        mean = filtered_data.groupby("LocationDesc")["Data_Value"].mean().reset_index()
        sorted_mean = mean.sort_values(by="Data_Value", ascending=True)
        res = dict(zip(sorted_mean["LocationDesc"], sorted_mean["Data_Value"]))
        logger.info(f"Mean for {type} is {res}")
        return res
    elif type == "state_mean":
        filtered_data = data_ingestor.data[(data_ingestor.data["Question"] == data["question"]) & (data_ingestor.data["LocationDesc"] == state)]
        mean = filtered_data["Data_Value"].mean()
        logger.info(f"Mean for {type} is {mean}")
        return { state : mean }
    elif type == "best5":
        if data["question"] in data_ingestor.questions_best_is_min:
            filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
            mean = filtered_data.groupby("LocationDesc")["Data_Value"].mean()
            sorted_mean = mean.sort_values(ascending=True).head(5).to_dict()
            return sorted_mean
        elif data["question"] in data_ingestor.questions_best_is_max:
            filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
            mean = filtered_data.groupby("LocationDesc")["Data_Value"].mean()
            sorted_mean = mean.sort_values(ascending=False).head(5).to_dict()
            return sorted_mean
    elif type == "worst5":
        if data["question"] in data_ingestor.questions_best_is_min:
            filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
            mean = filtered_data.groupby("LocationDesc")["Data_Value"].mean()
            sorted_mean = mean.sort_values(ascending=False).head(5).to_dict()
            return sorted_mean
        elif data["question"] in data_ingestor.questions_best_is_max:
            filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
            mean = filtered_data.groupby("LocationDesc")["Data_Value"].mean()
            sorted_mean = mean.sort_values(ascending=True).head(5).to_dict()
            return sorted_mean
    elif type == "global_mean":
        filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
        mean = filtered_data["Data_Value"].mean()
        return {"global_mean": mean}
    elif type == "diff_from_mean":
        filtered_data = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]
        global_mean = filtered_data["Data_Value"].mean()
        state_means = filtered_data.groupby("LocationDesc")["Data_Value"].mean()
        differences = global_mean - state_means
        diff_dict = differences.to_dict()
        return diff_dict
    elif type == "state_diff_from_mean":
        filtered_data = data_ingestor.data[(data_ingestor.data["Question"] == data["question"]) & (data_ingestor.data["LocationDesc"] == state)]
        global_mean = data_ingestor.data[data_ingestor.data["Question"] == data["question"]]["Data_Value"].mean()
        state_mean = filtered_data["Data_Value"].mean()
        return { state : global_mean - state_mean }
    elif type == "mean_by_category":
        filtered_data = data_ingestor.data[(data_ingestor.data["Question"] == data["question"])]
        mean = filtered_data.groupby(["LocationDesc", "StratificationCategory1", "Stratification1"])["Data_Value"].mean().reset_index()
        res = {}
        for _, row in mean.iterrows():
            key = tuple(row[['LocationDesc', 'StratificationCategory1', 'Stratification1']])
            value = row['Data_Value']
            res[str(key)] = value
        return res
    elif type == "state_mean_by_category":
        filtered_data = data_ingestor.data[(data_ingestor.data["Question"] == data["question"]) & (data_ingestor.data["LocationDesc"] == state)]
        mean_by_category = filtered_data.groupby(["StratificationCategory1", "Stratification1"])["Data_Value"].mean().reset_index()
        res = {}
        for _, row in mean_by_category.iterrows():
            key = tuple(row[['StratificationCategory1', 'Stratification1']])
            value = row['Data_Value']
            res[str(key)] = value
        return { state: res }
    else:
        return "Invalid task type"

class Task:
    def __init__(self, job_id, task_type, data):
        self.job_id = job_id
        self.task_type = task_type
        self.data = data

    def execute(self, data_ingestor):
        logger.info(f"Executing task {self.job_id}")
        return handleTask(self.data, self.task_type, data_ingestor, self.data.get("state", None))

class ThreadPool:
    def __init__(self):
        self.tasks = Queue()
        self.done_tasks = {}
        self.threads = []
        self.shutdown_event = Event()
        self.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

        if "TP_NUM_OF_THREADS" in os.environ:
            self.num_threads = int(os.environ["TP_NUM_OF_THREADS"])
        else:
            self.num_threads = os.cpu_count()

        for i in range(self.num_threads):
            self.threads.append(TaskRunner(self.tasks, self.done_tasks, self.data_ingestor))

    def start(self):
        for thread in self.threads:
            thread.start()

    def get_task_result(self, job_id):
        result = None
        with open(os.path.join("results", f"{job_id}.json"), "r") as f:
            result = json.load(f)
        return result

    def add_task(self, data, job_id, task_type):
        task = Task(job_id, task_type, data)
        self.tasks.put(task)
        self.done_tasks[job_id] = False

    def shutdown(self):
        self.shutdown_event.set()
        for thread in self.threads:
            thread.join()

    def graceful_shutdown(self):
        self.shutdown()

class TaskRunner(Thread):
    def __init__(self, queue, done_tasks, data_ingestor):
        super().__init__()
        self.shutdown_event = Event()
        self.done = Event()
        self.queue = queue
        self.done_tasks = done_tasks
        self.data_ingestor = data_ingestor

    def run(self):
        logger.info("Thread started")
        while not self.shutdown_event.is_set():
            while not self.queue.empty():
                task = self.queue.get()
                if task.task_type == "graceful_shutdown":
                    self.graceful_shutdown()
                    return
                elif task.task_type == "jobs":
                    tasksLeft = dict()
                    for key in self.done_tasks:
                        if self.done_tasks[key] == False:
                            tasksLeft[key] = "running"
                        else:
                            tasksLeft[key] = "done"
                    result = jsonify({"status": "done", "data": tasksLeft})
                elif task.task_type == "num_jobs":
                    result = len(filter(lambda x: x == True, self.doneTasks.values())) - len(filter(lambda x: x == True, self.doneTasks.values()))
                else:
                    result = task.execute(self.data_ingestor)
                    logger.info(f"Task {task.job_id} completed with result {result}")
                    with open(os.path.join("results", f"{task.job_id}.json"), "w") as f:
                        json.dump(result, f)
                        self.done_tasks[task.job_id] = True


    def shutdown(self):
        self.shutdown_event.set()

    def graceful_shutdown(self):
        self.done.wait()
        self.shutdown()
