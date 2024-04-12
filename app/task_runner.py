from queue import Queue
from threading import Thread, Event
import time
import os
from app import DataIngestor
from app.logger import logger
from threading import Lock

data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

def handleTask(data, type, state = None):
    logger.info(f"Handling task {type}, {data['question']}")
    if type == 'states_mean':
        filtered_data = data_ingestor.data[data_ingestor.data['Question'] == data['question']]
        logger.info(f"Filtered data for {type} is {filtered_data}")
        mean = filtered_data.groupby('LocationDesc')['Data_Value'].mean().reset_index()
        logger.info(f"Mean for {type} is {mean}")
        sorted_mean = mean.sort_values(by='Data_Value', ascending=True)
        logger.info(f"Sorted mean for {type} is {sorted_mean}")
        res = sorted_mean.set_index('LocationDesc').to_dict()['Data_Value']
        logger.info(f"Mean for {type} is {res}")
        return res

class Task:
    def __init__(self, job_id, task_type, data):
        self.job_id = job_id
        self.task_type = task_type
        self.data = data

    def execute(self):
        logger.info(f"Executing task {self.job_id}")
        return handleTask(self.data, self.task_type, self.data.get('state', None))

class ThreadPool:
    def __init__(self):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task
        
        self.tasks = Queue()
        self.doneTasks = dict()
        self.pendingTasks = []
        self.threads = []
        self.lock = Lock()
        self.shutdown_event = Event()
        self.job_id = 0

        if 'TP_NUM_OF_THREADS' in os.environ:
            self.num_threads = int(os.environ['TP_NUM_OF_THREADS'])
        else:
            self.num_threads = os.cpu_count()

        for i in range(self.num_threads):
            self.threads.append(TaskRunner(self.tasks, self.lock, self.doneTasks, self.pendingTasks))

    def start(self):
        for thread in self.threads:
            thread.start()

    def add_task(self, data, job_id, task_type):
        task = Task(job_id, task_type, data)
        self.tasks.put(task)
        self.pendingTasks.append(job_id)

    def shutdown(self):
        self.shutdown_event.set()
        for thread in self.threads:
            thread.join()

    def graceful_shutdown(self):
        self.shutdown()
        
class TaskRunner(Thread):
    def __init__(self, queue, lock, doneTasks, pendingTasks):
        super().__init__()
        self.shutdown_event = Event()
        self.done = Event()
        self.queue = queue
        self.lock = lock
        self.doneTasks = doneTasks
        self.pendingTasks = pendingTasks

    def run(self):
        logger.info("Thread started")
        while not self.shutdown_event.is_set():
            while not self.queue.empty():
                task = self.queue.get()
                result = task.execute()
                self.done.clear()
                logger.info(f"Task {task.job_id} completed with result {result}")
                self.queue.task_done()
                self.done.set()
                # Read here https://stackoverflow.com/questions/6953351/thread-safety-in-pythons-dictionary
                # That multiple operations on a dictionary might not be thread safe
                # For peace of mind, I will lock the dictionary before updating it
                with self.lock:
                    self.doneTasks[task.job_id] = result
                    self.pendingTasks.remove(task.job_id)


    def shutdown(self):
        self.shutdown_event.set()

    def graceful_shutdown(self):
        self.done.wait()
        self.shutdown()


