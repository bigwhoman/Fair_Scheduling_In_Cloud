import random
from heft import HEFT, ScheduledTask
from task_generator import Task

class DAG:
    def __init__(self, id: int, tasks: dict[int, Task], cpus: int):
        self.id = id
        self.tasks = tasks
        self.lowerbound = HEFT.calculate_makespan(tasks, cpus)
        self.release_time = random.randint(0, 1000)
        self.ranks = HEFT.rank(tasks)

class MinMax:
    @staticmethod
    def schedule(dags: list[DAG], cpus: int) -> dict[dict[int, ScheduledTask]]:
        result: dict[dict[int, ScheduledTask]] = {}
        current_time = 0
        while len(dags) != 0:
            # Always advance time in order to check for dynamic scheduling
            current_time += 1
            # Check if we can run any DAG
            runnable_dags = list(filter(lambda dag: dag.release_time <= current_time, dags))
            if len(runnable_dags) == 0:
                continue
            # If we have reached here, it means that we have at least one runnable dag
            candidate_index = min(range(len(runnable_dags)), key=lambda dag_index: runnable_dags[dag_index].lowerbound)
            # Schedule the task
            scheduled_dag = HEFT.schedule(runnable_dags[candidate_index].tasks, cpus)
            dags = list(filter(lambda dag: dag.id != runnable_dags[candidate_index].id, dags))
            # Fix the scheduled tasks based on current time
            time_to_advance = 0
            for scheduled_task in scheduled_dag.values():
                time_to_advance = max(time_to_advance, scheduled_task.computation_finish_time)
                scheduled_task.start_time += current_time
                scheduled_task.computation_finish_time += current_time
            result[runnable_dags[candidate_index].id] = scheduled_dag
            # Now advance the time
            current_time += time_to_advance
        return result

class FDWS:
    @staticmethod
    def schedule(dags: list[DAG], cpus: int) -> dict[dict[int, ScheduledTask]]:
        pass