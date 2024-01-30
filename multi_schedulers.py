import copy
import random
from heft import HEFT, ScheduledTask
from task_generator import Task, generate_tasks

class DAG:
    def __init__(self, id: int, tasks: dict[int, Task], cpus: int):
        self.id = id
        self.tasks = tasks
        self.lowerbound = HEFT.calculate_makespan(tasks, cpus)
        self.release_time = random.randint(0, 1000)
        self.ranks = HEFT.rank(tasks)

class MinMax:
    @staticmethod
    def schedule(dags: dict[int, DAG], cpus: int) -> dict[dict[int, ScheduledTask]]:
        dags_list = list(dags.values())
        result: dict[dict[int, ScheduledTask]] = {}
        current_time = 0
        while len(dags_list) != 0:
            # Always advance time in order to check for dynamic scheduling
            current_time += 1
            # Check if we can run any DAG
            runnable_dags = list(filter(lambda dag: dag.release_time <= current_time, dags_list))
            if len(runnable_dags) == 0:
                continue
            # If we have reached here, it means that we have at least one runnable dag
            candidate_index = min(range(len(runnable_dags)), key=lambda dag_index: runnable_dags[dag_index].lowerbound)
            # Schedule the task
            scheduled_dag = HEFT.schedule(runnable_dags[candidate_index].tasks, cpus)
            dags_list = list(filter(lambda dag: dag.id != runnable_dags[candidate_index].id, dags_list))
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

class FDWS_RANK_HYBRID:
    @staticmethod
    def reduce_task_list(scheduled_tasks: dict[int, dict[int, ScheduledTask]]) -> list[ScheduledTask]:
        result: list[ScheduledTask] = []
        for tasks in scheduled_tasks.values():
            result.extend(tasks.values())
        return result

    @staticmethod
    def schedule(dags: dict[int, DAG], cpus: int, is_fdws: bool) -> dict[dict[int, ScheduledTask]]:
        dags = copy.deepcopy(dags)
        # dag_id -> task_id -> task
        result: dict[int, dict[int, ScheduledTask]] = {}
        for dag_id in dags.keys():
            result[dag_id] = {}
        while True:
            # At first get the current time
            current_time = min(map(lambda dag: dag.release_time, dags.values())) # default is the release time of the first DAG
            if len(FDWS_RANK_HYBRID.reduce_task_list(result)) != 0:
                current_time = max(current_time, max(map(lambda task: task.computation_finish_time, FDWS_RANK_HYBRID.reduce_task_list(result))))
            # Fill the ready queue
            # (rank, dag_id, task)
            ready_queue: list[tuple[float, int, Task]] = []
            for dag_id, dag in filter(lambda d: d[1].release_time <= current_time, dags.items()):
                if len(dag.ranks) != 0:
                    (rank, task) = dag.ranks.pop(0)
                    # TODO: check + release_time
                    ready_queue.append((rank + dag.release_time, dag_id, task))
            if len(ready_queue) == 0: # everything is done
                break
            # Now sort the ready queue based on rank
            ready_queue = sorted(ready_queue, key=lambda x: x[0], reverse=is_fdws)
            # For each task in queue, schedule it by EFT
            # Just like EFT.schedule
            for _, dag_id, task in ready_queue:
                # cpu_id -> (start, finish) for each CPU core
                cpu_runtimes: list[tuple[int, int]] = [(0, 0)] * cpus
                for cpu_id in range(cpus):
                    processor_ready = dags[dag_id].release_time  # when does this core can become available for scheduling this task
                    for parent_id in task.fathers:
                        assert parent_id in result[dag_id]  # sanity check
                        communication_cost = dags[dag_id].tasks[parent_id].communication_cost[task.id][
                            result[dag_id][parent_id].ran_cpu_id
                        ][cpu_id]
                        start_delay = (
                            communication_cost
                            + result[dag_id][parent_id].computation_finish_time
                        )
                        processor_ready = max(processor_ready, start_delay)
                    # Now calculate when we can schedule this task
                    cpu_start_time = HEFT.find_gap(
                        FDWS_RANK_HYBRID.reduce_task_list(result),
                        cpu_id,
                        processor_ready,
                        task.computation_times[cpu_id],
                    )
                    cpu_runtimes[cpu_id] = (
                        cpu_start_time,
                        cpu_start_time + task.computation_times[cpu_id],
                    )
                # Now check what CPU yields the fastest one
                best_cpu_id = min(
                    range(len(cpu_runtimes)), key=lambda x: cpu_runtimes[x][1]
                )
                result[dag_id][task.id] = ScheduledTask(
                    task, best_cpu_id, cpu_runtimes[best_cpu_id][0]
                )
        return result
    
def sample_run():
    CORES = 4
    dags: dict[int, DAG] = {}
    for i in range(5):
        tasks = generate_tasks.FFT(5)
        for task in tasks.values():
            task.populate_cpu_dependant_variables(CORES)
        dags[i] = DAG(i, tasks, CORES)
    print("MinMax:", MinMax.schedule(dags, CORES))
    print("FDWS:", FDWS_RANK_HYBRID.schedule(dags, CORES, True))
    print("Rank Hybrid:", FDWS_RANK_HYBRID.schedule(dags, CORES, False))

if __name__ == "__main__":
    sample_run()