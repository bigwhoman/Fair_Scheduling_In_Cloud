import sys
from task_generator import Task, generate_tasks


class ScheduledTask:
    def __init__(self, task: Task, cpu_id: int, start_time: int):
        self.task_id = task.id
        self.start_time = start_time
        self.computation_finish_time = start_time + task.computation_times[cpu_id]
        self.ran_cpu_id = cpu_id


class HEFT:
    @staticmethod
    def rank(tasks: dict[int, Task]) -> list[tuple[float, Task]]:
        # task-id -> rank
        task_ranks: dict[int, float] = {}
        for id in range(1, len(tasks) + 1):
            if id in task_ranks:
                continue
            task_ranks[id] = HEFT.calculate_rank(tasks, task_ranks, id)
        result: list[tuple[float, Task]] = []
        for id, rank in sorted(
            task_ranks.items(), key=lambda item: item[1], reverse=True
        ):
            result.append((rank, tasks[id]))
        return result

    @staticmethod
    def calculate_rank(
        tasks: dict[int, Task], task_ranks: dict[int, float], wanted: int
    ) -> float:
        if wanted in task_ranks:
            return task_ranks[wanted]
        current_task = tasks[wanted]
        if len(current_task.children) == 0:
            return current_task.average_computation()
        current_rank = current_task.average_computation() + max(
            map(
                lambda child_task_id: current_task.average_communication(child_task_id)
                + HEFT.calculate_rank(tasks, task_ranks, child_task_id),
                current_task.children,
            )
        )
        task_ranks[wanted] = current_rank
        print(f"{wanted} -> {current_rank}")
        return current_rank

    @staticmethod
    def is_interval_occupied_in_time(
        intervals: list[tuple[int, int]], interval: tuple[int, int]
    ) -> bool:
        # https://stackoverflow.com/a/3269471/4213397
        return any(
            map(lambda i: i[0] <= interval[1] and interval[0] <= i[1], intervals)
        )

    @staticmethod
    def find_gap(
        scheduled_tasks: dict[int, ScheduledTask],
        cpu_id: int,
        fastest_start_time: int,
        computation_cost: int,
    ) -> int:
        """
        Finds the first time which we can schedule a task on a specific core
        """
        occupied_intervals = sorted(
            map(
                lambda task: (task.start_time, task.computation_finish_time),
                filter(
                    lambda item: item.ran_cpu_id == cpu_id, scheduled_tasks.values()
                ),
            ),
            key=lambda item: item[0],  # sort by start time
        )
        candidate_start_time = fastest_start_time
        # This is a horrible way to do it but whatever
        while HEFT.is_interval_occupied_in_time(
            occupied_intervals,
            (candidate_start_time, candidate_start_time + computation_cost),
        ):
            candidate_start_time += 1
        return candidate_start_time

    @staticmethod
    def schedule(tasks: dict[int, Task], cpus: int):
        ranks = HEFT.rank(tasks)
        # task_id -> task
        scheduled_tasks: dict[int, ScheduledTask] = {}
        for task in map(lambda rank: rank[1], ranks):
            # cpu_id -> (start, finish) for each CPU core
            cpu_runtimes: list[tuple[int, int]] = [(0, 0)] * cpus
            for cpu_id in range(cpus):
                processor_ready = 0  # when does this core can become available for scheduling this task
                for parent_id in task.fathers:
                    assert parent_id in scheduled_tasks  # sanity check
                    communication_cost = tasks[parent_id].communication_cost[task.id][
                        scheduled_tasks[parent_id].ran_cpu_id
                    ][cpu_id]
                    start_delay = (
                        communication_cost
                        + scheduled_tasks[parent_id].computation_finish_time
                    )
                    processor_ready = max(processor_ready, start_delay)
                # Now calculate when we can schedule this task
                cpu_start_time = HEFT.find_gap(
                    scheduled_tasks,
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
            scheduled_tasks[task.id] = ScheduledTask(
                task, best_cpu_id, cpu_runtimes[best_cpu_id][0]
            )
        print(scheduled_tasks)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Format should be as follows : method + num_of_tasks + cpu cores")
        exit(1)
    generation_method = sys.argv[1]
    number_of_tasks = int(sys.argv[2])
    cpu_cores = int(sys.argv[3])
    task_generator = generate_tasks()
    if generation_method == "GE":
        graph = task_generator.GE(number_of_tasks)
    elif generation_method == "FFT":
        graph = task_generator.FFT(number_of_tasks)
    else:
        print("Method should be : FFT - GE")
        exit(1)
    print("Currently have", len(graph), "tasks")
    for task in graph.values():
        task.populate_cpu_dependant_variables(cpu_cores)
    print("Ranked tasks:", HEFT.rank(graph))
    HEFT.schedule(graph, cpu_cores)
